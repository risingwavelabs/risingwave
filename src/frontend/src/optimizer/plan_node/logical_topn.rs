// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt;

use fixedbitset::FixedBitSet;
use itertools::Itertools;
use risingwave_common::error::{ErrorCode, Result, RwError};
use risingwave_common::util::sort_util::ColumnOrder;

use super::generic::{GenericPlanNode, Limit};
use super::{
    gen_filter_and_pushdown, generic, BatchGroupTopN, ColPrunable, ExprRewritable, PlanBase,
    PlanRef, PlanTreeNodeUnary, PredicatePushdown, StreamGroupTopN, StreamProject, ToBatch,
    ToStream,
};
use crate::expr::{ExprType, FunctionCall, InputRef};
use crate::optimizer::plan_node::{
    BatchTopN, ColumnPruningContext, LogicalProject, PredicatePushdownContext,
    RewriteStreamContext, StreamTopN, ToStreamContext,
};
use crate::optimizer::property::{Distribution, Order, RequiredDist};
use crate::planner::LIMIT_ALL_COUNT;
use crate::utils::{ColIndexMapping, ColIndexMappingRewriteExt, Condition};

/// `LogicalTopN` sorts the input data and fetches up to `limit` rows from `offset`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LogicalTopN {
    pub base: PlanBase,
    core: generic::TopN<PlanRef>,
}

impl LogicalTopN {
    pub fn new(input: PlanRef, limit: u64, offset: u64, with_ties: bool, order: Order) -> Self {
        if with_ties {
            assert!(offset == 0, "WITH TIES is not supported with OFFSET");
        }

        let limit_attr = Limit::new(limit, with_ties);
        let core = generic::TopN::without_group(input, limit_attr, offset, order);

        let ctx = core.ctx();
        let schema = core.schema();
        let pk_indices = core.logical_pk();
        let functional_dependency = core.input.functional_dependency().clone();

        let base = PlanBase::new_logical(ctx, schema, pk_indices.unwrap(), functional_dependency);

        LogicalTopN { base, core }
    }

    pub fn with_group(
        input: PlanRef,
        limit: u64,
        offset: u64,
        with_ties: bool,
        order: Order,
        group_key: Vec<usize>,
    ) -> Self {
        let mut topn = Self::new(input, limit, offset, with_ties, order);
        topn.core.group_key = group_key;
        topn
    }

    pub fn create(
        input: PlanRef,
        limit: u64,
        offset: u64,
        order: Order,
        with_ties: bool,
    ) -> Result<PlanRef> {
        if with_ties && offset > 0 {
            return Err(ErrorCode::NotImplemented(
                "WITH TIES is not supported with OFFSET".to_string(),
                None.into(),
            )
            .into());
        }
        Ok(Self::new(input, limit, offset, with_ties, order).into())
    }

    pub fn limit_attr(&self) -> Limit {
        self.core.limit_attr
    }

    pub fn offset(&self) -> u64 {
        self.core.offset
    }

    /// `topn_order` returns the order of the Top-N operator. This naming is because `order()`
    /// already exists and it was designed to return the operator's physical property order.
    ///
    /// Note that for streaming query, `order()` and `topn_order()` may differ. `order()` which
    /// implies the output ordering of an operator, is never guaranteed; while `topn_order()` must
    /// be non-null because it's a critical information for Top-N operators to work
    pub fn topn_order(&self) -> &Order {
        &self.core.order
    }

    pub fn group_key(&self) -> &[usize] {
        &self.core.group_key
    }

    fn gen_dist_stream_top_n_plan(&self, stream_input: PlanRef) -> Result<PlanRef> {
        let input_dist = stream_input.distribution().clone();

        let gen_single_plan = |stream_input: PlanRef| -> Result<PlanRef> {
            let input =
                RequiredDist::single().enforce_if_not_satisfies(stream_input, &Order::any())?;
            let mut logical = self.core.clone();
            logical.input = input;
            Ok(StreamTopN::new(logical).into())
        };

        // if it is append only, for now we don't generate 2-phase rules
        if stream_input.append_only() {
            return gen_single_plan(stream_input);
        }

        match input_dist {
            Distribution::Single | Distribution::SomeShard => gen_single_plan(stream_input),
            Distribution::Broadcast => Err(RwError::from(ErrorCode::NotImplemented(
                "topN does not support Broadcast".to_string(),
                None.into(),
            ))),
            Distribution::HashShard(dists) | Distribution::UpstreamHashShard(dists, _) => {
                self.gen_vnode_two_phase_streaming_top_n_plan(stream_input, &dists)
            }
        }
    }

    fn gen_vnode_two_phase_streaming_top_n_plan(
        &self,
        stream_input: PlanRef,
        dist_key: &[usize],
    ) -> Result<PlanRef> {
        let input_fields = stream_input.schema().fields();

        // use projectiton to add a column for vnode, and use this column as group key.
        let mut exprs: Vec<_> = input_fields
            .iter()
            .enumerate()
            .map(|(idx, field)| InputRef::new(idx, field.data_type.clone()).into())
            .collect();
        exprs.push(
            FunctionCall::new(
                ExprType::Vnode,
                dist_key
                    .iter()
                    .map(|idx| InputRef::new(*idx, input_fields[*idx].data_type()).into())
                    .collect(),
            )?
            .into(),
        );
        let vnode_col_idx = exprs.len() - 1;
        let project = StreamProject::new(LogicalProject::new(stream_input, exprs.clone()));
        let limit_attr = Limit::new(
            self.limit_attr().limit() + self.offset(),
            self.limit_attr().with_ties(),
        );
        let mut logical_top_n =
            generic::TopN::without_group(project.into(), limit_attr, 0, self.topn_order().clone());
        logical_top_n.group_key = vec![vnode_col_idx];
        let local_top_n = StreamGroupTopN::new(logical_top_n, Some(vnode_col_idx));
        let exchange =
            RequiredDist::single().enforce_if_not_satisfies(local_top_n.into(), &Order::any())?;
        let global_top_n = generic::TopN::without_group(
            exchange,
            self.limit_attr(),
            self.offset(),
            self.topn_order().clone(),
        );
        let global_top_n = StreamTopN::new(global_top_n);

        // use another projection to remove the column we added before.
        exprs.pop();
        let project = StreamProject::new(LogicalProject::new(global_top_n.into(), exprs));
        Ok(project.into())
    }
}

impl PlanTreeNodeUnary for LogicalTopN {
    fn input(&self) -> PlanRef {
        self.core.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::with_group(
            input,
            self.limit_attr().limit(),
            self.offset(),
            self.limit_attr().with_ties(),
            self.topn_order().clone(),
            self.group_key().to_vec(),
        )
    }

    #[must_use]
    fn rewrite_with_input(
        &self,
        input: PlanRef,
        input_col_change: ColIndexMapping,
    ) -> (Self, ColIndexMapping) {
        (
            Self::with_group(
                input,
                self.limit_attr().limit(),
                self.offset(),
                self.limit_attr().with_ties(),
                input_col_change
                    .rewrite_required_order(self.topn_order())
                    .unwrap(),
                self.group_key()
                    .iter()
                    .map(|idx| input_col_change.map(*idx))
                    .collect(),
            ),
            input_col_change,
        )
    }
}
impl_plan_tree_node_for_unary! {LogicalTopN}
impl fmt::Display for LogicalTopN {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.core.fmt_with_name(f, "LogicalTopN")
    }
}

impl ColPrunable for LogicalTopN {
    fn prune_col(&self, required_cols: &[usize], ctx: &mut ColumnPruningContext) -> PlanRef {
        let input_required_bitset = FixedBitSet::from_iter(required_cols.iter().copied());
        let order_required_cols = {
            let mut order_required_cols = FixedBitSet::with_capacity(self.input().schema().len());
            self.topn_order()
                .column_orders
                .iter()
                .for_each(|o| order_required_cols.insert(o.column_index));
            order_required_cols
        };
        let group_required_cols = {
            let mut group_required_cols = FixedBitSet::with_capacity(self.input().schema().len());
            self.group_key()
                .iter()
                .for_each(|idx| group_required_cols.insert(*idx));
            group_required_cols
        };

        let input_required_cols = {
            let mut tmp = order_required_cols;
            tmp.union_with(&input_required_bitset);
            tmp.union_with(&group_required_cols);
            tmp.ones().collect_vec()
        };
        let mapping = ColIndexMapping::with_remaining_columns(
            &input_required_cols,
            self.input().schema().len(),
        );
        let new_order = Order {
            column_orders: self
                .topn_order()
                .column_orders
                .iter()
                .map(|o| ColumnOrder::new(mapping.map(o.column_index), o.order_type))
                .collect(),
        };
        let new_group_key = self
            .group_key()
            .iter()
            .map(|group_key| mapping.map(*group_key))
            .collect();
        let new_input = self.input().prune_col(&input_required_cols, ctx);
        let top_n = Self::with_group(
            new_input,
            self.limit_attr().limit(),
            self.offset(),
            self.limit_attr().with_ties(),
            new_order,
            new_group_key,
        )
        .into();

        if input_required_cols == required_cols {
            top_n
        } else {
            let output_required_cols = required_cols
                .iter()
                .map(|&idx| mapping.map(idx))
                .collect_vec();
            let src_size = top_n.schema().len();
            LogicalProject::with_mapping(
                top_n,
                ColIndexMapping::with_remaining_columns(&output_required_cols, src_size),
            )
            .into()
        }
    }
}

impl ExprRewritable for LogicalTopN {}

impl PredicatePushdown for LogicalTopN {
    fn predicate_pushdown(
        &self,
        predicate: Condition,
        ctx: &mut PredicatePushdownContext,
    ) -> PlanRef {
        // filter can not transpose topN
        gen_filter_and_pushdown(self, predicate, Condition::true_cond(), ctx)
    }
}

impl ToBatch for LogicalTopN {
    fn to_batch(&self) -> Result<PlanRef> {
        let new_input = self.input().to_batch()?;
        let mut new_logical = self.core.clone();
        new_logical.input = new_input;
        if self.group_key().is_empty() {
            Ok(BatchTopN::new(new_logical).into())
        } else {
            Ok(BatchGroupTopN::new(new_logical).into())
        }
    }
}

impl ToStream for LogicalTopN {
    fn to_stream(&self, ctx: &mut ToStreamContext) -> Result<PlanRef> {
        if self.offset() != 0 && self.limit_attr().limit() == LIMIT_ALL_COUNT {
            return Err(RwError::from(ErrorCode::InvalidInputSyntax(
                "OFFSET without LIMIT in streaming mode".to_string(),
            )));
        }
        if self.limit_attr().limit() == 0 {
            return Err(RwError::from(ErrorCode::InvalidInputSyntax(
                "LIMIT 0 in streaming mode".to_string(),
            )));
        }
        Ok(if !self.group_key().is_empty() {
            let input = self.input().to_stream(ctx)?;
            let input = RequiredDist::hash_shard(self.group_key())
                .enforce_if_not_satisfies(input, &Order::any())?;
            let mut logical = self.core.clone();
            logical.input = input;
            StreamGroupTopN::new(logical, None).into()
        } else {
            self.gen_dist_stream_top_n_plan(self.input().to_stream(ctx)?)?
        })
    }

    fn logical_rewrite_for_stream(
        &self,
        ctx: &mut RewriteStreamContext,
    ) -> Result<(PlanRef, ColIndexMapping)> {
        let (input, input_col_change) = self.input().logical_rewrite_for_stream(ctx)?;
        let (top_n, out_col_change) = self.rewrite_with_input(input, input_col_change);
        Ok((top_n.into(), out_col_change))
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::types::DataType;

    use super::LogicalTopN;
    use crate::optimizer::optimizer_context::OptimizerContext;
    use crate::optimizer::plan_node::{ColPrunable, ColumnPruningContext, LogicalValues};
    use crate::optimizer::property::Order;
    use crate::PlanRef;

    #[tokio::test]
    async fn test_prune_col() {
        let ty = DataType::Int32;
        let ctx = OptimizerContext::mock().await;
        let fields: Vec<Field> = vec![
            Field::with_name(ty.clone(), "v1"),
            Field::with_name(ty.clone(), "v2"),
            Field::with_name(ty.clone(), "v3"),
        ];
        let values = LogicalValues::new(vec![], Schema { fields }, ctx);
        let input = PlanRef::from(values);

        let original_logical =
            LogicalTopN::with_group(input, 1, 0, false, Order::default(), vec![1]);
        assert_eq!(original_logical.group_key(), &[1]);
        let original_logical: PlanRef = original_logical.into();
        let pruned_node = original_logical.prune_col(
            &[0, 1, 2],
            &mut ColumnPruningContext::new(original_logical.clone()),
        );

        let pruned_logical = pruned_node.as_logical_top_n().unwrap();
        assert_eq!(pruned_logical.group_key(), &[1]);
    }
}

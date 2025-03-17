// Copyright 2025 RisingWave Labs
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

use fixedbitset::FixedBitSet;
use itertools::Itertools;
use risingwave_common::bail_not_implemented;
use risingwave_common::util::sort_util::ColumnOrder;

use super::generic::{GenericPlanRef, TopNLimit};
use super::utils::impl_distill_by_unit;
use super::{
    BatchGroupTopN, ColPrunable, ExprRewritable, Logical, PlanBase, PlanRef, PlanTreeNodeUnary,
    PredicatePushdown, StreamGroupTopN, StreamProject, ToBatch, ToStream, gen_filter_and_pushdown,
    generic,
};
use crate::error::{ErrorCode, Result, RwError};
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
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
    pub base: PlanBase<Logical>,
    core: generic::TopN<PlanRef>,
}

impl From<generic::TopN<PlanRef>> for LogicalTopN {
    fn from(core: generic::TopN<PlanRef>) -> Self {
        let base = PlanBase::new_logical_with_core(&core);
        Self { base, core }
    }
}

impl LogicalTopN {
    pub fn new(
        input: PlanRef,
        limit: u64,
        offset: u64,
        with_ties: bool,
        order: Order,
        group_key: Vec<usize>,
    ) -> Self {
        let limit_attr = TopNLimit::new(limit, with_ties);
        let core = generic::TopN::with_group(input, limit_attr, offset, order, group_key);
        core.into()
    }

    pub fn create(
        input: PlanRef,
        limit: u64,
        offset: u64,
        order: Order,
        with_ties: bool,
        group_key: Vec<usize>,
    ) -> Result<PlanRef> {
        if with_ties && offset > 0 {
            bail_not_implemented!("WITH TIES is not supported with OFFSET");
        }
        Ok(Self::new(input, limit, offset, with_ties, order, group_key).into())
    }

    pub fn limit_attr(&self) -> TopNLimit {
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

    /// decompose -> (input, limit, offset, `with_ties`, order, `group_key`)
    pub fn decompose(self) -> (PlanRef, u64, u64, bool, Order, Vec<usize>) {
        self.core.decompose()
    }

    fn gen_dist_stream_top_n_plan(&self, stream_input: PlanRef) -> Result<PlanRef> {
        use super::stream::prelude::*;

        let input_dist = stream_input.distribution().clone();

        // if it is append only, for now we don't generate 2-phase rules
        if stream_input.append_only() {
            return self.gen_single_stream_top_n_plan(stream_input);
        }

        match input_dist {
            Distribution::Single | Distribution::SomeShard => {
                self.gen_single_stream_top_n_plan(stream_input)
            }
            Distribution::Broadcast => bail_not_implemented!("topN does not support Broadcast"),
            Distribution::HashShard(dists) | Distribution::UpstreamHashShard(dists, _) => {
                self.gen_vnode_two_phase_stream_top_n_plan(stream_input, &dists)
            }
        }
    }

    fn gen_single_stream_top_n_plan(&self, stream_input: PlanRef) -> Result<PlanRef> {
        let input = RequiredDist::single().enforce_if_not_satisfies(stream_input, &Order::any())?;
        let mut core = self.core.clone();
        core.input = input;
        Ok(StreamTopN::new(core).into())
    }

    fn gen_vnode_two_phase_stream_top_n_plan(
        &self,
        stream_input: PlanRef,
        dist_key: &[usize],
    ) -> Result<PlanRef> {
        // use projectiton to add a column for vnode, and use this column as group key.
        let project = StreamProject::new(generic::Project::with_vnode_col(stream_input, dist_key));
        let vnode_col_idx = project.base.schema().len() - 1;

        let limit_attr = TopNLimit::new(
            self.limit_attr().limit() + self.offset(),
            self.limit_attr().with_ties(),
        );
        let local_top_n = generic::TopN::with_group(
            project.into(),
            limit_attr,
            0,
            self.topn_order().clone(),
            vec![vnode_col_idx],
        );
        let local_top_n = StreamGroupTopN::new(local_top_n, Some(vnode_col_idx));

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
        assert_eq!(vnode_col_idx, global_top_n.base.schema().len() - 1);
        let project = StreamProject::new(generic::Project::with_out_col_idx(
            global_top_n.into(),
            0..vnode_col_idx,
        ));
        Ok(project.into())
    }

    pub fn clone_with_input_and_prefix(&self, input: PlanRef, prefix: Order) -> Self {
        let mut core = self.core.clone();
        core.input = input;
        core.order = prefix.concat(core.order);
        core.into()
    }
}

impl PlanTreeNodeUnary for LogicalTopN {
    fn input(&self) -> PlanRef {
        self.core.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        let mut core = self.core.clone();
        core.input = input;
        core.into()
    }

    fn rewrite_with_input(
        &self,
        input: PlanRef,
        input_col_change: ColIndexMapping,
    ) -> (Self, ColIndexMapping) {
        let mut core = self.core.clone();
        core.input = input;
        core.order = input_col_change
            .rewrite_required_order(self.topn_order())
            .unwrap();
        for key in &mut core.group_key {
            *key = input_col_change.map(*key)
        }
        (core.into(), input_col_change)
    }
}
impl_plan_tree_node_for_unary! {LogicalTopN}
impl_distill_by_unit!(LogicalTopN, core, "LogicalTopN");

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
        let top_n = Self::new(
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

impl ExprVisitable for LogicalTopN {}

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
                "OFFSET without LIMIT in streaming mode".to_owned(),
            )));
        }
        if self.limit_attr().limit() == 0 {
            return Err(RwError::from(ErrorCode::InvalidInputSyntax(
                "LIMIT 0 in streaming mode".to_owned(),
            )));
        }
        Ok(if !self.group_key().is_empty() {
            let input = self.input().to_stream(ctx)?;
            let input = RequiredDist::hash_shard(self.group_key())
                .enforce_if_not_satisfies(input, &Order::any())?;
            let mut core = self.core.clone();
            core.input = input;
            StreamGroupTopN::new(core, None).into()
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
    use crate::PlanRef;
    use crate::optimizer::optimizer_context::OptimizerContext;
    use crate::optimizer::plan_node::{ColPrunable, ColumnPruningContext, LogicalValues};
    use crate::optimizer::property::Order;

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

        let original_logical = LogicalTopN::new(input, 1, 0, false, Order::default(), vec![1]);
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

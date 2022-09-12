// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashSet;
use std::fmt;

use fixedbitset::FixedBitSet;
use itertools::Itertools;
use risingwave_common::error::{ErrorCode, Result, RwError};
use risingwave_common::util::sort_util::OrderType;

use super::utils::TableCatalogBuilder;
use super::{
    gen_filter_and_pushdown, ColPrunable, PlanBase, PlanRef, PlanTreeNodeUnary, PredicatePushdown,
    StreamGroupTopN, StreamProject, ToBatch, ToStream,
};
use crate::expr::{ExprType, FunctionCall, InputRef};
use crate::optimizer::plan_node::{BatchTopN, LogicalProject, StreamTopN};
use crate::optimizer::property::{Distribution, FieldOrder, Order, OrderDisplay, RequiredDist};
use crate::planner::LIMIT_ALL_COUNT;
use crate::utils::{ColIndexMapping, Condition};
use crate::TableCatalog;

/// `LogicalTopN` sorts the input data and fetches up to `limit` rows from `offset`
#[derive(Debug, Clone)]
pub struct LogicalTopN {
    pub base: PlanBase,
    input: PlanRef,
    limit: usize,
    offset: usize,
    order: Order,
    group_key: Vec<usize>,
}

impl LogicalTopN {
    pub fn new(input: PlanRef, limit: usize, offset: usize, order: Order) -> Self {
        let ctx = input.ctx();
        let schema = input.schema().clone();
        let pk_indices = input.logical_pk().to_vec();
        let functional_dependency = input.functional_dependency().clone();
        let base = PlanBase::new_logical(ctx, schema, pk_indices, functional_dependency);
        LogicalTopN {
            base,
            input,
            limit,
            offset,
            order,
            group_key: vec![],
        }
    }

    pub fn with_group(
        input: PlanRef,
        limit: usize,
        offset: usize,
        order: Order,
        group_key: Vec<usize>,
    ) -> Self {
        let mut topn = Self::new(input, limit, offset, order);
        topn.group_key = group_key;
        topn
    }

    pub fn create(input: PlanRef, limit: usize, offset: usize, order: Order) -> PlanRef {
        Self::new(input, limit, offset, order).into()
    }

    pub fn limit(&self) -> usize {
        self.limit
    }

    pub fn offset(&self) -> usize {
        self.offset
    }

    /// `topn_order` returns the order of the Top-N operator. This naming is because `order()`
    /// already exists and it was designed to return the operator's physical property order.
    ///
    /// Note that for streaming query, `order()` and `topn_order()` may differ. `order()` which
    /// implies the output ordering of an operator, is never guaranteed; while `topn_order()` must
    /// be non-null because it's a critical information for Top-N operators to work
    pub fn topn_order(&self) -> &Order {
        &self.order
    }

    pub fn group_key(&self) -> &[usize] {
        &self.group_key
    }

    pub(super) fn fmt_with_name(&self, f: &mut fmt::Formatter, name: &str) -> fmt::Result {
        let mut builder = f.debug_struct(name);
        let input = self.input();
        let input_schema = input.schema();
        builder.field(
            "order",
            &format!(
                "{}",
                OrderDisplay {
                    order: self.topn_order(),
                    input_schema
                }
            ),
        );
        builder
            .field("limit", &format_args!("{}", self.limit()))
            .field("offset", &format_args!("{}", self.offset()));
        if !self.group_key.is_empty() {
            builder.field("group_key", &self.group_key);
        }
        builder.finish()
    }

    /// Infers the state table catalog for [`StreamTopN`] and [`StreamGroupTopN`].
    pub fn infer_internal_table_catalog(&self, group_key: Option<&[usize]>) -> TableCatalog {
        let schema = &self.base.schema;
        let pk_indices = &self.base.logical_pk;
        let columns_fields = schema.fields().to_vec();
        let field_order = &self.order.field_order;
        let mut internal_table_catalog_builder = TableCatalogBuilder::new();

        columns_fields.iter().for_each(|field| {
            internal_table_catalog_builder.add_column(field);
        });
        let mut order_cols = HashSet::new();

        if let Some(group_key) = group_key {
            // Here we want the state table to store the states in the order we want, fisrtly in
            // ascending order by the columns specified by the group key, then by the columns
            // specified by `order`. If we do that, when the later group topN operator
            // does a prefix scannimg with the group key, we can fetch the data in the
            // desired order.
            group_key.iter().for_each(|idx| {
                internal_table_catalog_builder.add_order_column(*idx, OrderType::Ascending);
                order_cols.insert(*idx);
            });
        }

        field_order.iter().for_each(|field_order| {
            if !order_cols.contains(&field_order.index) {
                internal_table_catalog_builder
                    .add_order_column(field_order.index, OrderType::from(field_order.direct));
                order_cols.insert(field_order.index);
            }
        });

        pk_indices.iter().for_each(|idx| {
            if !order_cols.contains(idx) {
                internal_table_catalog_builder.add_order_column(*idx, OrderType::Ascending);
                order_cols.insert(*idx);
            }
        });
        internal_table_catalog_builder.build(vec![], self.base.append_only, None)
    }

    fn gen_dist_stream_top_n_plan(&self, stream_input: PlanRef) -> Result<PlanRef> {
        let input_dist = stream_input.distribution().clone();

        let gen_single_plan = |stream_input: PlanRef| -> Result<PlanRef> {
            Ok(StreamTopN::new(self.clone_with_input(
                RequiredDist::single().enforce_if_not_satisfies(stream_input, &Order::any())?,
            ))
            .into())
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
            Distribution::HashShard(dists) | Distribution::UpstreamHashShard(dists) => {
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
        let local_top_n = StreamGroupTopN::new(LogicalTopN::with_group(
            project.into(),
            self.limit + self.offset,
            0,
            self.order.clone(),
            vec![vnode_col_idx],
        ));
        let exchange =
            RequiredDist::single().enforce_if_not_satisfies(local_top_n.into(), &Order::any())?;
        let global_top_n = StreamTopN::new(LogicalTopN::new(
            exchange,
            self.limit,
            self.offset,
            self.order.clone(),
        ));

        // use another projectiton to remove the column we added before.
        exprs.pop();
        let project = StreamProject::new(LogicalProject::new(global_top_n.into(), exprs));
        Ok(project.into())
    }
}

impl PlanTreeNodeUnary for LogicalTopN {
    fn input(&self) -> PlanRef {
        self.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::with_group(
            input,
            self.limit,
            self.offset,
            self.order.clone(),
            self.group_key.clone(),
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
                self.limit,
                self.offset,
                input_col_change
                    .rewrite_required_order(&self.order)
                    .unwrap(),
                self.group_key
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
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.fmt_with_name(f, "LogicalTopN")
    }
}

impl ColPrunable for LogicalTopN {
    fn prune_col(&self, required_cols: &[usize]) -> PlanRef {
        let input_required_bitset = FixedBitSet::from_iter(required_cols.iter().copied());
        let order_required_cols = {
            let mut order_required_cols = FixedBitSet::with_capacity(self.input().schema().len());
            self.order
                .field_order
                .iter()
                .for_each(|fo| order_required_cols.insert(fo.index));
            order_required_cols
        };
        let group_required_cols = {
            let mut group_required_cols = FixedBitSet::with_capacity(self.input().schema().len());
            self.group_key
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
            field_order: self
                .order
                .field_order
                .iter()
                .map(|fo| FieldOrder {
                    index: mapping.map(fo.index),
                    direct: fo.direct,
                })
                .collect(),
        };
        let new_input = self.input.prune_col(&input_required_cols);
        let top_n = Self::new(new_input, self.limit, self.offset, new_order).into();

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

impl PredicatePushdown for LogicalTopN {
    fn predicate_pushdown(&self, predicate: Condition) -> PlanRef {
        gen_filter_and_pushdown(self, predicate, Condition::true_cond())
    }
}

impl ToBatch for LogicalTopN {
    fn to_batch(&self) -> Result<PlanRef> {
        if !self.group_key.is_empty() {
            return Err(ErrorCode::NotImplemented(
                "Group TopN in batch mode".to_string(),
                4847.into(),
            )
            .into());
        }

        let new_input = self.input().to_batch()?;
        let new_logical = self.clone_with_input(new_input);
        Ok(BatchTopN::new(new_logical).into())
    }
}

impl ToStream for LogicalTopN {
    fn to_stream(&self) -> Result<PlanRef> {
        if self.offset() != 0 && self.limit == LIMIT_ALL_COUNT {
            return Err(RwError::from(ErrorCode::InvalidInputSyntax(
                "OFFSET without LIMIT in streaming mode".to_string(),
            )));
        }
        Ok(if !self.group_key.is_empty() {
            let input = self.input().to_stream()?;
            let input = RequiredDist::hash_shard(self.group_key())
                .enforce_if_not_satisfies(input, &Order::any())?;
            let logical = self.clone_with_input(input);
            StreamGroupTopN::new(logical).into()
        } else {
            self.gen_dist_stream_top_n_plan(self.input.to_stream()?)?
        })
    }

    fn logical_rewrite_for_stream(&self) -> Result<(PlanRef, ColIndexMapping)> {
        let (input, input_col_change) = self.input.logical_rewrite_for_stream()?;
        let (top_n, out_col_change) = self.rewrite_with_input(input, input_col_change);
        Ok((top_n.into(), out_col_change))
    }
}

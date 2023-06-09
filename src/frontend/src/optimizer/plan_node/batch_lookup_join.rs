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

use pretty_xmlish::Pretty;
use risingwave_common::catalog::{ColumnId, TableDesc};
use risingwave_common::error::Result;
use risingwave_pb::batch_plan::plan_node::NodeBody;
use risingwave_pb::batch_plan::{DistributedLookupJoinNode, LocalLookupJoinNode};

use super::generic::{self};
use super::utils::Distill;
use super::ExprRewritable;
use crate::expr::{Expr, ExprRewriter};
use crate::optimizer::plan_node::utils::IndicesDisplay;
use crate::optimizer::plan_node::{
    EqJoinPredicate, EqJoinPredicateDisplay, PlanBase, PlanTreeNodeUnary, ToBatchPb,
    ToDistributedBatch, ToLocalBatch,
};
use crate::optimizer::property::{Distribution, Order, RequiredDist};
use crate::optimizer::PlanRef;
use crate::utils::ColIndexMappingRewriteExt;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BatchLookupJoin {
    pub base: PlanBase,
    logical: generic::Join<PlanRef>,

    /// The join condition must be equivalent to `logical.on`, but separated into equal and
    /// non-equal parts to facilitate execution later
    eq_join_predicate: EqJoinPredicate,

    /// Table description of the right side table
    right_table_desc: TableDesc,

    /// Output column ids of the right side table
    right_output_column_ids: Vec<ColumnId>,

    /// The prefix length of the order key of right side table.
    lookup_prefix_len: usize,

    /// If `distributed_lookup` is true, it will generate `DistributedLookupJoinNode` for
    /// `ToBatchPb`. Otherwise, it will generate `LookupJoinNode`.
    distributed_lookup: bool,
}

impl BatchLookupJoin {
    pub fn new(
        logical: generic::Join<PlanRef>,
        eq_join_predicate: EqJoinPredicate,
        right_table_desc: TableDesc,
        right_output_column_ids: Vec<ColumnId>,
        lookup_prefix_len: usize,
        distributed_lookup: bool,
    ) -> Self {
        // We cannot create a `BatchLookupJoin` without any eq keys. We require eq keys to do the
        // lookup.
        assert!(eq_join_predicate.has_eq());
        assert!(eq_join_predicate.eq_keys_are_type_aligned());
        let dist = Self::derive_dist(logical.left.distribution(), &logical);
        let base = PlanBase::new_batch_from_logical(&logical, dist, Order::any());
        Self {
            base,
            logical,
            eq_join_predicate,
            right_table_desc,
            right_output_column_ids,
            lookup_prefix_len,
            distributed_lookup,
        }
    }

    fn derive_dist(left: &Distribution, logical: &generic::Join<PlanRef>) -> Distribution {
        match left {
            Distribution::Single => Distribution::Single,
            Distribution::HashShard(_) | Distribution::UpstreamHashShard(_, _) => {
                let l2o = logical
                    .l2i_col_mapping()
                    .composite(&logical.i2o_col_mapping());
                l2o.rewrite_provided_distribution(left)
            }
            _ => unreachable!(),
        }
    }

    fn eq_join_predicate(&self) -> &EqJoinPredicate {
        &self.eq_join_predicate
    }

    pub fn right_table_desc(&self) -> &TableDesc {
        &self.right_table_desc
    }

    fn clone_with_distributed_lookup(&self, input: PlanRef, distributed_lookup: bool) -> Self {
        let mut batch_lookup_join = self.clone_with_input(input);
        batch_lookup_join.distributed_lookup = distributed_lookup;
        batch_lookup_join
    }

    pub fn lookup_prefix_len(&self) -> usize {
        self.lookup_prefix_len
    }
}

impl Distill for BatchLookupJoin {
    fn distill<'a>(&self) -> Pretty<'a> {
        let verbose = self.base.ctx.is_explain_verbose();
        let mut vec = Vec::with_capacity(if verbose { 3 } else { 2 });
        vec.push(("type", Pretty::debug(&self.logical.join_type)));

        let concat_schema = self.logical.concat_schema();
        vec.push((
            "predicate",
            Pretty::debug(&EqJoinPredicateDisplay {
                eq_join_predicate: self.eq_join_predicate(),
                input_schema: &concat_schema,
            }),
        ));

        if verbose {
            let data = IndicesDisplay::from_join(&self.logical, &concat_schema)
                .map_or_else(|| Pretty::from("all"), |id| Pretty::display(&id));
            vec.push(("output", data));
        }

        Pretty::childless_record("BatchLookupJoin", vec)
    }
}

impl fmt::Display for BatchLookupJoin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let verbose = self.base.ctx.is_explain_verbose();
        let mut builder = f.debug_struct("BatchLookupJoin");
        builder.field("type", &self.logical.join_type);

        let concat_schema = self.logical.concat_schema();
        builder.field(
            "predicate",
            &EqJoinPredicateDisplay {
                eq_join_predicate: self.eq_join_predicate(),
                input_schema: &concat_schema,
            },
        );

        if verbose {
            match IndicesDisplay::from_join(&self.logical, &concat_schema) {
                None => builder.field("output", &format_args!("all")),
                Some(id) => builder.field("output", &id),
            };
        }

        builder.finish()
    }
}

impl PlanTreeNodeUnary for BatchLookupJoin {
    fn input(&self) -> PlanRef {
        self.logical.left.clone()
    }

    // Only change left side
    fn clone_with_input(&self, input: PlanRef) -> Self {
        let mut logical = self.logical.clone();
        logical.left = input;
        Self::new(
            logical,
            self.eq_join_predicate.clone(),
            self.right_table_desc.clone(),
            self.right_output_column_ids.clone(),
            self.lookup_prefix_len,
            self.distributed_lookup,
        )
    }
}

impl_plan_tree_node_for_unary! { BatchLookupJoin }

impl ToDistributedBatch for BatchLookupJoin {
    fn to_distributed(&self) -> Result<PlanRef> {
        let input = self.input().to_distributed_with_required(
            &Order::any(),
            &RequiredDist::PhysicalDist(Distribution::UpstreamHashShard(
                self.eq_join_predicate
                    .left_eq_indexes()
                    .into_iter()
                    .take(self.lookup_prefix_len)
                    .collect(),
                self.right_table_desc.table_id,
            )),
        )?;
        Ok(self.clone_with_distributed_lookup(input, true).into())
    }
}

impl ToBatchPb for BatchLookupJoin {
    fn to_batch_prost_body(&self) -> NodeBody {
        if self.distributed_lookup {
            NodeBody::DistributedLookupJoin(DistributedLookupJoinNode {
                join_type: self.logical.join_type as i32,
                condition: self
                    .eq_join_predicate
                    .other_cond()
                    .as_expr_unless_true()
                    .map(|x| x.to_expr_proto()),
                outer_side_key: self
                    .eq_join_predicate
                    .left_eq_indexes()
                    .into_iter()
                    .map(|a| a as _)
                    .collect(),
                inner_side_key: self
                    .eq_join_predicate
                    .right_eq_indexes()
                    .into_iter()
                    .map(|a| a as _)
                    .collect(),
                inner_side_table_desc: Some(self.right_table_desc.to_protobuf()),
                inner_side_column_ids: self
                    .right_output_column_ids
                    .iter()
                    .map(ColumnId::get_id)
                    .collect(),
                output_indices: self
                    .logical
                    .output_indices
                    .iter()
                    .map(|&x| x as u32)
                    .collect(),
                null_safe: self.eq_join_predicate.null_safes(),
                lookup_prefix_len: self.lookup_prefix_len as u32,
            })
        } else {
            NodeBody::LocalLookupJoin(LocalLookupJoinNode {
                join_type: self.logical.join_type as i32,
                condition: self
                    .eq_join_predicate
                    .other_cond()
                    .as_expr_unless_true()
                    .map(|x| x.to_expr_proto()),
                outer_side_key: self
                    .eq_join_predicate
                    .left_eq_indexes()
                    .into_iter()
                    .map(|a| a as _)
                    .collect(),
                inner_side_key: self
                    .eq_join_predicate
                    .right_eq_indexes()
                    .into_iter()
                    .map(|a| a as _)
                    .collect(),
                inner_side_table_desc: Some(self.right_table_desc.to_protobuf()),
                inner_side_vnode_mapping: vec![], // To be filled in at local.rs
                inner_side_column_ids: self
                    .right_output_column_ids
                    .iter()
                    .map(ColumnId::get_id)
                    .collect(),
                output_indices: self
                    .logical
                    .output_indices
                    .iter()
                    .map(|&x| x as u32)
                    .collect(),
                worker_nodes: vec![], // To be filled in at local.rs
                null_safe: self.eq_join_predicate.null_safes(),
                lookup_prefix_len: self.lookup_prefix_len as u32,
            })
        }
    }
}

impl ToLocalBatch for BatchLookupJoin {
    fn to_local(&self) -> Result<PlanRef> {
        let input = RequiredDist::single()
            .enforce_if_not_satisfies(self.input().to_local()?, &Order::any())?;

        Ok(self.clone_with_distributed_lookup(input, false).into())
    }
}

impl ExprRewritable for BatchLookupJoin {
    fn has_rewritable_expr(&self) -> bool {
        true
    }

    fn rewrite_exprs(&self, r: &mut dyn ExprRewriter) -> PlanRef {
        let base = self.base.clone_with_new_plan_id();
        let mut logical = self.logical.clone();
        logical.rewrite_exprs(r);
        Self {
            base,
            logical,
            eq_join_predicate: self.eq_join_predicate.rewrite_exprs(r),
            ..Self::clone(self)
        }
        .into()
    }
}

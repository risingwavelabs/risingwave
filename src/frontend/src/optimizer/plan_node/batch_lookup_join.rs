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

use pretty_xmlish::{Pretty, XmlNode};
use risingwave_common::catalog::{ColumnId, TableDesc};
use risingwave_common::error::Result;
use risingwave_pb::batch_plan::plan_node::NodeBody;
use risingwave_pb::batch_plan::{DistributedLookupJoinNode, LocalLookupJoinNode};

use super::batch::prelude::*;
use super::generic::{self, GenericPlanRef};
use super::utils::{childless_record, Distill};
use super::ExprRewritable;
use crate::expr::{Expr, ExprRewriter, ExprVisitor};
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::utils::IndicesDisplay;
use crate::optimizer::plan_node::{
    EqJoinPredicate, EqJoinPredicateDisplay, LogicalScan, PlanBase, PlanTreeNodeUnary, ToBatchPb,
    ToDistributedBatch, ToLocalBatch,
};
use crate::optimizer::property::{Distribution, Order, RequiredDist};
use crate::optimizer::PlanRef;
use crate::utils::ColIndexMappingRewriteExt;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BatchLookupJoin {
    pub base: PlanBase<Batch>,
    core: generic::Join<PlanRef>,

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
        core: generic::Join<PlanRef>,
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
        let dist = Self::derive_dist(core.left.distribution(), &core);
        let base = PlanBase::new_batch_with_core(&core, dist, Order::any());
        Self {
            base,
            core,
            eq_join_predicate,
            right_table_desc,
            right_output_column_ids,
            lookup_prefix_len,
            distributed_lookup,
        }
    }

    fn derive_dist(left: &Distribution, core: &generic::Join<PlanRef>) -> Distribution {
        match left {
            Distribution::Single => Distribution::Single,
            Distribution::HashShard(_) | Distribution::UpstreamHashShard(_, _) => {
                let l2o = core.l2i_col_mapping().composite(&core.i2o_col_mapping());
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
    fn distill<'a>(&self) -> XmlNode<'a> {
        let verbose = self.base.ctx().is_explain_verbose();
        let mut vec = Vec::with_capacity(if verbose { 3 } else { 2 });
        vec.push(("type", Pretty::debug(&self.core.join_type)));

        let concat_schema = self.core.concat_schema();
        vec.push((
            "predicate",
            Pretty::debug(&EqJoinPredicateDisplay {
                eq_join_predicate: self.eq_join_predicate(),
                input_schema: &concat_schema,
            }),
        ));

        if verbose {
            let data = IndicesDisplay::from_join(&self.core, &concat_schema);
            vec.push(("output", data));
        }

        if let Some(scan) = self.core.right.as_logical_scan() {
            let scan: &LogicalScan = scan;
            vec.push(("lookup table", Pretty::display(&scan.table_name())));
        }

        childless_record("BatchLookupJoin", vec)
    }
}

impl PlanTreeNodeUnary for BatchLookupJoin {
    fn input(&self) -> PlanRef {
        self.core.left.clone()
    }

    // Only change left side
    fn clone_with_input(&self, input: PlanRef) -> Self {
        let mut core = self.core.clone();
        core.left = input;
        Self::new(
            core,
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
        // Align left distribution keys with the right table.
        let mut exchange_dist_keys = vec![];
        let left_eq_indexes = self.eq_join_predicate.left_eq_indexes();
        let right_table_desc = self.right_table_desc();
        for dist_col_index in &right_table_desc.distribution_key {
            let dist_col_id = right_table_desc.columns[*dist_col_index].column_id;
            let output_pos = self
                .right_output_column_ids
                .iter()
                .position(|p| *p == dist_col_id)
                .unwrap();
            let dist_in_eq_indexes = self
                .eq_join_predicate
                .right_eq_indexes()
                .iter()
                .position(|col| *col == output_pos)
                .unwrap();
            assert!(dist_in_eq_indexes < self.lookup_prefix_len);
            exchange_dist_keys.push(left_eq_indexes[dist_in_eq_indexes]);
        }

        assert!(!exchange_dist_keys.is_empty());

        let input = self.input().to_distributed_with_required(
            &Order::any(),
            &RequiredDist::PhysicalDist(Distribution::UpstreamHashShard(
                exchange_dist_keys,
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
                join_type: self.core.join_type as i32,
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
                output_indices: self.core.output_indices.iter().map(|&x| x as u32).collect(),
                null_safe: self.eq_join_predicate.null_safes(),
                lookup_prefix_len: self.lookup_prefix_len as u32,
            })
        } else {
            NodeBody::LocalLookupJoin(LocalLookupJoinNode {
                join_type: self.core.join_type as i32,
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
                output_indices: self.core.output_indices.iter().map(|&x| x as u32).collect(),
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
        let mut core = self.core.clone();
        core.rewrite_exprs(r);
        Self {
            base,
            core,
            eq_join_predicate: self.eq_join_predicate.rewrite_exprs(r),
            ..Self::clone(self)
        }
        .into()
    }
}

impl ExprVisitable for BatchLookupJoin {
    fn visit_exprs(&self, v: &mut dyn ExprVisitor) {
        self.core.visit_exprs(v);
    }
}

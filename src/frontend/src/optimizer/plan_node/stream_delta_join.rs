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

use pretty_xmlish::{Pretty, XmlNode};
use risingwave_common::catalog::ColumnDesc;
use risingwave_common::util::functional::SameOrElseExt;
use risingwave_pb::plan_common::JoinType;
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_plan::{ArrangementInfo, DeltaIndexJoinNode};

use super::generic::GenericPlanNode;
use super::stream::prelude::*;
use super::utils::{Distill, childless_record};
use super::{ExprRewritable, PlanBase, PlanRef, PlanTreeNodeBinary, generic};
use crate::expr::{Expr, ExprRewriter, ExprVisitor};
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::utils::IndicesDisplay;
use crate::optimizer::plan_node::{EqJoinPredicate, EqJoinPredicateDisplay, TryToStreamPb};
use crate::optimizer::property::{Distribution, MonotonicityMap, WatermarkColumns};
use crate::scheduler::SchedulerResult;
use crate::stream_fragmenter::BuildFragmentGraphState;

/// [`StreamDeltaJoin`] implements [`super::LogicalJoin`] with delta join. It requires its two
/// inputs to be indexes.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamDeltaJoin {
    pub base: PlanBase<Stream>,
    core: generic::Join<PlanRef>,

    /// The join condition must be equivalent to `logical.on`, but separated into equal and
    /// non-equal parts to facilitate execution later
    eq_join_predicate: EqJoinPredicate,
}

impl StreamDeltaJoin {
    pub fn new(core: generic::Join<PlanRef>, eq_join_predicate: EqJoinPredicate) -> Self {
        let ctx = core.ctx();

        // Inner join won't change the append-only behavior of the stream. The rest might.
        let append_only = match core.join_type {
            JoinType::Inner => core.left.append_only() && core.right.append_only(),
            _ => todo!("delta join only supports inner join for now"),
        };
        if eq_join_predicate.has_non_eq() {
            todo!("non-eq condition not supported for delta join");
        }

        // FIXME: delta join could have arbitrary distribution.
        let dist = Distribution::SomeShard;

        let watermark_columns = {
            let from_left = core
                .left
                .watermark_columns()
                .map_clone(&core.l2i_col_mapping());
            let from_right = core
                .right
                .watermark_columns()
                .map_clone(&core.r2i_col_mapping());
            let mut res = WatermarkColumns::new();
            for (idx, l_wtmk_group) in from_left.iter() {
                if let Some(r_wtmk_group) = from_right.get_group(idx) {
                    res.insert(
                        idx,
                        l_wtmk_group.same_or_else(r_wtmk_group, || ctx.next_watermark_group_id()),
                    );
                }
            }
            res.map_clone(&core.i2o_col_mapping())
        };

        // TODO: derive from input
        let base = PlanBase::new_stream_with_core(
            &core,
            dist,
            append_only,
            false, // TODO(rc): derive EOWC property from input
            watermark_columns,
            MonotonicityMap::new(), // TODO: derive monotonicity
        );

        Self {
            base,
            core,
            eq_join_predicate,
        }
    }

    /// Get a reference to the delta hash join's eq join predicate.
    pub fn eq_join_predicate(&self) -> &EqJoinPredicate {
        &self.eq_join_predicate
    }
}

impl Distill for StreamDeltaJoin {
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

        childless_record("StreamDeltaJoin", vec)
    }
}

impl PlanTreeNodeBinary for StreamDeltaJoin {
    fn left(&self) -> PlanRef {
        self.core.left.clone()
    }

    fn right(&self) -> PlanRef {
        self.core.right.clone()
    }

    fn clone_with_left_right(&self, left: PlanRef, right: PlanRef) -> Self {
        let mut core = self.core.clone();
        core.left = left;
        core.right = right;
        Self::new(core, self.eq_join_predicate.clone())
    }
}

impl_plan_tree_node_for_binary! { StreamDeltaJoin }

impl TryToStreamPb for StreamDeltaJoin {
    fn try_to_stream_prost_body(
        &self,
        _state: &mut BuildFragmentGraphState,
    ) -> SchedulerResult<NodeBody> {
        let left = self.left();
        let right = self.right();

        let left_table = if let Some(stream_table_scan) = left.as_stream_table_scan() {
            stream_table_scan.core()
        } else {
            unreachable!();
        };
        let left_table_desc = &*left_table.table_desc;
        let right_table = if let Some(stream_table_scan) = right.as_stream_table_scan() {
            stream_table_scan.core()
        } else {
            unreachable!();
        };
        let right_table_desc = &*right_table.table_desc;

        // TODO: add a separate delta join node in proto, or move fragmenter to frontend so that we
        // don't need an intermediate representation.
        let eq_join_predicate = &self.eq_join_predicate;
        Ok(NodeBody::DeltaIndexJoin(Box::new(DeltaIndexJoinNode {
            join_type: self.core.join_type as i32,
            left_key: eq_join_predicate
                .left_eq_indexes()
                .iter()
                .map(|v| *v as i32)
                .collect(),
            right_key: eq_join_predicate
                .right_eq_indexes()
                .iter()
                .map(|v| *v as i32)
                .collect(),
            condition: eq_join_predicate
                .other_cond()
                .as_expr_unless_true()
                .map(|x| x.to_expr_proto()),
            left_table_id: left_table_desc.table_id.table_id(),
            right_table_id: right_table_desc.table_id.table_id(),
            left_info: Some(ArrangementInfo {
                // TODO: remove it
                arrange_key_orders: left_table_desc.arrange_key_orders_protobuf(),
                // TODO: remove it
                column_descs: left_table
                    .column_descs()
                    .iter()
                    .map(ColumnDesc::to_protobuf)
                    .collect(),
                table_desc: Some(left_table_desc.try_to_protobuf()?),
                output_col_idx: left_table
                    .output_col_idx
                    .iter()
                    .map(|&v| v as u32)
                    .collect(),
            }),
            right_info: Some(ArrangementInfo {
                // TODO: remove it
                arrange_key_orders: right_table_desc.arrange_key_orders_protobuf(),
                // TODO: remove it
                column_descs: right_table
                    .column_descs()
                    .iter()
                    .map(ColumnDesc::to_protobuf)
                    .collect(),
                table_desc: Some(right_table_desc.try_to_protobuf()?),
                output_col_idx: right_table
                    .output_col_idx
                    .iter()
                    .map(|&v| v as u32)
                    .collect(),
            }),
            output_indices: self.core.output_indices.iter().map(|&x| x as u32).collect(),
        })))
    }
}

impl ExprRewritable for StreamDeltaJoin {
    fn has_rewritable_expr(&self) -> bool {
        true
    }

    fn rewrite_exprs(&self, r: &mut dyn ExprRewriter) -> PlanRef {
        let mut core = self.core.clone();
        core.rewrite_exprs(r);
        Self::new(core, self.eq_join_predicate.rewrite_exprs(r)).into()
    }
}
impl ExprVisitable for StreamDeltaJoin {
    fn visit_exprs(&self, v: &mut dyn ExprVisitor) {
        self.core.visit_exprs(v);
    }
}

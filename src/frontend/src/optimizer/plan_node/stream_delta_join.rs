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
use std::ops::BitAnd;

use risingwave_common::catalog::{ColumnDesc, Schema};
use risingwave_pb::plan_common::JoinType;
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_plan::{ArrangementInfo, DeltaIndexJoinNode};

use super::generic::GenericPlanRef;
use super::{ExprRewritable, LogicalJoin, PlanBase, PlanRef, PlanTreeNodeBinary, StreamNode};
use crate::expr::{Expr, ExprRewriter};
use crate::optimizer::plan_node::stream::StreamPlanRef;
use crate::optimizer::plan_node::utils::IndicesDisplay;
use crate::optimizer::plan_node::{EqJoinPredicate, EqJoinPredicateDisplay};
use crate::optimizer::property::Distribution;
use crate::stream_fragmenter::BuildFragmentGraphState;
use crate::utils::ColIndexMappingRewriteExt;

/// [`StreamDeltaJoin`] implements [`super::LogicalJoin`] with delta join. It requires its two
/// inputs to be indexes.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamDeltaJoin {
    pub base: PlanBase,
    logical: LogicalJoin,

    /// The join condition must be equivalent to `logical.on`, but separated into equal and
    /// non-equal parts to facilitate execution later
    eq_join_predicate: EqJoinPredicate,
}

impl StreamDeltaJoin {
    pub fn new(logical: LogicalJoin, eq_join_predicate: EqJoinPredicate) -> Self {
        let ctx = logical.base.ctx.clone();
        // Inner join won't change the append-only behavior of the stream. The rest might.
        let append_only = match logical.join_type() {
            JoinType::Inner => logical.left().append_only() && logical.right().append_only(),
            _ => todo!("delta join only supports inner join for now"),
        };
        if eq_join_predicate.has_non_eq() {
            todo!("non-eq condition not supported for delta join");
        }

        // FIXME: delta join could have arbitrary distribution.
        let dist = Distribution::SomeShard;

        let watermark_columns = {
            let from_left = logical
                .l2i_col_mapping()
                .rewrite_bitset(logical.left().watermark_columns());
            let from_right = logical
                .r2i_col_mapping()
                .rewrite_bitset(logical.right().watermark_columns());
            let watermark_columns = from_left.bitand(&from_right);
            logical.i2o_col_mapping().rewrite_bitset(&watermark_columns)
        };
        // TODO: derive from input
        let base = PlanBase::new_stream(
            ctx,
            logical.schema().clone(),
            logical.base.logical_pk.to_vec(),
            logical.functional_dependency().clone(),
            dist,
            append_only,
            watermark_columns,
        );

        Self {
            base,
            logical,
            eq_join_predicate,
        }
    }

    /// Get a reference to the batch hash join's eq join predicate.
    pub fn eq_join_predicate(&self) -> &EqJoinPredicate {
        &self.eq_join_predicate
    }
}

impl fmt::Display for StreamDeltaJoin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let verbose = self.base.ctx.is_explain_verbose();
        let mut builder = f.debug_struct("StreamDeltaJoin");
        builder.field("type", &self.logical.join_type());

        let mut concat_schema = self.left().schema().fields.clone();
        concat_schema.extend(self.right().schema().fields.clone());
        let concat_schema = Schema::new(concat_schema);
        builder.field(
            "predicate",
            &EqJoinPredicateDisplay {
                eq_join_predicate: self.eq_join_predicate(),
                input_schema: &concat_schema,
            },
        );

        if verbose {
            if self
                .logical
                .output_indices()
                .iter()
                .copied()
                .eq(0..self.logical.internal_column_num())
            {
                builder.field("output", &format_args!("all"));
            } else {
                builder.field(
                    "output",
                    &IndicesDisplay {
                        indices: self.logical.output_indices(),
                        input_schema: &concat_schema,
                    },
                );
            }
        }

        builder.finish()
    }
}

impl PlanTreeNodeBinary for StreamDeltaJoin {
    fn left(&self) -> PlanRef {
        self.logical.left()
    }

    fn right(&self) -> PlanRef {
        self.logical.right()
    }

    fn clone_with_left_right(&self, left: PlanRef, right: PlanRef) -> Self {
        Self::new(
            self.logical.clone_with_left_right(left, right),
            self.eq_join_predicate.clone(),
        )
    }
}

impl_plan_tree_node_for_binary! { StreamDeltaJoin }

impl StreamNode for StreamDeltaJoin {
    fn to_stream_prost_body(&self, _state: &mut BuildFragmentGraphState) -> NodeBody {
        let left = self.left();
        let right = self.right();

        let left_table = if let Some(stream_index_scan) = left.as_stream_index_scan() {
            stream_index_scan.logical()
        } else if let Some(stream_table_scan) = left.as_stream_table_scan() {
            stream_table_scan.logical()
        } else {
            unreachable!();
        };
        let left_table_desc = left_table.table_desc();
        let right_table = if let Some(stream_index_scan) = right.as_stream_index_scan() {
            stream_index_scan.logical()
        } else if let Some(stream_table_scan) = right.as_stream_table_scan() {
            stream_table_scan.logical()
        } else {
            unreachable!();
        };
        let right_table_desc = right_table.table_desc();

        // TODO: add a separate delta join node in proto, or move fragmenter to frontend so that we
        // don't need an intermediate representation.
        NodeBody::DeltaIndexJoin(DeltaIndexJoinNode {
            join_type: self.logical.join_type() as i32,
            left_key: self
                .eq_join_predicate
                .left_eq_indexes()
                .iter()
                .map(|v| *v as i32)
                .collect(),
            right_key: self
                .eq_join_predicate
                .right_eq_indexes()
                .iter()
                .map(|v| *v as i32)
                .collect(),
            condition: self
                .eq_join_predicate
                .other_cond()
                .as_expr_unless_true()
                .map(|x| x.to_expr_proto()),
            left_table_id: left_table_desc.table_id.table_id(),
            right_table_id: right_table_desc.table_id.table_id(),
            left_info: Some(ArrangementInfo {
                // TODO: remove it
                arrange_key_orders: left_table_desc.arrange_key_orders_prost(),
                // TODO: remove it
                column_descs: left_table
                    .column_descs()
                    .iter()
                    .map(ColumnDesc::to_protobuf)
                    .collect(),
                table_desc: Some(left_table_desc.to_protobuf()),
            }),
            right_info: Some(ArrangementInfo {
                // TODO: remove it
                arrange_key_orders: right_table_desc.arrange_key_orders_prost(),
                // TODO: remove it
                column_descs: right_table
                    .column_descs()
                    .iter()
                    .map(ColumnDesc::to_protobuf)
                    .collect(),
                table_desc: Some(right_table_desc.to_protobuf()),
            }),
            output_indices: self
                .logical
                .output_indices()
                .iter()
                .map(|&x| x as u32)
                .collect(),
        })
    }
}

impl ExprRewritable for StreamDeltaJoin {
    fn has_rewritable_expr(&self) -> bool {
        true
    }

    fn rewrite_exprs(&self, r: &mut dyn ExprRewriter) -> PlanRef {
        Self::new(
            self.logical
                .rewrite_exprs(r)
                .as_logical_join()
                .unwrap()
                .clone(),
            self.eq_join_predicate.rewrite_exprs(r),
        )
        .into()
    }
}

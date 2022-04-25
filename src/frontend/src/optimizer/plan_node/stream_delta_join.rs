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

use std::fmt;

use risingwave_pb::plan_common::JoinType;
use risingwave_pb::stream_plan::stream_node::Node;
use risingwave_pb::stream_plan::DeltaIndexJoinNode;

use super::{LogicalJoin, PlanBase, PlanRef, PlanTreeNodeBinary, StreamHashJoin, ToStreamProst};
use crate::catalog::TableId;
use crate::expr::Expr;
use crate::optimizer::plan_node::EqJoinPredicate;

/// [`StreamDeltaJoin`] implements [`super::LogicalJoin`] with delta join. It requires its two
/// inputs to be indexes.
#[derive(Debug, Clone)]
pub struct StreamDeltaJoin {
    pub base: PlanBase,
    logical: LogicalJoin,

    /// The join condition must be equivalent to `logical.on`, but separated into equal and
    /// non-equal parts to facilitate execution later
    eq_join_predicate: EqJoinPredicate,

    /// Table id of the left side
    left_table_id: TableId,
    /// Table id of the right side
    right_table_id: TableId,
}

impl StreamDeltaJoin {
    pub fn new(
        logical: LogicalJoin,
        eq_join_predicate: EqJoinPredicate,
        left_table_id: TableId,
        right_table_id: TableId,
    ) -> Self {
        let ctx = logical.base.ctx.clone();
        // Inner join won't change the append-only behavior of the stream. The rest might.
        let append_only = match logical.join_type() {
            JoinType::Inner => logical.left().append_only() && logical.right().append_only(),
            _ => todo!("delta join only supports inner join for now"),
        };
        if eq_join_predicate.has_non_eq() {
            todo!("non-eq condition not supported for delta join");
        }
        let dist = StreamHashJoin::derive_dist(
            logical.left().distribution(),
            logical.right().distribution(),
            &eq_join_predicate,
            &logical.l2o_col_mapping(),
        );

        // TODO: derive from input
        let base = PlanBase::new_stream(
            ctx,
            logical.schema().clone(),
            logical.base.pk_indices.to_vec(),
            dist,
            append_only,
        );

        Self {
            base,
            logical,
            eq_join_predicate,
            left_table_id,
            right_table_id,
        }
    }

    /// Get a reference to the batch hash join's eq join predicate.
    pub fn eq_join_predicate(&self) -> &EqJoinPredicate {
        &self.eq_join_predicate
    }
}

impl fmt::Display for StreamDeltaJoin {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "StreamDeltaJoin {{ type: {:?}, predicate: {} }}",
            self.logical.join_type(),
            self.eq_join_predicate()
        )
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
            self.left_table_id,
            self.right_table_id,
        )
    }
}

impl_plan_tree_node_for_binary! { StreamDeltaJoin }

impl ToStreamProst for StreamDeltaJoin {
    fn to_stream_prost_body(&self) -> Node {
        // TODO: add a separate delta join node in proto, or move fragmenter to frontend so that we
        // don't need an intermediate representation.
        Node::DeltaIndexJoin(DeltaIndexJoinNode {
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
            left_table_id: self.left_table_id.table_id(),
            right_table_id: self.right_table_id.table_id(),
        })
    }
}

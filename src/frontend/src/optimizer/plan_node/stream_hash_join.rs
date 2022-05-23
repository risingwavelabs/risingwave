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

use itertools::Itertools;
use risingwave_pb::plan_common::JoinType;
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_plan::HashJoinNode;

use super::{LogicalJoin, PlanBase, PlanRef, PlanTreeNodeBinary, StreamDeltaJoin, ToStreamProst};
use crate::expr::Expr;
use crate::optimizer::plan_node::EqJoinPredicate;
use crate::optimizer::property::Distribution;
use crate::utils::ColIndexMapping;

/// [`StreamHashJoin`] implements [`super::LogicalJoin`] with hash table. It builds a hash table
/// from inner (right-side) relation and probes with data from outer (left-side) relation to
/// get output rows.
#[derive(Debug, Clone)]
pub struct StreamHashJoin {
    pub base: PlanBase,
    logical: LogicalJoin,

    /// The join condition must be equivalent to `logical.on`, but separated into equal and
    /// non-equal parts to facilitate execution later
    eq_join_predicate: EqJoinPredicate,

    /// Whether to force use delta join for this join node. If this is true, then indexes will
    /// be create automatically when building the executors on meta service. For testing purpose
    /// only. Will remove after we have fully support shared state and index.
    is_delta: bool,
}

pub static DELTA_JOIN: &str = "RW_FORCE_DELTA_JOIN";

impl StreamHashJoin {
    pub fn new(logical: LogicalJoin, eq_join_predicate: EqJoinPredicate) -> Self {
        let ctx = logical.base.ctx.clone();
        // Inner join won't change the append-only behavior of the stream. The rest might.
        let append_only = match logical.join_type() {
            JoinType::Inner => logical.left().append_only() && logical.right().append_only(),
            _ => false,
        };
        let dist = Self::derive_dist(
            logical.left().distribution(),
            logical.right().distribution(),
            &logical.l2o_col_mapping(),
        );

        let force_delta = if let Some(config) = ctx.inner().session_ctx.get_config(DELTA_JOIN) {
            config.is_set(false)
        } else {
            false
        };

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
            is_delta: force_delta,
        }
    }

    /// Get join type
    pub fn join_type(&self) -> JoinType {
        self.logical.join_type()
    }

    /// Get a reference to the batch hash join's eq join predicate.
    pub fn eq_join_predicate(&self) -> &EqJoinPredicate {
        &self.eq_join_predicate
    }

    pub(super) fn derive_dist(
        left: &Distribution,
        right: &Distribution,
        l2o_mapping: &ColIndexMapping,
    ) -> Distribution {
        match (left, right) {
            (Distribution::Single, Distribution::Single) => Distribution::Single,
            (Distribution::HashShard(_), Distribution::HashShard(_)) => {
                l2o_mapping.rewrite_provided_distribution(left)
            }
            (_, _) => panic!(),
        }
    }

    /// Convert this hash join to a delta join plan
    pub fn to_delta_join(&self) -> StreamDeltaJoin {
        StreamDeltaJoin::new(self.logical.clone(), self.eq_join_predicate.clone())
    }
}

impl fmt::Display for StreamHashJoin {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{} {{ type: {:?}, predicate: {} }}",
            if self.is_delta {
                "StreamDeltaHashJoin"
            } else {
                "StreamHashJoin"
            },
            self.logical.join_type(),
            self.eq_join_predicate()
        )
    }
}

impl PlanTreeNodeBinary for StreamHashJoin {
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

impl_plan_tree_node_for_binary! { StreamHashJoin }

impl ToStreamProst for StreamHashJoin {
    fn to_stream_prost_body(&self) -> NodeBody {
        NodeBody::HashJoin(HashJoinNode {
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
            distribution_keys: self
                .base
                .dist
                .dist_column_indices()
                .iter()
                .map(|idx| *idx as i32)
                .collect_vec(),
            is_delta_join: self.is_delta,
            ..Default::default()
        })
    }
}

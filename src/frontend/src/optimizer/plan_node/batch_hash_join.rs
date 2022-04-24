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

use risingwave_pb::batch_plan::plan_node::NodeBody;
use risingwave_pb::batch_plan::HashJoinNode;

use super::{
    EqJoinPredicate, LogicalJoin, PlanBase, PlanRef, PlanTreeNodeBinary, ToBatchProst,
    ToDistributedBatch,
};
use crate::optimizer::property::{Distribution, Order};
use crate::utils::ColIndexMapping;

/// `BatchHashJoin` implements [`super::LogicalJoin`] with hash table. It builds a hash table
/// from inner (right-side) relation and then probes with data from outer (left-side) relation to
/// get output rows.
#[derive(Debug, Clone)]
pub struct BatchHashJoin {
    pub base: PlanBase,
    logical: LogicalJoin,

    /// The join condition must be equivalent to `logical.on`, but separated into equal and
    /// non-equal parts to facilitate execution later
    eq_join_predicate: EqJoinPredicate,
}

impl BatchHashJoin {
    pub fn new(logical: LogicalJoin, eq_join_predicate: EqJoinPredicate) -> Self {
        let ctx = logical.base.ctx.clone();
        let dist = Self::derive_dist(
            logical.left().distribution(),
            logical.right().distribution(),
            &eq_join_predicate,
            &logical.l2o_col_mapping(),
        );
        let base = PlanBase::new_batch(ctx, logical.schema().clone(), dist, Order::any().clone());

        Self {
            base,
            logical,
            eq_join_predicate,
        }
    }

    fn derive_dist(
        left: &Distribution,
        right: &Distribution,
        predicate: &EqJoinPredicate,
        l2o_mapping: &ColIndexMapping,
    ) -> Distribution {
        match (left, right) {
            (Distribution::Any, Distribution::Any) => Distribution::Any,
            (Distribution::Single, Distribution::Single) => Distribution::Single,
            (Distribution::HashShard(_), Distribution::HashShard(_)) => {
                assert!(left.satisfies(&Distribution::HashShard(predicate.left_eq_indexes())));
                assert!(right.satisfies(&Distribution::HashShard(predicate.right_eq_indexes())));
                l2o_mapping.rewrite_provided_distribution(left)
            }
            (_, _) => panic!(),
        }
    }

    /// Get a reference to the batch hash join's eq join predicate.
    pub fn eq_join_predicate(&self) -> &EqJoinPredicate {
        &self.eq_join_predicate
    }
}

impl fmt::Display for BatchHashJoin {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "BatchHashJoin {{ type: {:?}, predicate: {} }}",
            self.logical.join_type(),
            self.eq_join_predicate()
        )
    }
}

impl PlanTreeNodeBinary for BatchHashJoin {
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

impl_plan_tree_node_for_binary! { BatchHashJoin }

impl ToDistributedBatch for BatchHashJoin {
    fn to_distributed(&self) -> PlanRef {
        let left = self.left().to_distributed_with_required(
            Order::any(),
            &Distribution::HashShard(self.eq_join_predicate().left_eq_indexes()),
        );
        let right = self.right().to_distributed_with_required(
            Order::any(),
            &Distribution::HashShard(self.eq_join_predicate().right_eq_indexes()),
        );

        self.clone_with_left_right(left, right).into()
    }
}

impl ToBatchProst for BatchHashJoin {
    fn to_batch_prost_body(&self) -> NodeBody {
        NodeBody::HashJoin(HashJoinNode {
            join_type: self.logical.join_type() as i32,
            left_key: self
                .eq_join_predicate
                .left_eq_indexes()
                .into_iter()
                .map(|a| a as i32)
                .collect(),
            right_key: self
                .eq_join_predicate
                .right_eq_indexes()
                .into_iter()
                .map(|a| a as i32)
                .collect(),
            condition: None,
        })
    }
}

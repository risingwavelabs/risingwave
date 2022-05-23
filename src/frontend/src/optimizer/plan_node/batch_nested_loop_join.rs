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

use risingwave_common::error::Result;
use risingwave_pb::batch_plan::plan_node::NodeBody;
use risingwave_pb::batch_plan::NestedLoopJoinNode;

use super::{LogicalJoin, PlanBase, PlanRef, PlanTreeNodeBinary, ToBatchProst, ToDistributedBatch};
use crate::expr::{Expr, ExprImpl};
use crate::optimizer::plan_node::ToLocalBatch;
use crate::optimizer::property::{Distribution, Order, RequiredDist};

/// `BatchNestedLoopJoin` implements [`super::LogicalJoin`] by checking the join condition
/// against all pairs of rows from inner & outer side within 2 layers of loops.
#[derive(Debug, Clone)]
pub struct BatchNestedLoopJoin {
    pub base: PlanBase,
    logical: LogicalJoin,
}

impl BatchNestedLoopJoin {
    pub fn new(logical: LogicalJoin) -> Self {
        let ctx = logical.base.ctx.clone();
        let dist = Self::derive_dist(
            logical.left().distribution(),
            logical.right().distribution(),
        );
        let base = PlanBase::new_batch(ctx, logical.schema().clone(), dist, Order::any().clone());
        Self { base, logical }
    }

    fn derive_dist(left: &Distribution, right: &Distribution) -> Distribution {
        match (left, right) {
            (Distribution::Single, Distribution::Single) => Distribution::Single,
            (_, _) => unreachable!(),
        }
    }
}

impl fmt::Display for BatchNestedLoopJoin {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "BatchNestedLoopJoin {{ type: {:?}, predicate: {} }}",
            self.logical.join_type(),
            self.logical.on()
        )
    }
}

impl PlanTreeNodeBinary for BatchNestedLoopJoin {
    fn left(&self) -> PlanRef {
        self.logical.left()
    }

    fn right(&self) -> PlanRef {
        self.logical.right()
    }

    fn clone_with_left_right(&self, left: PlanRef, right: PlanRef) -> Self {
        Self::new(self.logical.clone_with_left_right(left, right))
    }
}

impl_plan_tree_node_for_binary! { BatchNestedLoopJoin }

impl ToDistributedBatch for BatchNestedLoopJoin {
    fn to_distributed(&self) -> Result<PlanRef> {
        let left = self
            .left()
            .to_distributed_with_required(Order::any(), &RequiredDist::single())?;
        let right = self
            .right()
            .to_distributed_with_required(Order::any(), &RequiredDist::single())?;

        Ok(self.clone_with_left_right(left, right).into())
    }
}

impl ToBatchProst for BatchNestedLoopJoin {
    fn to_batch_prost_body(&self) -> NodeBody {
        NodeBody::NestedLoopJoin(NestedLoopJoinNode {
            join_type: self.logical.join_type() as i32,
            join_cond: Some(ExprImpl::from(self.logical.on().clone()).to_expr_proto()),
        })
    }
}

impl ToLocalBatch for BatchNestedLoopJoin {
    fn to_local(&self) -> Result<PlanRef> {
        let left = RequiredDist::single()
            .enforce_if_not_satisfies(self.left().to_local()?, Order::any())?;

        let right = RequiredDist::single()
            .enforce_if_not_satisfies(self.right().to_local()?, Order::any())?;

        Ok(self.clone_with_left_right(left, right).into())
    }
}

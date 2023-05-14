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

use risingwave_common::error::Result;
use risingwave_pb::batch_plan::plan_node::NodeBody;
use risingwave_pb::batch_plan::DeleteNode;

use super::{
    generic, ExprRewritable, PlanBase, PlanRef, PlanTreeNodeUnary, ToBatchPb, ToDistributedBatch,
};
use crate::optimizer::plan_node::ToLocalBatch;
use crate::optimizer::property::{Distribution, Order, RequiredDist};

/// `BatchDelete` implements [`LogicalDelete`]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BatchDelete {
    pub base: PlanBase,
    pub logical: generic::Delete<PlanRef>,
}

impl BatchDelete {
    pub fn new(logical: generic::Delete<PlanRef>) -> Self {
        let base = PlanBase::new_batch(
            logical.ctx(),
            logical.schema().clone(),
            Distribution::Single,
            Order::any(),
        );
        Self { base, logical }
    }
}

impl fmt::Display for BatchDelete {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.logical.fmt_with_name(f, "BatchDelete")
    }
}

impl PlanTreeNodeUnary for BatchDelete {
    fn input(&self) -> PlanRef {
        self.logical.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        let mut core = self.logical.clone();
        core.input = input;
        Self::new(core)
    }
}

impl_plan_tree_node_for_unary! { BatchDelete }

impl ToDistributedBatch for BatchDelete {
    fn to_distributed(&self) -> Result<PlanRef> {
        let new_input = RequiredDist::single()
            .enforce_if_not_satisfies(self.input().to_distributed()?, &Order::any())?;
        Ok(self.clone_with_input(new_input).into())
    }
}

impl ToBatchPb for BatchDelete {
    fn to_batch_prost_body(&self) -> NodeBody {
        NodeBody::Delete(DeleteNode {
            table_id: self.logical.table_id.table_id(),
            table_version_id: self.logical.table_version_id,
            returning: self.logical.returning,
        })
    }
}

impl ToLocalBatch for BatchDelete {
    fn to_local(&self) -> Result<PlanRef> {
        let new_input = RequiredDist::single()
            .enforce_if_not_satisfies(self.input().to_local()?, &Order::any())?;
        Ok(self.clone_with_input(new_input).into())
    }
}

impl ExprRewritable for BatchDelete {}

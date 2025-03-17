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

use risingwave_pb::batch_plan::GroupTopNNode;
use risingwave_pb::batch_plan::plan_node::NodeBody;

use super::batch::prelude::*;
use super::utils::impl_distill_by_unit;
use super::{
    ExprRewritable, PlanBase, PlanRef, PlanTreeNodeUnary, ToBatchPb, ToDistributedBatch, generic,
};
use crate::error::Result;
use crate::optimizer::plan_node::ToLocalBatch;
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::property::{Order, RequiredDist};

/// `BatchGroupTopN` implements [`super::LogicalTopN`] to find the top N elements with a heap
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BatchGroupTopN {
    pub base: PlanBase<Batch>,
    core: generic::TopN<PlanRef>,
}

impl BatchGroupTopN {
    pub fn new(core: generic::TopN<PlanRef>) -> Self {
        assert!(!core.group_key.is_empty());
        let base =
            PlanBase::new_batch_with_core(&core, core.input.distribution().clone(), Order::any());
        BatchGroupTopN { base, core }
    }

    fn group_key(&self) -> &[usize] {
        &self.core.group_key
    }
}

impl_distill_by_unit!(BatchGroupTopN, core, "BatchGroupTopN");

impl PlanTreeNodeUnary for BatchGroupTopN {
    fn input(&self) -> PlanRef {
        self.core.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        let mut core = self.core.clone();
        core.input = input;
        Self::new(core)
    }
}

impl_plan_tree_node_for_unary! {BatchGroupTopN}

impl ToDistributedBatch for BatchGroupTopN {
    fn to_distributed(&self) -> Result<PlanRef> {
        let input = self.input().to_distributed()?;
        let input = RequiredDist::hash_shard(self.group_key())
            .enforce_if_not_satisfies(input, &Order::any())?;
        Ok(self.clone_with_input(input).into())
    }
}

impl ToBatchPb for BatchGroupTopN {
    fn to_batch_prost_body(&self) -> NodeBody {
        let column_orders = self.core.order.to_protobuf();
        NodeBody::GroupTopN(GroupTopNNode {
            limit: self.core.limit_attr.limit(),
            offset: self.core.offset,
            column_orders,
            group_key: self.group_key().iter().map(|c| *c as u32).collect(),
            with_ties: self.core.limit_attr.with_ties(),
        })
    }
}

impl ToLocalBatch for BatchGroupTopN {
    fn to_local(&self) -> Result<PlanRef> {
        let input = self.input().to_local()?;
        let input = RequiredDist::single().enforce_if_not_satisfies(input, &Order::any())?;
        Ok(self.clone_with_input(input).into())
    }
}

impl ExprRewritable for BatchGroupTopN {}

impl ExprVisitable for BatchGroupTopN {}

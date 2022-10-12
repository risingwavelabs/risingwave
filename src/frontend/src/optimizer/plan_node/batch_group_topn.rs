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
use risingwave_pb::batch_plan::GroupTopNNode;

use super::{LogicalTopN, PlanBase, PlanRef, PlanTreeNodeUnary, ToBatchProst, ToDistributedBatch};
use crate::optimizer::plan_node::ToLocalBatch;
use crate::optimizer::property::{Order, RequiredDist};

/// `BatchGroupTopN` implements [`super::LogicalTopN`] to find the top N elements with a heap
#[derive(Debug, Clone)]
pub struct BatchGroupTopN {
    pub base: PlanBase,
    logical: LogicalTopN,
}

impl BatchGroupTopN {
    pub fn new(logical: LogicalTopN) -> Self {
        assert!(!logical.group_key().is_empty());
        let ctx = logical.base.ctx.clone();
        let base = PlanBase::new_batch(
            ctx,
            logical.schema().clone(),
            logical.input().distribution().clone(),
            // BatchGroupTopN outputs data in the order of specified order
            logical.topn_order().clone(),
        );
        BatchGroupTopN { base, logical }
    }

    fn group_key(&self) -> &[usize] {
        self.logical.group_key()
    }
}

impl fmt::Display for BatchGroupTopN {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.logical.fmt_with_name(f, "BatchGroupTopN")
    }
}

impl PlanTreeNodeUnary for BatchGroupTopN {
    fn input(&self) -> PlanRef {
        self.logical.input()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(self.logical.clone_with_input(input))
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

impl ToBatchProst for BatchGroupTopN {
    fn to_batch_prost_body(&self) -> NodeBody {
        let column_orders = self.logical.topn_order().to_protobuf(&self.base.schema);
        NodeBody::GroupTopN(GroupTopNNode {
            limit: self.logical.limit() as u64,
            offset: self.logical.offset() as u64,
            column_orders,
            group_key: self.group_key().iter().map(|c| *c as u32).collect(),
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

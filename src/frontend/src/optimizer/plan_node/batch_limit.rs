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
use risingwave_pb::batch_plan::LimitNode;

use super::{
    generic, ExprRewritable, PlanBase, PlanRef, PlanTreeNodeUnary, ToBatchPb, ToDistributedBatch,
};
use crate::optimizer::plan_node::ToLocalBatch;
use crate::optimizer::property::{Order, RequiredDist};

/// `BatchLimit` implements [`super::LogicalLimit`] to fetch specified rows from input
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BatchLimit {
    pub base: PlanBase,
    logical: generic::Limit<PlanRef>,
}

impl BatchLimit {
    pub fn new(logical: generic::Limit<PlanRef>) -> Self {
        let base = PlanBase::new_batch_from_logical(
            &logical,
            logical.input.distribution().clone(),
            logical.input.order().clone(),
        );
        BatchLimit { base, logical }
    }

    fn two_phase_limit(&self, input: PlanRef) -> Result<PlanRef> {
        let new_limit = self.logical.limit + self.logical.offset;
        let new_offset = 0;
        let logical_partial_limit = generic::Limit::new(input, new_limit, new_offset);
        let batch_partial_limit = Self::new(logical_partial_limit);
        let any_order = Order::any();

        let single_dist = RequiredDist::single();
        let ensure_single_dist = if !batch_partial_limit.distribution().satisfies(&single_dist) {
            single_dist.enforce_if_not_satisfies(batch_partial_limit.into(), &any_order)?
        } else {
            // The input's distribution is singleton, so use one phase limit is enough.
            return Ok(batch_partial_limit.into());
        };

        let batch_global_limit = self.clone_with_input(ensure_single_dist);
        Ok(batch_global_limit.into())
    }

    pub fn limit(&self) -> u64 {
        self.logical.limit
    }

    pub fn offset(&self) -> u64 {
        self.logical.offset
    }
}

impl fmt::Display for BatchLimit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.logical.fmt_with_name(f, "BatchLimit")
    }
}

impl PlanTreeNodeUnary for BatchLimit {
    fn input(&self) -> PlanRef {
        self.logical.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        let mut core = self.logical.clone();
        core.input = input;
        Self::new(core)
    }
}
impl_plan_tree_node_for_unary! {BatchLimit}
impl ToDistributedBatch for BatchLimit {
    fn to_distributed(&self) -> Result<PlanRef> {
        self.two_phase_limit(self.input().to_distributed()?)
    }
}

impl ToBatchPb for BatchLimit {
    fn to_batch_prost_body(&self) -> NodeBody {
        NodeBody::Limit(LimitNode {
            limit: self.logical.limit,
            offset: self.logical.offset,
        })
    }
}

impl ToLocalBatch for BatchLimit {
    fn to_local(&self) -> Result<PlanRef> {
        self.two_phase_limit(self.input().to_local()?)
    }
}

impl ExprRewritable for BatchLimit {}

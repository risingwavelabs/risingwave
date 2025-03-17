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

use risingwave_pb::batch_plan::LimitNode;
use risingwave_pb::batch_plan::plan_node::NodeBody;

use super::batch::prelude::*;
use super::utils::impl_distill_by_unit;
use super::{
    ExprRewritable, PlanBase, PlanRef, PlanTreeNodeUnary, ToBatchPb, ToDistributedBatch, generic,
};
use crate::error::Result;
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::{BatchExchange, ToLocalBatch};
use crate::optimizer::property::{Distribution, Order, RequiredDist};

/// `BatchLimit` implements [`super::LogicalLimit`] to fetch specified rows from input
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BatchLimit {
    pub base: PlanBase<Batch>,
    core: generic::Limit<PlanRef>,
}

const LIMIT_SEQUENTIAL_EXCHANGE_THRESHOLD: u64 = 1024;

impl BatchLimit {
    pub fn new(core: generic::Limit<PlanRef>) -> Self {
        let base = PlanBase::new_batch_with_core(
            &core,
            core.input.distribution().clone(),
            core.input.order().clone(),
        );
        BatchLimit { base, core }
    }

    fn two_phase_limit(&self, new_input: PlanRef) -> Result<PlanRef> {
        let new_limit = self.core.limit + self.core.offset;
        let new_offset = 0;
        let logical_partial_limit = generic::Limit::new(new_input.clone(), new_limit, new_offset);
        let batch_partial_limit = Self::new(logical_partial_limit);
        let any_order = Order::any();

        let single_dist = RequiredDist::single();
        let ensure_single_dist = if !batch_partial_limit.distribution().satisfies(&single_dist) {
            if new_limit < LIMIT_SEQUENTIAL_EXCHANGE_THRESHOLD {
                BatchExchange::new_with_sequential(
                    batch_partial_limit.into(),
                    any_order,
                    Distribution::Single,
                )
                .into()
            } else {
                BatchExchange::new(batch_partial_limit.into(), any_order, Distribution::Single)
                    .into()
            }
        } else {
            // The input's distribution is singleton, so use one phase limit is enough.
            return Ok(self.clone_with_input(new_input).into());
        };

        let batch_global_limit = self.clone_with_input(ensure_single_dist);
        Ok(batch_global_limit.into())
    }

    pub fn limit(&self) -> u64 {
        self.core.limit
    }

    pub fn offset(&self) -> u64 {
        self.core.offset
    }
}

impl PlanTreeNodeUnary for BatchLimit {
    fn input(&self) -> PlanRef {
        self.core.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        let mut core = self.core.clone();
        core.input = input;
        Self::new(core)
    }
}
impl_plan_tree_node_for_unary! {BatchLimit}
impl_distill_by_unit!(BatchLimit, core, "BatchLimit");

impl ToDistributedBatch for BatchLimit {
    fn to_distributed(&self) -> Result<PlanRef> {
        self.two_phase_limit(self.input().to_distributed()?)
    }
}

impl ToBatchPb for BatchLimit {
    fn to_batch_prost_body(&self) -> NodeBody {
        NodeBody::Limit(LimitNode {
            limit: self.core.limit,
            offset: self.core.offset,
        })
    }
}

impl ToLocalBatch for BatchLimit {
    fn to_local(&self) -> Result<PlanRef> {
        self.two_phase_limit(self.input().to_local()?)
    }
}

impl ExprRewritable for BatchLimit {}

impl ExprVisitable for BatchLimit {}

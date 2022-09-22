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
use risingwave_pb::batch_plan::HashAggNode;

use super::logical_agg::PlanAggCall;
use super::{LogicalAgg, PlanBase, PlanRef, PlanTreeNodeUnary, ToBatchProst, ToDistributedBatch};
use crate::optimizer::plan_node::ToLocalBatch;
use crate::optimizer::property::{Distribution, Order, RequiredDist};

#[derive(Debug, Clone)]
pub struct BatchHashAgg {
    pub base: PlanBase,
    logical: LogicalAgg,
}

impl BatchHashAgg {
    pub fn new(logical: LogicalAgg) -> Self {
        let ctx = logical.base.ctx.clone();
        let input = logical.input();
        let input_dist = input.distribution();
        let dist = match input_dist {
            Distribution::HashShard(_) => logical
                .i2o_col_mapping()
                .rewrite_provided_distribution(input_dist),
            d => d.clone(),
        };
        let base = PlanBase::new_batch(ctx, logical.schema().clone(), dist, Order::any());
        BatchHashAgg { base, logical }
    }

    pub fn agg_calls(&self) -> &[PlanAggCall] {
        self.logical.agg_calls()
    }

    pub fn group_key(&self) -> &[usize] {
        self.logical.group_key()
    }
}

impl fmt::Display for BatchHashAgg {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.logical.fmt_with_name(f, "BatchHashAgg")
    }
}

impl PlanTreeNodeUnary for BatchHashAgg {
    fn input(&self) -> PlanRef {
        self.logical.input()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(self.logical.clone_with_input(input))
    }
}
impl_plan_tree_node_for_unary! { BatchHashAgg }
impl ToDistributedBatch for BatchHashAgg {
    fn to_distributed(&self) -> Result<PlanRef> {
        let new_input = self.input().to_distributed_with_required(
            &Order::any(),
            &RequiredDist::shard_by_key(self.input().schema().len(), self.group_key()),
        )?;
        Ok(self.clone_with_input(new_input).into())
    }
}

impl ToBatchProst for BatchHashAgg {
    fn to_batch_prost_body(&self) -> NodeBody {
        NodeBody::HashAgg(HashAggNode {
            agg_calls: self
                .agg_calls()
                .iter()
                .map(PlanAggCall::to_protobuf)
                .collect(),
            group_key: self
                .group_key()
                .iter()
                .clone()
                .map(|index| *index as u32)
                .collect(),
        })
    }
}

impl ToLocalBatch for BatchHashAgg {
    fn to_local(&self) -> Result<PlanRef> {
        let new_input = self.input().to_local()?;

        let new_input =
            RequiredDist::single().enforce_if_not_satisfies(new_input, &Order::any())?;

        Ok(self.clone_with_input(new_input).into())
    }
}

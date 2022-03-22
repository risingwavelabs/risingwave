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
//
use std::fmt;

use itertools::Itertools;
use risingwave_common::catalog::Schema;

use super::logical_agg::PlanAggCall;
use super::{LogicalAgg, PlanBase, PlanRef, PlanTreeNodeUnary, ToBatchProst, ToDistributedBatch};
use crate::expr::InputRefDisplay;
use crate::optimizer::property::{Distribution, Order, WithSchema};

#[derive(Debug, Clone)]
pub struct BatchHashAgg {
    pub base: PlanBase,
    logical: LogicalAgg,
}

impl BatchHashAgg {
    pub fn new(logical: LogicalAgg) -> Self {
        let ctx = logical.base.ctx.clone();
        let base = PlanBase::new_batch(
            ctx,
            logical.schema().clone(),
            Distribution::any().clone(),
            Order::any().clone(),
        );
        BatchHashAgg { logical, base }
    }
    pub fn agg_calls(&self) -> &[PlanAggCall] {
        self.logical.agg_calls()
    }
    pub fn group_keys(&self) -> &[usize] {
        self.logical.group_keys()
    }
}

impl fmt::Display for BatchHashAgg {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("BatchHashAgg")
            .field(
                "group_keys",
                &self
                    .group_keys()
                    .iter()
                    .copied()
                    .map(InputRefDisplay)
                    .collect_vec(),
            )
            .field("aggs", &self.agg_calls())
            .finish()
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

impl WithSchema for BatchHashAgg {
    fn schema(&self) -> &Schema {
        self.logical.schema()
    }
}

impl ToDistributedBatch for BatchHashAgg {
    fn to_distributed(&self) -> PlanRef {
        let new_input = self.input().to_distributed_with_required(
            self.input_order_required(),
            &Distribution::HashShard(self.group_keys().to_vec()),
        );
        self.clone_with_input(new_input).into()
    }
}

impl ToBatchProst for BatchHashAgg {}

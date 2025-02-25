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

use risingwave_pb::batch_plan::UnionNode;
use risingwave_pb::batch_plan::plan_node::NodeBody;

use super::batch::prelude::*;
use super::utils::impl_distill_by_unit;
use super::{ExprRewritable, PlanRef, ToBatchPb, ToDistributedBatch, generic};
use crate::error::Result;
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::{PlanBase, PlanTreeNode, ToLocalBatch};
use crate::optimizer::property::{Distribution, Order, RequiredDist};

/// `BatchUnion` implements [`super::LogicalUnion`]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BatchUnion {
    pub base: PlanBase<Batch>,
    core: generic::Union<PlanRef>,
}

impl BatchUnion {
    pub fn new(core: generic::Union<PlanRef>) -> Self {
        let dist = if core
            .inputs
            .iter()
            .all(|input| *input.distribution() == Distribution::Single)
        {
            Distribution::Single
        } else {
            Distribution::SomeShard
        };

        let base = PlanBase::new_batch_with_core(&core, dist, Order::any());
        BatchUnion { base, core }
    }
}

impl_distill_by_unit!(BatchUnion, core, "BatchUnion");

impl PlanTreeNode for BatchUnion {
    fn inputs(&self) -> smallvec::SmallVec<[crate::optimizer::PlanRef; 2]> {
        smallvec::SmallVec::from_vec(self.core.inputs.clone())
    }

    fn clone_with_inputs(&self, inputs: &[crate::optimizer::PlanRef]) -> PlanRef {
        // For batch query, we don't need to clone `source_col`, so just use new.
        let mut new = self.core.clone();
        new.inputs = inputs.to_vec();
        Self::new(new).into()
    }
}

impl ToDistributedBatch for BatchUnion {
    fn to_distributed(&self) -> Result<PlanRef> {
        // TODO: use round robin distribution to improve it
        let new_inputs: Result<Vec<_>> = self
            .inputs()
            .iter()
            .map(|input| {
                RequiredDist::single()
                    .enforce_if_not_satisfies(input.to_distributed()?, &Order::any())
            })
            .collect();
        Ok(self.clone_with_inputs(&new_inputs?))
    }
}

impl ToBatchPb for BatchUnion {
    fn to_batch_prost_body(&self) -> NodeBody {
        NodeBody::Union(UnionNode {})
    }
}

impl ToLocalBatch for BatchUnion {
    fn to_local(&self) -> Result<PlanRef> {
        let new_inputs: Result<Vec<_>> = self
            .inputs()
            .iter()
            .map(|input| {
                RequiredDist::single().enforce_if_not_satisfies(input.to_local()?, &Order::any())
            })
            .collect();
        Ok(self.clone_with_inputs(&new_inputs?))
    }
}

impl ExprRewritable for BatchUnion {}

impl ExprVisitable for BatchUnion {}

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
use risingwave_pb::batch_plan::UnionNode;

use super::{PlanRef, ToBatchProst, ToDistributedBatch};
use crate::optimizer::plan_node::{LogicalUnion, PlanBase, PlanTreeNode, ToLocalBatch};
use crate::optimizer::property::{Distribution, Order, RequiredDist};

/// `BatchUnion` implements [`super::LogicalUnion`]
#[derive(Debug, Clone)]
pub struct BatchUnion {
    pub base: PlanBase,
    logical: LogicalUnion,
}

impl BatchUnion {
    pub fn new(logical: LogicalUnion) -> Self {
        let ctx = logical.base.ctx.clone();

        let dist = if logical
            .inputs()
            .iter()
            .all(|input| *input.distribution() == Distribution::Single)
        {
            Distribution::Single
        } else {
            Distribution::SomeShard
        };

        let base = PlanBase::new_batch(ctx, logical.schema().clone(), dist, Order::any());
        BatchUnion { base, logical }
    }
}

impl fmt::Display for BatchUnion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.logical.fmt_with_name(f, "BatchUnion")
    }
}

impl PlanTreeNode for BatchUnion {
    fn inputs(&self) -> smallvec::SmallVec<[crate::optimizer::PlanRef; 2]> {
        let mut vec = smallvec::SmallVec::new();
        vec.extend(self.logical.inputs().into_iter());
        vec
    }

    fn clone_with_inputs(&self, inputs: &[crate::optimizer::PlanRef]) -> PlanRef {
        Self::new(LogicalUnion::new(self.logical.all(), inputs.to_owned())).into()
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

impl ToBatchProst for BatchUnion {
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

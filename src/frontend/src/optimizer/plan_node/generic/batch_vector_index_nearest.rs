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

use crate::PlanRef;
use crate::optimizer::plan_node::{Batch, PlanBase, ToDistributedBatch};
use crate::optimizer::property::{Order, RequiredDist};

pub struct BatchVectorIndexNearest {
    plan_base: PlanBase<Batch>,
}

impl ToDistributedBatch for BatchVectorIndexNearest {
    fn to_distributed(&self) -> crate::error::Result<PlanRef> {
        self.to_distributed_with_required(&Order::any(), &RequiredDist::Any)
    }

    fn to_distributed_with_required(
        &self,
        required_order: &Order,
        required_dist: &RequiredDist,
    ) -> crate::error::Result<PlanRef> {
        todo!()
    }
}

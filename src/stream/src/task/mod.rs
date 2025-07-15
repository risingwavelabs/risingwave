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

use crate::executor::exchange::permit::{Receiver, Sender};

mod barrier_manager;
mod env;
mod stream_manager;

pub use barrier_manager::*;
pub use env::*;
pub use stream_manager::*;

pub type ConsumableChannelPair = (Option<Sender>, Option<Receiver>);
pub type ActorId = u32;
pub type FragmentId = u32;
pub type DispatcherId = u64;
/// (`upstream_actor_id`, `downstream_actor_id`)
pub type UpDownActorIds = (ActorId, ActorId);
pub type UpDownFragmentIds = (FragmentId, FragmentId);

#[derive(Hash, Eq, PartialEq, Copy, Clone, Debug)]
pub(crate) struct PartialGraphId(u32);

#[cfg(test)]
pub(crate) const TEST_DATABASE_ID: risingwave_common::catalog::DatabaseId =
    risingwave_common::catalog::DatabaseId::new(u32::MAX);

#[cfg(test)]
pub(crate) const TEST_PARTIAL_GRAPH_ID: PartialGraphId = PartialGraphId(u32::MAX);

impl PartialGraphId {
    fn new(id: u32) -> Self {
        Self(id)
    }
}

impl From<PartialGraphId> for u32 {
    fn from(val: PartialGraphId) -> u32 {
        val.0
    }
}

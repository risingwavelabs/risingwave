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

use risingwave_common::config::StreamingConfig;
use risingwave_common::util::addr::HostAddr;
use risingwave_rpc_client::ComputeClientPoolRef;

use crate::executor::exchange::permit::{Receiver, Sender};

mod barrier_manager;
mod env;
mod stream_manager;

pub use barrier_manager::*;
pub use env::*;
use risingwave_common::catalog::DatabaseId;
pub use stream_manager::*;

use crate::executor::exchange::permit;

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

/// Stores the information which may be modified from the data plane.
///
/// The data structure is created in `LocalBarrierWorker` and is shared by actors created
/// between two recoveries. In every recovery, the `LocalBarrierWorker` will create a new instance of
/// `SharedContext`, and the original one becomes stale. The new one is shared by actors created after
/// recovery.
pub struct SharedContext {
    pub(crate) database_id: DatabaseId,
    term_id: String,

    /// Stores the local address.
    ///
    /// It is used to test whether an actor is local or not,
    /// thus determining whether we should setup local channel only or remote rpc connection
    /// between two actors/actors.
    pub(crate) addr: HostAddr,

    /// Compute client pool for streaming gRPC exchange.
    // TODO: currently the client pool won't be cleared. Should remove compute clients when
    // disconnected.
    pub(crate) compute_client_pool: ComputeClientPoolRef,

    pub(crate) config: StreamingConfig,
}

impl std::fmt::Debug for SharedContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedContext")
            .field("addr", &self.addr)
            .finish_non_exhaustive()
    }
}

impl SharedContext {
    pub fn new(database_id: DatabaseId, env: &StreamEnvironment, term_id: String) -> Self {
        Self {
            database_id,
            term_id,
            addr: env.server_address().clone(),
            config: env.config().as_ref().to_owned(),
            compute_client_pool: env.client_pool(),
        }
    }

    pub fn term_id(&self) -> String {
        self.term_id.clone()
    }

    #[cfg(test)]
    pub fn for_test() -> Self {
        use std::sync::Arc;

        use risingwave_common::config::StreamingDeveloperConfig;
        use risingwave_rpc_client::ComputeClientPool;

        Self {
            database_id: TEST_DATABASE_ID,
            term_id: "for_test".into(),
            addr: LOCAL_TEST_ADDR.clone(),
            config: StreamingConfig {
                developer: StreamingDeveloperConfig {
                    exchange_initial_permits: permit::for_test::INITIAL_PERMITS,
                    exchange_batched_permits: permit::for_test::BATCHED_PERMITS,
                    exchange_concurrent_barriers: permit::for_test::CONCURRENT_BARRIERS,
                    ..Default::default()
                },
                ..Default::default()
            },
            compute_client_pool: Arc::new(ComputeClientPool::for_test()),
        }
    }

    /// Create a new channel pair.
    pub fn new_channel(&self) -> (Sender, Receiver) {
        permit::channel(
            self.config.developer.exchange_initial_permits,
            self.config.developer.exchange_batched_permits,
            self.config.developer.exchange_concurrent_barriers,
        )
    }

    pub fn config(&self) -> &StreamingConfig {
        &self.config
    }
}

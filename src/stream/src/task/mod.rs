// Copyright 2024 RisingWave Labs
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

use std::collections::{HashMap, HashSet};

use anyhow::anyhow;
use parking_lot::{MappedMutexGuard, Mutex, MutexGuard, RwLock};
use risingwave_common::config::StreamingConfig;
use risingwave_common::util::addr::HostAddr;
use risingwave_pb::common::ActorInfo;
use risingwave_rpc_client::ComputeClientPoolRef;

use crate::error::StreamResult;
use crate::executor::exchange::permit::{self, Receiver, Sender};

mod barrier_manager;
mod env;
mod stream_manager;

pub use barrier_manager::*;
pub use env::*;
use risingwave_common::catalog::DatabaseId;
pub use stream_manager::*;

pub type ConsumableChannelPair = (Option<Sender>, Option<Receiver>);
pub type ActorId = u32;
pub type FragmentId = u32;
pub type DispatcherId = u64;
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

    /// Stores the senders and receivers for later `Processor`'s usage.
    ///
    /// Each actor has several senders and several receivers. Senders and receivers are created
    /// during `update_actors` and stored in a channel map. Upon `build_actors`, all these channels
    /// will be taken out and built into the executors and outputs.
    /// One sender or one receiver can be uniquely determined by the upstream and downstream actor
    /// id.
    ///
    /// There are three cases when we need local channels to pass around messages:
    /// 1. pass `Message` between two local actors
    /// 2. The RPC client at the downstream actor forwards received `Message` to one channel in
    ///    `ReceiverExecutor` or `MergerExecutor`.
    /// 3. The RPC `Output` at the upstream actor forwards received `Message` to
    ///    `ExchangeServiceImpl`.
    ///
    /// The channel serves as a buffer because `ExchangeServiceImpl`
    /// is on the server-side and we will also introduce backpressure.
    pub(crate) channel_map: Mutex<HashMap<UpDownActorIds, ConsumableChannelPair>>,

    /// Stores all actor information.
    pub(crate) actor_infos: RwLock<HashMap<ActorId, ActorInfo>>,

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
    pub fn new(database_id: DatabaseId, env: &StreamEnvironment) -> Self {
        Self {
            database_id,
            channel_map: Default::default(),
            actor_infos: Default::default(),
            addr: env.server_address().clone(),
            config: env.config().as_ref().to_owned(),
            compute_client_pool: env.client_pool(),
        }
    }

    #[cfg(test)]
    pub fn for_test() -> Self {
        use std::sync::Arc;

        use risingwave_common::config::StreamingDeveloperConfig;
        use risingwave_rpc_client::ComputeClientPool;

        Self {
            database_id: TEST_DATABASE_ID,
            channel_map: Default::default(),
            actor_infos: Default::default(),
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

    /// Get the channel pair for the given actor ids. If the channel pair does not exist, create one
    /// with the configured permits.
    fn get_or_insert_channels(
        &self,
        ids: UpDownActorIds,
    ) -> MappedMutexGuard<'_, ConsumableChannelPair> {
        MutexGuard::map(self.channel_map.lock(), |map| {
            map.entry(ids).or_insert_with(|| {
                let (tx, rx) = permit::channel(
                    self.config.developer.exchange_initial_permits,
                    self.config.developer.exchange_batched_permits,
                    self.config.developer.exchange_concurrent_barriers,
                );
                (Some(tx), Some(rx))
            })
        })
    }

    pub fn take_sender(&self, ids: &UpDownActorIds) -> StreamResult<Sender> {
        self.get_or_insert_channels(*ids)
            .0
            .take()
            .ok_or_else(|| anyhow!("sender for {ids:?} has already been taken").into())
    }

    pub fn take_receiver(&self, ids: UpDownActorIds) -> StreamResult<Receiver> {
        self.get_or_insert_channels(ids)
            .1
            .take()
            .ok_or_else(|| anyhow!("receiver for {ids:?} has already been taken").into())
    }

    pub fn get_actor_info(&self, actor_id: &ActorId) -> StreamResult<ActorInfo> {
        self.actor_infos
            .read()
            .get(actor_id)
            .cloned()
            .ok_or_else(|| anyhow!("actor {} not found in info table", actor_id).into())
    }

    pub fn config(&self) -> &StreamingConfig {
        &self.config
    }

    pub(super) fn drop_actors(&self, actors: &HashSet<ActorId>) {
        self.channel_map
            .lock()
            .retain(|(up_id, _), _| !actors.contains(up_id));
        let mut actor_infos = self.actor_infos.write();
        for actor_id in actors {
            actor_infos.remove(actor_id);
        }
    }
}

/// Generate a globally unique executor id.
pub fn unique_executor_id(actor_id: u32, operator_id: u64) -> u64 {
    assert!(operator_id <= u32::MAX as u64);
    ((actor_id as u64) << 32) + operator_id
}

/// Generate a globally unique operator id.
pub fn unique_operator_id(fragment_id: u32, operator_id: u64) -> u64 {
    assert!(operator_id <= u32::MAX as u64);
    ((fragment_id as u64) << 32) + operator_id
}

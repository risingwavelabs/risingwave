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

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::anyhow;
use parking_lot::{Mutex, MutexGuard, RwLock};
use risingwave_common::config::StreamingConfig;
use risingwave_common::util::addr::HostAddr;
use risingwave_pb::common::ActorInfo;
use risingwave_rpc_client::ComputeClientPool;
use tokio::sync::mpsc::{Receiver, Sender};

use crate::error::StreamResult;
use crate::executor::Message;

mod barrier_manager;
mod env;
mod lru_manager;
mod stream_manager;

pub use barrier_manager::*;
pub use env::*;
pub use lru_manager::*;
use risingwave_storage::StateStoreImpl;
pub use stream_manager::*;

/// Default capacity of channel if two actors are on the same node
pub const LOCAL_OUTPUT_CHANNEL_SIZE: usize = 16;

pub type ConsumableChannelPair = (Option<Sender<Message>>, Option<Receiver<Message>>);
pub type ActorId = u32;
pub type FragmentId = u32;
pub type DispatcherId = u64;
pub type UpDownActorIds = (ActorId, ActorId);
pub type UpDownFragmentIds = (FragmentId, FragmentId);

/// Stores the information which may be modified from the data plane.
pub struct SharedContext {
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
    /// `ReceiverExecutor` or `MergerExecutor`.
    /// 3. The RPC `Output` at the upstream actor forwards received `Message` to
    /// `ExchangeServiceImpl`.
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

    /// The pool of compute clients.
    // TODO: currently the client pool won't be cleared. Should remove compute clients when
    // disconnected.
    pub(crate) compute_client_pool: ComputeClientPool,

    pub(crate) barrier_manager: Arc<Mutex<LocalBarrierManager>>,

    pub(crate) lru_manager: Option<LruManagerRef>,
}

impl std::fmt::Debug for SharedContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedContext")
            .field("addr", &self.addr)
            .finish_non_exhaustive()
    }
}

impl SharedContext {
    pub fn new(addr: HostAddr, state_store: StateStoreImpl,config: &StreamingConfig, enable_managed_cache: bool) -> Self {
        let create_lru_manager = || {
            LruManager::new(
                config.total_memory_available_bytes,
                config.checkpoint_interval_ms,
            )
        };
        Self {
            channel_map: Default::default(),
            actor_infos: Default::default(),
            addr,
            compute_client_pool: ComputeClientPool::default(),
            lru_manager: enable_managed_cache.then(create_lru_manager),
            barrier_manager: Arc::new(Mutex::new(LocalBarrierManager::new(state_store))),
        }
    }

    #[cfg(test)]
    pub fn for_test() -> Self {
        Self {
            channel_map: Default::default(),
            actor_infos: Default::default(),
            addr: LOCAL_TEST_ADDR.clone(),
            compute_client_pool: ComputeClientPool::default(),
            barrier_manager: Arc::new(Mutex::new(LocalBarrierManager::new(StateStoreImpl::for_test()))),
            lru_manager: Some(LruManager::for_test()),
        }
    }

    #[inline]
    fn lock_channel_map(&self) -> MutexGuard<HashMap<UpDownActorIds, ConsumableChannelPair>> {
        self.channel_map.lock()
    }

    pub fn lock_barrier_manager(&self) -> MutexGuard<LocalBarrierManager> {
        self.barrier_manager.lock()
    }

    #[inline]
    pub fn take_sender(&self, ids: &UpDownActorIds) -> StreamResult<Sender<Message>> {
        self.lock_channel_map()
            .get_mut(ids)
            .ok_or_else(|| anyhow!("channel between {} and {} does not exist", ids.0, ids.1))?
            .0
            .take()
            .ok_or_else(|| anyhow!("sender from {} to {} does no exist", ids.0, ids.1).into())
    }

    #[inline]
    pub fn take_receiver(&self, ids: &UpDownActorIds) -> StreamResult<Receiver<Message>> {
        self.lock_channel_map()
            .get_mut(ids)
            .ok_or_else(|| anyhow!("channel between {} and {} does not exist", ids.0, ids.1))?
            .1
            .take()
            .ok_or_else(|| anyhow!("receiver from {} to {} does not exist", ids.0, ids.1).into())
    }

    #[inline]
    pub fn add_channel_pairs(&self, ids: UpDownActorIds, channels: ConsumableChannelPair) {
        assert!(
            self.lock_channel_map().insert(ids, channels).is_none(),
            "channel already exists: {:?}",
            ids
        );
    }

    pub fn retain_channel<F>(&self, mut f: F)
    where
        F: FnMut(&(u32, u32)) -> bool,
    {
        self.lock_channel_map()
            .retain(|up_down_ids, _| f(up_down_ids));
    }

    pub fn get_actor_info(&self, actor_id: &ActorId) -> StreamResult<ActorInfo> {
        self.actor_infos
            .read()
            .get(actor_id)
            .cloned()
            .ok_or_else(|| anyhow!("actor {} not found in info table", actor_id).into())
    }
}

/// Generate a globally unique executor id. Useful when constructing per-actor keyspace
pub fn unique_executor_id(actor_id: u32, operator_id: u64) -> u64 {
    assert!(operator_id <= u32::MAX as u64);
    ((actor_id as u64) << 32) + operator_id
}

/// Generate a globally unique operator id. Useful when constructing per-fragment keyspace.
pub fn unique_operator_id(fragment_id: u32, operator_id: u64) -> u64 {
    assert!(operator_id <= u32::MAX as u64);
    ((fragment_id as u64) << 32) + operator_id
}

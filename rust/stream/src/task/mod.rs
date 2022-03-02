use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Mutex, MutexGuard};

use futures::channel::mpsc::{Receiver, Sender};
use risingwave_common::error::{ErrorCode, Result, RwError};
use risingwave_storage::StateStoreImpl;

use crate::executor::Message;

mod barrier_manager;
mod env;
mod stream_manager;
pub use barrier_manager::*;
pub use env::*;
pub use stream_manager::*;
#[cfg(test)]
mod tests;

/// Default capacity of channel if two actors are on the same node
pub const LOCAL_OUTPUT_CHANNEL_SIZE: usize = 16;

pub type ConsumableChannelPair = (Option<Sender<Message>>, Option<Receiver<Message>>);
pub type ConsumableChannelVecPair = (Vec<Sender<Message>>, Vec<Receiver<Message>>);
pub type UpDownActorIds = (u32, u32);

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

    /// Stores the local address.
    ///
    /// It is used to test whether an actor is local or not,
    /// thus determining whether we should setup local channel only or remote rpc connection
    /// between two actors/actors.
    pub(crate) addr: SocketAddr,

    pub(crate) barrier_manager: Mutex<LocalBarrierManager>,

    pub(crate) state_store: StateStoreImpl,
}

impl SharedContext {
    pub fn new(addr: SocketAddr, state_store: StateStoreImpl) -> Self {
        Self {
            channel_map: Mutex::new(HashMap::new()),
            addr,
            barrier_manager: Mutex::new(LocalBarrierManager::new()),
            state_store,
        }
    }

    #[cfg(test)]
    pub fn for_test() -> Self {
        Self {
            channel_map: Mutex::new(HashMap::new()),
            addr: *LOCAL_TEST_ADDR,
            barrier_manager: Mutex::new(LocalBarrierManager::for_test()),
            state_store: StateStoreImpl::shared_in_memory_store(),
        }
    }

    #[inline]
    fn lock_channel_map(&self) -> MutexGuard<HashMap<UpDownActorIds, ConsumableChannelPair>> {
        self.channel_map.lock().unwrap()
    }

    pub fn lock_barrier_manager(&self) -> MutexGuard<LocalBarrierManager> {
        self.barrier_manager.lock().unwrap()
    }

    #[inline]
    pub fn take_sender(&self, ids: &UpDownActorIds) -> Result<Sender<Message>> {
        self.lock_channel_map()
            .get_mut(ids)
            .ok_or_else(|| {
                RwError::from(ErrorCode::InternalError(format!(
                    "channel between {} and {} does not exist",
                    ids.0, ids.1
                )))
            })?
            .0
            .take()
            .ok_or_else(|| {
                RwError::from(ErrorCode::InternalError(format!(
                    "sender from {} to {} does no exist",
                    ids.0, ids.1
                )))
            })
    }

    #[inline]
    pub fn take_receiver(&self, ids: &UpDownActorIds) -> Result<Receiver<Message>> {
        self.lock_channel_map()
            .get_mut(ids)
            .ok_or_else(|| {
                RwError::from(ErrorCode::InternalError(format!(
                    "channel between {} and {} does not exist",
                    ids.0, ids.1
                )))
            })?
            .1
            .take()
            .ok_or_else(|| {
                RwError::from(ErrorCode::InternalError(format!(
                    "receiver from {} to {} does no exist",
                    ids.0, ids.1
                )))
            })
    }

    #[inline]
    pub fn add_channel_pairs(&self, ids: UpDownActorIds, channels: ConsumableChannelPair) {
        self.lock_channel_map().insert(ids, channels);
    }

    pub fn retain<F>(&self, mut f: F)
    where
        F: FnMut(&(u32, u32)) -> bool,
    {
        self.lock_channel_map()
            .retain(|up_down_ids, _| f(up_down_ids));
    }

    #[cfg(test)]
    pub fn get_channel_pair_number(&self) -> u32 {
        self.lock_channel_map().len() as u32
    }
}

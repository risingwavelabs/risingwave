use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Mutex, MutexGuard};

use futures::channel::mpsc::Receiver;

use crate::executor::Message;

mod barrier_manager;
mod env;
mod stream_manager;
pub use barrier_manager::*;
pub use env::*;
pub use stream_manager::*;
#[cfg(test)]
mod tests;

#[cfg(test)]
mod test_mv;

/// Stores the data which may be modified from the data plane.
pub struct SharedContext {
    /// Stores the senders and receivers for later `Processor`'s use.
    ///
    /// Each actor has several senders and several receivers. Senders and receivers are created
    /// during `update_actors`. Upon `build_actorss`, all these channels will be taken out and
    /// built into the executors and outputs.
    /// One sender or one receiver can be uniquely determined by the upstream and downstream actor
    /// id.
    ///
    /// There are three cases that we need local channels to pass around messages:
    /// 1. pass `Message` between two local actors
    /// 2. The RPC client at the downstream actor forwards received `Message` to one channel in
    /// `ReceiverExecutor` or `MergerExecutor`.
    /// 3. The RPC `Output` at the upstream actor forwards received `Message` to
    /// `ExchangeServiceImpl`.        The channel servers as a buffer because `ExchangeServiceImpl`
    /// is on the server-side and we will        also introduce backpressure.
    pub(crate) channel_pool: Mutex<HashMap<UpDownActorIds, ConsumableChannelPair>>,

    /// The receiver is on the other side of rpc `Output`. The `ExchangeServiceImpl` take it
    /// when it receives request for streaming data from downstream clients.
    pub(crate) receivers_for_exchange_service: Mutex<HashMap<UpDownActorIds, Receiver<Message>>>,

    /// Stores the local address
    ///
    /// It is used to test whether an actor is local or not,
    /// thus determining whether we should setup channel or rpc connection between
    /// two actors/actors.
    pub(crate) addr: SocketAddr,

    pub(crate) barrier_manager: Mutex<LocalBarrierManager>,
}

impl SharedContext {
    pub fn new(addr: SocketAddr) -> Self {
        Self {
            channel_pool: Mutex::new(HashMap::new()),
            receivers_for_exchange_service: Mutex::new(HashMap::new()),
            addr,
            barrier_manager: Mutex::new(LocalBarrierManager::new()),
        }
    }

    #[cfg(test)]
    pub fn for_test() -> Self {
        Self {
            channel_pool: Mutex::new(HashMap::new()),
            receivers_for_exchange_service: Mutex::new(HashMap::new()),
            addr: *LOCAL_TEST_ADDR,
            barrier_manager: Mutex::new(LocalBarrierManager::for_test()),
        }
    }

    #[inline]
    pub fn lock_channel_pool(&self) -> MutexGuard<HashMap<UpDownActorIds, ConsumableChannelPair>> {
        self.channel_pool.lock().unwrap()
    }

    pub fn lock_barrier_manager(&self) -> MutexGuard<LocalBarrierManager> {
        self.barrier_manager.lock().unwrap()
    }

    #[inline]
    pub fn lock_receivers_for_exchange_service(
        &self,
    ) -> MutexGuard<HashMap<UpDownActorIds, Receiver<Message>>> {
        self.receivers_for_exchange_service.lock().unwrap()
    }
}

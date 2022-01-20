use std::collections::HashMap;

use itertools::Itertools;
use risingwave_common::error::Result;
use tokio::sync::mpsc::UnboundedSender;

use crate::executor::*;

/// [`LocalBarrierManager`] manages barrier control flow, used by local stream manager.
pub struct LocalBarrierManager {
    /// Stores all materialized view source sender.
    senders: HashMap<u32, UnboundedSender<Message>>,

    /// Span of the current epoch
    span: Option<tracing::Span>,
}

impl Default for LocalBarrierManager {
    fn default() -> Self {
        Self::new()
    }
}

impl LocalBarrierManager {
    pub fn new() -> Self {
        Self {
            senders: HashMap::new(),
            span: None,
        }
    }
    /// register sender for materialized view, used to send barriers.
    pub fn register_sender(&mut self, actor_id: u32, sender: UnboundedSender<Message>) {
        debug!("register sender: {}", actor_id);
        self.senders.insert(actor_id, sender);
    }

    /// broadcast a barrier to all senders with specific epoch.
    /// TODO: async collect barrier flush state from hummock.
    pub fn send_barrier(&mut self, barrier: &Barrier) -> Result<()> {
        let mut barrier = barrier.clone();

        if ENABLE_BARRIER_EVENT {
            let receiver_ids = self.senders.keys().cloned().join(", ");
            // TODO: not a correct usage of span -- the span ends once it goes out of scope, but we
            // still have events in the background.
            let span = tracing::info_span!("send_barrier", epoch = barrier.epoch, mutation = ?barrier.mutation, receivers = %receiver_ids);
            barrier.span = Some(span);
        }

        for sender in self.senders.values() {
            sender.send(Message::Barrier(barrier.clone())).unwrap();
        }

        if let Some(Mutation::Stop(actors)) = barrier.mutation.as_deref() {
            for actor in actors {
                self.senders.remove(actor);
            }
        }

        Ok(())
    }
}

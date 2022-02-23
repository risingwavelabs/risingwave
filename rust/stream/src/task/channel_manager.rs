use std::sync::Arc;

use futures::channel::mpsc::channel;

use crate::task::{SharedContext, LOCAL_OUTPUT_CHANNEL_SIZE};
pub struct ChannelManager {
    actor_id: u32,
    context: Arc<SharedContext>,
}

impl ChannelManager {
    pub fn new(actor_id: u32, context: Arc<SharedContext>) -> Self {
        Self { actor_id, context }
    }

    pub fn update_upstreams(&self, ids: &[u32]) {
        ids.iter()
            .map(|id| {
                let (tx, rx) = channel(LOCAL_OUTPUT_CHANNEL_SIZE);
                self.context
                    .add_channel_pairs((self.actor_id, *id), (Some(tx), Some(rx)));
            })
            .count();
    }
}

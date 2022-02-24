use std::sync::Arc;

use futures::channel::mpsc::channel;

use crate::task::{SharedContext, UpDownActorIds, LOCAL_OUTPUT_CHANNEL_SIZE};

pub fn update_upstreams(context: Arc<SharedContext>, ids: &[UpDownActorIds]) {
    ids.iter()
        .map(|id| {
            let (tx, rx) = channel(LOCAL_OUTPUT_CHANNEL_SIZE);
            context.add_channel_pairs(*id, (Some(tx), Some(rx)));
        })
        .count();
}

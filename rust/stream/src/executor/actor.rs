use risingwave_common::error::Result;
use tracing_futures::Instrument;

use super::StreamConsumer;

/// `Actor` is the basic execution unit in the streaming framework.
pub struct Actor {
    consumer: Box<dyn StreamConsumer>,
    /// Fragment Id
    fragment_id: u32,
}

impl Actor {
    pub fn new(consumer: Box<dyn StreamConsumer>, fragment_id: u32) -> Self {
        Self {
            consumer,
            fragment_id,
        }
    }

    pub async fn run(mut self) -> Result<()> {
        // Drive the streaming task with an infinite loop
        loop {
            let message = self
                .consumer
                .next()
                .instrument(tracing::trace_span!(
                    "actor_poll",
                    next = "Dispatcher",
                    // For the upstream trace pipe, its output is our input.
                    fragment_id = self.fragment_id,
                ))
                .await;
            match message {
                Ok(has_next) => {
                    if !has_next {
                        break;
                    }
                }
                Err(err) => {
                    warn!("Actor polling failed: {:?}", err);
                    return Err(err);
                }
            }
        }
        Ok(())
    }
}

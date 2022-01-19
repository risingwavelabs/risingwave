use risingwave_common::error::Result;
use tracing_futures::Instrument;

use super::{Mutation, StreamConsumer};

/// `Actor` is the basic execution unit in the streaming framework.
pub struct Actor {
    consumer: Box<dyn StreamConsumer>,
    id: u32,
}

impl Actor {
    pub fn new(consumer: Box<dyn StreamConsumer>, id: u32) -> Self {
        Self { consumer, id }
    }

    pub async fn run(mut self) -> Result<()> {
        let span_name = format!("actor_poll_{:03}", self.id);
        let mut span = tracing::trace_span!(
            "actor_poll",
            otel.name = span_name.as_str(),
            // For the upstream trace pipe, its output is our input.
            actor_id = self.id,
            next = "Outbound",
            epoch = -1
        );
        // Drive the streaming task with an infinite loop
        loop {
            let message = self.consumer.next().instrument(span.clone()).await;
            match message {
                Ok(Some(barrier)) => {
                    if let Some(Mutation::Stop(actors)) = barrier.mutation.as_deref() {
                        if actors.contains(&self.id) {
                            debug!("actor exit: {}", self.id);
                            break;
                        }
                    }
                    if let Some(ref span_parent) = barrier.span {
                        span = tracing::trace_span!(
                            parent: span_parent,
                            "actor_poll",
                            otel.name = span_name.as_str(),
                            // For the upstream trace pipe, its output is our input.
                            actor_id = self.id,
                            next = "Outbound",
                            epoch = barrier.epoch,
                        );
                    } else {
                        span = tracing::trace_span!(
                            "actor_poll",
                            otel.name = span_name.as_str(),
                            // For the upstream trace pipe, its output is our input.
                            actor_id = self.id,
                            next = "Outbound",
                            epoch = barrier.epoch,
                        );
                    }
                }
                Ok(None) => {
                    continue;
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

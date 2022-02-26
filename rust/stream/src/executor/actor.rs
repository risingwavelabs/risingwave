use std::sync::Arc;

use risingwave_common::error::Result;
use tracing_futures::Instrument;

use super::{Mutation, StreamConsumer};
use crate::task::SharedContext;

/// `Actor` is the basic execution unit in the streaming framework.
pub struct Actor {
    consumer: Box<dyn StreamConsumer>,

    id: u32,

    context: Arc<SharedContext>,
}

impl Actor {
    pub fn new(consumer: Box<dyn StreamConsumer>, id: u32, context: Arc<SharedContext>) -> Self {
        Self {
            consumer,
            id,
            context,
        }
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
                    // collect barriers to local barrier manager
                    self.context
                        .lock_barrier_manager()
                        .collect(self.id, &barrier)?;

                    // then stop this actor if asked
                    if let Some(Mutation::Stop(actors)) = barrier.mutation.as_deref() {
                        if actors.contains(&self.id) {
                            debug!("actor exit: {}", self.id);
                            break;
                        }
                    }

                    // tracing related work
                    let span_parent = barrier.span;
                    if !span_parent.is_none() {
                        span = tracing::trace_span!(
                            parent: span_parent,
                            "actor_poll",
                            otel.name = span_name.as_str(),
                            // For the upstream trace pipe, its output is our input.
                            actor_id = self.id,
                            next = "Outbound",
                            epoch = barrier.epoch.curr,
                        );
                    } else {
                        span = tracing::trace_span!(
                            "actor_poll",
                            otel.name = span_name.as_str(),
                            // For the upstream trace pipe, its output is our input.
                            actor_id = self.id,
                            next = "Outbound",
                            epoch = barrier.epoch.curr,
                        );
                    }
                }
                Ok(None) => {}
                Err(err) => {
                    warn!("Actor polling failed: {:?}", err);
                    return Err(err);
                }
            }
        }
        Ok(())
    }
}

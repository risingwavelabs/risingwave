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

use std::sync::Arc;

use futures::pin_mut;
use risingwave_common::error::Result;
use tokio_stream::StreamExt;
use tracing_futures::Instrument;

use super::StreamConsumer;
use crate::task::{ActorId, SharedContext};

/// `Actor` is the basic execution unit in the streaming framework.
pub struct Actor<C> {
    consumer: C,

    id: ActorId,

    context: Arc<SharedContext>,
}

impl<C> Actor<C>
where
    C: StreamConsumer,
{
    pub fn new(consumer: C, id: ActorId, context: Arc<SharedContext>) -> Self {
        Self {
            consumer,
            id,
            context,
        }
    }

    pub async fn run(self) -> Result<()> {
        let span_name = format!("actor_poll_{:03}", self.id);
        let mut span = tracing::trace_span!(
            "actor_poll",
            otel.name = span_name.as_str(),
            // For the upstream trace pipe, its output is our input.
            actor_id = self.id,
            next = "Outbound",
            epoch = -1
        );

        let stream = Box::new(self.consumer).execute();
        pin_mut!(stream);

        // Drive the streaming task with an infinite loop
        while let Some(barrier) = stream.next().instrument(span).await.transpose()? {
            // Collect barriers to local barrier manager
            self.context
                .lock_barrier_manager()
                .collect(self.id, &barrier)?;

            // Then stop this actor if asked
            let to_stop = barrier.is_to_stop_actor(self.id);
            if to_stop {
                tracing::trace!(actor_id = self.id, "actor exit");
                return Ok(());
            }

            // Tracing related work
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

        tracing::error!(actor_id = self.id, "actor exit without stop barrier");

        Ok(())
    }
}

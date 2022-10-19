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

use async_stack_trace::{SpanValue, StackTrace};
use futures::future::join_all;
use futures::{pin_mut, FutureExt};
use minitrace::prelude::*;
use parking_lot::Mutex;
use risingwave_common::util::epoch::EpochPair;
use risingwave_expr::ExprError;
use tokio_stream::StreamExt;

use super::monitor::StreamingMetrics;
use super::subtask::SubtaskHandle;
use super::StreamConsumer;
use crate::error::StreamResult;
use crate::task::{ActorId, SharedContext};

/// Shared by all operators of an actor.
#[derive(Default)]
pub struct ActorContext {
    pub id: ActorId,

    // TODO: report errors and prompt the user.
    pub errors: Mutex<HashMap<String, Vec<ExprError>>>,

    pub mview_definition: String,
}

pub type ActorContextRef = Arc<ActorContext>;

impl ActorContext {
    pub fn new(id: ActorId, mview_definition: String) -> ActorContextRef {
        Arc::new(Self {
            id,
            mview_definition,
            ..Default::default()
        })
    }

    pub fn for_test(id: ActorId) -> ActorContextRef {
        Self::new(id, "".to_owned())
    }

    pub fn on_compute_error(&self, err: ExprError, identity: &str) {
        tracing::error!("Compute error: {}, executor: {identity}", err);
        self.errors
            .lock()
            .entry(identity.to_owned())
            .or_default()
            .push(err);
    }
}

/// `Actor` is the basic execution unit in the streaming framework.
pub struct Actor<C> {
    /// The [`StreamConsumer`] of the actor.
    consumer: C,
    /// The subtasks to execute concurrently.
    subtasks: Vec<SubtaskHandle>,

    context: Arc<SharedContext>,
    _metrics: Arc<StreamingMetrics>,
    actor_context: ActorContextRef,
}

impl<C> Actor<C>
where
    C: StreamConsumer,
{
    pub fn new(
        consumer: C,
        subtasks: Vec<SubtaskHandle>,
        context: Arc<SharedContext>,
        metrics: Arc<StreamingMetrics>,
        actor_context: ActorContextRef,
    ) -> Self {
        Self {
            consumer,
            subtasks,
            context,
            _metrics: metrics,
            actor_context,
        }
    }

    #[inline(always)]
    pub async fn run(mut self) -> StreamResult<()> {
        let context = self.actor_context.clone();

        let run = async move {
            tokio::join!(
                // Drive the subtasks concurrently.
                join_all(std::mem::take(&mut self.subtasks)),
                self.run_consumer(),
            )
            .1
        };

        if cfg!(debug_assertions) {
            std::panic::AssertUnwindSafe(run)
                .catch_unwind()
                .await
                .unwrap_or_else(|e| {
                    println!(
                        "*** unwinding panic {e:p} *** Actor {}: `{}`",
                        context.id, context.mview_definition
                    );
                    std::panic::resume_unwind(e)
                })
        } else {
            run.await
        }
    }

    async fn run_consumer(self) -> StreamResult<()> {
        let id = self.actor_context.id;

        let span_name = format!("actor_poll_{:03}", id);
        let mut span = {
            let mut span = Span::enter_with_local_parent("actor_poll");
            span.add_property(|| ("otel.name", span_name.to_string()));
            span.add_property(|| ("next", id.to_string()));
            span.add_property(|| ("next", "Outbound".to_string()));
            span.add_property(|| ("epoch", (-1).to_string()));
            span
        };

        let mut last_epoch: Option<EpochPair> = None;

        let stream = Box::new(self.consumer).execute();
        pin_mut!(stream);

        // Drive the streaming task with an infinite loop
        while let Some(barrier) = stream
            .next()
            .in_span(span)
            .stack_trace(last_epoch.map_or(SpanValue::Slice("Epoch <initial>"), |e| {
                format!("Epoch {}", e.curr).into()
            }))
            .await
            .transpose()?
        {
            last_epoch = Some(barrier.epoch);

            // Collect barriers to local barrier manager
            self.context.lock_barrier_manager().collect(id, &barrier)?;

            // Then stop this actor if asked
            let to_stop = barrier.is_stop_or_update_drop_actor(id);
            if to_stop {
                tracing::trace!(actor_id = id, "actor exit");
                return Ok(());
            }

            // Tracing related work
            span = {
                let mut span = Span::enter_with_local_parent("actor_poll");
                span.add_property(|| ("otel.name", span_name.to_string()));
                span.add_property(|| ("next", id.to_string()));
                span.add_property(|| ("next", "Outbound".to_string()));
                span.add_property(|| ("epoch", barrier.epoch.curr.to_string()));
                span
            };
        }

        tracing::error!(actor_id = id, "actor exit without stop barrier");

        Ok(())
    }
}

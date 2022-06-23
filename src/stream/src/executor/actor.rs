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

use std::collections::VecDeque;
use std::sync::Arc;

use futures::pin_mut;
use madsim::time::Instant;
use parking_lot::Mutex;
use risingwave_common::error::Result;
use risingwave_common::monitor::StreamingMetrics;
use tokio_stream::StreamExt;
use tracing_futures::Instrument;

use super::{Message, StreamConsumer};
use crate::task::{ActorId, SharedContext};

pub struct OperatorInfo {
    pub operator_id: u64,
    pub source_barrier_at: VecDeque<(u64, Instant)>,
    pub source_first_chunk_at: VecDeque<(u64, Instant)>,
}

impl OperatorInfo {
    pub fn new(operator_id: u64) -> Self {
        Self {
            operator_id,
            source_barrier_at: VecDeque::new(),
            source_first_chunk_at: VecDeque::new(),
        }
    }
}

pub struct OperatorInfoStatus {
    last_barrier_curr_epoch: Option<u64>,
    ctx: ActorContextRef,
    actor_context_position: usize,
}

impl OperatorInfoStatus {
    pub fn new(ctx: ActorContextRef, receiver_id: u64) -> Self {
        let actor_context_position = {
            let mut ctx = ctx.lock();
            let actor_context_position = ctx.info.len();
            ctx.info.push(OperatorInfo::new(receiver_id));
            actor_context_position
        };

        Self {
            last_barrier_curr_epoch: None,
            ctx,
            actor_context_position,
        }
    }

    pub fn next_message(&mut self, msg: &Message) {
        match msg {
            Message::Barrier(barrier) => {
                let mut ctx = self.ctx.lock();
                let info = &mut ctx.info[self.actor_context_position];
                info.source_barrier_at
                    .push_back((barrier.epoch.prev, Instant::now()));
                self.last_barrier_curr_epoch = Some(barrier.epoch.curr);
            }
            Message::Chunk(_) => {
                if let Some(epoch) = self.last_barrier_curr_epoch.take() {
                    let mut ctx = self.ctx.lock();
                    let info = &mut ctx.info[self.actor_context_position];
                    info.source_first_chunk_at
                        .push_back((epoch, Instant::now()))
                }
            }
        }
    }
}

/// Shared by all operators in the stream.
#[derive(Default)]
pub struct ActorContext {
    pub info: Vec<OperatorInfo>,
}

pub type ActorContextRef = Arc<Mutex<ActorContext>>;

impl ActorContext {
    pub fn create() -> ActorContextRef {
        Arc::new(Mutex::new(Self::default()))
    }
}

/// `Actor` is the basic execution unit in the streaming framework.
pub struct Actor<C> {
    consumer: C,
    id: ActorId,
    context: Arc<SharedContext>,
    metrics: Arc<StreamingMetrics>,
    actor_context: Arc<Mutex<ActorContext>>,
}

impl<C> Actor<C>
where
    C: StreamConsumer,
{
    pub fn new(
        consumer: C,
        id: ActorId,
        context: Arc<SharedContext>,
        metrics: Arc<StreamingMetrics>,
        actor_context: Arc<Mutex<ActorContext>>,
    ) -> Self {
        Self {
            consumer,
            id,
            context,
            metrics,
            actor_context,
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

        let actor_id_string = self.id.to_string();
        let operator_id_string = {
            let mut res = vec![];
            let ctx = self.actor_context.lock();
            for operator in &ctx.info {
                res.push(operator.operator_id.to_string());
            }
            res
        };

        let stream = Box::new(self.consumer).execute();
        pin_mut!(stream);

        // Drive the streaming task with an infinite loop
        while let Some(barrier) = stream.next().instrument(span).await.transpose()? {
            {
                // Calculate metrics
                let prev_epoch = barrier.epoch.prev;
                let mut ctx = self.actor_context.lock();
                for (idx, operator) in ctx.info.iter_mut().enumerate() {
                    let operator_id_string = &operator_id_string[idx];
                    if let Some(&(op_prev_epoch, _)) = operator.source_first_chunk_at.front() {
                        if op_prev_epoch <= prev_epoch {
                            let (op_prev_epoch, time) =
                                operator.source_first_chunk_at.pop_front().unwrap();
                            assert_eq!(op_prev_epoch, prev_epoch);
                            self.metrics
                                .actor_processing_time
                                .with_label_values(&[&actor_id_string, operator_id_string])
                                .set(time.elapsed().as_secs_f64());
                        }
                    }
                    let (op_prev_epoch, time) = operator.source_barrier_at.pop_front().unwrap();
                    assert_eq!(op_prev_epoch, prev_epoch);
                    self.metrics
                        .actor_barrier_time
                        .with_label_values(&[&actor_id_string, operator_id_string])
                        .set(time.elapsed().as_secs_f64());
                }
            }

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

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

// FIXME: This is a false-positive clippy test, remove this while bumping toolchain.
// https://github.com/tokio-rs/tokio/issues/4836
// https://github.com/rust-lang/rust-clippy/issues/8493
#![expect(clippy::declare_interior_mutable_const)]

use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::Instant;

use async_stack_trace::{SpanValue, StackTrace};
use futures::{pin_mut, Future};
use minitrace::prelude::*;
use parking_lot::Mutex;
use risingwave_common::error::Result;
use risingwave_expr::ExprError;
use tokio_stream::StreamExt;

use super::monitor::StreamingMetrics;
use super::{Message, StreamConsumer};
use crate::executor::Epoch;
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

/// Shared by all operators of an actor.
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

tokio::task_local! {
    /// TODO: report errors and prompt the user.
    static ACTOR_ERRORS: RefCell<HashMap<String,Vec<ExprError>>>;
}

pub fn on_compute_error(err: ExprError, identity: &str) {
    ACTOR_ERRORS.with(|errors| {
        log::error!("Compute error: {}, executor: {identity}", err);
        errors
            .borrow_mut()
            .entry(identity.to_owned())
            .or_default()
            .push(err);
    })
}

type TaskLocalFuture<T: Future> = impl Future<Output = T::Output>;

trait CollectErrors: Future + Sized {
    fn collect_errors(self) -> TaskLocalFuture<Self> {
        ACTOR_ERRORS.scope(RefCell::new(HashMap::new()), self)
    }
}

impl<F: Future + Sized> CollectErrors for F {}

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
        let mut span = {
            let mut span = Span::enter_with_local_parent("actor_poll");
            span.add_property(|| ("otel.name", span_name.to_string()));
            span.add_property(|| ("next", self.id.to_string()));
            span.add_property(|| ("next", "Outbound".to_string()));
            span.add_property(|| ("epoch", (-1).to_string()));
            span
        };

        let actor_id_string = self.id.to_string();
        let operator_id_string = {
            let mut res = vec![];
            let ctx = self.actor_context.lock();
            for operator in &ctx.info {
                res.push(operator.operator_id.to_string());
            }
            res
        };

        let mut last_epoch: Option<Epoch> = None;

        let stream = Box::new(self.consumer).execute();
        pin_mut!(stream);

        // Drive the streaming task with an infinite loop
        while let Some(barrier) = stream
            .next()
            .in_span(span)
            .stack_trace(last_epoch.map_or(SpanValue::Slice("Epoch <initial>"), |e| {
                format!("Epoch {}", e.curr).into()
            }))
            .collect_errors()
            .await
            .transpose()?
        {
            last_epoch = Some(barrier.epoch);
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
            let to_stop = barrier.is_stop_or_update_drop_actor(self.id);
            if to_stop {
                tracing::trace!(actor_id = self.id, "actor exit");
                return Ok(());
            }

            // Tracing related work
            span = {
                let mut span = Span::enter_with_local_parent("actor_poll");
                span.add_property(|| ("otel.name", span_name.to_string()));
                span.add_property(|| ("next", self.id.to_string()));
                span.add_property(|| ("next", "Outbound".to_string()));
                span.add_property(|| ("epoch", barrier.epoch.curr.to_string()));
                span
            };
        }

        tracing::error!(actor_id = self.id, "actor exit without stop barrier");

        Ok(())
    }
}

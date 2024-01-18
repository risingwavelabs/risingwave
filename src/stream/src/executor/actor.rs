// Copyright 2024 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock};

use anyhow::anyhow;
use await_tree::InstrumentAwait;
use futures::future::join_all;
use hytra::TrAdder;
use parking_lot::Mutex;
use risingwave_common::error::ErrorSuppressor;
use risingwave_common::log::LogSuppresser;
use risingwave_common::metrics::GLOBAL_ERROR_METRICS;
use risingwave_common::util::epoch::EpochPair;
use risingwave_expr::expr_context::expr_context_scope;
use risingwave_expr::ExprError;
use risingwave_pb::plan_common::ExprContext;
use risingwave_pb::stream_plan::PbStreamActor;
use thiserror_ext::AsReport;
use tokio_stream::StreamExt;
use tracing::Instrument;

use super::monitor::StreamingMetrics;
use super::subtask::SubtaskHandle;
use super::StreamConsumer;
use crate::error::StreamResult;
use crate::task::{ActorId, SharedContext};

/// Shared by all operators of an actor.
pub struct ActorContext {
    pub id: ActorId,
    pub fragment_id: u32,
    pub stream_actor: PbStreamActor,

    // TODO(eric): these seem to be useless now?
    last_mem_val: Arc<AtomicUsize>,
    cur_mem_val: Arc<AtomicUsize>,
    total_mem_val: Arc<TrAdder<i64>>,

    pub streaming_metrics: Arc<StreamingMetrics>,
    pub error_suppressor: Arc<Mutex<ErrorSuppressor>>,
}

pub type ActorContextRef = Arc<ActorContext>;

impl ActorContext {
    pub fn create(id: ActorId) -> ActorContextRef {
        Arc::new(Self {
            id,
            fragment_id: 0,
            stream_actor: PbStreamActor {
                actor_id: id,
                ..Default::default()
            },
            cur_mem_val: Arc::new(0.into()),
            last_mem_val: Arc::new(0.into()),
            total_mem_val: Arc::new(TrAdder::new()),
            streaming_metrics: Arc::new(StreamingMetrics::unused()),
            error_suppressor: Arc::new(Mutex::new(ErrorSuppressor::new(10))),
        })
    }

    pub fn create_with_metrics(
        stream_actor: PbStreamActor,
        total_mem_val: Arc<TrAdder<i64>>,
        streaming_metrics: Arc<StreamingMetrics>,
        unique_user_errors: usize,
    ) -> ActorContextRef {
        Arc::new(Self {
            id: stream_actor.actor_id,
            fragment_id: stream_actor.fragment_id,
            stream_actor,
            cur_mem_val: Arc::new(0.into()),
            last_mem_val: Arc::new(0.into()),
            total_mem_val,
            streaming_metrics,
            error_suppressor: Arc::new(Mutex::new(ErrorSuppressor::new(unique_user_errors))),
        })
    }

    pub fn on_compute_error(&self, err: ExprError, identity: &str) {
        static LOG_SUPPERSSER: LazyLock<LogSuppresser> = LazyLock::new(LogSuppresser::default);
        if let Ok(suppressed_count) = LOG_SUPPERSSER.check() {
            tracing::error!(identity, error = %err.as_report(), suppressed_count, "failed to evaluate expression");
        }

        let executor_name = identity.split(' ').next().unwrap_or("name_not_found");
        let mut err_str = err.to_report_string();

        if self.error_suppressor.lock().suppress_error(&err_str) {
            err_str = format!(
                "error msg suppressed (due to per-actor error limit: {})",
                self.error_suppressor.lock().max()
            );
        }
        GLOBAL_ERROR_METRICS.user_compute_error.report([
            "ExprError".to_owned(),
            err_str,
            executor_name.to_owned(),
            self.fragment_id.to_string(),
        ]);
    }

    pub fn store_mem_usage(&self, val: usize) {
        // Record the last mem val.
        // Calculate the difference between old val and new value, and apply the diff to total
        // memory usage value.
        let old_value = self.cur_mem_val.load(Ordering::Relaxed);
        self.last_mem_val.store(old_value, Ordering::Relaxed);
        let diff = val as i64 - old_value as i64;

        self.total_mem_val.inc(diff);

        self.cur_mem_val.store(val, Ordering::Relaxed);
    }

    pub fn mem_usage(&self) -> usize {
        self.cur_mem_val.load(Ordering::Relaxed)
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
    pub actor_context: ActorContextRef,
    expr_context: ExprContext,
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
        expr_context: ExprContext,
    ) -> Self {
        Self {
            consumer,
            subtasks,
            context,
            _metrics: metrics,
            actor_context,
            expr_context,
        }
    }

    #[inline(always)]
    pub async fn run(mut self) -> StreamResult<()> {
        expr_context_scope(self.expr_context.clone(), async move {
            tokio::join!(
                // Drive the subtasks concurrently.
                join_all(std::mem::take(&mut self.subtasks)),
                self.run_consumer(),
            )
            .1
        })
        .await
    }

    async fn run_consumer(self) -> StreamResult<()> {
        fail::fail_point!("start_actors_err", |_| Err(anyhow::anyhow!(
            "intentional start_actors_err"
        )
        .into()));

        let id = self.actor_context.id;

        let span_name = format!("Actor {id}");

        let new_span = |epoch: Option<EpochPair>| {
            tracing::info_span!(
                parent: None,
                "actor",
                "otel.name" = span_name,
                actor_id = id,
                prev_epoch = epoch.map(|e| e.prev),
                curr_epoch = epoch.map(|e| e.curr),
            )
        };
        let mut span = new_span(None);

        let mut last_epoch: Option<EpochPair> = None;
        let mut stream = Box::pin(Box::new(self.consumer).execute());

        // Drive the streaming task with an infinite loop
        let result = loop {
            let barrier = match stream
                .try_next()
                .instrument(span.clone())
                .instrument_await(
                    last_epoch.map_or("Epoch <initial>".into(), |e| format!("Epoch {}", e.curr)),
                )
                .await
            {
                Ok(Some(barrier)) => barrier,
                Ok(None) => break Err(anyhow!("actor exited unexpectedly").into()),
                Err(err) => break Err(err),
            };

            fail::fail_point!("collect_actors_err", id == 10, |_| Err(anyhow::anyhow!(
                "intentional collect_actors_err"
            )
            .into()));

            // Collect barriers to local barrier manager
            self.context.barrier_manager().collect(id, &barrier);

            // Then stop this actor if asked
            if barrier.is_stop(id) {
                break Ok(());
            }

            // Tracing related work
            last_epoch = Some(barrier.epoch);
            span = barrier.tracing_context().attach(new_span(last_epoch));
        };

        spawn_blocking_drop_stream(stream).await;

        tracing::trace!(actor_id = id, "actor exit");
        result
    }
}

/// Drop the stream in a blocking task to avoid interfering with other actors.
///
/// Logically the actor is dropped after we send the barrier with `Drop` mutation to the
/// downstream, thus making the `drop`'s progress asynchronous. However, there might be a
/// considerable amount of data in the executors' in-memory cache, dropping these structures might
/// be a CPU-intensive task. This may lead to the runtime being unable to schedule other actors if
/// the `drop` is called on the current thread.
pub async fn spawn_blocking_drop_stream<T: Send + 'static>(stream: T) {
    let _ = tokio::task::spawn_blocking(move || drop(stream))
        .instrument_await("drop_stream")
        .await;
}

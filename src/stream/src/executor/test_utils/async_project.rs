// Copyright 2026 RisingWave Labs
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

//! A batch-RPC mock exercised through the real projection executor, shared by tests and benchmarks.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::{StreamExt, stream};
use multimap::MultiMap;
use risingwave_common::array::{Array, ArrayRef, DataChunk, I64Array, Op, StreamChunk};
use risingwave_common::bitmap::Bitmap;
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::config::StreamingConfig;
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, Datum};
use risingwave_common::util::epoch::test_epoch;
use risingwave_expr::expr::{
    AsyncExpression, AsyncExpressionBoxExt, ExpressionInfo, NonStrictExpression,
};
use serde::Serialize;
use tokio::time::Instant;

use crate::executor::project::ProjectExecutor;
use crate::executor::{
    ActorContext, Barrier, BoxedMessageStream, Execute, Executor, ExecutorInfo, Message, Mutation,
    StopMutation,
};

/// One batch request, including invisible rows in its physical representation.
#[derive(Clone, Debug)]
pub struct MockProjectBatch {
    pub capacity: usize,
    pub visible_rows: usize,
    pub latency: Duration,
}

#[derive(Clone, Debug)]
pub struct MockProjectWorkload {
    pub chunk_limit: usize,
    pub row_limit: usize,
    pub batches: Vec<MockProjectBatch>,
    /// Insert a checkpoint barrier after this many chunks. Zero inserts only the final stop barrier.
    pub barrier_every: usize,
}

/// Durations use the runtime clock: virtual time in unit tests, wall time in the benchmark.
/// Latencies are sampled per chunk. All input batches are queued when measurement begins.
#[derive(Debug, Serialize)]
pub struct MockProjectResult {
    pub elapsed_ms: f64,
    pub visible_rows: usize,
    pub rows_per_second: f64,
    pub output_latency_p50_ms: f64,
    pub output_latency_p95_ms: f64,
    pub output_latency_p99_ms: f64,
    pub evaluation_latency_p95_ms: f64,
    pub ordered_wait_p95_ms: f64,
    pub barrier_latency_p95_ms: f64,
    pub peak_active_chunks: usize,
    pub peak_active_rows: usize,
    pub peak_window_chunks: usize,
    /// Completed evaluations not yet observed by the downstream consumer, including ready output.
    pub peak_completed_buffered_chunks: usize,
}

#[derive(Debug, Default)]
struct BatchTiming {
    started: Option<Instant>,
    completed: Option<Instant>,
    emitted: Option<Instant>,
}

#[derive(Debug, Default)]
struct Observations {
    batches: Vec<BatchTiming>,
    active_chunks: usize,
    active_rows: usize,
    window_chunks: usize,
    completed_buffered_chunks: usize,
    peak_active_chunks: usize,
    peak_active_rows: usize,
    peak_window_chunks: usize,
    peak_completed_buffered_chunks: usize,
    cancelled: usize,
    barrier_inputs: VecDeque<Instant>,
    barrier_latencies: Vec<Duration>,
}

/// Its lifetime matches evaluation rather than ordered output, including cancellation.
struct ActiveEvaluation {
    observations: Arc<Mutex<Observations>>,
    id: usize,
    rows: usize,
    completed: bool,
}

impl ActiveEvaluation {
    fn start(observations: Arc<Mutex<Observations>>, id: usize, rows: usize) -> Self {
        {
            let mut stats = observations.lock().unwrap();
            assert!(stats.batches[id].started.replace(Instant::now()).is_none());
            stats.active_chunks += 1;
            stats.active_rows += rows;
            stats.peak_active_chunks = stats.peak_active_chunks.max(stats.active_chunks);
            stats.peak_active_rows = stats.peak_active_rows.max(stats.active_rows);
        }
        Self {
            observations,
            id,
            rows,
            completed: false,
        }
    }

    fn complete(&mut self) {
        let mut stats = self.observations.lock().unwrap();
        stats.batches[self.id].completed = Some(Instant::now());
        stats.completed_buffered_chunks += 1;
        stats.peak_completed_buffered_chunks = stats
            .peak_completed_buffered_chunks
            .max(stats.completed_buffered_chunks);
        self.completed = true;
    }
}

impl Drop for ActiveEvaluation {
    fn drop(&mut self) {
        let mut stats = self.observations.lock().unwrap();
        stats.active_chunks -= 1;
        stats.active_rows -= self.rows;
        stats.cancelled += usize::from(!self.completed);
    }
}

#[derive(Debug)]
struct MockAsyncExpression {
    latencies: Vec<Duration>,
    observations: Arc<Mutex<Observations>>,
}

impl ExpressionInfo for MockAsyncExpression {
    fn return_type(&self) -> DataType {
        DataType::Int64
    }
}

impl AsyncExpression for MockAsyncExpression {
    async fn eval(&self, input: &DataChunk) -> risingwave_expr::Result<ArrayRef> {
        let id = input.column_at(0).as_int64().value_at(0).unwrap() as usize;
        let mut evaluation =
            ActiveEvaluation::start(self.observations.clone(), id, input.cardinality());
        // A zero-latency baseline deliberately avoids a timer, measuring instrumented scheduling.
        if !self.latencies[id].is_zero() {
            tokio::time::sleep(self.latencies[id]).await;
        }
        evaluation.complete();
        Ok(input.column_at(0).clone())
    }

    async fn eval_row(&self, _: &OwnedRow) -> risingwave_expr::Result<Datum> {
        unreachable!("the mock models chunk-based evaluation")
    }
}

struct ObservedInput {
    messages: Vec<Message>,
    observations: Arc<Mutex<Observations>>,
}

impl Execute for ObservedInput {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        let Self {
            messages,
            observations,
        } = *self;
        stream::iter(messages)
            .map(move |message| {
                let mut stats = observations.lock().unwrap();
                match &message {
                    Message::Chunk(_) => {
                        stats.window_chunks += 1;
                        stats.peak_window_chunks =
                            stats.peak_window_chunks.max(stats.window_chunks);
                    }
                    Message::Barrier(barrier) if barrier.epoch.curr != test_epoch(1) => {
                        stats.barrier_inputs.push_back(Instant::now());
                    }
                    _ => {}
                }
                Ok(message)
            })
            .boxed()
    }
}

struct PreparedProject {
    output: BoxedMessageStream,
    expected: VecDeque<Message>,
    observations: Arc<Mutex<Observations>>,
}

fn prepare(workload: &MockProjectWorkload) -> PreparedProject {
    assert!(!workload.batches.is_empty());
    let observations = Arc::new(Mutex::new(Observations {
        batches: (0..workload.batches.len())
            .map(|_| BatchTiming::default())
            .collect(),
        ..Default::default()
    }));
    let mut messages = vec![Message::Barrier(Barrier::new_test_barrier(test_epoch(1)))];
    let mut epoch = 2;
    for (id, batch) in workload.batches.iter().enumerate() {
        assert!(batch.capacity > 0 && batch.visible_rows <= batch.capacity);
        let column = I64Array::from_iter(std::iter::repeat_n(Some(id as i64), batch.capacity));
        messages.push(Message::Chunk(StreamChunk::with_visibility(
            vec![Op::Insert; batch.capacity],
            vec![column.into_ref()],
            Bitmap::from_iter((0..batch.capacity).map(|i| i < batch.visible_rows)),
        )));
        if workload.barrier_every > 0 && (id + 1).is_multiple_of(workload.barrier_every) {
            messages.push(Message::Barrier(Barrier::new_test_barrier(test_epoch(
                epoch,
            ))));
            epoch += 1;
        }
    }
    messages.push(Message::Barrier(
        Barrier::new_test_barrier(test_epoch(epoch)).with_mutation(Mutation::Stop(StopMutation {
            dropped_actors: std::iter::once(123.into()).collect(),
            ..Default::default()
        })),
    ));
    let expected = messages.iter().skip(1).cloned().collect();
    let input = Executor::new(
        ExecutorInfo::for_test(
            Schema::new(vec![Field::unnamed(DataType::Int64)]),
            vec![],
            "MockAsyncProjectInput".into(),
            0,
        ),
        ObservedInput {
            messages,
            observations: observations.clone(),
        }
        .boxed(),
    );
    let mut config = StreamingConfig::default();
    config.developer.project_expr_concurrency = workload.chunk_limit;
    config.developer.project_expr_inflight_request_concurrency = workload.row_limit;
    let project = ProjectExecutor::new(
        ActorContext::for_test_with_config(123, config),
        input,
        vec![NonStrictExpression::for_test(
            MockAsyncExpression {
                latencies: workload.batches.iter().map(|batch| batch.latency).collect(),
                observations: observations.clone(),
            }
            .boxed(),
        )],
        MultiMap::new(),
        vec![],
        false,
    );
    PreparedProject {
        output: project.boxed().execute(),
        expected,
        observations,
    }
}

fn percentile_ms(mut durations: Vec<Duration>, percentile: usize) -> f64 {
    if durations.is_empty() {
        return 0.0;
    }
    durations.sort_unstable();
    durations[(durations.len() * percentile).div_ceil(100) - 1].as_secs_f64() * 1000.0
}

/// Run finite, ready input through a real executor and verify every output chunk and barrier.
/// Construction and statistics aggregation are excluded from elapsed time. Source admission,
/// expression instrumentation, downstream observation and output validation are included.
pub async fn run_mock_project(workload: &MockProjectWorkload) -> MockProjectResult {
    let PreparedProject {
        mut output,
        mut expected,
        observations,
    } = prepare(workload);
    assert!(matches!(
        output.next().await.unwrap().unwrap(),
        Message::Barrier(_)
    ));
    let start = Instant::now();
    let mut emitted = 0;
    while let Some(message) = output.next().await {
        let now = Instant::now();
        match (message.unwrap(), expected.pop_front().unwrap()) {
            (Message::Chunk(actual), Message::Chunk(expected)) => {
                {
                    let mut stats = observations.lock().unwrap();
                    assert!(stats.batches[emitted].completed.is_some());
                    stats.batches[emitted].emitted = Some(now);
                    stats.completed_buffered_chunks -= 1;
                    stats.window_chunks -= 1;
                }
                assert_eq!(actual, expected);
                emitted += 1;
            }
            (Message::Barrier(actual), Message::Barrier(expected)) => {
                assert_eq!(actual.epoch, expected.epoch);
                assert_eq!(actual.is_stop(123.into()), expected.is_stop(123.into()));
                let mut stats = observations.lock().unwrap();
                let admitted = stats.barrier_inputs.pop_front().unwrap();
                stats.barrier_latencies.push(now - admitted);
            }
            other => panic!("unexpected output order: {other:?}"),
        }
    }
    let elapsed = start.elapsed();
    assert!(expected.is_empty());
    assert_eq!(emitted, workload.batches.len());
    let stats = observations.lock().unwrap();
    assert_eq!(stats.active_chunks, 0);
    assert_eq!(stats.active_rows, 0);
    assert_eq!(stats.completed_buffered_chunks, 0);
    assert_eq!(stats.window_chunks, 0);
    assert_eq!(stats.cancelled, 0);
    assert!(stats.barrier_inputs.is_empty());
    let output_latencies: Vec<_> = stats
        .batches
        .iter()
        .map(|t| t.emitted.unwrap() - start)
        .collect();
    let visible_rows = workload
        .batches
        .iter()
        .map(|batch| batch.visible_rows)
        .sum();
    MockProjectResult {
        elapsed_ms: elapsed.as_secs_f64() * 1000.0,
        visible_rows,
        rows_per_second: visible_rows as f64 / elapsed.as_secs_f64(),
        output_latency_p50_ms: percentile_ms(output_latencies.clone(), 50),
        output_latency_p95_ms: percentile_ms(output_latencies.clone(), 95),
        output_latency_p99_ms: percentile_ms(output_latencies, 99),
        evaluation_latency_p95_ms: percentile_ms(
            stats
                .batches
                .iter()
                .map(|t| t.completed.unwrap() - t.started.unwrap())
                .collect(),
            95,
        ),
        ordered_wait_p95_ms: percentile_ms(
            stats
                .batches
                .iter()
                .map(|t| t.emitted.unwrap() - t.completed.unwrap())
                .collect(),
            95,
        ),
        barrier_latency_p95_ms: percentile_ms(stats.barrier_latencies.clone(), 95),
        peak_active_chunks: stats.peak_active_chunks,
        peak_active_rows: stats.peak_active_rows,
        peak_window_chunks: stats.peak_window_chunks,
        peak_completed_buffered_chunks: stats.peak_completed_buffered_chunks,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn batches(rows: &[usize]) -> Vec<MockProjectBatch> {
        rows.iter()
            .map(|&rows| MockProjectBatch {
                capacity: rows,
                visible_rows: rows,
                latency: Duration::from_millis(100),
            })
            .collect()
    }

    #[tokio::test(start_paused = true)]
    async fn both_limits_control_real_executor_admission() {
        for (chunk_limit, row_limit, active_chunks) in [
            (1, 0, 1),
            (2, 0, 2),
            (4, 0, 4),
            (0, 0, 8),
            (8, 1, 1),
            (8, 128, 1),
            (8, 129, 2),
            (8, 256, 2),
            (2, 10000, 2),
            (0, 256, 2),
        ] {
            let result = run_mock_project(&MockProjectWorkload {
                chunk_limit,
                row_limit,
                batches: batches(&[128; 8]),
                barrier_every: 3,
            })
            .await;
            assert_eq!(
                result.peak_active_chunks, active_chunks,
                "C={chunk_limit}, R={row_limit}"
            );
            assert_eq!(result.peak_active_rows, 128 * active_chunks);
            assert_eq!(result.visible_rows, 1024);
            assert!(chunk_limit == 0 || result.peak_window_chunks <= chunk_limit);
            // This asserts the simulated latency model, not a host-dependent performance threshold.
            let expected_ms = (8_usize.div_ceil(active_chunks) * 100) as f64;
            assert!((expected_ms..=expected_ms + 20.0).contains(&result.elapsed_ms));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn invisible_rows_oversized_batches_and_slow_head() {
        let mut batches = batches(&[3, 4, 8, 1]);
        batches[0].capacity = 256;
        batches[0].latency = Duration::from_secs(1);
        let result = run_mock_project(&MockProjectWorkload {
            chunk_limit: 4,
            row_limit: 5,
            batches,
            barrier_every: 2,
        })
        .await;
        assert_eq!(result.visible_rows, 16);
        // While the first three rows are blocked, the oversized batch of eight must start.
        // A one-row waiter may also enter before that batch, depending on wakeup order.
        // Physical capacity and completed-but-buffered rows must not be charged.
        assert!((11..=12).contains(&result.peak_active_rows));
        assert_eq!(result.peak_completed_buffered_chunks, 4);
        assert!(result.ordered_wait_p95_ms >= 800.0);
    }

    #[tokio::test(start_paused = true)]
    async fn empty_visible_batches_make_progress_and_cancellation_is_observable() {
        let mut workload = MockProjectWorkload {
            chunk_limit: 4,
            row_limit: 1,
            batches: batches(&[128; 4]),
            barrier_every: 0,
        };
        workload.batches[1].visible_rows = 0;
        let result = run_mock_project(&workload).await;
        assert_eq!(result.visible_rows, 384);
        assert_eq!(result.peak_active_rows, 128);
        assert_eq!(result.peak_active_chunks, 2);

        let PreparedProject {
            mut output,
            observations,
            ..
        } = prepare(&workload);
        assert!(matches!(
            output.next().await.unwrap().unwrap(),
            Message::Barrier(_)
        ));
        assert!(futures::poll!(output.next()).is_pending());
        assert_eq!(observations.lock().unwrap().active_chunks, 2);
        drop(output);
        let stats = observations.lock().unwrap();
        assert_eq!(stats.active_rows, 0);
        assert_eq!(stats.active_chunks, 0);
        assert_eq!(stats.cancelled, 2);
    }
}

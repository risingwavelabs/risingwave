// Copyright 2022 RisingWave Labs
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

use std::sync::Arc;

use itertools::Itertools;
use risingwave_common::array::Op;
use risingwave_common::row;
use risingwave_common::types::{DefaultOrdered, Interval, Timestamptz, ToDatumRef};
use risingwave_expr::capture_context;
use risingwave_expr::expr::{
    EvalErrorReport, InputRefExpression, LiteralExpression, NonStrictExpression,
    SyncExpressionBoxExt, build_func_non_strict,
};
use risingwave_expr::expr_context::TIME_ZONE;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio_stream::wrappers::UnboundedReceiverStream;

use crate::executor::monitor::StreamingMetrics;
use crate::executor::prelude::*;
use crate::task::{ActorEvalErrorReport, FragmentId};

pub struct NowExecutor<S: StateStore> {
    data_types: Vec<DataType>,

    mode: NowMode,
    eval_error_report: ActorEvalErrorReport,

    /// Receiver of barrier channel.
    barrier_receiver: UnboundedReceiver<Barrier>,

    state_table: StateTable<S>,

    progress_ratio: Option<f32>,

    barrier_interval_ms: u32,

    /// Metrics for observing the streaming NOW() clock and its drift from wall time.
    metrics: Arc<StreamingMetrics>,
    fragment_id: FragmentId,
}

pub enum NowMode {
    /// Emit current timestamp on startup, update it on barrier.
    UpdateCurrent,
    /// Generate a series of timestamps starting from `start_timestamp` with `interval`.
    /// Keep generating new timestamps on barrier.
    GenerateSeries {
        start_timestamp: Timestamptz,
        interval: Interval,
    },
}

enum ModeVars {
    UpdateCurrent,
    GenerateSeries {
        chunk_builder: StreamChunkBuilder,
        add_interval_expr: NonStrictExpression,
    },
}

impl<S: StateStore> NowExecutor<S> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        data_types: Vec<DataType>,
        mode: NowMode,
        eval_error_report: ActorEvalErrorReport,
        barrier_receiver: UnboundedReceiver<Barrier>,
        state_table: StateTable<S>,
        progress_ratio: Option<f32>,
        barrier_interval_ms: u32,
        streaming_metrics: Arc<StreamingMetrics>,
        fragment_id: FragmentId,
    ) -> Self {
        Self {
            data_types,
            mode,
            eval_error_report,
            barrier_receiver,
            state_table,
            progress_ratio,
            barrier_interval_ms,
            metrics: streaming_metrics,
            fragment_id,
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(self) {
        let Self {
            data_types,
            mode,
            eval_error_report,
            barrier_receiver,
            mut state_table,
            progress_ratio,
            barrier_interval_ms,
            metrics,
            fragment_id,
        } = self;

        let mut executor_metrics = None;

        info!(
            "NowExecutor started. progress_ratio: {:?}, barrier_interval_ms: {:?}",
            progress_ratio, barrier_interval_ms
        );

        let max_chunk_size = crate::config::chunk_size();

        // Whether the executor is paused.
        let mut paused = false;
        // The last timestamp **sent** to the downstream.
        let mut last_timestamp_datum: Datum = None;

        // Whether the first barrier is handled and `last_timestamp` is initialized.
        let mut initialized = false;

        let mut mode_vars = match &mode {
            NowMode::UpdateCurrent => ModeVars::UpdateCurrent,
            NowMode::GenerateSeries { interval, .. } => {
                // in most cases there won't be more than one row except for the first time
                let chunk_builder = StreamChunkBuilder::unlimited(data_types.clone(), Some(1));
                let add_interval_expr =
                    build_add_interval_expr_captured(*interval, eval_error_report)?;
                ModeVars::GenerateSeries {
                    chunk_builder,
                    add_interval_expr,
                }
            }
        };

        const MAX_MERGE_BARRIER_SIZE: usize = 64;

        #[for_await]
        for barriers in
            UnboundedReceiverStream::new(barrier_receiver).ready_chunks(MAX_MERGE_BARRIER_SIZE)
        {
            let mut curr_timestamp_datum: Datum = None;
            // Wall-clock reference derived from the most recently processed barrier's epoch.
            // Used to observe how far the streaming NOW() lags real time.
            let mut last_barrier_wall_ms: Option<i64> = None;
            if barriers.len() > 1 {
                warn!(
                    "handle multiple barriers at once in now executor: {}",
                    barriers.len()
                );
            }
            for barrier in barriers {
                let curr_epoch = barrier.get_curr_epoch();
                let new_timestamp = curr_epoch.as_timestamptz();
                last_barrier_wall_ms = Some(new_timestamp.timestamp_millis());
                let pause_mutation =
                    barrier
                        .mutation
                        .as_deref()
                        .and_then(|mutation| match mutation {
                            Mutation::Pause => Some(true),
                            Mutation::Resume => Some(false),
                            _ => None,
                        });

                if !initialized {
                    let first_epoch = barrier.epoch;
                    let is_pause_on_startup = barrier.is_pause_on_startup();
                    yield Message::Barrier(barrier);
                    // Handle the initial barrier.
                    state_table.init_epoch(first_epoch).await?;
                    last_timestamp_datum = state_table.get_from_one_value_table().await?;
                    paused = is_pause_on_startup;
                    initialized = true;
                } else {
                    state_table
                        .commit_assert_no_update_vnode_bitmap(barrier.epoch)
                        .await?;
                    yield Message::Barrier(barrier);
                }

                // Extract timestamp from the current epoch.
                if let Some(datum) = &last_timestamp_datum
                    && let Some(progress_ratio) = progress_ratio
                    && progress_ratio > 1.0
                {
                    let last_timestamp = datum.as_timestamptz();
                    // curr_timestamp = min(last_timestamp + barrier_interval * progress_ratio, timestamp from epoch)
                    // to avoid having a big gap between the last timestamp and the current timestamp,
                    // which may cause excessive changes in downstream dynamic filter
                    let progress_timestamp = last_timestamp
                        .timestamp_millis()
                        .checked_add((barrier_interval_ms as f32 * progress_ratio).ceil() as i64)
                        .expect("progress_timestamp is out of i64 range");
                    let adjusted_timestamp = if progress_timestamp
                        < new_timestamp.timestamp_millis()
                    {
                        debug!(
                            "adjusted next now timestamp from {} to {}. curr_epoch: {}, barrier_interval_ms: {}, progress_ratio: {}",
                            new_timestamp.timestamp_millis(),
                            progress_timestamp,
                            curr_epoch,
                            barrier_interval_ms,
                            progress_ratio
                        );
                        Timestamptz::from_millis(progress_timestamp)
                            .expect("progress_timestamp is out of timestamptz range")
                    } else {
                        new_timestamp
                    };
                    curr_timestamp_datum = Some(adjusted_timestamp.into());
                } else {
                    curr_timestamp_datum = Some(new_timestamp.into());
                }

                // Update paused state.
                if let Some(pause_mutation) = pause_mutation {
                    paused = pause_mutation;
                }
            }

            // Do not yield any messages if paused.
            if paused {
                continue;
            }

            match (&mode, &mut mode_vars) {
                (NowMode::UpdateCurrent, ModeVars::UpdateCurrent) => {
                    let chunk = if last_timestamp_datum.is_some() {
                        let last_row = row::once(&last_timestamp_datum);
                        let row = row::once(&curr_timestamp_datum);
                        state_table.update(last_row, row);

                        StreamChunk::from_rows(
                            &[(Op::Delete, last_row), (Op::Insert, row)],
                            &data_types,
                        )
                    } else {
                        let row = row::once(&curr_timestamp_datum);
                        state_table.insert(row);

                        StreamChunk::from_rows(&[(Op::Insert, row)], &data_types)
                    };

                    yield Message::Chunk(chunk);
                    last_timestamp_datum.clone_from(&curr_timestamp_datum)
                }
                (
                    &NowMode::GenerateSeries {
                        start_timestamp, ..
                    },
                    &mut ModeVars::GenerateSeries {
                        ref mut chunk_builder,
                        ref add_interval_expr,
                    },
                ) => {
                    if last_timestamp_datum.is_none() {
                        // We haven't emit any timestamp yet. Let's emit the first one and populate the state table.
                        let first = Some(start_timestamp.into());
                        let first_row = row::once(&first);
                        let _ = chunk_builder.append_row(Op::Insert, first_row);
                        state_table.insert(first_row);
                        last_timestamp_datum = first;
                    }

                    // Now let's step through the timestamps from the last timestamp to the current timestamp.
                    // We use `last_row` as a temporary cursor to track the progress, and won't touch `last_timestamp`
                    // until the end of the loop, so that `last_timestamp` is always synced with the state table.
                    let mut last_row = OwnedRow::new(vec![last_timestamp_datum.clone()]);

                    loop {
                        if chunk_builder.size() >= max_chunk_size {
                            // Manually yield the chunk when size exceeds the limit. We don't want to use chunk builder
                            // with limited size here because the initial capacity can be too large for most cases.
                            // Basically only the first several chunks can potentially exceed the `max_chunk_size`.
                            if let Some(chunk) = chunk_builder.take() {
                                yield Message::Chunk(chunk);
                            }
                        }

                        let next = add_interval_expr.eval_row_infallible(&last_row).await;
                        if DefaultOrdered(next.to_datum_ref())
                            > DefaultOrdered(curr_timestamp_datum.to_datum_ref())
                        {
                            // We only increase the timestamp to the current timestamp.
                            break;
                        }

                        let next_row = OwnedRow::new(vec![next]);
                        let _ = chunk_builder.append_row(Op::Insert, &next_row);
                        last_row = next_row;
                    }

                    if let Some(chunk) = chunk_builder.take() {
                        yield Message::Chunk(chunk);
                    }

                    // Update the last timestamp.
                    state_table.update(row::once(&last_timestamp_datum), &last_row);
                    last_timestamp_datum =
                        Itertools::exactly_one(last_row.into_inner().into_vec().into_iter())
                            .unwrap();
                }
                _ => unreachable!(),
            }

            let curr_timestamp_datum = curr_timestamp_datum.unwrap();

            if let Some(wall_ms) = last_barrier_wall_ms
                && let ScalarImpl::Timestamptz(ts) = &curr_timestamp_datum
            {
                let streaming_now_ms = ts.timestamp_millis();
                let (streaming_clock_ms, wall_clock_drift_ms) = executor_metrics
                    .get_or_insert_with(|| {
                        // NOW fragments are singleton, and recovery waits for the old actor to
                        // stop, so fragment_id identifies a single writer. Multiple actors on
                        // this node using the same label would overwrite each other's gauges.
                        let label = fragment_id.to_string();
                        (
                            metrics
                                .now_streaming_clock_ms
                                .with_guarded_label_values(&[&label]),
                            metrics
                                .now_wall_clock_drift_ms
                                .with_guarded_label_values(&[&label]),
                        )
                    });
                streaming_clock_ms.set(streaming_now_ms);
                wall_clock_drift_ms.set(wall_ms.saturating_sub(streaming_now_ms));
            }

            yield Message::Watermark(Watermark::new(
                0,
                DataType::Timestamptz,
                curr_timestamp_datum,
            ));
        }
    }
}

impl<S: StateStore> Execute for NowExecutor<S> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.execute_inner().boxed()
    }
}

#[capture_context(TIME_ZONE)]
pub fn build_add_interval_expr(
    time_zone: &str,
    interval: Interval,
    eval_error_report: impl EvalErrorReport + 'static,
) -> risingwave_expr::Result<NonStrictExpression> {
    let timestamptz_input = InputRefExpression::new(DataType::Timestamptz, 0);
    let interval = LiteralExpression::new(DataType::Interval, Some(interval.into()));
    let time_zone = LiteralExpression::new(DataType::Varchar, Some(time_zone.into()));

    use risingwave_pb::expr::expr_node::PbType as PbExprType;
    build_func_non_strict(
        PbExprType::AddWithTimeZone,
        DataType::Timestamptz,
        vec![
            timestamptz_input.boxed(),
            interval.boxed(),
            time_zone.boxed(),
        ],
        eval_error_report,
    )
}

#[cfg(test)]
mod tests {
    use prometheus::Registry;
    use risingwave_common::catalog::{ColumnDesc, ColumnId, TableId};
    use risingwave_common::config::MetricLevel;
    use risingwave_common::test_prelude::StreamChunkTestExt;
    use risingwave_common::types::test_utils::IntervalTestExt;
    use risingwave_common::util::epoch::test_epoch;
    use risingwave_storage::memory::MemoryStateStore;
    use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};

    use super::*;
    use crate::common::table::test_utils::gen_pbtable;
    use crate::executor::test_utils::StreamExecutorTestExt;

    #[tokio::test]
    async fn test_now_metrics_lifecycle() -> StreamExecutorResult<()> {
        let registry = Registry::new();
        let metrics = Arc::new(StreamingMetrics::new(&registry, MetricLevel::Info));
        let samples = || {
            let mut samples = registry
                .gather()
                .into_iter()
                .filter(|family| family.name().starts_with("stream_now_"))
                .map(|family| {
                    assert_eq!(family.get_metric().len(), 1);
                    let metric = &family.get_metric()[0];
                    assert_eq!(metric.get_label().len(), 1);
                    assert_eq!(metric.get_label()[0].name(), "fragment_id");
                    assert_eq!(metric.get_label()[0].value(), "1");
                    (
                        family.name().to_owned(),
                        metric.get_gauge().as_ref().unwrap().value() as i64,
                    )
                })
                .collect::<Vec<_>>();
            samples.sort();
            samples
        };
        assert!(samples().is_empty());

        // Reuse the fragment label after the previous executor has been dropped.
        let mut previous_cleaned_up = true;
        for cleanup_before_recreation in [true, false, true] {
            let state_store = create_state_store();
            let (tx, mut executor) =
                build_executor(NowMode::UpdateCurrent, &state_store, Some(2.0)).await;
            executor.metrics = metrics.clone();
            executor.fragment_id = 1.into();
            let mut now = executor.boxed().execute();

            tx.send(Barrier::new_test_barrier(test_epoch(1))).unwrap();
            now.next_unwrap_ready_barrier()?;
            now.next_unwrap_ready_chunk()?;
            // Before the first watermark, no zero-valued series is registered.
            if previous_cleaned_up {
                assert!(samples().is_empty());
            }
            now.next_unwrap_ready_watermark()?;
            let initial_clock = "2021-04-01T00:00:00.001Z"
                .parse::<Timestamptz>()
                .unwrap()
                .timestamp_millis();
            assert_eq!(
                samples(),
                vec![
                    ("stream_now_streaming_clock_ms".into(), initial_clock),
                    ("stream_now_wall_clock_drift_ms".into(), 0),
                ]
            );

            tx.send(Barrier::with_prev_epoch_for_test(
                test_epoch(5000),
                test_epoch(1),
            ))
            .unwrap();
            now.next_unwrap_ready_barrier()?;
            now.next_unwrap_ready_chunk()?;
            now.next_unwrap_ready_watermark()?;
            let expected = vec![
                ("stream_now_streaming_clock_ms".into(), initial_clock + 2000),
                ("stream_now_wall_clock_drift_ms".into(), 2999),
            ];
            assert_eq!(samples(), expected);
            now.next_unwrap_pending();
            assert_eq!(samples(), expected);

            drop(now);
            if cleanup_before_recreation {
                // Guarded metrics retain the last sample for one scrape.
                assert_eq!(samples(), expected);
                assert!(samples().is_empty());
            }
            previous_cleaned_up = cleanup_before_recreation;
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_now() -> StreamExecutorResult<()> {
        let state_store = create_state_store();
        let (tx, mut now) = create_executor(NowMode::UpdateCurrent, &state_store).await;

        // Init barrier
        tx.send(Barrier::new_test_barrier(test_epoch(1))).unwrap();

        // Consume the barrier
        now.next_unwrap_ready_barrier()?;

        // Consume the data chunk
        let chunk_msg = now.next_unwrap_ready_chunk()?;

        assert_eq!(
            chunk_msg.compact_vis(),
            StreamChunk::from_pretty(
                " TZ
                + 2021-04-01T00:00:00.001Z"
            )
        );

        // Consume the watermark
        let watermark = now.next_unwrap_ready_watermark()?;

        assert_eq!(
            watermark,
            Watermark::new(
                0,
                DataType::Timestamptz,
                ScalarImpl::Timestamptz("2021-04-01T00:00:00.001Z".parse().unwrap())
            )
        );

        tx.send(Barrier::with_prev_epoch_for_test(
            test_epoch(2),
            test_epoch(1),
        ))
        .unwrap();

        // Consume the barrier
        now.next_unwrap_ready_barrier()?;

        // Consume the data chunk
        let chunk_msg = now.next_unwrap_ready_chunk()?;

        assert_eq!(
            chunk_msg.compact_vis(),
            StreamChunk::from_pretty(
                " TZ
                - 2021-04-01T00:00:00.001Z
                + 2021-04-01T00:00:00.002Z"
            )
        );

        // Consume the watermark
        let watermark = now.next_unwrap_ready_watermark()?;

        assert_eq!(
            watermark,
            Watermark::new(
                0,
                DataType::Timestamptz,
                ScalarImpl::Timestamptz("2021-04-01T00:00:00.002Z".parse().unwrap())
            )
        );

        // No more messages until the next barrier
        now.next_unwrap_pending();

        // Recovery
        drop((tx, now));
        let (tx, mut now) = create_executor(NowMode::UpdateCurrent, &state_store).await;
        tx.send(Barrier::with_prev_epoch_for_test(
            test_epoch(3),
            test_epoch(1),
        ))
        .unwrap();

        // Consume the barrier
        now.next_unwrap_ready_barrier()?;

        // Consume the data chunk
        let chunk_msg = now.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk_msg.compact_vis(),
            // the last chunk was not checkpointed so the deleted old value should be `001`
            StreamChunk::from_pretty(
                " TZ
                - 2021-04-01T00:00:00.001Z
                + 2021-04-01T00:00:00.003Z"
            )
        );

        // Consume the watermark
        let watermark = now.next_unwrap_ready_watermark()?;

        assert_eq!(
            watermark,
            Watermark::new(
                0,
                DataType::Timestamptz,
                ScalarImpl::Timestamptz("2021-04-01T00:00:00.003Z".parse().unwrap())
            )
        );

        // Recovery with paused
        drop((tx, now));
        let (tx, mut now) = create_executor(NowMode::UpdateCurrent, &state_store).await;
        tx.send(
            Barrier::with_prev_epoch_for_test(test_epoch(4), test_epoch(1))
                .with_mutation(Mutation::Pause),
        )
        .unwrap();

        // Consume the barrier
        now.next_unwrap_ready_barrier()?;

        // There should be no messages until `Resume`
        now.next_unwrap_pending();

        // Resume barrier
        tx.send(
            Barrier::with_prev_epoch_for_test(test_epoch(5), test_epoch(4))
                .with_mutation(Mutation::Resume),
        )
        .unwrap();

        // Consume the barrier
        now.next_unwrap_ready_barrier()?;

        // Consume the data chunk
        let chunk_msg = now.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk_msg.compact_vis(),
            StreamChunk::from_pretty(
                " TZ
                - 2021-04-01T00:00:00.001Z
                + 2021-04-01T00:00:00.005Z"
            )
        );

        // Consume the watermark
        let watermark = now.next_unwrap_ready_watermark()?;

        assert_eq!(
            watermark,
            Watermark::new(
                0,
                DataType::Timestamptz,
                ScalarImpl::Timestamptz("2021-04-01T00:00:00.005Z".parse().unwrap())
            )
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_now_start_with_paused() -> StreamExecutorResult<()> {
        let state_store = create_state_store();
        let (tx, mut now) = create_executor(NowMode::UpdateCurrent, &state_store).await;

        // Init barrier
        tx.send(Barrier::new_test_barrier(test_epoch(1)).with_mutation(Mutation::Pause))
            .unwrap();

        // Consume the barrier
        now.next_unwrap_ready_barrier()?;

        // There should be no messages until `Resume`
        now.next_unwrap_pending();

        // Resume barrier
        tx.send(
            Barrier::with_prev_epoch_for_test(test_epoch(2), test_epoch(1))
                .with_mutation(Mutation::Resume),
        )
        .unwrap();

        // Consume the barrier
        now.next_unwrap_ready_barrier()?;

        // Consume the data chunk
        let chunk_msg = now.next_unwrap_ready_chunk()?;

        assert_eq!(
            chunk_msg.compact_vis(),
            StreamChunk::from_pretty(
                " TZ
                + 2021-04-01T00:00:00.002Z" // <- the timestamp is extracted from the current epoch
            )
        );

        // Consume the watermark
        let watermark = now.next_unwrap_ready_watermark()?;

        assert_eq!(
            watermark,
            Watermark::new(
                0,
                DataType::Timestamptz,
                ScalarImpl::Timestamptz("2021-04-01T00:00:00.002Z".parse().unwrap())
            )
        );

        // No more messages until the next barrier
        now.next_unwrap_pending();

        Ok(())
    }

    #[tokio::test]
    async fn test_now_generate_series() -> StreamExecutorResult<()> {
        TIME_ZONE::scope("UTC".to_owned(), test_now_generate_series_inner()).await
    }

    #[tokio::test]
    async fn test_now_with_progress_ratio() -> StreamExecutorResult<()> {
        let state_store = create_state_store();
        let progress_ratio = Some(2.0);
        let (tx, mut now) = create_executor_with_progress_ratio(
            NowMode::UpdateCurrent,
            &state_store,
            progress_ratio,
        )
        .await;

        // Init barrier at epoch 1 (timestamp 2021-04-01T00:00:00.001Z)
        tx.send(Barrier::new_test_barrier(test_epoch(1))).unwrap();

        // Consume the barrier
        now.next_unwrap_ready_barrier()?;

        // Consume the data chunk
        let chunk_msg = now.next_unwrap_ready_chunk()?;

        assert_eq!(
            chunk_msg.compact_vis(),
            StreamChunk::from_pretty(
                " TZ
                + 2021-04-01T00:00:00.001Z"
            )
        );

        // Consume the watermark
        let watermark = now.next_unwrap_ready_watermark()?;

        assert_eq!(
            watermark,
            Watermark::new(
                0,
                DataType::Timestamptz,
                ScalarImpl::Timestamptz("2021-04-01T00:00:00.001Z".parse().unwrap())
            )
        );

        // Send next barrier at epoch 5000 (timestamp 2021-04-01T00:00:00.005Z)
        // With progress_ratio = 2.0 and barrier_interval_ms = 1000,
        // adjusted timestamp should be: 1 + (1000 * 2.0) = 2001ms = 2021-04-01T00:00:02.001Z
        // Since 2001 < 5000, the adjusted timestamp should be used
        tx.send(Barrier::with_prev_epoch_for_test(
            test_epoch(5000),
            test_epoch(1),
        ))
        .unwrap();

        // Consume the barrier
        now.next_unwrap_ready_barrier()?;

        // Consume the data chunk - should show adjusted timestamp
        let chunk_msg = now.next_unwrap_ready_chunk()?;

        assert_eq!(
            chunk_msg.compact_vis(),
            StreamChunk::from_pretty(
                " TZ
                - 2021-04-01T00:00:00.001Z
                + 2021-04-01T00:00:02.001Z" // adjusted timestamp
            )
        );

        // Consume the watermark
        let watermark = now.next_unwrap_ready_watermark()?;

        assert_eq!(
            watermark,
            Watermark::new(
                0,
                DataType::Timestamptz,
                ScalarImpl::Timestamptz("2021-04-01T00:00:02.001Z".parse().unwrap())
            )
        );

        // Send another barrier at epoch 10000 (timestamp 2021-04-01T00:00:00.010Z)
        // With progress_ratio = 2.0, adjusted timestamp should be: 2001 + (1000 * 2.0) = 4001ms
        // Since 4001 < 10000, the adjusted timestamp should be used again
        tx.send(Barrier::with_prev_epoch_for_test(
            test_epoch(10000),
            test_epoch(5000),
        ))
        .unwrap();

        // Consume the barrier
        now.next_unwrap_ready_barrier()?;

        // Consume the data chunk
        let chunk_msg = now.next_unwrap_ready_chunk()?;

        assert_eq!(
            chunk_msg.compact_vis(),
            StreamChunk::from_pretty(
                " TZ
                - 2021-04-01T00:00:02.001Z
                + 2021-04-01T00:00:04.001Z" // adjusted timestamp
            )
        );

        // Consume the watermark
        let watermark = now.next_unwrap_ready_watermark()?;

        assert_eq!(
            watermark,
            Watermark::new(
                0,
                DataType::Timestamptz,
                ScalarImpl::Timestamptz("2021-04-01T00:00:04.001Z".parse().unwrap())
            )
        );

        // Send another barrier at epoch 15 (timestamp 2021-04-01T00:00:00.015Z)
        // With progress_ratio = 2.0, adjusted timestamp should be: 4001 + (1000 * 2.0) = 6001ms
        // Since 6001 < 15, the adjusted timestamp should be used
        tx.send(Barrier::with_prev_epoch_for_test(
            test_epoch(15000),
            test_epoch(10000),
        ))
        .unwrap();

        // Consume the barrier
        now.next_unwrap_ready_barrier()?;

        // Consume the data chunk
        let chunk_msg = now.next_unwrap_ready_chunk()?;

        assert_eq!(
            chunk_msg.compact_vis(),
            StreamChunk::from_pretty(
                " TZ
                - 2021-04-01T00:00:04.001Z
                + 2021-04-01T00:00:06.001Z" // adjusted timestamp
            )
        );

        // Consume the watermark
        let watermark = now.next_unwrap_ready_watermark()?;

        assert_eq!(
            watermark,
            Watermark::new(
                0,
                DataType::Timestamptz,
                ScalarImpl::Timestamptz("2021-04-01T00:00:06.001Z".parse().unwrap())
            )
        );

        // Now send a barrier at epoch 20 (timestamp 2021-04-01T00:00:00.020Z)
        // With progress_ratio = 2.0, adjusted timestamp should be: 6001 + (1000 * 2.0) = 8001ms
        // Since 8001 < 20, the adjusted timestamp should be used
        tx.send(Barrier::with_prev_epoch_for_test(
            test_epoch(20000),
            test_epoch(15000),
        ))
        .unwrap();

        // Consume the barrier
        now.next_unwrap_ready_barrier()?;

        // Consume the data chunk
        let chunk_msg = now.next_unwrap_ready_chunk()?;

        assert_eq!(
            chunk_msg.compact_vis(),
            StreamChunk::from_pretty(
                " TZ
                - 2021-04-01T00:00:06.001Z
                + 2021-04-01T00:00:08.001Z" // adjusted timestamp
            )
        );

        // Consume the watermark
        let watermark = now.next_unwrap_ready_watermark()?;

        assert_eq!(
            watermark,
            Watermark::new(
                0,
                DataType::Timestamptz,
                ScalarImpl::Timestamptz("2021-04-01T00:00:08.001Z".parse().unwrap())
            )
        );

        // Test case where epoch timestamp is smaller than adjusted timestamp
        // Send barrier at epoch 25 (timestamp 2021-04-01T00:00:00.025Z)
        // Adjusted timestamp would be: 8001 + (1000 * 2.0) = 10001ms = 2021-04-01T00:00:10.001Z
        // Since 10001 < 25, use adjusted timestamp
        tx.send(Barrier::with_prev_epoch_for_test(
            test_epoch(25000),
            test_epoch(20000),
        ))
        .unwrap();

        // Consume the barrier
        now.next_unwrap_ready_barrier()?;

        // Consume the data chunk
        let chunk_msg = now.next_unwrap_ready_chunk()?;

        assert_eq!(
            chunk_msg.compact_vis(),
            StreamChunk::from_pretty(
                " TZ
                - 2021-04-01T00:00:08.001Z
                + 2021-04-01T00:00:10.001Z" // adjusted timestamp
            )
        );

        // Consume the watermark
        let watermark = now.next_unwrap_ready_watermark()?;

        assert_eq!(
            watermark,
            Watermark::new(
                0,
                DataType::Timestamptz,
                ScalarImpl::Timestamptz("2021-04-01T00:00:10.001Z".parse().unwrap())
            )
        );

        // Finally test when epoch timestamp is larger than adjusted timestamp
        // Send barrier at epoch 30 (timestamp 2021-04-01T00:00:00.030Z)
        // Adjusted timestamp would be: 10001 + (1000 * 2.0) = 12001ms = 2021-04-01T00:00:12.001Z
        // Since 12001 < 30, use adjusted timestamp
        tx.send(Barrier::with_prev_epoch_for_test(
            test_epoch(30000),
            test_epoch(25000),
        ))
        .unwrap();

        // Consume the barrier
        now.next_unwrap_ready_barrier()?;

        // Consume the data chunk
        let chunk_msg = now.next_unwrap_ready_chunk()?;

        assert_eq!(
            chunk_msg.compact_vis(),
            StreamChunk::from_pretty(
                " TZ
                - 2021-04-01T00:00:10.001Z
                + 2021-04-01T00:00:12.001Z" // adjusted timestamp
            )
        );

        // Consume the watermark
        let watermark = now.next_unwrap_ready_watermark()?;

        assert_eq!(
            watermark,
            Watermark::new(
                0,
                DataType::Timestamptz,
                ScalarImpl::Timestamptz("2021-04-01T00:00:12.001Z".parse().unwrap())
            )
        );

        Ok(())
    }

    async fn test_now_generate_series_inner() -> StreamExecutorResult<()> {
        let start_timestamp = Timestamptz::from_secs(1617235190).unwrap(); // 2021-03-31 23:59:50 UTC
        let interval = Interval::from_millis(1000); // 1s interval

        let state_store = create_state_store();
        let (tx, mut now) = create_executor(
            NowMode::GenerateSeries {
                start_timestamp,
                interval,
            },
            &state_store,
        )
        .await;

        // Init barrier
        tx.send(Barrier::new_test_barrier(test_epoch(1000)))
            .unwrap();
        now.next_unwrap_ready_barrier()?;

        // Initial timestamps
        let chunk = now.next_unwrap_ready_chunk()?;
        assert_eq!(chunk.cardinality(), 12); // seconds from 23:59:50 to 00:00:01 (inclusive)

        assert_eq!(
            now.next_unwrap_ready_watermark()?,
            Watermark::new(
                0,
                DataType::Timestamptz,
                ScalarImpl::Timestamptz("2021-04-01T00:00:01.000Z".parse().unwrap())
            )
        );

        tx.send(Barrier::with_prev_epoch_for_test(
            test_epoch(2000),
            test_epoch(1000),
        ))
        .unwrap();
        tx.send(Barrier::with_prev_epoch_for_test(
            test_epoch(3000),
            test_epoch(2000),
        ))
        .unwrap();

        now.next_unwrap_ready_barrier()?;
        now.next_unwrap_ready_barrier()?;

        let chunk = now.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk.compact_vis(),
            StreamChunk::from_pretty(
                " TZ
                + 2021-04-01T00:00:02.000Z
                + 2021-04-01T00:00:03.000Z"
            )
        );

        let watermark = now.next_unwrap_ready_watermark()?;
        assert_eq!(
            watermark,
            Watermark::new(
                0,
                DataType::Timestamptz,
                ScalarImpl::Timestamptz("2021-04-01T00:00:03.000Z".parse().unwrap())
            )
        );

        // Recovery
        drop((tx, now));
        let (tx, mut now) = create_executor(
            NowMode::GenerateSeries {
                start_timestamp,
                interval,
            },
            &state_store,
        )
        .await;

        tx.send(Barrier::with_prev_epoch_for_test(
            test_epoch(4000),
            test_epoch(2000),
        ))
        .unwrap();

        now.next_unwrap_ready_barrier()?;

        let chunk = now.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk.compact_vis(),
            StreamChunk::from_pretty(
                " TZ
                + 2021-04-01T00:00:02.000Z
                + 2021-04-01T00:00:03.000Z
                + 2021-04-01T00:00:04.000Z"
            )
        );

        let watermark = now.next_unwrap_ready_watermark()?;
        assert_eq!(
            watermark,
            Watermark::new(
                0,
                DataType::Timestamptz,
                ScalarImpl::Timestamptz("2021-04-01T00:00:04.000Z".parse().unwrap())
            )
        );

        Ok(())
    }

    fn create_state_store() -> MemoryStateStore {
        MemoryStateStore::new()
    }

    async fn build_executor(
        mode: NowMode,
        state_store: &MemoryStateStore,
        progress_ratio: Option<f32>,
    ) -> (UnboundedSender<Barrier>, NowExecutor<MemoryStateStore>) {
        let table_id = TableId::new(1);
        let column_descs = vec![ColumnDesc::unnamed(ColumnId::new(0), DataType::Timestamptz)];
        let state_table = StateTable::from_table_catalog(
            &gen_pbtable(table_id, column_descs, vec![], vec![], 0),
            state_store.clone(),
            None,
        )
        .await;

        let (sender, barrier_receiver) = unbounded_channel();

        let eval_error_report = ActorEvalErrorReport {
            actor_context: ActorContext::for_test(123),
            identity: "NowExecutor".into(),
        };
        let barrier_interval_ms = 1000;
        let now_executor = NowExecutor::new(
            vec![DataType::Timestamptz],
            mode,
            eval_error_report,
            barrier_receiver,
            state_table,
            progress_ratio,
            barrier_interval_ms,
            Arc::new(StreamingMetrics::unused()),
            0.into(),
        );
        (sender, now_executor)
    }

    async fn create_executor_with_progress_ratio(
        mode: NowMode,
        state_store: &MemoryStateStore,
        progress_ratio: Option<f32>,
    ) -> (UnboundedSender<Barrier>, BoxedMessageStream) {
        let (sender, executor) = build_executor(mode, state_store, progress_ratio).await;
        (sender, executor.boxed().execute())
    }

    async fn create_executor(
        mode: NowMode,
        state_store: &MemoryStateStore,
    ) -> (UnboundedSender<Barrier>, BoxedMessageStream) {
        create_executor_with_progress_ratio(mode, state_store, None).await
    }
}

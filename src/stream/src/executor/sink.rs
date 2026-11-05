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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::mem;
use std::time::{Duration, Instant};

use anyhow::anyhow;
use futures::stream::select;
use futures::{FutureExt, TryFutureExt, TryStreamExt};
use itertools::Itertools;
use risingwave_common::array::Op;
use risingwave_common::array::stream_chunk::StreamChunkMut;
use risingwave_common::bitmap::Bitmap;
use risingwave_common::catalog::{ColumnCatalog, Field};
use risingwave_common::metrics::{GLOBAL_ERROR_METRICS, RelabeledAggregatedIntGauge};
use risingwave_common::row::RowExt;
use risingwave_common::util::retry::exponential_backoff;
use risingwave_common_estimate_size::EstimateSize;
use risingwave_common_estimate_size::collections::EstimatedVec;
use risingwave_common_rate_limit::RateLimit;
use risingwave_connector::dispatch_sink;
use risingwave_connector::sink::catalog::SinkId;
use risingwave_connector::sink::log_store::{
    FlushCurrentEpochOptions, LogReader, LogReaderExt, LogReaderMetrics, LogStoreFactory,
    LogWriter, LogWriterExt, LogWriterMetrics,
};
use risingwave_connector::sink::{
    GLOBAL_SINK_METRICS, LogSinker, SINK_USER_FORCE_COMPACTION,
    SINK_USER_PRESERVE_ROW_LEVEL_CHANGES, Sink, SinkImpl, SinkParam, SinkWriterParam,
};
use risingwave_pb::common::ThrottleType;
use risingwave_pb::id::FragmentId;
use thiserror_ext::AsReport;
use tokio::select;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio::sync::oneshot;
use tokio_retry::strategy::jitter;

use crate::executor::prelude::*;

mod compaction_plan;
use compaction_plan::{
    Compaction, CompactionConfig, CompactionPlan, EpochCompaction, EpochFinish, ReaderCompaction,
    WriterCompaction,
};

#[cfg(test)]
mod compaction_tests;

pub struct SinkExecutor<F: LogStoreFactory> {
    actor_context: ActorContextRef,
    input: Executor,
    sink: SinkImpl,
    input_columns: Vec<ColumnCatalog>,
    sink_param: SinkParam,
    log_store_factory: F,
    sink_writer_param: SinkWriterParam,
    chunk_size: usize,
    input_data_types: Vec<DataType>,
    compaction: CompactionPlan,
    rate_limit: Option<u32>,
}

const SINK_RETRY_BACKOFF_RESET_INTERVAL: Duration = Duration::from_secs(60);

fn sink_retry_backoff() -> impl Iterator<Item = Duration> {
    exponential_backoff(Duration::from_secs(1), 2, Duration::from_secs(30)).map(jitter)
}

// Drop all the DELETE messages in this chunk and convert UPDATE INSERT into INSERT.
fn force_append_only(c: StreamChunk) -> StreamChunk {
    let mut c: StreamChunkMut = c.into();
    for (_, mut r) in c.to_rows_mut() {
        match r.op() {
            Op::Insert => {}
            Op::Delete | Op::UpdateDelete => r.set_vis(false),
            Op::UpdateInsert => r.set_op(Op::Insert),
        }
    }
    c.into()
}

// Drop all the INSERT messages in this chunk and convert UPDATE DELETE into DELETE.
fn force_delete_only(c: StreamChunk) -> StreamChunk {
    let mut c: StreamChunkMut = c.into();
    for (_, mut r) in c.to_rows_mut() {
        match r.op() {
            Op::Delete => {}
            Op::Insert | Op::UpdateInsert => r.set_vis(false),
            Op::UpdateDelete => r.set_op(Op::Delete),
        }
    }
    c.into()
}

// Drop the DELETE messages whose downstream key is inserted again in the same barrier, since such
// a key is updated rather than deleted from the downstream's perspective.
fn drop_deletes_of_reinserted_keys(
    delete_chunks: Vec<StreamChunk>,
    insert_chunks: &[StreamChunk],
    downstream_pk: &[usize],
) -> Vec<StreamChunk> {
    let inserted_keys: HashSet<_> = insert_chunks
        .iter()
        .flat_map(|c| c.rows())
        .map(|(_, row)| row.project(downstream_pk))
        .collect();
    delete_chunks
        .into_iter()
        .map(|c| {
            let mut c: StreamChunkMut = c.into();
            for (row, mut r) in c.to_rows_mut() {
                if inserted_keys.contains(&row.project(downstream_pk)) {
                    r.set_vis(false);
                }
            }
            c.into()
        })
        .collect()
}

// Rewrite each update whose two rows differ in `key` to a delete and an insert.
fn split_updates_changing_key(c: StreamChunk, key: &[usize]) -> StreamChunk {
    let mut c: StreamChunkMut = c.into();
    let mut update_delete = None;
    for (row, mut r) in c.to_rows_mut() {
        match r.op() {
            Op::UpdateDelete => update_delete = Some((row, r)),
            Op::UpdateInsert => {
                if let Some((old_row, mut old)) = update_delete.take()
                    && !Row::eq(&old_row.project(key), row.project(key))
                {
                    old.set_op(Op::Delete);
                    r.set_op(Op::Insert);
                }
            }
            Op::Insert | Op::Delete => update_delete = None,
        }
    }
    c.into()
}

// The inserts of an epoch, with those of the rows gone at the barrier first and those of the rows
// still there last. A key of `live_by` still has a row if its last change is an insert.
fn live_rows_last(chunks: Vec<StreamChunk>, live_by: &Compaction) -> Vec<StreamChunk> {
    let live = (live_by.compact_inline(chunks.clone()).into_iter())
        .map(force_append_only)
        .collect_vec();
    let gone = chunks.into_iter().zip_eq(&live).map(|(chunk, live)| {
        let inserts = force_append_only(chunk);
        let vis = inserts.visibility() & &!live.visibility();
        inserts.clone_with_vis(vis)
    });
    let mut ordered = gone.collect_vec();
    ordered.extend(live);
    ordered
}

// Rebuild the visible rows of the chunks into chunks of `chunk_size` rows.
fn rebuild_chunks(
    chunks: impl IntoIterator<Item = StreamChunk>,
    chunk_size: usize,
    data_types: Vec<DataType>,
) -> Vec<StreamChunk> {
    let mut builder = StreamChunkBuilder::new(chunk_size, data_types);
    let mut rebuilt = vec![];
    for chunk in chunks {
        for (op, row) in chunk.rows() {
            rebuilt.extend(builder.append_row(op, row));
        }
    }
    rebuilt.extend(builder.take());
    rebuilt
}

impl<F: LogStoreFactory> SinkExecutor<F> {
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        actor_context: ActorContextRef,
        info: ExecutorInfo,
        input: Executor,
        sink_writer_param: SinkWriterParam,
        sink: SinkImpl,
        sink_param: SinkParam,
        columns: Vec<ColumnCatalog>,
        log_store_factory: F,
        chunk_size: usize,
        input_data_types: Vec<DataType>,
        rate_limit: Option<u32>,
    ) -> StreamExecutorResult<Self> {
        let sink_input_schema: Schema = columns
            .iter()
            .map(|column| Field::from(&column.column_desc))
            .collect();

        if let Some(col_dix) = sink_writer_param.extra_partition_col_idx {
            // Remove the partition column from the schema.
            assert_eq!(sink_input_schema.data_types(), {
                let mut data_type = info.schema.data_types();
                data_type.remove(col_dix);
                data_type
            });
        } else {
            assert_eq!(sink_input_schema.data_types(), info.schema.data_types());
        }

        let enabled = |key: &str| {
            (sink_param.properties.get(key)).is_some_and(|v| v.eq_ignore_ascii_case("true"))
        };
        let compaction = CompactionPlan::new(&CompactionConfig {
            sink_type: sink_param.sink_type,
            input_kind: input.stream_kind(),
            stream_key: &info.stream_key,
            downstream_pk: sink_param.downstream_pk.as_deref(),
            force_compaction: enabled(SINK_USER_FORCE_COMPACTION),
            preserve_row_level_changes: enabled(SINK_USER_PRESERVE_ROW_LEVEL_CHANGES),
            skip_writer_compaction: sink.is_blackhole(),
            ignore_delete: sink_param.ignore_delete,
            has_log_store: !sink.is_sink_into_table(),
            partition_col: sink_writer_param.extra_partition_col_idx,
        });

        tracing::info!(
            sink_id = %sink_param.sink_id,
            actor_id = %actor_context.id,
            ?compaction,
            "Sink executor info"
        );

        Ok(Self {
            actor_context,
            input,
            sink,
            input_columns: columns,
            sink_param,
            log_store_factory,
            sink_writer_param,
            chunk_size,
            input_data_types,
            compaction,
            rate_limit,
        })
    }

    fn execute_inner(self) -> BoxedMessageStream {
        let sink_id = self.sink_param.sink_id;
        let actor_id = self.actor_context.id;
        let fragment_id = self.actor_context.fragment_id;

        let metrics = self.actor_context.streaming_metrics.new_sink_exec_metrics(
            sink_id,
            actor_id,
            fragment_id,
        );

        let input = self.input.execute();

        let input = input.inspect_ok(move |msg| {
            if let Message::Chunk(c) = msg {
                metrics.sink_input_row_count.inc_by(c.capacity() as u64);
                metrics.sink_input_bytes.inc_by(c.estimated_size() as u64);
            }
        });

        let processed_input = Self::process_msg(
            input,
            self.compaction.drop_deletes_first,
            self.compaction.writer,
            self.chunk_size,
            self.input_data_types,
            metrics.sink_chunk_buffer_size,
        );

        if self.sink.is_sink_into_table() {
            // TODO(hzxa21): support rate limit?
            processed_input.boxed()
        } else {
            let labels = [
                &actor_id.to_string(),
                &sink_id.to_string(),
                self.sink_param.sink_name.as_str(),
            ];
            let log_store_first_write_epoch = GLOBAL_SINK_METRICS
                .log_store_first_write_epoch
                .with_guarded_label_values(&labels);
            let log_store_latest_write_epoch = GLOBAL_SINK_METRICS
                .log_store_latest_write_epoch
                .with_guarded_label_values(&labels);
            let log_store_write_rows = GLOBAL_SINK_METRICS
                .log_store_write_rows
                .with_guarded_label_values(&labels);
            let log_writer_metrics = LogWriterMetrics {
                log_store_first_write_epoch,
                log_store_latest_write_epoch,
                log_store_write_rows,
            };

            let (rate_limit_tx, rate_limit_rx) = unbounded_channel();
            // Init the rate limit
            rate_limit_tx.send(self.rate_limit.into()).unwrap();

            let (rebuild_sink_tx, rebuild_sink_rx) = unbounded_channel();

            self.log_store_factory
                .build()
                .map(move |(log_reader, log_writer)| {
                    let write_log_stream = Self::execute_write_log(
                        processed_input,
                        log_writer.monitored(log_writer_metrics),
                        actor_id,
                        fragment_id,
                        sink_id,
                        rate_limit_tx,
                        rebuild_sink_tx,
                    );

                    let consume_log_stream_future = dispatch_sink!(self.sink, sink, {
                        let consume_log_stream = Self::execute_consume_log(
                            *sink,
                            log_reader,
                            self.input_columns,
                            self.sink_param,
                            self.sink_writer_param,
                            self.compaction.reader,
                            self.actor_context,
                            rate_limit_rx,
                            rebuild_sink_rx,
                        )
                        .instrument_await(
                            await_tree::span!("consume_log (sink_id {sink_id})").long_running(),
                        )
                        .map_ok(|never| never); // unify return type to `Message`

                        consume_log_stream.boxed()
                    });
                    select(consume_log_stream_future.into_stream(), write_log_stream)
                })
                .into_stream()
                .flatten()
                .boxed()
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_write_log<W: LogWriter>(
        input: impl MessageStream,
        mut log_writer: W,
        actor_id: ActorId,
        fragment_id: FragmentId,
        sink_id: SinkId,
        rate_limit_tx: UnboundedSender<RateLimit>,
        rebuild_sink_tx: UnboundedSender<RebuildSinkMessage>,
    ) {
        pin_mut!(input);
        let barrier = expect_first_barrier(&mut input).await?;
        let epoch_pair = barrier.epoch;
        let is_pause_on_startup = barrier.is_pause_on_startup();
        // Propagate the first barrier
        yield Message::Barrier(barrier);

        log_writer.init(epoch_pair, is_pause_on_startup).await?;

        let mut is_paused = false;

        #[for_await]
        for msg in input {
            match msg? {
                Message::Watermark(w) => yield Message::Watermark(w),
                Message::Chunk(chunk) => {
                    assert!(
                        !is_paused,
                        "Actor {actor_id} should not receive any data after pause"
                    );
                    log_writer.write_chunk(chunk.clone()).await?;
                    yield Message::Chunk(chunk);
                }
                Message::Barrier(barrier) => {
                    let update_vnode_bitmap = barrier.as_update_vnode_bitmap(actor_id);
                    let schema_change = barrier.as_sink_schema_change(sink_id);
                    let wait_log_store_flush = barrier.should_flush_sink_log_store(sink_id);
                    if let Some(schema_change) = &schema_change {
                        info!(?schema_change, %sink_id, "sink receive schema change");
                    }
                    if wait_log_store_flush {
                        info!(%sink_id, "sink wait for log store flush");
                    }
                    let post_flush = log_writer
                        .flush_current_epoch(
                            barrier.epoch.curr,
                            FlushCurrentEpochOptions {
                                is_checkpoint: barrier.kind.is_checkpoint(),
                                new_vnode_bitmap: update_vnode_bitmap.clone(),
                                is_stop: barrier.is_stop(actor_id),
                                schema_change,
                                wait_log_store_flush,
                            },
                        )
                        .await?;

                    let mutation = barrier.mutation.clone();
                    yield Message::Barrier(barrier);
                    if F::REBUILD_SINK_ON_UPDATE_VNODE_BITMAP
                        && let Some(new_vnode_bitmap) = update_vnode_bitmap.clone()
                    {
                        let (tx, rx) = oneshot::channel();
                        rebuild_sink_tx
                            .send(RebuildSinkMessage::RebuildSink(new_vnode_bitmap, tx))
                            .map_err(|_| {
                                anyhow!("failed to send the rebuild-sink request to the reader")
                            })?;
                        rx.await
                            .map_err(|_| anyhow!("failed to wait for sink rebuild to finish"))?;
                    }
                    post_flush.post_yield_barrier().await?;

                    if let Some(mutation) = mutation.as_deref() {
                        match mutation {
                            Mutation::Pause => {
                                log_writer.pause()?;
                                is_paused = true;
                            }
                            Mutation::Resume => {
                                log_writer.resume()?;
                                is_paused = false;
                            }
                            Mutation::Throttle(fragment_to_apply) => {
                                if let Some(entry) = fragment_to_apply.get(&fragment_id)
                                    && entry.throttle_type() == ThrottleType::Sink
                                {
                                    tracing::info!(
                                        rate_limit = entry.rate_limit,
                                        "received sink rate limit on actor {actor_id}"
                                    );
                                    if let Err(e) = rate_limit_tx.send(entry.rate_limit.into()) {
                                        error!(
                                            error = %e.as_report(),
                                            "failed to send the sink rate limit update"
                                        );
                                        return Err(StreamExecutorError::from(
                                            e.to_report_string(),
                                        ));
                                    }
                                }
                            }
                            Mutation::ConnectorPropsChange(config) => {
                                if let Some(map) = config.get(&sink_id.as_raw_id())
                                    && let Err(e) = rebuild_sink_tx
                                        .send(RebuildSinkMessage::UpdateConfig(map.clone()))
                                {
                                    error!(
                                        error = %e.as_report(),
                                        "failed to send sink property updates"
                                    );
                                    return Err(StreamExecutorError::from(e.to_report_string()));
                                }
                            }
                            _ => (),
                        }
                    }
                }
            }
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn process_msg(
        input: impl MessageStream,
        drop_deletes_first: bool,
        compaction: WriterCompaction,
        chunk_size: usize,
        input_data_types: Vec<DataType>,
        sink_chunk_buffer_size_metrics: RelabeledAggregatedIntGauge,
    ) {
        let input = input.map_ok(move |msg| match msg {
            // Drop UPDATE/DELETE messages if specified `ignore_delete` (formerly `force_append_only`).
            Message::Chunk(chunk) if drop_deletes_first => Message::Chunk(force_append_only(chunk)),
            other => other,
        });
        match compaction {
            WriterCompaction::PerEpoch(epoch_compaction) => {
                let mut chunk_buffer = EstimatedVec::new();
                let mut watermark: Option<super::Watermark> = None;
                #[for_await]
                for msg in input {
                    match msg? {
                        Message::Watermark(w) => watermark = Some(w),
                        Message::Chunk(c) => {
                            chunk_buffer.push(c);
                            sink_chunk_buffer_size_metrics
                                .set(chunk_buffer.estimated_size() as i64);
                        }
                        Message::Barrier(barrier) => {
                            let chunks = mem::take(&mut chunk_buffer).into_inner();
                            match &epoch_compaction {
                                EpochCompaction::Once(compaction) => {
                                    for c in compaction.compact_reconstructed(
                                        chunks,
                                        chunk_size,
                                        input_data_types.clone(),
                                    ) {
                                        yield Message::Chunk(c);
                                    }
                                }
                                EpochCompaction::Reorder {
                                    by_stream_key,
                                    then,
                                } => {
                                    let mut delete_chunks = vec![];
                                    let mut insert_chunks = vec![];
                                    for c in by_stream_key.compact_inline(chunks) {
                                        let chunk = force_delete_only(c.clone());
                                        if chunk.cardinality() > 0 {
                                            delete_chunks.push(chunk);
                                        }
                                        let chunk = force_append_only(c);
                                        if chunk.cardinality() > 0 {
                                            insert_chunks.push(chunk);
                                        }
                                    }
                                    if let EpochFinish::ByDownstreamPk(by_downstream_pk) = then {
                                        let chunks = delete_chunks
                                            .into_iter()
                                            .chain(insert_chunks)
                                            .collect();
                                        for c in by_downstream_pk.compact_reconstructed(
                                            chunks,
                                            chunk_size,
                                            input_data_types.clone(),
                                        ) {
                                            yield Message::Chunk(c);
                                        }
                                    } else {
                                        if let EpochFinish::DropReinsertedDeletes {
                                            downstream_pk,
                                        } = then
                                        {
                                            delete_chunks = drop_deletes_of_reinserted_keys(
                                                delete_chunks,
                                                &insert_chunks,
                                                downstream_pk,
                                            );
                                        }
                                        for c in rebuild_chunks(
                                            delete_chunks.into_iter().chain(insert_chunks),
                                            chunk_size,
                                            input_data_types.clone(),
                                        ) {
                                            yield Message::Chunk(c);
                                        }
                                    }
                                }
                                EpochCompaction::LiveRowsLast { live_by, then } => {
                                    let chunks = live_rows_last(chunks, live_by);
                                    let chunks = match then {
                                        Some(by_downstream_pk) => by_downstream_pk
                                            .compact_reconstructed(
                                                chunks,
                                                chunk_size,
                                                input_data_types.clone(),
                                            ),
                                        None => rebuild_chunks(
                                            chunks,
                                            chunk_size,
                                            input_data_types.clone(),
                                        ),
                                    };
                                    for c in chunks {
                                        yield Message::Chunk(c);
                                    }
                                }
                            }
                            if let Some(w) = mem::take(&mut watermark) {
                                yield Message::Watermark(w)
                            }
                            yield Message::Barrier(barrier);
                        }
                    }
                }
            }
            WriterCompaction::PerChunk(compaction) =>
            {
                #[for_await]
                for msg in input {
                    match msg? {
                        Message::Chunk(chunk) => {
                            yield Message::Chunk(compaction.compact_chunk(chunk))
                        }
                        other => yield other,
                    }
                }
            }
            WriterCompaction::SplitUpdates { key } =>
            {
                #[for_await]
                for msg in input {
                    match msg? {
                        Message::Chunk(chunk) => {
                            yield Message::Chunk(split_updates_changing_key(chunk, &key))
                        }
                        other => yield other,
                    }
                }
            }
            WriterCompaction::None =>
            {
                #[for_await]
                for msg in input {
                    yield msg?;
                }
            }
        }
    }

    #[expect(clippy::too_many_arguments)]
    async fn execute_consume_log<S: Sink, R: LogReader>(
        mut sink: S,
        log_reader: R,
        columns: Vec<ColumnCatalog>,
        mut sink_param: SinkParam,
        mut sink_writer_param: SinkWriterParam,
        compaction: Option<ReaderCompaction>,
        actor_context: ActorContextRef,
        rate_limit_rx: UnboundedReceiver<RateLimit>,
        mut rebuild_sink_rx: UnboundedReceiver<RebuildSinkMessage>,
    ) -> StreamExecutorResult<!> {
        let mut visible_columns = columns
            .iter()
            .enumerate()
            .filter_map(|(idx, column)| (!column.is_hidden).then_some(idx))
            .collect_vec();

        let needs_projection = visible_columns.len() != columns.len();

        if needs_projection
            && let Some(extra_partition_col_idx) = sink_writer_param.extra_partition_col_idx
        {
            // The extra partition column is appended after all normal sink columns.
            debug_assert_eq!(extra_partition_col_idx, columns.len());
            sink_writer_param.extra_partition_col_idx = Some(visible_columns.len());
            visible_columns.push(extra_partition_col_idx);
        }

        let labels = [
            &actor_context.id.to_string(),
            sink_writer_param.connector.as_str(),
            &sink_writer_param.sink_id.to_string(),
            sink_writer_param.sink_name.as_str(),
        ];
        let log_store_reader_wait_new_future_duration_ns = GLOBAL_SINK_METRICS
            .log_store_reader_wait_new_future_duration_ns
            .with_guarded_label_values(&labels);
        let log_store_read_rows = GLOBAL_SINK_METRICS
            .log_store_read_rows
            .with_guarded_label_values(&labels);
        let log_store_read_bytes = GLOBAL_SINK_METRICS
            .log_store_read_bytes
            .with_guarded_label_values(&labels);
        let log_store_latest_read_epoch = GLOBAL_SINK_METRICS
            .log_store_latest_read_epoch
            .with_guarded_label_values(&labels);
        let metrics = LogReaderMetrics {
            log_store_latest_read_epoch,
            log_store_read_rows,
            log_store_read_bytes,
            log_store_reader_wait_new_future_duration_ns,
        };

        let mut log_reader = log_reader
            .transform_chunk(move |chunk| {
                let chunk = match &compaction {
                    Some(compaction) => compaction.compact_chunk(chunk),
                    None => chunk,
                };
                if needs_projection {
                    // Do projection here because we may have columns that aren't visible to
                    // the downstream.
                    chunk.project(&visible_columns)
                } else {
                    chunk
                }
            })
            .monitored(metrics)
            .rate_limited(rate_limit_rx);

        log_reader.init().await?;
        let mut retry_backoff = sink_retry_backoff();
        loop {
            // Boxed so that `drop` below releases its borrows on `log_reader` and `sink`.
            let mut future = Box::pin(async {
                loop {
                    let attempt_started_at = Instant::now();
                    let Err(e) = sink
                        .new_log_sinker(sink_writer_param.clone())
                        .and_then(|log_sinker| log_sinker.consume_log_and_sink(&mut log_reader))
                        .await;
                    let attempt_duration = attempt_started_at.elapsed();
                    GLOBAL_ERROR_METRICS.user_sink_error.report([
                        "sink_executor_error".to_owned(),
                        sink_param.sink_id.to_string(),
                        sink_param.sink_name.clone(),
                        actor_context.fragment_id.to_string(),
                    ]);

                    if let Some(meta_client) = sink_writer_param.meta_client.as_ref() {
                        meta_client
                            .add_sink_fail_evet_log(
                                sink_writer_param.sink_id,
                                sink_writer_param.sink_name.clone(),
                                sink_writer_param.connector.clone(),
                                e.to_report_string(),
                            )
                            .await;
                    }

                    if F::ALLOW_REWIND {
                        match log_reader.rewind().await {
                            Ok(()) => {
                                if attempt_duration >= SINK_RETRY_BACKOFF_RESET_INTERVAL {
                                    retry_backoff = sink_retry_backoff();
                                }
                                let retry_delay =
                                    retry_backoff.next().expect("retry strategy is infinite");
                                error!(
                                    error = %e.as_report(),
                                    executor_id = %sink_writer_param.executor_id,
                                    sink_id = %sink_param.sink_id,
                                    ?retry_delay,
                                    "reset log reader stream successfully after sink error; retrying after backoff"
                                );
                                tokio::time::sleep(retry_delay).await;
                                Ok(())
                            }
                            Err(rewind_err) => {
                                error!(
                                    error = %rewind_err.as_report(),
                                    "failed to rewind the log reader"
                                );
                                Err(e)
                            }
                        }
                    } else {
                        Err(e)
                    }
                    .map_err(|e| StreamExecutorError::from((e, sink_param.sink_id)))?;
                }
            });
            let message = loop {
                select! {
                    result = &mut future => {
                        let Err(e): StreamExecutorResult<!> = result;
                        return Err(e);
                    }
                    result = rebuild_sink_rx.recv() => {
                        let message = result.ok_or_else(|| anyhow!("failed to receive rebuild sink notify"))?;
                        // Dropping the consumer costs a rewind, or a recovery when the log
                        // reader cannot rewind. Not worth it for an update that changes nothing.
                        if let RebuildSinkMessage::UpdateConfig(config) = &message
                            && !sink_config_has_changes(&sink_param.properties, config)
                        {
                            info!(
                                executor_id = %sink_writer_param.executor_id,
                                sink_id = %sink_param.sink_id,
                                "skip alter sink config because properties are unchanged"
                            );
                            continue;
                        }
                        break message;
                    }
                }
            };
            drop(future);
            match message {
                RebuildSinkMessage::RebuildSink(new_vnode, notify) => {
                    sink_writer_param.vnode_bitmap = (*new_vnode).clone();
                    if notify.send(()).is_err() {
                        warn!("failed to notify rebuild sink");
                    }
                    log_reader.init().await?;
                }
                RebuildSinkMessage::UpdateConfig(config) => {
                    if F::ALLOW_REWIND {
                        match log_reader.rewind().await {
                            Ok(()) => {
                                sink_param.properties.extend(config);
                                sink = TryFrom::try_from(sink_param.clone()).map_err(|e| StreamExecutorError::from((e, sink_param.sink_id)))?;
                                info!(
                                    executor_id = %sink_writer_param.executor_id,
                                    sink_id = %sink_param.sink_id,
                                    "alter sink config successfully with rewind"
                                );
                                Ok(())
                            }
                            Err(rewind_err) => {
                                error!(
                                    error = %rewind_err.as_report(),
                                    "failed to rewind the log reader for ALTER SINK CONFIG"
                                );
                                Err(anyhow!("failed to rewind the log after ALTER SINK CONFIG").into())
                            }
                        }
                    } else {
                        sink_param.properties.extend(config);
                        sink = TryFrom::try_from(sink_param.clone()).map_err(|e| StreamExecutorError::from((e, sink_param.sink_id)))?;
                        Err(anyhow!("This is not an actual error condition. The system is intentionally triggering recovery procedures to ensure ALTER SINK CONFIG are fully applied.").into())
                    }
                    .map_err(|e| StreamExecutorError::from((e, sink_param.sink_id)))?;
                }
            }
        }
    }
}

enum RebuildSinkMessage {
    RebuildSink(Arc<Bitmap>, oneshot::Sender<()>),
    UpdateConfig(HashMap<String, String>),
}

fn sink_config_has_changes(
    current: &BTreeMap<String, String>,
    incoming: &HashMap<String, String>,
) -> bool {
    incoming
        .iter()
        .any(|(key, value)| current.get(key) != Some(value))
}

impl<F: LogStoreFactory> Execute for SinkExecutor<F> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.execute_inner()
    }
}

#[cfg(test)]
mod test {
    use std::future::pending;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use risingwave_common::catalog::{ColumnDesc, ColumnId};
    use risingwave_common::util::epoch::test_epoch;
    use risingwave_connector::sink::build_sink;
    use risingwave_connector::sink::catalog::SinkType;
    use risingwave_connector::sink::log_store::{LogStoreReadItem, LogStoreResult, TruncateOffset};
    use risingwave_connector::sink::trivial::BlackHoleSink;
    use tokio::sync::Notify;

    use super::*;
    use crate::common::log_store_impl::in_mem::{
        BoundedInMemLogStoreFactory, BoundedInMemLogStoreWriter,
    };
    use crate::executor::test_utils::*;

    #[derive(Default)]
    struct RewindRequiredLogReaderState {
        start_count: AtomicUsize,
        rewind_count: AtomicUsize,
        started: Notify,
    }

    impl RewindRequiredLogReaderState {
        async fn wait_for_start_count(&self, expected: usize) {
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    let started = self.started.notified();
                    if self.start_count.load(Ordering::SeqCst) >= expected {
                        break;
                    }
                    started.await;
                }
            })
            .await
            .unwrap_or_else(|_| panic!("log reader was not started {expected} times"));
        }
    }

    struct RewindRequiredLogReader {
        is_reset: bool,
        state: Arc<RewindRequiredLogReaderState>,
    }

    impl LogReader for RewindRequiredLogReader {
        async fn init(&mut self) -> LogStoreResult<()> {
            self.is_reset = true;
            Ok(())
        }

        async fn start_from(&mut self, _start_offset: Option<u64>) -> LogStoreResult<()> {
            assert!(
                self.is_reset,
                "log reader must be rewound before restarting"
            );
            self.is_reset = false;
            self.state.start_count.fetch_add(1, Ordering::SeqCst);
            self.state.started.notify_waiters();
            Ok(())
        }

        async fn next_item(&mut self) -> LogStoreResult<(u64, LogStoreReadItem)> {
            pending().await
        }

        fn truncate(&mut self, _offset: TruncateOffset) -> LogStoreResult<()> {
            Ok(())
        }

        async fn rewind(&mut self) -> LogStoreResult<()> {
            self.is_reset = true;
            self.state.rewind_count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    struct TestLogStoreFactory<const CAN_REWIND: bool>;

    impl<const CAN_REWIND: bool> LogStoreFactory for TestLogStoreFactory<CAN_REWIND> {
        type Reader = RewindRequiredLogReader;
        type Writer = BoundedInMemLogStoreWriter;

        const ALLOW_REWIND: bool = CAN_REWIND;
        const REBUILD_SINK_ON_UPDATE_VNODE_BITMAP: bool = false;

        async fn build(self) -> (Self::Reader, Self::Writer) {
            unreachable!()
        }
    }

    fn sink_param_for_config_update_test() -> SinkParam {
        SinkParam {
            sink_id: 0.into(),
            sink_name: "test".into(),
            properties: BTreeMap::from([
                ("connector".to_owned(), "blackhole".to_owned()),
                ("commit_checkpoint_interval".to_owned(), "1".to_owned()),
            ]),
            columns: vec![],
            downstream_pk: None,
            sink_type: SinkType::AppendOnly,
            ignore_delete: false,
            format_desc: None,
            db_name: "test".into(),
            sink_from_name: "test".into(),
        }
    }

    #[test]
    fn test_sink_config_has_changes() {
        let current = BTreeMap::from([
            ("connector".to_owned(), "blackhole".to_owned()),
            ("commit_checkpoint_interval".to_owned(), "1".to_owned()),
        ]);

        assert!(!sink_config_has_changes(
            &current,
            &HashMap::from([("commit_checkpoint_interval".to_owned(), "1".to_owned())])
        ));
        assert!(sink_config_has_changes(
            &current,
            &HashMap::from([("commit_checkpoint_interval".to_owned(), "2".to_owned())])
        ));
        assert!(sink_config_has_changes(
            &current,
            &HashMap::from([("force_append_only".to_owned(), "true".to_owned())])
        ));
    }

    fn no_op_config_update() -> RebuildSinkMessage {
        RebuildSinkMessage::UpdateConfig(HashMap::from([(
            "commit_checkpoint_interval".to_owned(),
            "1".to_owned(),
        )]))
    }

    fn changed_config_update() -> RebuildSinkMessage {
        RebuildSinkMessage::UpdateConfig(HashMap::from([(
            "commit_checkpoint_interval".to_owned(),
            "2".to_owned(),
        )]))
    }

    async fn assert_no_op_config_update_keeps_consuming<const CAN_REWIND: bool>() {
        let sink_param = sink_param_for_config_update_test();
        let sink = BlackHoleSink::try_from(sink_param.clone()).unwrap();
        let state = Arc::new(RewindRequiredLogReaderState::default());
        let log_reader = RewindRequiredLogReader {
            is_reset: false,
            state: state.clone(),
        };
        let (rate_limit_tx, rate_limit_rx) = unbounded_channel();
        let (rebuild_sink_tx, rebuild_sink_rx) = unbounded_channel();

        let consume_log = SinkExecutor::<TestLogStoreFactory<CAN_REWIND>>::execute_consume_log(
            sink,
            log_reader,
            vec![],
            sink_param,
            SinkWriterParam::for_test(),
            None,
            ActorContext::for_test(0),
            rate_limit_rx,
            rebuild_sink_rx,
        );
        tokio::pin!(consume_log);

        tokio::select! {
            result = &mut consume_log => panic!("log consumer exited unexpectedly: {result:?}"),
            () = state.wait_for_start_count(1) => {},
        }

        rebuild_sink_tx.send(no_op_config_update()).unwrap();

        // Messages are handled in order, so an acknowledged rebuild proves the consumer survived
        // the no-op update.
        let (notify_tx, notify_rx) = oneshot::channel();
        rebuild_sink_tx
            .send(RebuildSinkMessage::RebuildSink(
                Arc::new(Bitmap::ones(1)),
                notify_tx,
            ))
            .unwrap();
        tokio::select! {
            result = &mut consume_log => panic!("log consumer exited unexpectedly: {result:?}"),
            result = notify_rx => result.expect("rebuild sink should be acknowledged"),
        }

        assert_eq!(state.rewind_count.load(Ordering::SeqCst), 0);
        drop(rate_limit_tx);
    }

    #[tokio::test]
    async fn test_no_op_sink_config_update_keeps_consuming_with_rewind() {
        assert_no_op_config_update_keeps_consuming::<true>().await;
    }

    #[tokio::test]
    async fn test_no_op_sink_config_update_keeps_consuming_without_rewind() {
        assert_no_op_config_update_keeps_consuming::<false>().await;
    }

    #[tokio::test]
    async fn test_sink_config_update_rewinds_log_reader() {
        let sink_param = sink_param_for_config_update_test();
        let sink = BlackHoleSink::try_from(sink_param.clone()).unwrap();
        let state = Arc::new(RewindRequiredLogReaderState::default());
        let log_reader = RewindRequiredLogReader {
            is_reset: false,
            state: state.clone(),
        };
        let (rate_limit_tx, rate_limit_rx) = unbounded_channel();
        let (rebuild_sink_tx, rebuild_sink_rx) = unbounded_channel();

        let consume_log = SinkExecutor::<TestLogStoreFactory<true>>::execute_consume_log(
            sink,
            log_reader,
            vec![],
            sink_param,
            SinkWriterParam::for_test(),
            None,
            ActorContext::for_test(0),
            rate_limit_rx,
            rebuild_sink_rx,
        );
        tokio::pin!(consume_log);

        tokio::select! {
            result = &mut consume_log => panic!("log consumer exited unexpectedly: {result:?}"),
            () = state.wait_for_start_count(1) => {},
        }

        rebuild_sink_tx.send(changed_config_update()).unwrap();

        tokio::select! {
            result = &mut consume_log => panic!("log consumer exited unexpectedly: {result:?}"),
            () = state.wait_for_start_count(2) => {},
        }

        assert_eq!(state.rewind_count.load(Ordering::SeqCst), 1);
        drop(rate_limit_tx);
    }

    #[tokio::test]
    async fn test_sink_config_update_triggers_recovery_without_rewind() {
        let sink_param = sink_param_for_config_update_test();
        let sink = BlackHoleSink::try_from(sink_param.clone()).unwrap();
        let state = Arc::new(RewindRequiredLogReaderState::default());
        let log_reader = RewindRequiredLogReader {
            is_reset: false,
            state: state.clone(),
        };
        let (rate_limit_tx, rate_limit_rx) = unbounded_channel();
        let (rebuild_sink_tx, rebuild_sink_rx) = unbounded_channel();

        let consume_log = SinkExecutor::<TestLogStoreFactory<false>>::execute_consume_log(
            sink,
            log_reader,
            vec![],
            sink_param,
            SinkWriterParam::for_test(),
            None,
            ActorContext::for_test(0),
            rate_limit_rx,
            rebuild_sink_rx,
        );
        tokio::pin!(consume_log);

        tokio::select! {
            result = &mut consume_log => panic!("log consumer exited unexpectedly: {result:?}"),
            () = state.wait_for_start_count(1) => {},
        }

        rebuild_sink_tx.send(changed_config_update()).unwrap();

        tokio::time::timeout(Duration::from_secs(5), consume_log)
            .await
            .expect("log consumer should trigger recovery")
            .expect_err("log consumer should return an error");
        assert_eq!(state.start_count.load(Ordering::SeqCst), 1);
        assert_eq!(state.rewind_count.load(Ordering::SeqCst), 0);
        drop(rate_limit_tx);
    }

    #[test]
    fn test_sink_retry_backoff_is_bounded() {
        let mut retry_backoff = sink_retry_backoff();
        for max_delay in [1, 2, 4, 8, 16, 30, 30].map(Duration::from_secs) {
            assert!(retry_backoff.next().expect("retry strategy is infinite") <= max_delay);
        }
    }

    #[tokio::test]
    async fn test_force_append_only_sink() {
        use risingwave_common::array::StreamChunkTestExt;
        use risingwave_common::array::stream_chunk::StreamChunk;
        use risingwave_common::types::DataType;

        use crate::executor::Barrier;

        let properties = maplit::btreemap! {
            "connector".into() => "blackhole".into(),
            "type".into() => "append-only".into(),
            "force_append_only".into() => "true".into()
        };

        // We have two visible columns and one hidden column. The hidden column will be pruned out
        // within the sink executor.
        let columns = vec![
            ColumnCatalog {
                column_desc: ColumnDesc::unnamed(ColumnId::new(0), DataType::Int64),
                is_hidden: false,
            },
            ColumnCatalog {
                column_desc: ColumnDesc::unnamed(ColumnId::new(1), DataType::Int64),
                is_hidden: false,
            },
            ColumnCatalog {
                column_desc: ColumnDesc::unnamed(ColumnId::new(2), DataType::Int64),
                is_hidden: true,
            },
        ];
        let schema: Schema = columns
            .iter()
            .map(|column| Field::from(column.column_desc.clone()))
            .collect();
        let stream_key = vec![0];

        let source = MockSource::with_messages(vec![
            Message::Barrier(Barrier::new_test_barrier(test_epoch(1))),
            Message::Chunk(std::mem::take(&mut StreamChunk::from_pretty(
                " I I I
                    + 3 2 1",
            ))),
            Message::Barrier(Barrier::new_test_barrier(test_epoch(2))),
            Message::Chunk(std::mem::take(&mut StreamChunk::from_pretty(
                "  I I I
                    U- 3 2 1
                    U+ 3 4 1
                     + 5 6 7",
            ))),
            Message::Chunk(std::mem::take(&mut StreamChunk::from_pretty(
                " I I I
                    - 5 6 7",
            ))),
        ])
        .into_executor(schema.clone(), stream_key.clone());

        let sink_param = SinkParam {
            sink_id: 0.into(),
            sink_name: "test".into(),
            properties,

            columns: columns
                .iter()
                .filter(|col| !col.is_hidden)
                .map(|col| col.column_desc.clone())
                .collect(),
            downstream_pk: Some(stream_key.clone()),
            sink_type: SinkType::AppendOnly,
            ignore_delete: true,
            format_desc: None,
            db_name: "test".into(),
            sink_from_name: "test".into(),
        };

        let info = ExecutorInfo::for_test(schema, stream_key, "SinkExecutor".to_owned(), 0);

        let sink = build_sink(sink_param.clone()).unwrap();

        let sink_executor = SinkExecutor::new(
            ActorContext::for_test(0),
            info,
            source,
            SinkWriterParam::for_test(),
            sink,
            sink_param,
            columns.clone(),
            BoundedInMemLogStoreFactory::for_test(1),
            1024,
            vec![DataType::Int32, DataType::Int32, DataType::Int32],
            None,
        )
        .unwrap();

        let mut executor = sink_executor.boxed().execute();

        // Barrier message.
        executor.next().await.unwrap().unwrap();

        let chunk_msg = executor.next().await.unwrap().unwrap();
        assert_eq!(
            chunk_msg.into_chunk().unwrap().compact_vis(),
            StreamChunk::from_pretty(
                " I I I
                + 3 2 1",
            )
        );

        // Barrier message.
        executor.next().await.unwrap().unwrap();

        let chunk_msg = executor.next().await.unwrap().unwrap();
        assert_eq!(
            chunk_msg.into_chunk().unwrap().compact_vis(),
            StreamChunk::from_pretty(
                " I I I
                + 3 4 1
                + 5 6 7",
            )
        );

        // Should not receive the third stream chunk message because the force-append-only sink
        // executor will drop all DELETE messages.

        // The last barrier message.
        executor.next().await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_empty_barrier_sink() {
        use risingwave_common::types::DataType;

        use crate::executor::Barrier;

        let properties = maplit::btreemap! {
            "connector".into() => "blackhole".into(),
            "type".into() => "append-only".into(),
            "force_append_only".into() => "true".into()
        };
        let columns = vec![
            ColumnCatalog {
                column_desc: ColumnDesc::unnamed(ColumnId::new(0), DataType::Int64),
                is_hidden: false,
            },
            ColumnCatalog {
                column_desc: ColumnDesc::unnamed(ColumnId::new(1), DataType::Int64),
                is_hidden: false,
            },
        ];
        let schema: Schema = columns
            .iter()
            .map(|column| Field::from(column.column_desc.clone()))
            .collect();
        let stream_key = vec![0];

        let source = MockSource::with_messages(vec![
            Message::Barrier(Barrier::new_test_barrier(test_epoch(1))),
            Message::Barrier(Barrier::new_test_barrier(test_epoch(2))),
            Message::Barrier(Barrier::new_test_barrier(test_epoch(3))),
        ])
        .into_executor(schema.clone(), stream_key.clone());

        let sink_param = SinkParam {
            sink_id: 0.into(),
            sink_name: "test".into(),
            properties,

            columns: columns
                .iter()
                .filter(|col| !col.is_hidden)
                .map(|col| col.column_desc.clone())
                .collect(),
            downstream_pk: Some(stream_key.clone()),
            sink_type: SinkType::AppendOnly,
            ignore_delete: true,
            format_desc: None,
            db_name: "test".into(),
            sink_from_name: "test".into(),
        };

        let info = ExecutorInfo::for_test(schema, stream_key, "SinkExecutor".to_owned(), 0);

        let sink = build_sink(sink_param.clone()).unwrap();

        let sink_executor = SinkExecutor::new(
            ActorContext::for_test(0),
            info,
            source,
            SinkWriterParam::for_test(),
            sink,
            sink_param,
            columns,
            BoundedInMemLogStoreFactory::for_test(1),
            1024,
            vec![DataType::Int64, DataType::Int64],
            None,
        )
        .unwrap();

        let mut executor = sink_executor.boxed().execute();

        // Barrier message.
        assert_eq!(
            executor.next().await.unwrap().unwrap(),
            Message::Barrier(Barrier::new_test_barrier(test_epoch(1)))
        );

        // Barrier message.
        assert_eq!(
            executor.next().await.unwrap().unwrap(),
            Message::Barrier(Barrier::new_test_barrier(test_epoch(2)))
        );

        // The last barrier message.
        assert_eq!(
            executor.next().await.unwrap().unwrap(),
            Message::Barrier(Barrier::new_test_barrier(test_epoch(3)))
        );
    }
}

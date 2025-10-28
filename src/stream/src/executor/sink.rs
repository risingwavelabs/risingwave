// Copyright 2025 RisingWave Labs
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

use std::assert_matches::assert_matches;
use std::collections::HashMap;
use std::mem;

use anyhow::anyhow;
use futures::stream::select;
use futures::{FutureExt, TryFutureExt, TryStreamExt};
use itertools::Itertools;
use risingwave_common::array::Op;
use risingwave_common::array::stream_chunk::StreamChunkMut;
use risingwave_common::bitmap::Bitmap;
use risingwave_common::catalog::{ColumnCatalog, Field};
use risingwave_common::metrics::{GLOBAL_ERROR_METRICS, LabelGuardedIntGauge};
use risingwave_common_estimate_size::EstimateSize;
use risingwave_common_estimate_size::collections::EstimatedVec;
use risingwave_common_rate_limit::RateLimit;
use risingwave_connector::dispatch_sink;
use risingwave_connector::sink::catalog::{SinkId, SinkType};
use risingwave_connector::sink::log_store::{
    FlushCurrentEpochOptions, LogReader, LogReaderExt, LogReaderMetrics, LogStoreFactory,
    LogWriter, LogWriterExt, LogWriterMetrics,
};
use risingwave_connector::sink::{
    GLOBAL_SINK_METRICS, LogSinker, Sink, SinkImpl, SinkParam, SinkWriterParam,
};
use risingwave_pb::stream_plan::stream_node::StreamKind;
use thiserror_ext::AsReport;
use tokio::select;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio::sync::oneshot;

use crate::common::change_buffer::{OutputKind, output_kind};
use crate::common::compact_chunk::{
    InconsistencyBehavior, StreamChunkCompactor, compact_chunk_inline,
};
use crate::executor::prelude::*;
pub struct SinkExecutor<F: LogStoreFactory> {
    actor_context: ActorContextRef,
    info: ExecutorInfo,
    input: Executor,
    sink: SinkImpl,
    input_columns: Vec<ColumnCatalog>,
    sink_param: SinkParam,
    log_store_factory: F,
    sink_writer_param: SinkWriterParam,
    chunk_size: usize,
    input_data_types: Vec<DataType>,
    non_append_only_behavior: Option<NonAppendOnlyBehavior>,
    rate_limit: Option<u32>,
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

/// When the sink is non-append-only, i.e. upsert or retract, we need to do some extra work for
/// correctness and performance.
#[derive(Clone, Copy, Debug)]
struct NonAppendOnlyBehavior {
    /// Whether the user specifies a primary key for the sink, and it matches the derived stream
    /// key of the stream.
    ///
    /// By matching, we mean that stream key is a subset of the downstream pk.
    pk_specified_and_matched: bool,
}

impl NonAppendOnlyBehavior {
    /// NOTE(kwannoel):
    ///
    /// After the optimization in <https://github.com/risingwavelabs/risingwave/pull/12250>,
    /// `DELETE`s will be sequenced before `INSERT`s in JDBC sinks and PG rust sink.
    /// There's a risk that adjacent chunks with `DELETE`s on the same PK will get
    /// merged into a single chunk, since the logstore format doesn't preserve chunk
    /// boundaries. Then we will have double `DELETE`s followed by unspecified sequence
    /// of `INSERT`s, and lead to inconsistent data downstream.
    ///
    /// We only need to do the compaction for non-append-only sinks, when the upstream and
    /// downstream PKs are matched. When the upstream and downstream PKs are not matched,
    /// we will buffer the chunks between two barriers, so the compaction is not needed,
    /// since the barriers will preserve chunk boundaries.
    ///
    /// When the sink is an append-only sink, it is either `force_append_only` or
    /// `append_only`, we should only append to downstream, so there should not be any
    /// overlapping keys.
    fn should_compact_in_log_reader(self) -> bool {
        self.pk_specified_and_matched
    }

    /// When stream key is different from the user defined primary key columns for sinks.
    /// The operations could be out of order.
    ///
    /// For example, we have a stream with derived stream key `a, b` and user-specified sink
    /// primary key `a`. Assume that we have `(1, 1)` in the table. Then, we perform two `UPDATE`
    /// operations:
    ///
    /// ```text
    /// UPDATE SET b = 2 WHERE a = 1 ... which issues:
    ///   - (1, 1)
    ///   + (1, 2)
    ///
    /// UPDATE SET b = 3 WHERE a = 1 ... which issues:
    ///   - (1, 2)
    ///   + (1, 3)
    /// ```
    ///
    /// When these changes go into streaming pipeline, they could be shuffled to different parallelism
    /// (actor), given that they are under different stream keys.
    ///
    /// ```text
    /// Actor 1:
    /// - (1, 1)
    ///
    /// Actor 2:
    /// + (1, 2)
    /// - (1, 2)
    ///
    /// Actor 3:
    /// + (1, 3)
    /// ```
    ///
    /// When these records are merged back into sink actor, we may get the records from different
    /// parallelism in arbitrary order, like:
    ///
    /// ```text
    /// + (1, 2) -- Actor 2, first row
    /// + (1, 3) -- Actor 3
    /// - (1, 1) -- Actor 1
    /// - (1, 2) -- Actor 2, second row
    /// ```
    ///
    /// Note that in terms of stream key (`a, b`), the operations in the order above are completely
    /// correct, because we are operating on 3 different rows. However, in terms of user defined sink
    /// primary key `a`, we're violating the unique constraint all the time.
    ///
    /// Therefore, in this case, we have to do additional reordering in the sink executor per barrier.
    /// Specifically, we need to:
    ///
    /// First, compact all the changes with the stream key, so we have:
    /// ```text
    /// + (1, 3)
    /// - (1, 1)
    /// ```
    ///
    /// Then, sink all the delete events before sinking all insert events, so we have:
    /// ```text
    /// - (1, 1)
    /// + (1, 3)
    /// ```
    /// Since we've compacted the chunk with the stream key, the `DELETE` records survived must be to
    /// delete an existing row, so we can safely move them to the front. After the deletion is done,
    /// we can then safely sink the insert events with uniqueness guarantee.
    fn should_reorder_records(self) -> bool {
        !self.pk_specified_and_matched
    }
}

fn compact_output_kind(sink_type: SinkType) -> OutputKind {
    match sink_type {
        SinkType::AppendOnly | SinkType::ForceAppendOnly => {
            unreachable!("should not compact append-only sink")
        }
        SinkType::Upsert => output_kind::UPSERT,
        SinkType::Retract => output_kind::RETRACT,
    }
}

macro_rules! dispatch_output_kind {
    ($sink_type:expr, $KIND:ident, $body:tt) => {
        #[allow(unused_braces)]
        match compact_output_kind($sink_type) {
            output_kind::UPSERT => {
                const KIND: OutputKind = output_kind::UPSERT;
                $body
            }
            output_kind::RETRACT => {
                const KIND: OutputKind = output_kind::RETRACT;
                $body
            }
        }
    };
}

impl<F: LogStoreFactory> SinkExecutor<F> {
    #[allow(clippy::too_many_arguments)]
    #[expect(clippy::unused_async)]
    pub async fn new(
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

        let non_append_only_behavior = if !sink_param.sink_type.is_append_only() {
            let stream_key = &info.stream_key;
            let pk_specified_and_matched = (sink_param.downstream_pk.as_ref())
                .is_some_and(|downstream_pk| stream_key.iter().all(|i| downstream_pk.contains(i)));
            Some(NonAppendOnlyBehavior {
                pk_specified_and_matched,
            })
        } else {
            None
        };

        tracing::info!(
            sink_id = sink_param.sink_id.sink_id,
            actor_id = actor_context.id,
            ?non_append_only_behavior,
            "Sink executor info"
        );

        Ok(Self {
            actor_context,
            info,
            input,
            sink,
            input_columns: columns,
            sink_param,
            log_store_factory,
            sink_writer_param,
            chunk_size,
            input_data_types,
            non_append_only_behavior,
            rate_limit,
        })
    }

    fn execute_inner(self) -> BoxedMessageStream {
        let sink_id = self.sink_param.sink_id;
        let actor_id = self.actor_context.id;
        let fragment_id = self.actor_context.fragment_id;

        let stream_key = self.info.stream_key.clone();
        let metrics = self.actor_context.streaming_metrics.new_sink_exec_metrics(
            sink_id,
            actor_id,
            fragment_id,
        );

        // When processing upsert stream, we need to tolerate the inconsistency (mismatched `DELETE`
        // and `INSERT` pairs) when compacting input chunks with derived stream key.
        let input_compact_ib = if self.input.stream_kind() == StreamKind::Upsert {
            InconsistencyBehavior::Tolerate
        } else {
            InconsistencyBehavior::Panic
        };

        let input = self.input.execute();

        let input = input.inspect_ok(move |msg| {
            if let Message::Chunk(c) = msg {
                metrics.sink_input_row_count.inc_by(c.capacity() as u64);
                metrics.sink_input_bytes.inc_by(c.estimated_size() as u64);
            }
        });

        let processed_input = Self::process_msg(
            input,
            self.sink_param.sink_type,
            stream_key,
            self.chunk_size,
            self.input_data_types,
            input_compact_ib,
            self.sink_param.downstream_pk.clone(),
            self.non_append_only_behavior,
            metrics.sink_chunk_buffer_size,
            self.sink.is_blackhole(), // skip compact for blackhole for better benchmark results
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
                            self.non_append_only_behavior,
                            input_compact_ib,
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
                    let add_columns = barrier.as_sink_add_columns(sink_id);
                    if let Some(add_columns) = &add_columns {
                        info!(?add_columns, %sink_id, "sink receive add columns");
                    }
                    let post_flush = log_writer
                        .flush_current_epoch(
                            barrier.epoch.curr,
                            FlushCurrentEpochOptions {
                                is_checkpoint: barrier.kind.is_checkpoint(),
                                new_vnode_bitmap: update_vnode_bitmap.clone(),
                                is_stop: barrier.is_stop(actor_id),
                                add_columns,
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
                            .map_err(|_| anyhow!("fail to send rebuild sink to reader"))?;
                        rx.await
                            .map_err(|_| anyhow!("fail to wait rebuild sink finish"))?;
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
                            Mutation::Throttle(actor_to_apply) => {
                                if let Some(new_rate_limit) = actor_to_apply.get(&actor_id) {
                                    tracing::info!(
                                        rate_limit = new_rate_limit,
                                        "received sink rate limit on actor {actor_id}"
                                    );
                                    if let Err(e) = rate_limit_tx.send((*new_rate_limit).into()) {
                                        error!(
                                            error = %e.as_report(),
                                            "fail to send sink ate limit update"
                                        );
                                        return Err(StreamExecutorError::from(
                                            e.to_report_string(),
                                        ));
                                    }
                                }
                            }
                            Mutation::ConnectorPropsChange(config) => {
                                if let Some(map) = config.get(&sink_id.sink_id)
                                    && let Err(e) = rebuild_sink_tx
                                        .send(RebuildSinkMessage::UpdateConfig(map.clone()))
                                {
                                    error!(
                                        error = %e.as_report(),
                                        "fail to send sink alter props"
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

    #[allow(clippy::too_many_arguments)]
    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn process_msg(
        input: impl MessageStream,
        sink_type: SinkType,
        stream_key: StreamKey,
        chunk_size: usize,
        input_data_types: Vec<DataType>,
        input_compact_ib: InconsistencyBehavior,
        downstream_pk: Option<Vec<usize>>,
        non_append_only_behavior: Option<NonAppendOnlyBehavior>,
        sink_chunk_buffer_size_metrics: LabelGuardedIntGauge,
        skip_compact: bool,
    ) {
        // To reorder records, we need to buffer chunks of the entire epoch.
        if let Some(b) = non_append_only_behavior
            && b.should_reorder_records()
        {
            assert_matches!(sink_type, SinkType::Upsert | SinkType::Retract);

            let mut chunk_buffer = EstimatedVec::new();
            let mut watermark: Option<super::Watermark> = None;
            #[for_await]
            for msg in input {
                match msg? {
                    Message::Watermark(w) => watermark = Some(w),
                    Message::Chunk(c) => {
                        chunk_buffer.push(c);
                        sink_chunk_buffer_size_metrics.set(chunk_buffer.estimated_size() as i64);
                    }
                    Message::Barrier(barrier) => {
                        let chunks = mem::take(&mut chunk_buffer).into_inner();

                        // 1. Compact the chunk based on the **stream key**, so that we have at most 2 rows for each
                        //    stream key. Then, move all delete records to the front.
                        let mut delete_chunks = vec![];
                        let mut insert_chunks = vec![];

                        for c in dispatch_output_kind!(sink_type, KIND, {
                            StreamChunkCompactor::new(stream_key.clone(), chunks)
                                .into_compacted_chunks_inline::<KIND>(input_compact_ib)
                        }) {
                            let chunk = force_delete_only(c.clone());
                            if chunk.cardinality() > 0 {
                                delete_chunks.push(chunk);
                            }
                            let chunk = force_append_only(c);
                            if chunk.cardinality() > 0 {
                                insert_chunks.push(chunk);
                            }
                        }
                        let chunks = delete_chunks
                            .into_iter()
                            .chain(insert_chunks.into_iter())
                            .collect();

                        // 2. If user specifies a primary key, compact the chunk based on the **downstream pk**
                        //    to eliminate any unnecessary updates to external systems. This also rewrites the
                        //    `DELETE` and `INSERT` operations on the same key into `UPDATE` operations, which
                        //    usually have more efficient implementation.
                        if let Some(downstream_pk) = &downstream_pk {
                            let chunks = dispatch_output_kind!(sink_type, KIND, {
                                StreamChunkCompactor::new(downstream_pk.clone(), chunks)
                                    .into_compacted_chunks_reconstructed::<KIND>(
                                        chunk_size,
                                        input_data_types.clone(),
                                        // When compacting based on user provided primary key, we should never panic
                                        // on inconsistency in case the user provided primary key is not unique.
                                        InconsistencyBehavior::Warn,
                                    )
                            });
                            for c in chunks {
                                yield Message::Chunk(c);
                            }
                        } else {
                            let mut chunk_builder =
                                StreamChunkBuilder::new(chunk_size, input_data_types.clone());
                            for chunk in chunks {
                                for (op, row) in chunk.rows() {
                                    if let Some(c) = chunk_builder.append_row(op, row) {
                                        yield Message::Chunk(c);
                                    }
                                }
                            }

                            if let Some(c) = chunk_builder.take() {
                                yield Message::Chunk(c);
                            }
                        };

                        // 3. Forward watermark and barrier.
                        if let Some(w) = mem::take(&mut watermark) {
                            yield Message::Watermark(w)
                        }
                        yield Message::Barrier(barrier);
                    }
                }
            }
        } else {
            #[for_await]
            for msg in input {
                match msg? {
                    Message::Watermark(w) => yield Message::Watermark(w),
                    Message::Chunk(mut chunk) => {
                        // Compact the chunk to eliminate any unnecessary updates to external systems.
                        if !skip_compact {
                            chunk = dispatch_output_kind!(sink_type, KIND, {
                                compact_chunk_inline::<KIND>(chunk, &stream_key, input_compact_ib)
                            });
                        }
                        match sink_type {
                            SinkType::AppendOnly => yield Message::Chunk(chunk),
                            SinkType::ForceAppendOnly => {
                                // Force append-only by dropping UPDATE/DELETE messages. We do this when the
                                // user forces the sink to be append-only while it is actually not based on
                                // the frontend derivation result.
                                yield Message::Chunk(force_append_only(chunk))
                            }
                            SinkType::Upsert | SinkType::Retract => yield Message::Chunk(chunk),
                        }
                    }
                    Message::Barrier(barrier) => {
                        yield Message::Barrier(barrier);
                    }
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
        non_append_only_behavior: Option<NonAppendOnlyBehavior>,
        input_compact_ib: InconsistencyBehavior,
        actor_context: ActorContextRef,
        rate_limit_rx: UnboundedReceiver<RateLimit>,
        mut rebuild_sink_rx: UnboundedReceiver<RebuildSinkMessage>,
    ) -> StreamExecutorResult<!> {
        let visible_columns = columns
            .iter()
            .enumerate()
            .filter_map(|(idx, column)| (!column.is_hidden).then_some(idx))
            .collect_vec();

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

        let downstream_pk = sink_param.downstream_pk.clone();

        let mut log_reader = log_reader
            .transform_chunk(move |chunk| {
                let chunk = if let Some(b) = non_append_only_behavior
                    && b.should_compact_in_log_reader()
                {
                    // This guarantees that user has specified a `downstream_pk`.
                    let downstream_pk = downstream_pk.as_ref().unwrap();
                    dispatch_output_kind!(sink_param.sink_type, KIND, {
                        compact_chunk_inline::<KIND>(chunk, downstream_pk, input_compact_ib)
                    })
                } else {
                    chunk
                };
                if visible_columns.len() != columns.len() {
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
        loop {
            let future = async {
                loop {
                    let Err(e) = sink
                        .new_log_sinker(sink_writer_param.clone())
                        .and_then(|log_sinker| log_sinker.consume_log_and_sink(&mut log_reader))
                        .await;
                    GLOBAL_ERROR_METRICS.user_sink_error.report([
                        "sink_executor_error".to_owned(),
                        sink_param.sink_id.to_string(),
                        sink_param.sink_name.clone(),
                        actor_context.fragment_id.to_string(),
                    ]);

                    if let Some(meta_client) = sink_writer_param.meta_client.as_ref() {
                        meta_client
                            .add_sink_fail_evet_log(
                                sink_writer_param.sink_id.sink_id,
                                sink_writer_param.sink_name.clone(),
                                sink_writer_param.connector.clone(),
                                e.to_report_string(),
                            )
                            .await;
                    }

                    if F::ALLOW_REWIND {
                        match log_reader.rewind().await {
                            Ok(()) => {
                                error!(
                                    error = %e.as_report(),
                                    executor_id = sink_writer_param.executor_id,
                                    sink_id = sink_param.sink_id.sink_id,
                                    "reset log reader stream successfully after sink error"
                                );
                                Ok(())
                            }
                            Err(rewind_err) => {
                                error!(
                                    error = %rewind_err.as_report(),
                                    "fail to rewind log reader"
                                );
                                Err(e)
                            }
                        }
                    } else {
                        Err(e)
                    }
                    .map_err(|e| StreamExecutorError::from((e, sink_param.sink_id.sink_id)))?;
                }
            };
            select! {
                result = future => {
                    let Err(e): StreamExecutorResult<!> = result;
                    return Err(e);
                }
                result = rebuild_sink_rx.recv() => {
                    match result.ok_or_else(|| anyhow!("failed to receive rebuild sink notify"))? {
                        RebuildSinkMessage::RebuildSink(new_vnode, notify) => {
                            sink_writer_param.vnode_bitmap = Some((*new_vnode).clone());
                            if notify.send(()).is_err() {
                                warn!("failed to notify rebuild sink");
                            }
                            log_reader.init().await?;
                        },
                        RebuildSinkMessage::UpdateConfig(config) => {
                            if F::ALLOW_REWIND {
                                match log_reader.rewind().await {
                                    Ok(()) => {
                                        sink_param.properties.extend(config.into_iter());
                                        sink = TryFrom::try_from(sink_param.clone()).map_err(|e| StreamExecutorError::from((e, sink_param.sink_id.sink_id)))?;
                                        info!(
                                            executor_id = sink_writer_param.executor_id,
                                            sink_id = sink_param.sink_id.sink_id,
                                            "alter sink config successfully with rewind"
                                        );
                                        Ok(())
                                    }
                                    Err(rewind_err) => {
                                        error!(
                                            error = %rewind_err.as_report(),
                                            "fail to rewind log reader for alter sink config "
                                        );
                                        Err(anyhow!("fail to rewind log after alter table").into())
                                    }
                                }
                            } else {
                                sink_param.properties.extend(config.into_iter());
                                sink = TryFrom::try_from(sink_param.clone()).map_err(|e| StreamExecutorError::from((e, sink_param.sink_id.sink_id)))?;
                                Err(anyhow!("This is not an actual error condition. The system is intentionally triggering recovery procedures to ensure ALTER SINK CONFIG are fully applied.").into())
                            }
                            .map_err(|e| StreamExecutorError::from((e, sink_param.sink_id.sink_id)))?;
                        },
                    }
                }
            }
        }
    }
}

enum RebuildSinkMessage {
    RebuildSink(Arc<Bitmap>, oneshot::Sender<()>),
    UpdateConfig(HashMap<String, String>),
}

impl<F: LogStoreFactory> Execute for SinkExecutor<F> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.execute_inner()
    }
}

#[cfg(test)]
mod test {
    use risingwave_common::catalog::{ColumnDesc, ColumnId};
    use risingwave_common::util::epoch::test_epoch;
    use risingwave_connector::sink::build_sink;

    use super::*;
    use crate::common::log_store_impl::in_mem::BoundedInMemLogStoreFactory;
    use crate::executor::test_utils::*;

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
            sink_type: SinkType::ForceAppendOnly,
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
        .await
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
    async fn stream_key_sink_pk_mismatch() {
        use risingwave_common::array::StreamChunkTestExt;
        use risingwave_common::array::stream_chunk::StreamChunk;
        use risingwave_common::types::DataType;

        use crate::executor::Barrier;

        let properties = maplit::btreemap! {
            "connector".into() => "blackhole".into(),
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

        let source = MockSource::with_messages(vec![
            Message::Barrier(Barrier::new_test_barrier(test_epoch(1))),
            Message::Chunk(std::mem::take(&mut StreamChunk::from_pretty(
                " I I I
                    + 1 1 10",
            ))),
            Message::Barrier(Barrier::new_test_barrier(test_epoch(2))),
            Message::Chunk(std::mem::take(&mut StreamChunk::from_pretty(
                " I I I
                    + 1 3 30",
            ))),
            Message::Chunk(std::mem::take(&mut StreamChunk::from_pretty(
                " I I I
                    + 1 2 20
                    - 1 2 20",
            ))),
            Message::Chunk(std::mem::take(&mut StreamChunk::from_pretty(
                " I I I
                    - 1 1 10
                    + 1 1 40",
            ))),
            Message::Barrier(Barrier::new_test_barrier(test_epoch(3))),
        ])
        .into_executor(schema.clone(), vec![0, 1]);

        let sink_param = SinkParam {
            sink_id: 0.into(),
            sink_name: "test".into(),
            properties,

            columns: columns
                .iter()
                .filter(|col| !col.is_hidden)
                .map(|col| col.column_desc.clone())
                .collect(),
            downstream_pk: Some(vec![0]),
            sink_type: SinkType::Upsert,
            format_desc: None,
            db_name: "test".into(),
            sink_from_name: "test".into(),
        };

        let info = ExecutorInfo::for_test(schema, vec![0, 1], "SinkExecutor".to_owned(), 0);

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
            vec![DataType::Int64, DataType::Int64, DataType::Int64],
            None,
        )
        .await
        .unwrap();

        let mut executor = sink_executor.boxed().execute();

        // Barrier message.
        executor.next().await.unwrap().unwrap();

        let chunk_msg = executor.next().await.unwrap().unwrap();
        assert_eq!(
            chunk_msg.into_chunk().unwrap().compact_vis(),
            StreamChunk::from_pretty(
                " I I I
                + 1 1 10",
            )
        );

        // Barrier message.
        executor.next().await.unwrap().unwrap();

        let chunk_msg = executor.next().await.unwrap().unwrap();
        assert_eq!(
            chunk_msg.into_chunk().unwrap(),
            StreamChunk::from_pretty(
                " I I I
                + 1 1 40", // For upsert format, there won't be `U- 1 1 10`.
            )
        );

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
            sink_type: SinkType::ForceAppendOnly,
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
        .await
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

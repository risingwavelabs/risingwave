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
use thiserror_ext::AsReport;
use tokio::select;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio::sync::oneshot;

use crate::common::compact_chunk::{StreamChunkCompactor, merge_chunk_row};
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
    need_advance_delete: bool,
    re_construct_with_sink_pk: bool,
    compact_chunk: bool,
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

        let stream_key = info.pk_indices.clone();
        let stream_key_sink_pk_mismatch = {
            stream_key
                .iter()
                .any(|i| !sink_param.downstream_pk.contains(i))
        };
        // When stream key is different from the user defined primary key columns for sinks. The operations could be out of order
        // stream key: a,b
        // sink pk: a

        // original:
        // (1,1) -> (1,2)
        // (1,2) -> (1,3)

        // mv fragment 1:
        // delete (1,1)

        // mv fragment 2:
        // insert (1,2)
        // delete (1,2)

        // mv fragment 3:
        // insert (1,3)

        // merge to sink fragment:
        // insert (1,3)
        // insert (1,2)
        // delete (1,2)
        // delete (1,1)
        // So we do additional compaction in the sink executor per barrier.

        //     1. compact all the changes with the stream key.
        //     2. sink all the delete events and then sink all insert events.

        // after compacting with the stream key, the two event with the same user defined sink pk must have different stream key.
        // So the delete event is not to delete the inserted record in our internal streaming SQL semantic.
        let need_advance_delete =
            stream_key_sink_pk_mismatch && sink_param.sink_type != SinkType::AppendOnly;
        // NOTE(st1page): reconstruct with sink pk need extra cost to buffer a barrier's data, so currently we bind it with mismatch case.
        let re_construct_with_sink_pk = need_advance_delete
            && sink_param.sink_type == SinkType::Upsert
            && !sink_param.downstream_pk.is_empty();
        // Don't compact chunk for blackhole sink for better benchmark performance.
        let compact_chunk = !sink.is_blackhole();

        tracing::info!(
            sink_id = sink_param.sink_id.sink_id,
            actor_id = actor_context.id,
            need_advance_delete,
            re_construct_with_sink_pk,
            compact_chunk,
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
            need_advance_delete,
            re_construct_with_sink_pk,
            compact_chunk,
            rate_limit,
        })
    }

    fn execute_inner(self) -> BoxedMessageStream {
        let sink_id = self.sink_param.sink_id;
        let actor_id = self.actor_context.id;
        let fragment_id = self.actor_context.fragment_id;

        let stream_key = self.info.pk_indices.clone();
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
            self.sink_param.sink_type,
            stream_key,
            self.need_advance_delete,
            self.re_construct_with_sink_pk,
            self.chunk_size,
            self.input_data_types,
            self.sink_param.downstream_pk.clone(),
            metrics.sink_chunk_buffer_size,
            self.compact_chunk,
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
                    let post_flush = log_writer
                        .flush_current_epoch(
                            barrier.epoch.curr,
                            FlushCurrentEpochOptions {
                                is_checkpoint: barrier.kind.is_checkpoint(),
                                new_vnode_bitmap: update_vnode_bitmap.clone(),
                                is_stop: barrier.is_stop(actor_id),
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
                                if let Some(map) = config.get(&sink_id.sink_id) {
                                    if let Err(e) = rebuild_sink_tx
                                        .send(RebuildSinkMessage::UpdateConfig(map.clone()))
                                    {
                                        error!(
                                            error = %e.as_report(),
                                            "fail to send sink alter props"
                                        );
                                        return Err(StreamExecutorError::from(
                                            e.to_report_string(),
                                        ));
                                    }
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
        stream_key: PkIndices,
        need_advance_delete: bool,
        re_construct_with_sink_pk: bool,
        chunk_size: usize,
        input_data_types: Vec<DataType>,
        down_stream_pk: Vec<usize>,
        sink_chunk_buffer_size_metrics: LabelGuardedIntGauge,
        compact_chunk: bool,
    ) {
        // need to buffer chunks during one barrier
        if need_advance_delete || re_construct_with_sink_pk {
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
                        let chunks = if need_advance_delete {
                            let mut delete_chunks = vec![];
                            let mut insert_chunks = vec![];

                            for c in StreamChunkCompactor::new(stream_key.clone(), chunks)
                                .into_compacted_chunks()
                            {
                                if sink_type != SinkType::ForceAppendOnly {
                                    // Force append-only by dropping UPDATE/DELETE messages. We do this when the
                                    // user forces the sink to be append-only while it is actually not based on
                                    // the frontend derivation result.
                                    let chunk = force_delete_only(c.clone());
                                    if chunk.cardinality() > 0 {
                                        delete_chunks.push(chunk);
                                    }
                                }
                                let chunk = force_append_only(c);
                                if chunk.cardinality() > 0 {
                                    insert_chunks.push(chunk);
                                }
                            }
                            delete_chunks
                                .into_iter()
                                .chain(insert_chunks.into_iter())
                                .collect()
                        } else {
                            chunks
                        };
                        if re_construct_with_sink_pk {
                            let chunks = StreamChunkCompactor::new(down_stream_pk.clone(), chunks)
                                .reconstructed_compacted_chunks(
                                    chunk_size,
                                    input_data_types.clone(),
                                    sink_type != SinkType::ForceAppendOnly,
                                );
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
                        // Compact the chunk to eliminate any useless intermediate result (e.g. UPDATE
                        // V->V).
                        if compact_chunk {
                            chunk = merge_chunk_row(chunk, &stream_key);
                        }
                        match sink_type {
                            SinkType::AppendOnly => yield Message::Chunk(chunk),
                            SinkType::ForceAppendOnly => {
                                // Force append-only by dropping UPDATE/DELETE messages. We do this when the
                                // user forces the sink to be append-only while it is actually not based on
                                // the frontend derivation result.
                                yield Message::Chunk(force_append_only(chunk))
                            }
                            SinkType::Upsert => {}
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
                let chunk = if downstream_pk.is_empty() {
                    chunk
                } else {
                    merge_chunk_row(chunk, &downstream_pk)
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
                                warn!(
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
                                        sink_param.properties = config.into_iter().collect();
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
                                sink_param.properties = config.into_iter().collect();
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
        let pk_indices = vec![0];

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
        .into_executor(schema.clone(), pk_indices.clone());

        let sink_param = SinkParam {
            sink_id: 0.into(),
            sink_name: "test".into(),
            properties,

            columns: columns
                .iter()
                .filter(|col| !col.is_hidden)
                .map(|col| col.column_desc.clone())
                .collect(),
            downstream_pk: pk_indices.clone(),
            sink_type: SinkType::ForceAppendOnly,
            format_desc: None,
            db_name: "test".into(),
            sink_from_name: "test".into(),
        };

        let info = ExecutorInfo::new(schema, pk_indices, "SinkExecutor".to_owned(), 0);

        let sink = build_sink(sink_param.clone()).unwrap();

        let sink_executor = SinkExecutor::new(
            ActorContext::for_test(0),
            info,
            source,
            SinkWriterParam::for_test(),
            sink,
            sink_param,
            columns.clone(),
            BoundedInMemLogStoreFactory::new(1),
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
            chunk_msg.into_chunk().unwrap().compact(),
            StreamChunk::from_pretty(
                " I I I
                + 3 2 1",
            )
        );

        // Barrier message.
        executor.next().await.unwrap().unwrap();

        let chunk_msg = executor.next().await.unwrap().unwrap();
        assert_eq!(
            chunk_msg.into_chunk().unwrap().compact(),
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
            downstream_pk: vec![0],
            sink_type: SinkType::Upsert,
            format_desc: None,
            db_name: "test".into(),
            sink_from_name: "test".into(),
        };

        let info = ExecutorInfo::new(schema, vec![0, 1], "SinkExecutor".to_owned(), 0);

        let sink = build_sink(sink_param.clone()).unwrap();

        let sink_executor = SinkExecutor::new(
            ActorContext::for_test(0),
            info,
            source,
            SinkWriterParam::for_test(),
            sink,
            sink_param,
            columns.clone(),
            BoundedInMemLogStoreFactory::new(1),
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
            chunk_msg.into_chunk().unwrap().compact(),
            StreamChunk::from_pretty(
                " I I I
                + 1 1 10",
            )
        );

        // Barrier message.
        executor.next().await.unwrap().unwrap();

        let chunk_msg = executor.next().await.unwrap().unwrap();
        assert_eq!(
            chunk_msg.into_chunk().unwrap().compact(),
            StreamChunk::from_pretty(
                " I I I
                U- 1 1 10
                U+ 1 1 40",
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
        let pk_indices = vec![0];

        let source = MockSource::with_messages(vec![
            Message::Barrier(Barrier::new_test_barrier(test_epoch(1))),
            Message::Barrier(Barrier::new_test_barrier(test_epoch(2))),
            Message::Barrier(Barrier::new_test_barrier(test_epoch(3))),
        ])
        .into_executor(schema.clone(), pk_indices.clone());

        let sink_param = SinkParam {
            sink_id: 0.into(),
            sink_name: "test".into(),
            properties,

            columns: columns
                .iter()
                .filter(|col| !col.is_hidden)
                .map(|col| col.column_desc.clone())
                .collect(),
            downstream_pk: pk_indices.clone(),
            sink_type: SinkType::ForceAppendOnly,
            format_desc: None,
            db_name: "test".into(),
            sink_from_name: "test".into(),
        };

        let info = ExecutorInfo::new(schema, pk_indices, "SinkExecutor".to_owned(), 0);

        let sink = build_sink(sink_param.clone()).unwrap();

        let sink_executor = SinkExecutor::new(
            ActorContext::for_test(0),
            info,
            source,
            SinkWriterParam::for_test(),
            sink,
            sink_param,
            columns,
            BoundedInMemLogStoreFactory::new(1),
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

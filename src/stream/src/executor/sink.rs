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

use std::mem;

use anyhow::anyhow;
use futures::stream::select;
use futures::{pin_mut, FutureExt, StreamExt, TryFutureExt, TryStreamExt};
use futures_async_stream::try_stream;
use itertools::Itertools;
use risingwave_common::array::stream_chunk::StreamChunkMut;
use risingwave_common::array::{merge_chunk_row, Op, StreamChunk, StreamChunkCompactor};
use risingwave_common::catalog::{ColumnCatalog, Field, Schema};
use risingwave_common::metrics::GLOBAL_ERROR_METRICS;
use risingwave_common::types::DataType;
use risingwave_connector::dispatch_sink;
use risingwave_connector::sink::catalog::SinkType;
use risingwave_connector::sink::log_store::{
    LogReader, LogReaderExt, LogStoreFactory, LogWriter, LogWriterExt,
};
use risingwave_connector::sink::{
    build_sink, LogSinker, Sink, SinkImpl, SinkParam, SinkWriterParam,
};
use thiserror_ext::AsReport;

use super::error::{StreamExecutorError, StreamExecutorResult};
use super::{Execute, Executor, ExecutorInfo, Message, PkIndices};
use crate::executor::{
    expect_first_barrier, ActorContextRef, BoxedMessageStream, MessageStream, Mutation,
};
use crate::task::ActorId;

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
        sink_param: SinkParam,
        columns: Vec<ColumnCatalog>,
        log_store_factory: F,
        chunk_size: usize,
        input_data_types: Vec<DataType>,
    ) -> StreamExecutorResult<Self> {
        let sink = build_sink(sink_param.clone())?;
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
        })
    }

    fn execute_inner(self) -> BoxedMessageStream {
        let sink_id = self.sink_param.sink_id;
        let actor_id = self.actor_context.id;

        let stream_key = self.info.pk_indices.clone();

        let stream_key_sink_pk_mismatch = {
            stream_key
                .iter()
                .any(|i| !self.sink_param.downstream_pk.contains(i))
        };

        let input = self.input.execute();

        let input_row_count = self
            .actor_context
            .streaming_metrics
            .sink_input_row_count
            .with_guarded_label_values(&[
                &sink_id.to_string(),
                &actor_id.to_string(),
                &self.actor_context.fragment_id.to_string(),
            ]);

        let input = input.inspect_ok(move |msg| {
            if let Message::Chunk(c) = msg {
                input_row_count.inc_by(c.capacity() as u64);
            }
        });

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

        //     1. compact all the chanes with the stream key.
        //     2. sink all the delete events and then sink all insert evernt.

        // after compacting with the stream key, the two event with the same used defined sink pk must have different stream key.
        // So the delete event is not to delete the inserted record in our internal streaming SQL semantic.
        let need_advance_delete =
            stream_key_sink_pk_mismatch && self.sink_param.sink_type != SinkType::AppendOnly;

        let processed_input = Self::process_msg(
            input,
            self.sink_param.sink_type,
            stream_key,
            need_advance_delete,
            // NOTE(st1page): reconstruct with sink pk need extra cost to buffer a barrier's data, so currently we bind it with mismatch case.
            need_advance_delete,
            self.chunk_size,
            self.input_data_types,
        );

        if self.sink.is_sink_into_table() {
            processed_input.boxed()
        } else {
            self.log_store_factory
                .build()
                .map(move |(log_reader, log_writer)| {
                    let write_log_stream = Self::execute_write_log(
                        processed_input,
                        log_writer.monitored(self.sink_writer_param.sink_metrics.clone()),
                        actor_id,
                    );

                    dispatch_sink!(self.sink, sink, {
                        let consume_log_stream = Self::execute_consume_log(
                            sink,
                            log_reader,
                            self.input_columns,
                            self.sink_writer_param,
                            self.actor_context,
                            self.info,
                        );
                        // TODO: may try to remove the boxed
                        select(consume_log_stream.into_stream(), write_log_stream).boxed()
                    })
                })
                .into_stream()
                .flatten()
                .boxed()
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_write_log(
        input: impl MessageStream,
        mut log_writer: impl LogWriter,
        actor_id: ActorId,
    ) {
        pin_mut!(input);
        let barrier = expect_first_barrier(&mut input).await?;

        let epoch_pair = barrier.epoch;

        log_writer
            .init(epoch_pair, barrier.is_pause_on_startup())
            .await?;

        let mut is_paused = false;

        // Propagate the first barrier
        yield Message::Barrier(barrier);

        #[for_await]
        for msg in input {
            match msg? {
                Message::Watermark(w) => yield Message::Watermark(w),
                Message::Chunk(chunk) => {
                    assert!(!is_paused, "Should not receive any data after pause");
                    log_writer.write_chunk(chunk.clone()).await?;
                    yield Message::Chunk(chunk);
                }
                Message::Barrier(barrier) => {
                    log_writer
                        .flush_current_epoch(barrier.epoch.curr, barrier.kind.is_checkpoint())
                        .await?;

                    if let Some(mutation) = barrier.mutation.as_deref() {
                        match mutation {
                            Mutation::Pause => {
                                log_writer.pause()?;
                                is_paused = true;
                            }
                            Mutation::Resume => {
                                log_writer.resume()?;
                                is_paused = false;
                            }
                            _ => (),
                        }
                    }

                    if let Some(vnode_bitmap) = barrier.as_update_vnode_bitmap(actor_id) {
                        log_writer.update_vnode_bitmap(vnode_bitmap).await?;
                    }
                    yield Message::Barrier(barrier);
                }
            }
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn process_msg(
        input: impl MessageStream,
        sink_type: SinkType,
        stream_key: PkIndices,
        need_advance_delete: bool,
        re_construct_with_sink_pk: bool,
        chunk_size: usize,
        input_data_types: Vec<DataType>,
    ) {
        // need to buffer chunks during one barrier
        if need_advance_delete || re_construct_with_sink_pk {
            let mut chunk_buffer = vec![];
            let mut watermark: Option<super::Watermark> = None;
            #[for_await]
            for msg in input {
                match msg? {
                    Message::Watermark(w) => watermark = Some(w),
                    Message::Chunk(c) => {
                        chunk_buffer.push(c);
                    }
                    Message::Barrier(barrier) => {
                        let chunks = mem::take(&mut chunk_buffer);
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
                                    delete_chunks.push(force_delete_only(c.clone()));
                                }
                                insert_chunks.push(force_append_only(c));
                            }
                            delete_chunks
                                .into_iter()
                                .chain(insert_chunks.into_iter())
                                .collect()
                        } else {
                            chunks
                        };
                        let chunks = if re_construct_with_sink_pk {
                            StreamChunkCompactor::new(stream_key.clone(), chunks)
                                .reconstructed_compacted_chunks(
                                    chunk_size,
                                    input_data_types.clone(),
                                )
                        } else {
                            chunks
                        };

                        for c in chunks {
                            yield Message::Chunk(c);
                        }
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
                    Message::Chunk(chunk) => {
                        // Compact the chunk to eliminate any useless intermediate result (e.g. UPDATE
                        // V->V).
                        let chunk = merge_chunk_row(chunk, &stream_key);
                        let chunk = if sink_type == SinkType::ForceAppendOnly {
                            // Force append-only by dropping UPDATE/DELETE messages. We do this when the
                            // user forces the sink to be append-only while it is actually not based on
                            // the frontend derivation result.
                            force_append_only(chunk)
                        } else {
                            chunk
                        };

                        yield Message::Chunk(chunk);
                    }
                    Message::Barrier(barrier) => {
                        yield Message::Barrier(barrier);
                    }
                }
            }
        }
    }

    async fn execute_consume_log<S: Sink, R: LogReader>(
        sink: S,
        log_reader: R,
        columns: Vec<ColumnCatalog>,
        mut sink_writer_param: SinkWriterParam,
        actor_context: ActorContextRef,
        info: ExecutorInfo,
    ) -> StreamExecutorResult<Message> {
        let metrics = sink_writer_param.sink_metrics.clone();

        let visible_columns = columns
            .iter()
            .enumerate()
            .filter_map(|(idx, column)| (!column.is_hidden).then_some(idx))
            .collect_vec();

        let mut log_reader = log_reader
            .transform_chunk(move |chunk| {
                if visible_columns.len() != columns.len() {
                    // Do projection here because we may have columns that aren't visible to
                    // the downstream.
                    chunk.project(&visible_columns)
                } else {
                    chunk
                }
            })
            .monitored(metrics);

        log_reader.init().await?;

        while let Err(e) = sink
            .new_log_sinker(sink_writer_param.clone())
            .and_then(|log_sinker| log_sinker.consume_log_and_sink(&mut log_reader))
            .await
        {
            let mut err_str = e.to_report_string();
            if actor_context
                .error_suppressor
                .lock()
                .suppress_error(&err_str)
            {
                err_str = format!(
                    "error msg suppressed (due to per-actor error limit: {})",
                    actor_context.error_suppressor.lock().max()
                );
            }
            GLOBAL_ERROR_METRICS.user_sink_error.report([
                S::SINK_NAME.to_owned(),
                info.identity.clone(),
                err_str,
            ]);

            match log_reader.rewind().await {
                Ok((true, curr_vnode_bitmap)) => {
                    warn!(
                        error = %e.as_report(),
                        executor_id = sink_writer_param.executor_id,
                        "rewind successfully after sink error"
                    );
                    sink_writer_param.vnode_bitmap = curr_vnode_bitmap;
                    Ok(())
                }
                Ok((false, _)) => Err(e),
                Err(rewind_err) => {
                    error!(
                        error = %rewind_err.as_report(),
                        "fail to rewind log reader"
                    );
                    Err(e)
                }
            }?;
        }
        Err(anyhow!("end of stream").into())
    }
}

impl<F: LogStoreFactory> Execute for SinkExecutor<F> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.execute_inner()
    }
}

#[cfg(test)]
mod test {
    use risingwave_common::catalog::{ColumnDesc, ColumnId};

    use super::*;
    use crate::common::log_store_impl::in_mem::BoundedInMemLogStoreFactory;
    use crate::executor::test_utils::*;
    use crate::executor::ActorContext;

    #[tokio::test]
    async fn test_force_append_only_sink() {
        use risingwave_common::array::stream_chunk::StreamChunk;
        use risingwave_common::array::StreamChunkTestExt;
        use risingwave_common::types::DataType;

        use crate::executor::Barrier;

        let properties = maplit::hashmap! {
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
            Message::Barrier(Barrier::new_test_barrier(1)),
            Message::Chunk(std::mem::take(&mut StreamChunk::from_pretty(
                " I I I
                    + 3 2 1",
            ))),
            Message::Barrier(Barrier::new_test_barrier(2)),
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

        let info = ExecutorInfo {
            schema,
            pk_indices,
            identity: "SinkExecutor".to_string(),
        };

        let sink_executor = SinkExecutor::new(
            ActorContext::for_test(0),
            info,
            source,
            SinkWriterParam::for_test(),
            sink_param,
            columns.clone(),
            BoundedInMemLogStoreFactory::new(1),
            1024,
            vec![DataType::Int32, DataType::Int32, DataType::Int32],
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
        use risingwave_common::array::stream_chunk::StreamChunk;
        use risingwave_common::array::StreamChunkTestExt;
        use risingwave_common::types::DataType;

        use crate::executor::Barrier;

        let properties = maplit::hashmap! {
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
            Message::Barrier(Barrier::new_test_barrier(1)),
            Message::Chunk(std::mem::take(&mut StreamChunk::from_pretty(
                " I I I
                    + 1 1 10",
            ))),
            Message::Barrier(Barrier::new_test_barrier(2)),
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
            Message::Barrier(Barrier::new_test_barrier(3)),
        ])
        .into_executor(schema.clone(), vec![0, 1]);

        let sink_param = SinkParam {
            sink_id: 0.into(),
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

        let info = ExecutorInfo {
            schema,
            pk_indices: vec![0, 1],
            identity: "SinkExecutor".to_string(),
        };

        let sink_executor = SinkExecutor::new(
            ActorContext::for_test(0),
            info,
            source,
            SinkWriterParam::for_test(),
            sink_param,
            columns.clone(),
            BoundedInMemLogStoreFactory::new(1),
            1024,
            vec![DataType::Int64, DataType::Int64, DataType::Int64],
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
                U+ 1 1 40
                +  1 3 30",
            )
        );

        // The last barrier message.
        executor.next().await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_empty_barrier_sink() {
        use risingwave_common::types::DataType;

        use crate::executor::Barrier;

        let properties = maplit::hashmap! {
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
            Message::Barrier(Barrier::new_test_barrier(1)),
            Message::Barrier(Barrier::new_test_barrier(2)),
            Message::Barrier(Barrier::new_test_barrier(3)),
        ])
        .into_executor(schema.clone(), pk_indices.clone());

        let sink_param = SinkParam {
            sink_id: 0.into(),
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

        let info = ExecutorInfo {
            schema,
            pk_indices,
            identity: "SinkExecutor".to_string(),
        };

        let sink_executor = SinkExecutor::new(
            ActorContext::for_test(0),
            info,
            source,
            SinkWriterParam::for_test(),
            sink_param,
            columns,
            BoundedInMemLogStoreFactory::new(1),
            1024,
            vec![DataType::Int64, DataType::Int64],
        )
        .await
        .unwrap();

        let mut executor = sink_executor.boxed().execute();

        // Barrier message.
        assert_eq!(
            executor.next().await.unwrap().unwrap(),
            Message::Barrier(Barrier::new_test_barrier(1))
        );

        // Barrier message.
        assert_eq!(
            executor.next().await.unwrap().unwrap(),
            Message::Barrier(Barrier::new_test_barrier(2))
        );

        // The last barrier message.
        assert_eq!(
            executor.next().await.unwrap().unwrap(),
            Message::Barrier(Barrier::new_test_barrier(3))
        );
    }
}

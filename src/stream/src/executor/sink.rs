// Copyright 2023 RisingWave Labs
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
use futures::{FutureExt, StreamExt};
use futures_async_stream::try_stream;
use itertools::Itertools;
use risingwave_common::array::stream_chunk::StreamChunkMut;
use risingwave_common::array::{merge_chunk_row, Op, StreamChunk, StreamChunkCompactor};
use risingwave_common::catalog::{ColumnCatalog, Field, Schema};
use risingwave_common::metrics::GLOBAL_ERROR_METRICS;
use risingwave_connector::dispatch_sink;
use risingwave_connector::sink::catalog::{SinkId, SinkType};
use risingwave_connector::sink::log_store::{
    LogReader, LogReaderExt, LogStoreFactory, LogWriter, LogWriterExt,
};
use risingwave_connector::sink::{
    build_sink, LogSinker, Sink, SinkImpl, SinkParam, SinkWriterParam,
};

use super::error::{StreamExecutorError, StreamExecutorResult};
use super::{BoxedExecutor, Executor, Message, PkIndices};
use crate::executor::{expect_first_barrier, ActorContextRef, BoxedMessageStream};

pub struct SinkExecutor<F: LogStoreFactory> {
    input: BoxedExecutor,
    sink: SinkImpl,
    identity: String,
    pk_indices: PkIndices,
    input_columns: Vec<ColumnCatalog>,
    input_schema: Schema,
    sink_param: SinkParam,
    actor_context: ActorContextRef,
    log_reader: F::Reader,
    log_writer: F::Writer,
    sink_writer_param: SinkWriterParam,
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
    pub async fn new(
        input: BoxedExecutor,
        sink_writer_param: SinkWriterParam,
        sink_param: SinkParam,
        columns: Vec<ColumnCatalog>,
        actor_context: ActorContextRef,
        log_store_factory: F,
        pk_indices: PkIndices,
    ) -> StreamExecutorResult<Self> {
        let (log_reader, log_writer) = log_store_factory.build().await;

        let sink = build_sink(sink_param.clone())?;
        let input_schema = columns
            .iter()
            .map(|column| Field::from(&column.column_desc))
            .collect();
        Ok(Self {
            input,
            sink,
            identity: format!("SinkExecutor {:X?}", sink_writer_param.executor_id),
            pk_indices,
            input_columns: columns,
            input_schema,
            sink_param,
            actor_context,
            log_reader,
            log_writer,
            sink_writer_param,
        })
    }

    fn execute_inner(self) -> BoxedMessageStream {
        let stream_key = self.pk_indices;

        let stream_key_sink_pk_mismatch = {
            stream_key
                .iter()
                .any(|i| !self.sink_param.downstream_pk.contains(i))
        };

        let write_log_stream = Self::execute_write_log(
            self.input,
            stream_key,
            self.log_writer
                .monitored(self.sink_writer_param.sink_metrics.clone()),
            self.sink_param.sink_id,
            self.sink_param.sink_type,
            self.actor_context.clone(),
            stream_key_sink_pk_mismatch,
        );

        dispatch_sink!(self.sink, sink, {
            let consume_log_stream = Self::execute_consume_log(
                sink,
                self.log_reader,
                self.input_columns,
                self.sink_writer_param,
                self.actor_context,
            );
            select(consume_log_stream.into_stream(), write_log_stream).boxed()
        })
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_write_log(
        input: BoxedExecutor,
        stream_key: PkIndices,
        mut log_writer: impl LogWriter,
        sink_id: SinkId,
        sink_type: SinkType,
        actor_context: ActorContextRef,
        stream_key_sink_pk_mismatch: bool,
    ) {
        let mut input = input.execute();

        let barrier = expect_first_barrier(&mut input).await?;

        let epoch_pair = barrier.epoch;

        log_writer.init(epoch_pair).await?;

        // Propagate the first barrier
        yield Message::Barrier(barrier);

        // for metrics
        let sink_id_str = sink_id.to_string();
        let actor_id_str = actor_context.id.to_string();
        let fragment_id_str = actor_context.fragment_id.to_string();

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
        if stream_key_sink_pk_mismatch && sink_type != SinkType::AppendOnly {
            let mut chunk_buffer = StreamChunkCompactor::new(stream_key.clone());
            let mut watermark = None;
            #[for_await]
            for msg in input {
                match msg? {
                    Message::Watermark(w) => watermark = Some(w),
                    Message::Chunk(c) => {
                        actor_context
                            .streaming_metrics
                            .sink_input_row_count
                            .with_label_values(&[&sink_id_str, &actor_id_str, &fragment_id_str])
                            .inc_by(c.capacity() as u64);

                        chunk_buffer.push_chunk(c);
                    }
                    Message::Barrier(barrier) => {
                        let mut delete_chunks = vec![];
                        let mut insert_chunks = vec![];
                        for c in mem::replace(
                            &mut chunk_buffer,
                            StreamChunkCompactor::new(stream_key.clone()),
                        )
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

                        for c in delete_chunks.into_iter().chain(insert_chunks.into_iter()) {
                            log_writer.write_chunk(c.clone()).await?;
                            yield Message::Chunk(c);
                        }
                        if let Some(w) = mem::take(&mut watermark) {
                            yield Message::Watermark(w)
                        }
                        log_writer
                            .flush_current_epoch(barrier.epoch.curr, barrier.kind.is_checkpoint())
                            .await?;
                        if let Some(vnode_bitmap) = barrier.as_update_vnode_bitmap(actor_context.id)
                        {
                            log_writer.update_vnode_bitmap(vnode_bitmap).await?;
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

                        log_writer.write_chunk(chunk.clone()).await?;

                        // Use original chunk instead of the reordered one as the executor output.
                        yield Message::Chunk(chunk);
                    }
                    Message::Barrier(barrier) => {
                        log_writer
                            .flush_current_epoch(barrier.epoch.curr, barrier.kind.is_checkpoint())
                            .await?;
                        if let Some(vnode_bitmap) = barrier.as_update_vnode_bitmap(actor_context.id)
                        {
                            log_writer.update_vnode_bitmap(vnode_bitmap).await?;
                        }
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
        sink_writer_param: SinkWriterParam,
        actor_context: ActorContextRef,
    ) -> StreamExecutorResult<Message> {
        let metrics = sink_writer_param.sink_metrics.clone();
        let identity = format!("SinkExecutor {:X?}", sink_writer_param.executor_id);
        let log_sinker = sink.new_log_sinker(sink_writer_param).await?;

        let visible_columns = columns
            .iter()
            .enumerate()
            .filter_map(|(idx, column)| (!column.is_hidden).then_some(idx))
            .collect_vec();

        let log_reader = log_reader
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

        if let Err(e) = log_sinker.consume_log_and_sink(log_reader).await {
            let mut err_str = e.to_string();
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
                identity,
                err_str,
            ]);
            return Err(e.into());
        }
        Err(anyhow!("end of stream").into())
    }
}

impl<F: LogStoreFactory> Executor for SinkExecutor<F> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.execute_inner()
    }

    fn schema(&self) -> &Schema {
        &self.input_schema
    }

    fn pk_indices(&self) -> super::PkIndicesRef<'_> {
        &self.pk_indices
    }

    fn identity(&self) -> &str {
        &self.identity
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
        let pk = vec![0];

        let mock = MockSource::with_messages(
            schema,
            pk.clone(),
            vec![
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
            ],
        );

        let sink_param = SinkParam {
            sink_id: 0.into(),
            properties,
            columns: columns
                .iter()
                .filter(|col| !col.is_hidden)
                .map(|col| col.column_desc.clone())
                .collect(),
            downstream_pk: pk.clone(),
            sink_type: SinkType::ForceAppendOnly,
            format_desc: None,
            db_name: "test".into(),
            sink_from_name: "test".into(),
        };

        let sink_executor = SinkExecutor::new(
            Box::new(mock),
            SinkWriterParam::for_test(),
            sink_param,
            columns.clone(),
            ActorContext::create(0),
            BoundedInMemLogStoreFactory::new(1),
            pk,
        )
        .await
        .unwrap();

        let mut executor = SinkExecutor::execute(Box::new(sink_executor));

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

        let mock = MockSource::with_messages(
            schema,
            vec![0, 1],
            vec![
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
                    - 1 1 10",
                ))),
                Message::Barrier(Barrier::new_test_barrier(3)),
            ],
        );

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

        let sink_executor = SinkExecutor::new(
            Box::new(mock),
            SinkWriterParam::for_test(),
            sink_param,
            columns.clone(),
            ActorContext::create(0),
            BoundedInMemLogStoreFactory::new(1),
            vec![0, 1],
        )
        .await
        .unwrap();

        let mut executor = SinkExecutor::execute(Box::new(sink_executor));

        // Barrier message.
        executor.next().await.unwrap().unwrap();

        let chunk_msg = executor.next().await.unwrap().unwrap();
        assert_eq!(chunk_msg.into_chunk().unwrap().cardinality(), 0);

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
        assert_eq!(chunk_msg.into_chunk().unwrap().cardinality(), 0);
        let chunk_msg = executor.next().await.unwrap().unwrap();
        assert_eq!(chunk_msg.into_chunk().unwrap().cardinality(), 0);

        let chunk_msg = executor.next().await.unwrap().unwrap();
        assert_eq!(
            chunk_msg.into_chunk().unwrap().compact(),
            StreamChunk::from_pretty(
                " I I I
                - 1 1 10",
            )
        );

        let chunk_msg = executor.next().await.unwrap().unwrap();
        assert_eq!(
            chunk_msg.into_chunk().unwrap().compact(),
            StreamChunk::from_pretty(
                " I I I
                + 1 3 30",
            )
        );
        let chunk_msg = executor.next().await.unwrap().unwrap();
        assert_eq!(chunk_msg.into_chunk().unwrap().cardinality(), 0);
        let chunk_msg = executor.next().await.unwrap().unwrap();
        assert_eq!(chunk_msg.into_chunk().unwrap().cardinality(), 0);

        // Should not receive the third stream chunk message because the force-append-only sink
        // executor will drop all DELETE messages.

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
        let pk = vec![0];

        let mock = MockSource::with_messages(
            schema,
            pk.clone(),
            vec![
                Message::Barrier(Barrier::new_test_barrier(1)),
                Message::Barrier(Barrier::new_test_barrier(2)),
                Message::Barrier(Barrier::new_test_barrier(3)),
            ],
        );

        let sink_param = SinkParam {
            sink_id: 0.into(),
            properties,
            columns: columns
                .iter()
                .filter(|col| !col.is_hidden)
                .map(|col| col.column_desc.clone())
                .collect(),
            downstream_pk: pk.clone(),
            sink_type: SinkType::ForceAppendOnly,
            format_desc: None,
            db_name: "test".into(),
            sink_from_name: "test".into(),
        };

        let sink_executor = SinkExecutor::new(
            Box::new(mock),
            SinkWriterParam::for_test(),
            sink_param,
            columns,
            ActorContext::create(0),
            BoundedInMemLogStoreFactory::new(1),
            pk,
        )
        .await
        .unwrap();

        let mut executor = SinkExecutor::execute(Box::new(sink_executor));

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

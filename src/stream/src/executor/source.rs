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

use std::fmt::{Debug, Formatter};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use either::Either;
use futures::stream::{select_with_strategy, PollNext};
use futures::{Future, Stream, StreamExt};
use futures_async_stream::try_stream;
use risingwave_common::array::column::Column;
use risingwave_common::array::{ArrayBuilder, ArrayImpl, I64ArrayBuilder, StreamChunk};
use risingwave_common::catalog::{ColumnId, Field, Schema, TableId};
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::{Result, RwError, ToRwResult};
use risingwave_common::try_match_expand;
use risingwave_connector::{state, SplitImpl};
use risingwave_pb::stream_plan;
use risingwave_pb::stream_plan::stream_node::Node;
use risingwave_source::connector_source::ConnectorStreamSource;
use risingwave_source::*;
use risingwave_storage::{Keyspace, StateStore};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver};

use crate::executor::monitor::StreamingMetrics;
use crate::executor::{Executor, ExecutorBuilder, Message, PkIndices, PkIndicesRef};
use crate::task::{ExecutorParams, LocalStreamManagerCore};

struct SourceReader {
    /// the future that builds stream_reader. It is required because source should not establish
    /// connections to the upstream before `next` is called
    pub stream_reader_future: Option<StreamReaderFuture>,
    /// The reader for stream source
    pub stream_reader: Option<Box<dyn StreamSourceReader>>,
    /// The reader for barrier
    pub barrier_receiver: UnboundedReceiver<Message>,
}

/// `SourceReader` will be turned into this stream type.
type ReaderStream =
    Pin<Box<dyn Stream<Item = Either<Result<Message>, Result<StreamChunk>>> + Send>>;
type StreamReaderFuture =
    Pin<Box<dyn Future<Output = Result<Box<dyn StreamSourceReader>>> + Send + Sync>>;

/// [`SourceExecutor`] is a streaming source, from risingwave's batch table, or external systems
/// such as Kafka.
pub struct SourceExecutor {
    source_id: TableId,
    source_desc: SourceDesc,
    column_ids: Vec<ColumnId>,
    schema: Schema,
    pk_indices: PkIndices,

    /// current allocated row id
    next_row_id: AtomicU64,

    /// Identity string
    identity: String,

    /// Logical Operator Info
    op_info: String,

    /// The reader object. When `next` is called for the first time on `SourceExecutor`, the
    /// `reader` will be turned into `reader_stream`, and this field will become `None`.
    reader: Option<SourceReader>,

    /// Stream object for reader. When `next` is called for the first time on `SourceExecutor`, the
    /// `reader` will be turned into a `futures::Stream`.
    reader_stream: Option<ReaderStream>,

    // monitor
    metrics: Arc<StreamingMetrics>,

    /// Split info for stream source
    #[allow(dead_code)]
    stream_source_splits: Vec<SplitImpl>,

    source_identify: String,
}

pub struct SourceExecutorBuilder {}

impl ExecutorBuilder for SourceExecutorBuilder {
    fn new_boxed_executor(
        params: ExecutorParams,
        node: &stream_plan::StreamNode,
        store: impl StateStore,
        stream: &mut LocalStreamManagerCore,
    ) -> Result<Box<dyn Executor>> {
        let node = try_match_expand!(node.get_node().unwrap(), Node::SourceNode)?;
        let (sender, barrier_receiver) = unbounded_channel();
        stream
            .context
            .lock_barrier_manager()
            .register_sender(params.actor_id, sender);

        let source_id = TableId::from(&node.table_ref_id);
        let source_desc = params.env.source_manager().get_source(&source_id)?;

        let stream_source_splits = match &node.stream_source_state {
            Some(splits) => splits
                .stream_source_splits
                .iter()
                .map(|split| SplitImpl::restore_from_bytes(splits.get_split_type().clone(), split))
                .collect::<anyhow::Result<Vec<SplitImpl>>>()
                .to_rw_result(),
            _ => Ok(vec![]),
        }?;

        let column_ids: Vec<_> = node
            .get_column_ids()
            .iter()
            .map(|i| ColumnId::from(*i))
            .collect();
        let mut fields = Vec::with_capacity(column_ids.len());
        for &column_id in &column_ids {
            let column_desc = source_desc
                .columns
                .iter()
                .find(|c| c.column_id == column_id)
                .unwrap();
            fields.push(Field::with_name(
                column_desc.data_type.clone(),
                column_desc.name.clone(),
            ));
        }
        let schema = Schema::new(fields);
        let keyspace = Keyspace::executor_root(store, params.executor_id);

        Ok(Box::new(SourceExecutor::new(
            source_id,
            source_desc,
            keyspace,
            column_ids,
            schema,
            params.pk_indices,
            barrier_receiver,
            params.executor_id,
            params.operator_id,
            params.op_info,
            params.executor_stats,
            stream_source_splits,
        )?))
    }
}

async fn build_stream_reader<S: StateStore>(
    source: Arc<SourceImpl>,
    operator_id: u64,
    column_ids: Vec<ColumnId>,
    keyspace: Keyspace<S>,
) -> Result<Box<dyn StreamSourceReader>> {
    let stream_reader: Box<dyn StreamSourceReader> = match source.as_ref() {
        SourceImpl::HighLevelKafka(s) => Box::new(s.stream_reader(
            HighLevelKafkaSourceReaderContext {
                query_id: Some(format!("source-operator-{}", operator_id)),
                bound_timestamp_ms: None,
            },
            column_ids,
        )?),
        SourceImpl::TableV2(s) => Box::new(s.stream_reader(TableV2ReaderContext, column_ids)?),
        SourceImpl::Connector(s) => Box::new(ConnectorStreamSource {
            source_reader: s.clone(),
            state_store: state::SourceStateHandler::new(keyspace),
        }),
    };

    Ok(stream_reader)
}

impl SourceExecutor {
    #[allow(clippy::too_many_arguments)]
    pub fn new<S: StateStore>(
        source_id: TableId,
        source_desc: SourceDesc,
        keyspace: Keyspace<S>,
        column_ids: Vec<ColumnId>,
        schema: Schema,
        pk_indices: PkIndices,
        barrier_receiver: UnboundedReceiver<Message>,
        executor_id: u64,
        operator_id: u64,
        op_info: String,
        streaming_metrics: Arc<StreamingMetrics>,
        stream_source_splits: Vec<SplitImpl>,
    ) -> Result<Self> {
        let source = source_desc.clone().source;
        let stream_reader_future: StreamReaderFuture = Box::pin(build_stream_reader(
            source,
            operator_id,
            column_ids.clone(),
            keyspace,
        ));

        Ok(Self {
            source_id,
            source_desc,
            column_ids,
            schema,
            pk_indices,
            reader: Some(SourceReader {
                stream_reader_future: Some(stream_reader_future),
                stream_reader: None,
                barrier_receiver,
            }),
            next_row_id: AtomicU64::from(0u64),
            identity: format!("SourceExecutor {:X}", executor_id),
            op_info,
            reader_stream: None,
            metrics: streaming_metrics,
            stream_source_splits,
            source_identify: "Table_".to_string() + &source_id.table_id().to_string(),
        })
    }

    fn gen_row_column(&mut self, len: usize) -> Column {
        let mut builder = I64ArrayBuilder::new(len).unwrap();

        for _ in 0..len {
            builder
                .append(Some(self.next_row_id.fetch_add(1, Ordering::Relaxed) as i64))
                .unwrap();
        }

        Column::new(Arc::new(ArrayImpl::from(builder.finish().unwrap())))
    }

    fn refill_row_id_column(&mut self, chunk: StreamChunk) -> StreamChunk {
        if let Some(row_id_index) = self.source_desc.row_id_index {
            let row_id_column_id = self.source_desc.columns[row_id_index as usize].column_id;

            if let Some(idx) = self
                .column_ids
                .iter()
                .position(|column_id| *column_id == row_id_column_id)
            {
                let (ops, mut columns, bitmap) = chunk.into_inner();
                columns[idx] = self.gen_row_column(columns[idx].array().len());
                return StreamChunk::new(ops, columns, bitmap);
            }
        }
        chunk
    }
}

impl SourceReader {
    #[try_stream(ok = StreamChunk, error = RwError)]
    async fn stream_reader(mut stream_reader: Box<dyn StreamSourceReader>) {
        loop {
            match stream_reader.next().await {
                Err(e) => {
                    // TODO: report this error to meta service to mark the actors failed.
                    error!("hang up stream reader due to polling error: {}", e);

                    // Drop the reader, then the error might be caught by the writer side.
                    drop(stream_reader);
                    // Then hang up this stream by breaking the loop.
                    break;
                }
                Ok(chunk) => yield chunk,
            }
        }

        futures::future::pending().await
    }

    #[try_stream(ok = Message, error = RwError)]
    async fn barrier_receiver(mut barrier_receiver: UnboundedReceiver<Message>) {
        while let Some(msg) = barrier_receiver.recv().await {
            yield msg;
        }
        return Err(RwError::from(InternalError(
            "barrier reader closed unexpectedly".to_string(),
        )));
    }

    fn prio_left(_: &mut ()) -> PollNext {
        PollNext::Left
    }

    pub fn into_stream(self) -> impl Stream<Item = Either<Result<Message>, Result<StreamChunk>>> {
        let stream_reader = Self::stream_reader(self.stream_reader.unwrap());
        let barrier_receiver = Self::barrier_receiver(self.barrier_receiver);
        select_with_strategy(
            barrier_receiver.map(Either::Left),
            stream_reader.map(Either::Right),
            Self::prio_left,
        )
    }
}

#[async_trait]
impl Executor for SourceExecutor {
    async fn next(&mut self) -> Result<Message> {
        if let Some(mut reader) = self.reader.take() {
            reader
                .stream_reader
                .replace(reader.stream_reader_future.as_mut().unwrap().await?);
            reader.stream_reader.as_mut().unwrap().open().await?;
            self.reader_stream.replace(reader.into_stream().boxed());
        }

        match self.reader_stream.as_mut().unwrap().next().await {
            // This branch will be preferred.
            Some(Either::Left(message)) => message,

            // If there's barrier, this branch will be deferred.
            Some(Either::Right(chunk)) => {
                let mut chunk = chunk?;

                // Refill row id only if not a table source.
                // Note(eric): Currently, rows from external sources are filled with row_ids here,
                // but rows from tables (by insert statements) are filled in InsertExecutor.
                //
                // TODO: in the future, we may add row_id column here for TableV2 as well
                if !matches!(self.source_desc.source.as_ref(), SourceImpl::TableV2(_)) {
                    chunk = self.refill_row_id_column(chunk);
                }

                self.metrics
                    .source_output_row_count
                    .with_label_values(&[self.source_identify.as_str()])
                    .inc_by(chunk.cardinality() as u64);
                Ok(Message::Chunk(chunk))
            }

            None => unreachable!(),
        }
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn pk_indices(&self) -> PkIndicesRef {
        &self.pk_indices
    }

    fn identity(&self) -> &str {
        self.identity.as_str()
    }

    fn logical_operator_info(&self) -> &str {
        &self.op_info
    }
}

impl Debug for SourceExecutor {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SourceExecutor")
            .field("source_id", &self.source_id)
            .field("column_ids", &self.column_ids)
            .field("pk_indices", &self.pk_indices)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;

    use itertools::Itertools;
    use risingwave_common::array::column::Column;
    use risingwave_common::array::{ArrayImpl, I32Array, I64Array, Op, StreamChunk, Utf8Array};
    use risingwave_common::array_nonnull;
    use risingwave_common::catalog::{ColumnDesc, Field, Schema};
    use risingwave_common::types::DataType;
    use risingwave_source::*;
    use risingwave_storage::memory::MemoryStateStore;
    use tokio::sync::mpsc::unbounded_channel;

    use super::*;
    use crate::executor::{Barrier, Epoch, Mutation, SourceExecutor};

    #[tokio::test]
    async fn test_table_source() -> Result<()> {
        let table_id = TableId::default();

        let rowid_type = DataType::Int64;
        let col1_type = DataType::Int32;
        let col2_type = DataType::Varchar;

        let table_columns = vec![
            ColumnDesc {
                column_id: ColumnId::from(0),
                data_type: rowid_type.clone(),
                name: String::new(),
                field_descs: vec![],
                type_name: "".to_string(),
            },
            ColumnDesc {
                column_id: ColumnId::from(1),
                data_type: col1_type.clone(),
                name: String::new(),
                field_descs: vec![],
                type_name: "".to_string(),
            },
            ColumnDesc {
                column_id: ColumnId::from(2),
                data_type: col2_type.clone(),
                name: String::new(),
                field_descs: vec![],
                type_name: "".to_string(),
            },
        ];
        let source_manager = MemSourceManager::new();
        source_manager.create_table_source_v2(&table_id, table_columns)?;
        let source_desc = source_manager.get_source(&table_id)?;
        let source = source_desc.clone().source;

        // Prepare test data chunks
        let rowid_arr1: Arc<ArrayImpl> = Arc::new(array_nonnull! { I64Array, [0, 0, 0] }.into());
        let col1_arr1: Arc<ArrayImpl> = Arc::new(array_nonnull! { I32Array, [1, 2, 3] }.into());
        let col2_arr1: Arc<ArrayImpl> =
            Arc::new(array_nonnull! { Utf8Array, ["foo", "bar", "baz"] }.into());
        let rowid_arr2: Arc<ArrayImpl> = Arc::new(array_nonnull! { I64Array, [0, 0, 0] }.into());
        let col1_arr2: Arc<ArrayImpl> = Arc::new(array_nonnull! { I32Array, [4, 5, 6] }.into());
        let col2_arr2: Arc<ArrayImpl> =
            Arc::new(Utf8Array::from_slice(&[Some("hello"), None, Some("world")])?.into());

        let chunk1 = {
            let rowid = Column::new(rowid_arr1.clone());
            let col1 = Column::new(col1_arr1.clone());
            let col2 = Column::new(col2_arr1.clone());
            let vis = vec![Op::Insert, Op::Insert, Op::Insert];
            StreamChunk::new(vis, vec![rowid, col1, col2], None)
        };

        let chunk2 = {
            let rowid = Column::new(rowid_arr2.clone());
            let col1 = Column::new(col1_arr2.clone());
            let col2 = Column::new(col2_arr2.clone());
            let vis = vec![Op::Insert, Op::Insert, Op::Insert];
            StreamChunk::new(vis, vec![rowid, col1, col2], None)
        };

        let schema = Schema {
            fields: vec![
                Field::unnamed(rowid_type),
                Field::unnamed(col1_type),
                Field::unnamed(col2_type),
            ],
        };

        let column_ids = vec![0, 1, 2].into_iter().map(ColumnId::from).collect();
        let pk_indices = vec![0];

        let (barrier_sender, barrier_receiver) = unbounded_channel();
        let keyspace = Keyspace::executor_root(MemoryStateStore::new(), 0x2333);

        let mut source_executor = SourceExecutor::new(
            table_id,
            source_desc,
            keyspace,
            column_ids,
            schema,
            pk_indices,
            barrier_receiver,
            1,
            1,
            "SourceExecutor".to_string(),
            Arc::new(StreamingMetrics::new(prometheus::Registry::new())),
            vec![],
        )
        .unwrap();

        let write_chunk = |chunk: StreamChunk| {
            let source = source.clone();
            tokio::spawn(async move {
                let table_source = source.as_table_v2().unwrap();
                table_source.blocking_write_chunk(chunk).await.unwrap();
            });
        };

        barrier_sender
            .send(Message::Barrier(Barrier {
                epoch: Epoch::new_test_epoch(1),
                ..Barrier::default()
            }))
            .unwrap();

        // Write 1st chunk
        write_chunk(chunk1);

        for _ in 0..2 {
            match source_executor.next().await.unwrap() {
                Message::Chunk(chunk) => {
                    assert_eq!(3, chunk.columns().len());
                    assert_eq!(
                        col1_arr1.iter().collect_vec(),
                        chunk.column_at(1).array_ref().iter().collect_vec(),
                    );
                    assert_eq!(
                        col2_arr1.iter().collect_vec(),
                        chunk.column_at(2).array_ref().iter().collect_vec()
                    );
                    assert_eq!(vec![Op::Insert; 3], chunk.ops());
                }
                Message::Barrier(barrier) => {
                    assert_eq!(barrier.epoch, Epoch::new_test_epoch(1))
                }
            }
        }

        // Write 2nd chunk
        write_chunk(chunk2);

        if let Message::Chunk(chunk) = source_executor.next().await.unwrap() {
            assert_eq!(3, chunk.columns().len());
            assert_eq!(
                col1_arr2.iter().collect_vec(),
                chunk.column_at(1).array_ref().iter().collect_vec()
            );
            assert_eq!(
                col2_arr2.iter().collect_vec(),
                chunk.column_at(2).array_ref().iter().collect_vec()
            );
            assert_eq!(vec![Op::Insert; 3], chunk.ops());
        } else {
            unreachable!();
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_table_dropped() -> Result<()> {
        let table_id = TableId::default();

        let rowid_type = DataType::Int64;
        let col1_type = DataType::Int32;
        let col2_type = DataType::Varchar;

        let table_columns = vec![
            ColumnDesc {
                column_id: ColumnId::from(0),
                data_type: rowid_type.clone(),
                name: String::new(),
                field_descs: vec![],
                type_name: "".to_string(),
            },
            ColumnDesc {
                column_id: ColumnId::from(1),
                data_type: col1_type.clone(),
                name: String::new(),
                field_descs: vec![],
                type_name: "".to_string(),
            },
            ColumnDesc {
                column_id: ColumnId::from(2),
                data_type: col2_type.clone(),
                name: String::new(),
                field_descs: vec![],
                type_name: "".to_string(),
            },
        ];
        let source_manager = MemSourceManager::new();
        source_manager.create_table_source_v2(&table_id, table_columns)?;
        let source_desc = source_manager.get_source(&table_id)?;
        let source = source_desc.clone().source;

        // Prepare test data chunks
        let rowid_arr1: Arc<ArrayImpl> = Arc::new(array_nonnull! { I64Array, [0, 0, 0] }.into());
        let col1_arr1: Arc<ArrayImpl> = Arc::new(array_nonnull! { I32Array, [1, 2, 3] }.into());
        let col2_arr1: Arc<ArrayImpl> =
            Arc::new(array_nonnull! { Utf8Array, ["foo", "bar", "baz"] }.into());

        let chunk = {
            let rowid = Column::new(rowid_arr1.clone());
            let col1 = Column::new(col1_arr1.clone());
            let col2 = Column::new(col2_arr1.clone());
            let vis = vec![Op::Insert, Op::Insert, Op::Insert];
            StreamChunk::new(vis, vec![rowid, col1, col2], None)
        };

        let schema = Schema {
            fields: vec![
                Field::unnamed(rowid_type),
                Field::unnamed(col1_type),
                Field::unnamed(col2_type),
            ],
        };

        let column_ids = vec![0.into(), 1.into(), 2.into()];
        let pk_indices = vec![0];

        let (barrier_sender, barrier_receiver) = unbounded_channel();
        let keyspace = Keyspace::executor_root(MemoryStateStore::new(), 0x2333);
        let mut source_executor = SourceExecutor::new(
            table_id,
            source_desc,
            keyspace,
            column_ids,
            schema,
            pk_indices,
            barrier_receiver,
            1,
            1,
            "SourceExecutor".to_string(),
            Arc::new(StreamingMetrics::unused()),
            vec![],
        )
        .unwrap();

        let write_chunk = |chunk: StreamChunk| {
            let source = source.clone();
            tokio::spawn(async move {
                let table_source = source.as_table_v2().unwrap();
                table_source.blocking_write_chunk(chunk).await.unwrap();
            });
        };

        write_chunk(chunk.clone());

        barrier_sender
            .send(Message::Barrier(
                Barrier::new_test_barrier(1).with_mutation(Mutation::Stop(HashSet::default())),
            ))
            .unwrap();

        source_executor.next().await.unwrap();
        source_executor.next().await.unwrap();
        write_chunk(chunk);

        Ok(())
    }
}

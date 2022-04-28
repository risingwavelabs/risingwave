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
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use futures::stream::{select_with_strategy, PollNext};
use futures::{Stream, StreamExt};
use futures_async_stream::try_stream;
use risingwave_common::array::column::Column;
use risingwave_common::array::{ArrayBuilder, ArrayImpl, I64ArrayBuilder, StreamChunk};
use risingwave_common::catalog::{ColumnId, Field, Schema, TableId};
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::{Result, RwError, ToRwResult};
use risingwave_common::try_match_expand;
use risingwave_connector::SplitImpl;
use risingwave_pb::stream_plan;
use risingwave_pb::stream_plan::stream_node::Node;
use risingwave_source::*;
use risingwave_storage::{Keyspace, StateStore};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver};

use super::error::StreamExecutorError;
use super::{BoxedExecutor, BoxedMessageStream, Executor};
use crate::executor::monitor::StreamingMetrics;
use crate::executor::{Barrier, ExecutorBuilder, Message, PkIndices, PkIndicesRef};
use crate::task::{ExecutorParams, LocalStreamManagerCore};

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

    /// Receiver of barrier channel.
    barrier_receiver: Option<UnboundedReceiver<Barrier>>,

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
    ) -> Result<BoxedExecutor> {
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

impl SourceExecutor {
    #[allow(clippy::too_many_arguments)]
    pub fn new<S: StateStore>(
        source_id: TableId,
        source_desc: SourceDesc,
        _keyspace: Keyspace<S>,
        column_ids: Vec<ColumnId>,
        schema: Schema,
        pk_indices: PkIndices,
        barrier_receiver: UnboundedReceiver<Barrier>,
        executor_id: u64,
        _operator_id: u64,
        _op_info: String,
        streaming_metrics: Arc<StreamingMetrics>,
        stream_source_splits: Vec<SplitImpl>,
    ) -> Result<Self> {
        // todo(chen): dirty code to generate row_id start position
        let row_id_start = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        let row_id_start = row_id_start << 32;
        Ok(Self {
            source_id,
            source_desc,
            column_ids,
            schema,
            pk_indices,
            barrier_receiver: Some(barrier_receiver),
            // fixme(chen): may conflict
            next_row_id: AtomicU64::from(row_id_start),
            identity: format!("SourceExecutor {:X}", executor_id),
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

struct SourceReader {
    /// The reader for stream source
    stream_reader: Box<dyn StreamSourceReader>,
    /// The reader for barrier
    barrier_receiver: UnboundedReceiver<Barrier>,
}

impl SourceReader {
    #[try_stream(ok = Message, error = RwError)]
    async fn stream_reader(mut stream_reader: Box<dyn StreamSourceReader>) {
        loop {
            match stream_reader.next().await {
                Ok(chunk) => yield Message::Chunk(chunk),
                Err(e) => {
                    // TODO: report this error to meta service to mark the actors failed.
                    error!("hang up stream reader due to polling error: {}", e);

                    // Drop the reader, then the error might be caught by the writer side.
                    drop(stream_reader);
                    // Then hang up this stream by breaking the loop.
                    break;
                }
            }
        }

        futures::future::pending().await
    }

    #[try_stream(ok = Message, error = RwError)]
    async fn barrier_receiver(mut rx: UnboundedReceiver<Barrier>) {
        while let Some(barrier) = rx.recv().await {
            yield Message::Barrier(barrier);
        }
        return Err(RwError::from(InternalError(
            "barrier reader closed unexpectedly".to_string(),
        )));
    }

    fn into_stream(self) -> impl Stream<Item = Result<Message>> {
        select_with_strategy(
            Self::barrier_receiver(self.barrier_receiver),
            Self::stream_reader(self.stream_reader),
            |_: &mut ()| PollNext::Left, // perfer barrier
        )
    }
}

impl SourceExecutor {
    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn into_stream(mut self) {
        let mut barrier_receiver = self.barrier_receiver.take().unwrap();
        let barrier = barrier_receiver.recv().await.unwrap();

        // todo: use epoch from msg to restore state from state store
        let stream_reader = self
            .source_desc
            .source
            .stream_reader(
                match self.source_desc.source.as_ref() {
                    SourceImpl::TableV2(_) => SourceReaderContext::None(()),
                    SourceImpl::Connector(_c) => SourceReaderContext::ConnectorReaderContext(
                        self.stream_source_splits.clone(),
                    ),
                },
                self.column_ids.clone(),
            )
            .await
            .map_err(StreamExecutorError::source_error)?;

        let reader = SourceReader {
            stream_reader: Box::new(stream_reader),
            barrier_receiver,
        };
        yield Message::Barrier(barrier);

        #[for_await]
        for msg in reader.into_stream() {
            match msg.map_err(StreamExecutorError::source_error)? {
                // This branch will be preferred.
                Message::Barrier(barrier) => yield Message::Barrier(barrier),
                // If there's barrier, this branch will be deferred.
                Message::Chunk(mut chunk) => {
                    // Refill row id only if not a table source.
                    // Note(eric): Currently, rows from external sources are filled with row_ids
                    // here, but rows from tables (by insert statements)
                    // are filled in InsertExecutor.
                    //
                    // TODO: in the future, we may add row_id column here for TableV2 as well
                    if !matches!(self.source_desc.source.as_ref(), SourceImpl::TableV2(_)) {
                        chunk = self.refill_row_id_column(chunk);
                    }

                    self.metrics
                        .source_output_row_count
                        .with_label_values(&[self.source_identify.as_str()])
                        .inc_by(chunk.cardinality() as u64);
                    yield Message::Chunk(chunk);
                }
            }
        }
        unreachable!();
    }
}

impl Executor for SourceExecutor {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.into_stream().boxed()
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
    use std::sync::Arc;

    use futures::StreamExt;
    use risingwave_common::array::stream_chunk::StreamChunkTestExt;
    use risingwave_common::array::StreamChunk;
    use risingwave_common::catalog::{ColumnDesc, Field, Schema};
    use risingwave_common::types::DataType;
    use risingwave_source::*;
    use risingwave_storage::memory::MemoryStateStore;
    use tokio::sync::mpsc::unbounded_channel;

    use super::*;
    use crate::executor::{Barrier, Epoch};

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
        source_manager.create_table_source(&table_id, table_columns)?;
        let source_desc = source_manager.get_source(&table_id)?;
        let source = source_desc.clone().source;

        let chunk1 = StreamChunk::from_pretty(
            " I i T
            + 0 1 foo
            + 0 2 bar
            + 0 3 baz",
        );
        let chunk2 = StreamChunk::from_pretty(
            " I i T
            + 0 4 hello
            + 0 5 .
            + 0 6 world",
        );

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

        let executor = SourceExecutor::new(
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
        let mut executor = Box::new(executor).execute();

        let write_chunk = |chunk: StreamChunk| {
            let source = source.clone();
            tokio::spawn(async move {
                let table_source = source.as_table_v2().unwrap();
                table_source.blocking_write_chunk(chunk).await.unwrap();
            });
        };

        barrier_sender.send(Barrier::new_test_barrier(1)).unwrap();

        // Write 1st chunk
        write_chunk(chunk1);

        for _ in 0..2 {
            match executor.next().await.unwrap().unwrap() {
                Message::Chunk(chunk) => assert_eq!(
                    chunk,
                    StreamChunk::from_pretty(
                        " I i T
                        + 0 1 foo
                        + 0 2 bar
                        + 0 3 baz",
                    )
                ),
                Message::Barrier(barrier) => {
                    assert_eq!(barrier.epoch, Epoch::new_test_epoch(1))
                }
            }
        }

        // Write 2nd chunk
        write_chunk(chunk2);

        let msg = executor.next().await.unwrap().unwrap();
        assert_eq!(
            msg.into_chunk().unwrap(),
            StreamChunk::from_pretty(
                " I i T
                + 0 4 hello
                + 0 5 .
                + 0 6 world",
            )
        );

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
        source_manager.create_table_source(&table_id, table_columns)?;
        let source_desc = source_manager.get_source(&table_id)?;
        let source = source_desc.clone().source;

        // Prepare test data chunks
        let chunk = StreamChunk::from_pretty(
            " I i T
            + 0 1 foo
            + 0 2 bar
            + 0 3 baz",
        );

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
        let executor = SourceExecutor::new(
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
        let mut executor = Box::new(executor).execute();

        let write_chunk = |chunk: StreamChunk| {
            let source = source.clone();
            tokio::spawn(async move {
                let table_source = source.as_table_v2().unwrap();
                table_source.blocking_write_chunk(chunk).await.unwrap();
            });
        };

        write_chunk(chunk.clone());

        barrier_sender
            .send(Barrier::new_test_barrier(1).with_stop())
            .unwrap();

        executor.next().await.unwrap().unwrap();
        executor.next().await.unwrap().unwrap();
        write_chunk(chunk);

        Ok(())
    }
}

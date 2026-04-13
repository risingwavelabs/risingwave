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

use anyhow::Context;
use risingwave_common::array::{
    Array, ArrayBuilder, ArrayRef, DataChunk, I64ArrayBuilder, Op, Utf8ArrayBuilder,
};
use risingwave_common::row::{OwnedRow, Row, RowExt};
use risingwave_common::types::{Datum, ScalarImpl};
use risingwave_common::util::epoch::EpochPair;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_connector::sink::iceberg::common::{DataFileCommitMetadata, IcebergV3CommitPayload};
use risingwave_pb::connector_service::{SinkMetadata, sink_metadata};
use risingwave_rpc_client::CoordinatorStreamHandle;
use risingwave_storage::StateStore;

use super::CoordinatorStreamHandleInit;
use crate::executor::prelude::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowPosition {
    pub file_path: String,
    pub offset: i64,
}

/// Trait abstracting the Iceberg data file writing for testability.
///
/// Implementations are responsible for writing rows to Iceberg data files
/// and tracking row positions. Serialization of commit metadata is handled
/// by the executor, not the writer.
#[async_trait::async_trait]
pub trait IcebergWriter: Send + 'static {
    /// Write a batch of insert rows. Returns the position of each row in the chunk (in order).
    async fn write_chunk(&mut self, chunk: &DataChunk) -> StreamExecutorResult<Vec<RowPosition>>;

    /// Flush current data files on barrier. Returns the written data files
    /// and table metadata, or `None` if no files were written this epoch.
    async fn flush(&mut self) -> StreamExecutorResult<Option<DataFileCommitMetadata>>;
}

/// Writer Executor for Iceberg V3 Sink without Equality Delete.
///
/// This stateful executor maintains a PK index that maps primary key values to
/// their position in data files (`file_path`, `row_offset`). It processes change logs
/// from upstream:
///
/// - **Insert**: Writes the row to a data file via [`IcebergWriter`], records the
///   position in the PK index state table.
/// - **Delete**: Looks up the PK index to find the data file position, emits a
///   delete position message downstream to the DV Merger, removes from index.
/// - **Update**: Treated as Delete + Insert.
///
/// The output stream contains delete position messages with schema
/// `[file_path: Varchar, position: Int64]` that flow to the DV Merger executor.
pub struct WriterExecutor<S, W>
where
    S: StateStore,
    W: IcebergWriter,
{
    ctx: ActorContextRef,
    input: Option<Executor>,
    /// Column indices of the primary key in the input schema.
    pk_indices: Vec<usize>,
    /// State table storing the PK index: `pk_columns` -> (`file_path`, `row_position`).
    /// Schema: [`pk_col_0`, ..., `pk_col_n`, `file_path`: Varchar, position: Int64]
    pk_index_state_table: StateTable<S>,
    /// The Iceberg data file writer.
    writer: W,
    /// Handle for sending commit metadata (data files) to the coordinator.
    coordinator_handle: Option<CoordinatorStreamHandle>,
    coordinator_handle_init: Option<CoordinatorStreamHandleInit>,
}

impl<S, W> WriterExecutor<S, W>
where
    S: StateStore,
    W: IcebergWriter,
{
    pub fn new(
        ctx: ActorContextRef,
        input: Executor,
        pk_indices: Vec<usize>,
        pk_index_state_table: StateTable<S>,
        writer: W,
        coordinator_handle_init: Option<CoordinatorStreamHandleInit>,
    ) -> Self {
        Self {
            ctx,
            input: Some(input),
            pk_indices,
            pk_index_state_table,
            writer,
            coordinator_handle: None,
            coordinator_handle_init,
        }
    }

    async fn initialize_coordinator_handle(
        &mut self,
        first_epoch: EpochPair,
    ) -> StreamExecutorResult<()> {
        let Some(init) = self.coordinator_handle_init.take() else {
            return Ok(());
        };

        let (mut handle, log_store_rewind_start_epoch) = init.create_handle().await?;
        if log_store_rewind_start_epoch.is_none() {
            handle.align_initial_epoch(first_epoch.curr).await?;
        }
        self.coordinator_handle = Some(handle);
        Ok(())
    }

    /// Build an output chunk containing delete position messages for the DV Merger.
    /// Output schema: [`file_path`: Varchar, position: Int64]
    fn build_delete_position_chunk(positions: &[RowPosition]) -> StreamExecutorResult<StreamChunk> {
        let len = positions.len();
        let mut file_path_builder = Utf8ArrayBuilder::new(len);
        let mut position_builder = I64ArrayBuilder::new(len);
        let ops = vec![Op::Insert; len];

        for pos in positions {
            file_path_builder.append(Some(&pos.file_path));
            position_builder.append(Some(pos.offset));
        }

        let columns: Vec<ArrayRef> = vec![
            file_path_builder.finish().into_ref(),
            position_builder.finish().into_ref(),
        ];

        Ok(StreamChunk::from_parts(ops, DataChunk::new(columns, len)))
    }

    /// Process a single stream chunk. Returns delete position messages to emit downstream.
    async fn process_chunk(
        &mut self,
        chunk: StreamChunk,
    ) -> StreamExecutorResult<Option<StreamChunk>> {
        let chunk = chunk.compact_vis();

        // Collect insert and delete rows
        let mut insert_rows: Vec<OwnedRow> = Vec::new();
        let mut delete_pks: Vec<OwnedRow> = Vec::new();

        for (op, row) in chunk.rows() {
            match op {
                Op::Insert | Op::UpdateInsert => {
                    insert_rows.push(row.to_owned_row());
                }
                Op::Delete | Op::UpdateDelete => {
                    let pk = row.project(&self.pk_indices).to_owned_row();
                    delete_pks.push(pk);
                }
            }
        }

        // Process deletes: look up PK index to get file positions
        let mut delete_positions = Vec::new();
        for pk in &delete_pks {
            if let Some(index_row) = self.pk_index_state_table.get_row(pk).await? {
                // The state table value columns are [file_path, position]
                // get_row returns the full row including pk columns.
                let num_cols = index_row.len();
                let file_path = index_row
                    .datum_at(num_cols - 2)
                    .map(|d| d.into_utf8().to_owned())
                    .unwrap_or_default();
                let offset = index_row
                    .datum_at(num_cols - 1)
                    .map(|d| d.into_int64())
                    .unwrap_or(0);

                let pos = RowPosition {
                    file_path: file_path.clone(),
                    offset,
                };

                // Build the full stored row for deletion: [pk_cols..., file_path, position]
                let mut stored_row_data: Vec<Datum> = Vec::new();
                for datum in pk.iter() {
                    stored_row_data.push(datum.map(ScalarImpl::from));
                }
                stored_row_data.push(Some(ScalarImpl::Utf8(file_path.into())));
                stored_row_data.push(Some(ScalarImpl::Int64(offset)));
                self.pk_index_state_table
                    .delete(OwnedRow::new(stored_row_data));

                delete_positions.push(pos);
            }
        }

        // Process inserts: write data and record in PK index
        if !insert_rows.is_empty() {
            let insert_chunk = DataChunk::from_rows(&insert_rows, &chunk.data_chunk().data_types());
            let positions = self.writer.write_chunk(&insert_chunk).await?;
            assert_eq!(positions.len(), insert_rows.len());

            // Insert into PK index state table
            for (row, pos) in insert_rows.iter().zip_eq_fast(positions.iter()) {
                let pk = row.project(&self.pk_indices);
                let mut index_row_data: Vec<Datum> = Vec::new();
                for datum in pk.iter() {
                    index_row_data.push(datum.map(ScalarImpl::from));
                }
                index_row_data.push(Some(ScalarImpl::Utf8(pos.file_path.clone().into())));
                index_row_data.push(Some(ScalarImpl::Int64(pos.offset)));
                self.pk_index_state_table
                    .insert(OwnedRow::new(index_row_data));
            }
        }

        self.pk_index_state_table.try_flush().await?;

        // Emit delete positions downstream if any
        if delete_positions.is_empty() {
            Ok(None)
        } else {
            Ok(Some(Self::build_delete_position_chunk(&delete_positions)?))
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(mut self) {
        let mut input = self.input.take().unwrap().execute();

        // Consume the first barrier.
        // IMPORTANT: yield the barrier BEFORE init_epoch and coordinator setup, because for
        // newly created streaming jobs the internal tables and sink coordinator are only
        // registered after the first barrier is collected by the meta service (via `commit_epoch`).
        // Calling init_epoch or coordinator setup before yielding would deadlock.
        let barrier = expect_first_barrier(&mut input).await?;
        let first_epoch = barrier.epoch;
        yield Message::Barrier(barrier);
        self.pk_index_state_table.init_epoch(first_epoch).await?;
        self.initialize_coordinator_handle(first_epoch).await?;

        #[for_await]
        for msg in input {
            match msg? {
                Message::Chunk(chunk) => {
                    if let Some(delete_pos_chunk) = self.process_chunk(chunk).await? {
                        yield Message::Chunk(delete_pos_chunk);
                    }
                }
                Message::Barrier(barrier) => {
                    let flush_result = self.writer.flush().await?;
                    let epoch = barrier.epoch;
                    let update_vnode_bitmap = barrier.as_update_vnode_bitmap(self.ctx.id);
                    let post_commit = self.pk_index_state_table.commit(epoch).await?;

                    yield Message::Barrier(barrier);

                    // Serialize DataFileCommitMetadata into IcebergV3CommitPayload
                    // bytes for the coordinator.
                    let metadata_bytes = match flush_result {
                        Some(data_file_meta) => {
                            let payload = IcebergV3CommitPayload::DataFiles(data_file_meta);
                            serde_json::to_vec(&payload).context(
                                "fail to serialize DataFileCommitMetadata for coordinator",
                            )?
                        }
                        None => Vec::new(),
                    };

                    let sink_metadata = SinkMetadata {
                        metadata: Some(sink_metadata::Metadata::Serialized(
                            risingwave_pb::connector_service::sink_metadata::SerializedMetadata {
                                metadata: metadata_bytes,
                            },
                        )),
                    };
                    if let Some(handle) = &mut self.coordinator_handle {
                        handle
                            .commit(epoch.curr, sink_metadata, None)
                            .await
                            .map_err(|e| e.context("coordinator commit failed"))?;
                    }

                    post_commit.post_yield_barrier(update_vnode_bitmap).await?;
                }
                Message::Watermark(w) => {
                    yield Message::Watermark(w);
                }
            }
        }
    }
}

impl<S, W> Execute for WriterExecutor<S, W>
where
    S: StateStore,
    W: IcebergWriter,
{
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.execute_inner().boxed()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use risingwave_common::array::stream_chunk::StreamChunkTestExt;
    use risingwave_common::catalog::{ColumnDesc, ColumnId, Field, TableId};
    use risingwave_common::types::DataType;
    use risingwave_common::util::epoch::test_epoch;
    use risingwave_common::util::sort_util::OrderType;
    use risingwave_storage::memory::MemoryStateStore;

    use super::*;
    use crate::common::table::test_utils::gen_pbtable;
    use crate::executor::test_utils::MockSource;

    /// A mock writer that assigns incrementing positions within a single file.
    struct IcebergWriterMock {
        file_path: String,
        next_offset: i64,
        written_chunks: Arc<Mutex<Vec<StreamChunk>>>,
    }

    impl IcebergWriterMock {
        fn new(file_path: &str) -> Self {
            Self {
                file_path: file_path.to_string(),
                next_offset: 0,
                written_chunks: Arc::new(Mutex::new(Vec::new())),
            }
        }
    }

    #[async_trait::async_trait]
    impl IcebergWriter for IcebergWriterMock {
        async fn write_chunk(
            &mut self,
            chunk: &DataChunk,
        ) -> StreamExecutorResult<Vec<RowPosition>> {
            let n = chunk.cardinality();
            let mut positions = Vec::with_capacity(n);
            for _ in 0..n {
                positions.push(RowPosition {
                    file_path: self.file_path.clone(),
                    offset: self.next_offset,
                });
                self.next_offset += 1;
            }
            self.written_chunks
                .lock()
                .unwrap()
                .push(StreamChunk::from_parts(vec![Op::Insert; n], chunk.clone()));
            Ok(positions)
        }

        async fn flush(&mut self) -> StreamExecutorResult<Option<DataFileCommitMetadata>> {
            Ok(None)
        }
    }

    /// Create the PK index state table for testing.
    /// Schema: [pk_col (Int64), file_path (Varchar), position (Int64)]
    async fn create_pk_index_state_table(
        store: MemoryStateStore,
        table_id: TableId,
    ) -> StateTable<MemoryStateStore> {
        let column_descs = vec![
            ColumnDesc::unnamed(ColumnId::new(0), DataType::Int64), // pk
            ColumnDesc::unnamed(ColumnId::new(1), DataType::Varchar), // file_path
            ColumnDesc::unnamed(ColumnId::new(2), DataType::Int64), // position
        ];
        let order_types = vec![OrderType::ascending()];
        let pk_indices = vec![0]; // pk is the first column

        StateTable::from_table_catalog(
            &gen_pbtable(table_id, column_descs, order_types, pk_indices, 0),
            store,
            None,
        )
        .await
    }

    #[tokio::test]
    async fn test_writer_executor_insert_only() {
        let store = MemoryStateStore::new();
        let state_table = create_pk_index_state_table(store, TableId::new(1)).await;
        let writer = IcebergWriterMock::new("file1.parquet");

        let schema = Schema::new(vec![
            Field::unnamed(DataType::Int64), // pk
            Field::unnamed(DataType::Int64), // value
        ]);
        let pk_indices = vec![0];

        let (mut tx, source) = MockSource::channel();
        let source = source.into_executor(schema, pk_indices.clone());

        let mut executor = WriterExecutor::new(
            ActorContext::for_test(123),
            source,
            pk_indices,
            state_table,
            writer,
            None,
        )
        .boxed()
        .execute();

        // First barrier to init
        tx.push_barrier(test_epoch(1), false);
        let msg = executor.next().await.unwrap().unwrap();
        assert!(msg.is_barrier());

        // Send inserts
        let chunk = StreamChunk::from_pretty(
            " I I
            + 1 10
            + 2 20
            + 3 30",
        );
        tx.push_chunk(chunk);

        // Insert-only should not produce downstream chunks (no deletes)
        // We need to send a barrier to trigger flush
        tx.push_barrier(test_epoch(2), false);

        // Next message should be a barrier (no delete positions to emit)
        let msg = executor.next().await.unwrap().unwrap();
        assert!(msg.is_barrier());
    }

    #[tokio::test]
    async fn test_writer_executor_insert_then_delete() {
        let store = MemoryStateStore::new();
        let state_table = create_pk_index_state_table(store, TableId::new(1)).await;
        let writer = IcebergWriterMock::new("file1.parquet");

        let schema = Schema::new(vec![
            Field::unnamed(DataType::Int64), // pk
            Field::unnamed(DataType::Int64), // value
        ]);
        let pk_indices = vec![0];

        let (mut tx, source) = MockSource::channel();
        let source = source.into_executor(schema, pk_indices.clone());

        let mut executor = WriterExecutor::new(
            ActorContext::for_test(123),
            source,
            pk_indices,
            state_table,
            writer,
            None,
        )
        .boxed()
        .execute();

        // First barrier
        tx.push_barrier(test_epoch(1), false);
        executor.next().await.unwrap().unwrap();

        // Send inserts for pk=1,2,3
        let chunk = StreamChunk::from_pretty(
            " I I
            + 1 10
            + 2 20
            + 3 30",
        );
        tx.push_chunk(chunk);

        // Barrier to commit the inserts
        tx.push_barrier(test_epoch(2), false);
        let msg = executor.next().await.unwrap().unwrap();
        assert!(msg.is_barrier());

        // Now delete pk=2
        let chunk = StreamChunk::from_pretty(
            " I I
            - 2 20",
        );
        tx.push_chunk(chunk);

        // Should emit delete position for pk=2
        let msg = executor.next().await.unwrap().unwrap();
        let delete_chunk = msg.into_chunk().unwrap();

        // Verify the delete position chunk
        // Schema: [file_path: Varchar, position: Int64]
        assert_eq!(delete_chunk.cardinality(), 1);
        let (ops, columns, _) = delete_chunk.into_inner();
        assert_eq!(ops[0], Op::Insert); // delete position is emitted as INSERT

        // pk=2 was the second row written, so offset should be 1
        let position_col = columns[1].as_int64();
        assert_eq!(position_col.value_at(0), Some(1)); // 0-indexed: pk=1→0, pk=2→1

        let file_path_col = columns[0].as_utf8();
        assert_eq!(file_path_col.value_at(0), Some("file1.parquet"));
    }

    #[tokio::test]
    async fn test_writer_executor_update() {
        let store = MemoryStateStore::new();
        let state_table = create_pk_index_state_table(store, TableId::new(1)).await;
        let writer = IcebergWriterMock::new("file1.parquet");

        let schema = Schema::new(vec![
            Field::unnamed(DataType::Int64), // pk
            Field::unnamed(DataType::Int64), // value
        ]);
        let pk_indices = vec![0];

        let (mut tx, source) = MockSource::channel();
        let source = source.into_executor(schema, pk_indices.clone());

        let mut executor = WriterExecutor::new(
            ActorContext::for_test(123),
            source,
            pk_indices,
            state_table,
            writer,
            None,
        )
        .boxed()
        .execute();

        // First barrier
        tx.push_barrier(test_epoch(1), false);
        executor.next().await.unwrap().unwrap();

        // Insert pk=1
        let chunk = StreamChunk::from_pretty(
            " I I
            + 1 10",
        );
        tx.push_chunk(chunk);

        // Barrier to commit
        tx.push_barrier(test_epoch(2), false);
        let msg = executor.next().await.unwrap().unwrap();
        assert!(msg.is_barrier());

        // Update pk=1: delete old value, insert new value
        let chunk = StreamChunk::from_pretty(
            "  I I
            U- 1 10
            U+ 1 99",
        );
        tx.push_chunk(chunk);

        // Should emit a delete position for the old row
        let msg = executor.next().await.unwrap().unwrap();
        let delete_chunk = msg.into_chunk().unwrap();
        assert_eq!(delete_chunk.cardinality(), 1);

        let (ops, columns, _) = delete_chunk.into_inner();
        assert_eq!(ops[0], Op::Insert);

        // pk=1 was written at offset 0
        let position_col = columns[1].as_int64();
        assert_eq!(position_col.value_at(0), Some(0));

        // After the update, pk=1 should now point to the new position (offset 1)
        // Verify by sending another delete
        tx.push_barrier(test_epoch(3), false);
        let msg = executor.next().await.unwrap().unwrap();
        assert!(msg.is_barrier());

        let chunk = StreamChunk::from_pretty(
            " I I
            - 1 99",
        );
        tx.push_chunk(chunk);

        let msg = executor.next().await.unwrap().unwrap();
        let delete_chunk = msg.into_chunk().unwrap();
        let (_, columns, _) = delete_chunk.into_inner();
        let position_col = columns[1].as_int64();
        // The new position should be offset 1 (second write)
        assert_eq!(position_col.value_at(0), Some(1));
    }
}

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

use std::collections::HashMap;

use anyhow::Context;
use iceberg::writer::PositionDeleteInput;
use risingwave_common::array::DataChunk;
use risingwave_common::array::stream_record::Record;
use risingwave_common::bitmap::Bitmap;
use risingwave_common::row::{Project, Row, RowExt};
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_common::util::epoch::EpochPair;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_pb::connector_service::SinkMetadata;
use risingwave_rpc_client::CoordinatorStreamHandle;
use risingwave_storage::StateStore;
use thiserror_ext::AsReport;

use super::CoordinatorStreamHandleInit;
use crate::executor::iceberg_with_pk_index::Payload;
use crate::executor::prelude::*;

pub type IcebergWriterFlushOutput = (SinkMetadata, Vec<(String, Vec<u8>)>);
type PkRow<'a> = Project<'a, RowRef<'a>>;

fn new_payload_chunk_builder(chunk_size: usize) -> DataChunkBuilder {
    DataChunkBuilder::new(vec![DataType::Varchar, DataType::Bytea], chunk_size)
}

fn append_payload_row(
    builder: &mut DataChunkBuilder,
    file_path: &str,
    payload: &[u8],
) -> Option<DataChunk> {
    builder.append_one_row([
        Some(ScalarRefImpl::Utf8(file_path)),
        Some(ScalarRefImpl::Bytea(payload)),
    ])
}

fn append_insert_row<R: Row>(
    insert_chunk: &mut DataChunkBuilder,
    row: R,
) -> StreamExecutorResult<usize> {
    if insert_chunk.append_one_row(row).is_some() {
        return Err(StreamExecutorError::from(
            "insert chunk exceeds capacity".to_owned(),
        ));
    }

    Ok(insert_chunk.buffered_count() - 1)
}

/// Trait abstracting the Iceberg data file writing for testability.
///
/// Implementations are responsible for writing rows to Iceberg data files
/// and tracking row positions. Commit is handled by the executor, not the writer.
#[async_trait::async_trait]
pub trait IcebergWriter: Send + 'static {
    /// Write a batch of insert rows. Returns the position of each row in the chunk (in order).
    async fn write_chunk(
        &mut self,
        chunk: DataChunk,
    ) -> StreamExecutorResult<Vec<PositionDeleteInput>>;

    /// Flush current data files on barrier. Returns the written data files
    /// and each file's partition information (if partitioned).
    async fn flush(&mut self) -> StreamExecutorResult<Option<IcebergWriterFlushOutput>>;
}

/// Writer Executor for Iceberg V3 Sink with PK index
///
/// This stateful executor maintains a PK index that maps primary key values to
/// their position in data files (`file_path`, `row_offset`). It processes change logs
/// from upstream:
///
/// - **Insert**: Writes the row to a data file via [`IcebergWriter`], records the
///   position in the PK index state table.
/// - **Delete**: Looks up the PK index to find the data file position, emits a
///   delete position message downstream to the DV Merger, removes from index.
/// - **Update**: Treated as Delete + Insert. The planner guarantees the old and
///   new rows share the same PK, so the executor can reuse the projected PK from
///   the old row when updating the PK index.
///
/// The output stream contains encoded payload messages with schema
/// `[file_path: Varchar, payload: Bytea]` that flow to the DV Merger executor.
/// `payload` is [`Payload::Position`] for deletes and [`Payload::PartitionInfo`]
/// for partition metadata emitted on barrier.
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
    /// Buffer for accumulating delete position messages before the next barrier flush.
    delete_position_buffer: Option<DataChunkBuilder>,
    /// Whether the iceberg table is partitioned. If true, the executor will emit partition metadata
    /// on barrier in addition to delete positions.
    partitioned: bool,
    chunk_size: usize,
    /// Handle for sending commit metadata (data files) to the coordinator.
    coordinator_handle: Option<CoordinatorStreamHandle>,
    coordinator_handle_init: Option<CoordinatorStreamHandleInit>,
}

impl<S, W> WriterExecutor<S, W>
where
    S: StateStore,
    W: IcebergWriter,
{
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        ctx: ActorContextRef,
        input: Executor,
        pk_indices: Vec<usize>,
        pk_index_state_table: StateTable<S>,
        writer: W,
        partitioned: bool,
        chunk_size: usize,
        coordinator_handle_init: Option<CoordinatorStreamHandleInit>,
    ) -> Self {
        Self {
            ctx,
            input: Some(input),
            pk_indices,
            pk_index_state_table,
            writer,
            delete_position_buffer: None,
            partitioned,
            chunk_size,
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

        // Retry coordinator connection to handle the case where the coordinator
        // is being (re)started (e.g., after snapshot backfill transition).
        let max_retries = 10;
        let mut last_err = None;
        for attempt in 0..=max_retries {
            match init.try_create_handle().await {
                Ok((mut handle, log_store_rewind_start_epoch)) => {
                    if log_store_rewind_start_epoch.is_none() {
                        handle.align_initial_epoch(first_epoch.curr).await?;
                    }
                    self.coordinator_handle = Some(handle);
                    return Ok(());
                }
                Err(e) => {
                    if attempt < max_retries {
                        tracing::warn!(
                            attempt,
                            error = %e.as_report(),
                            "failed to create coordinator handle, retrying",
                        );
                        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    }
                    last_err = Some(e);
                }
            }
        }
        Err(last_err.unwrap())
    }

    async fn delete_existing_row(
        &mut self,
        pk_row: PkRow<'_>,
        delete_position_buffer: &mut DataChunkBuilder,
    ) -> StreamExecutorResult<Option<DataChunk>> {
        let Some(index_row) = self.pk_index_state_table.get_row(pk_row).await? else {
            return Ok(None);
        };

        let num_cols = index_row.len();
        let file_path = index_row
            .datum_at(num_cols - 2)
            .context("file_path should not be null")?
            .into_utf8();
        let position = index_row
            .datum_at(num_cols - 1)
            .context("position should not be null")?
            .into_int64();
        let payload = Payload::encode_position(position)?;
        let chunk = append_payload_row(delete_position_buffer, file_path, &payload);
        self.pk_index_state_table.delete(index_row);
        Ok(chunk)
    }

    // Process the current stream chunk in two phases:
    // 1. Scan changelog records, emit delete positions immediately, and buffer candidate inserts.
    // 2. After the scan, write only the inserts that still survive within this chunk and update
    //    the PK index with their final Iceberg positions.
    //
    // This executor has two scopes of state within one checkpoint:
    // - `insert_chunk` / `insert_pos` are per-stream-chunk scratch state. They only coalesce
    //   records that appear in this `process_chunk` call.
    // - `pk_index_state_table` / `delete_position_buffer` live until the next barrier, so a
    //   later stream chunk in the same checkpoint can still observe earlier writes/deletes.
    //
    // The edge cases below can be understood with two conceptual flags:
    // - `is_same_stream_chunk`: both operations are seen in this `process_chunk` call, so they
    //   can still cancel each other before any new row is written to Iceberg.
    // - `is_prev_pk_exist`: `delete_existing_row` finds a row in `pk_index_state_table`, which
    //   means this PK already points to a materialized Iceberg row from an older checkpoint or
    //   from an earlier stream chunk in the current checkpoint.
    //
    // With those two flags, the changelog edge cases work as follows within one checkpoint:
    // - DELETE + INSERT:
    //   - if `is_prev_pk_exist`, emit one position delete for the previous row, then buffer the
    //     new row and install its new position after the scan.
    //   - otherwise, the delete is a no-op and the insert behaves like a fresh insert.
    // - DELETE + DELETE:
    //   - only the first delete that sees `is_prev_pk_exist` emits a position delete and removes
    //     the PK entry; later deletes become no-ops.
    // - INSERT + DELETE:
    //   - if `is_same_stream_chunk`, the delete removes the pending entry from `insert_pos`, and
    //     `delete_existing_row` still sees no state-table entry for that insert, so the pair
    //     cancels out completely.
    //   - otherwise, the insert was already written by an earlier stream chunk in the same
    //     checkpoint, so the later delete reads it back from `pk_index_state_table` and emits a
    //     position delete for that freshly written row.
    #[try_stream(ok = DataChunk, error = StreamExecutorError)]
    async fn process_chunk(&mut self, chunk: StreamChunk) {
        let mut delete_position_buffer = self
            .delete_position_buffer
            .take()
            .unwrap_or_else(|| new_payload_chunk_builder(self.chunk_size));
        let pk_indices = self.pk_indices.clone();

        // `insert_chunk` keeps every candidate insert row in append order. Some buffered rows may be
        // canceled by a later delete/update with the same PK in the same chunk, but we still keep
        // them here so the remaining rows preserve the original write order.
        let mut insert_chunk =
            DataChunkBuilder::new(chunk.data_chunk().data_types(), chunk.capacity() + 1);
        // `insert_pos` tracks the latest buffered row index for each PK that should still be written
        // after this chunk is fully scanned. Deletes remove a pending insert, and updates replace the
        // previous index with the newly buffered row.
        let mut insert_pos: HashMap<PkRow<'_>, usize> = HashMap::new();

        for record in chunk.records() {
            match record {
                Record::Insert { new_row } => {
                    let insert_idx = append_insert_row(&mut insert_chunk, new_row)?;
                    let pk_row = new_row.project(&pk_indices);
                    insert_pos.insert(pk_row, insert_idx);
                }
                Record::Delete { old_row } => {
                    let pk_row = old_row.project(&pk_indices);
                    insert_pos.remove(&pk_row);
                    if let Some(chunk) = self
                        .delete_existing_row(pk_row, &mut delete_position_buffer)
                        .await?
                    {
                        yield chunk;
                    }
                }
                Record::Update { old_row, new_row } => {
                    let pk_row = old_row.project(&pk_indices);
                    if let Some(chunk) = self
                        .delete_existing_row(pk_row, &mut delete_position_buffer)
                        .await?
                    {
                        yield chunk;
                    }
                    let insert_idx = append_insert_row(&mut insert_chunk, new_row)?;
                    insert_pos.insert(pk_row, insert_idx);
                }
            }
        }

        // Only rows whose PK still exists in `insert_pos` survive this chunk. Build visibility from
        // those buffered row indexes, write them in buffered order, then persist the returned
        // Iceberg positions into the PK index state table.
        if !insert_pos.is_empty() {
            let mut ordered_insert_pos = insert_pos.into_iter().collect::<Vec<_>>();
            ordered_insert_pos.sort_unstable_by_key(|(_, idx)| *idx);

            let mut insert_chunk = insert_chunk.finish();
            let visibility = Bitmap::from_indices(
                insert_chunk.capacity(),
                ordered_insert_pos.iter().map(|(_, idx)| *idx),
            );
            insert_chunk.set_visibility(visibility);
            let positions = self.writer.write_chunk(insert_chunk).await?;

            // Insert into PK index state table
            for ((pk, _), pos) in ordered_insert_pos.into_iter().zip_eq_fast(positions) {
                let mut index_row_data = Vec::with_capacity(self.pk_indices.len() + 2);
                for datum in pk.iter() {
                    index_row_data.push(datum);
                }
                index_row_data.push(Some(ScalarRefImpl::Utf8(&pos.path)));
                index_row_data.push(Some(ScalarRefImpl::Int64(pos.pos)));
                self.pk_index_state_table.insert(index_row_data.as_slice());
            }
        }

        self.delete_position_buffer = Some(delete_position_buffer);
        self.pk_index_state_table.try_flush().await?;
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(mut self) {
        let mut input = self.input.take().unwrap().execute();

        // Consume the first barrier.
        let barrier = expect_first_barrier(&mut input).await?;
        let first_epoch = barrier.epoch;
        yield Message::Barrier(barrier);
        self.pk_index_state_table.init_epoch(first_epoch).await?;
        self.initialize_coordinator_handle(first_epoch).await?;

        #[for_await]
        for msg in input {
            match msg? {
                Message::Chunk(chunk) =>
                {
                    #[for_await]
                    for data_chunk in self.process_chunk(chunk) {
                        yield Message::Chunk(data_chunk?.into());
                    }
                }
                Message::Barrier(barrier) => {
                    if let Some(chunk) = self
                        .delete_position_buffer
                        .take()
                        .and_then(|mut b| b.consume_all())
                    {
                        yield Message::Chunk(chunk.into());
                    }

                    let (sink_metadata, partitions) =
                        self.writer.flush().await?.unwrap_or_default();
                    if self.partitioned {
                        let mut partition_chunk = new_payload_chunk_builder(self.chunk_size);
                        for (file_path, partition) in partitions {
                            let payload = Payload::encode_partition_info(partition)?;
                            if let Some(chunk) =
                                append_payload_row(&mut partition_chunk, &file_path, &payload)
                            {
                                yield Message::Chunk(chunk.into());
                            }
                        }
                        if let Some(chunk) = partition_chunk.consume_all() {
                            yield Message::Chunk(chunk.into());
                        }
                    }

                    let epoch = barrier.epoch;
                    let update_vnode_bitmap = barrier.as_update_vnode_bitmap(self.ctx.id);
                    let post_commit = self.pk_index_state_table.commit(epoch).await?;

                    if barrier.is_stop(self.ctx.id)
                        && let Some(mut handle) = self.coordinator_handle.take()
                    {
                        let sink_metadata = sink_metadata.clone();
                        let defer_future = async move {
                            handle
                                .commit(epoch.curr, sink_metadata, None)
                                .await
                                .context("coordinator commit failed")?;
                            handle.stop().await.context("coordinator stop failed")?;
                            Ok::<(), anyhow::Error>(())
                        };

                        tokio::spawn(async move {
                            if let Err(e) = defer_future.await {
                                tracing::error!(error = %e.as_report(), "deferred coordinator commit/stop failed");
                            }
                        });
                    }

                    yield Message::Barrier(barrier);

                    if let Some(handle) = &mut self.coordinator_handle {
                        handle
                            .commit(epoch.curr, sink_metadata, None)
                            .await
                            .context("coordinator commit failed")?;
                        if let Some(update_vnode_bitmap) = &update_vnode_bitmap {
                            handle
                                .update_vnode_bitmap(update_vnode_bitmap)
                                .await
                                .context("report vnode bitmap failed")?;
                        }
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

    use iceberg::writer::PositionDeleteInput;
    use risingwave_common::array::Op;
    use risingwave_common::catalog::{ColumnDesc, ColumnId, Field, Schema, TableId};
    use risingwave_common::test_prelude::StreamChunkTestExt;
    use risingwave_common::types::DataType;
    use risingwave_common::util::epoch::test_epoch;
    use risingwave_common::util::sort_util::OrderType;
    use risingwave_storage::memory::MemoryStateStore;

    use super::*;
    use crate::common::table::test_utils::gen_pbtable;
    use crate::executor::test_utils::{MessageSender, MockSource, StreamExecutorTestExt};

    const CHUNK_SIZE: usize = 1024;
    const TEST_FILE_PATH: &str = "file1.parquet";

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

        fn written_chunks(&self) -> Arc<Mutex<Vec<StreamChunk>>> {
            self.written_chunks.clone()
        }
    }

    #[async_trait::async_trait]
    impl IcebergWriter for IcebergWriterMock {
        async fn write_chunk(
            &mut self,
            chunk: DataChunk,
        ) -> StreamExecutorResult<Vec<PositionDeleteInput>> {
            let row_count = chunk.cardinality();
            let mut positions = Vec::with_capacity(row_count);
            for _ in 0..row_count {
                positions.push(PositionDeleteInput::new(
                    Arc::<str>::from(self.file_path.as_str()),
                    self.next_offset,
                ));
                self.next_offset += 1;
            }
            self.written_chunks.lock().unwrap().push(chunk.into());
            Ok(positions)
        }

        async fn flush(&mut self) -> StreamExecutorResult<Option<IcebergWriterFlushOutput>> {
            Ok(None)
        }
    }

    async fn create_pk_index_state_table(
        store: MemoryStateStore,
        table_id: TableId,
    ) -> StateTable<MemoryStateStore> {
        let column_descs = vec![
            ColumnDesc::unnamed(ColumnId::new(0), DataType::Int64),
            ColumnDesc::unnamed(ColumnId::new(1), DataType::Varchar),
            ColumnDesc::unnamed(ColumnId::new(2), DataType::Int64),
        ];
        let order_types = vec![OrderType::ascending()];
        let pk_indices = vec![0];

        StateTable::from_table_catalog(
            &gen_pbtable(table_id, column_descs, order_types, pk_indices, 0),
            store,
            None,
        )
        .await
    }

    fn input_schema() -> Schema {
        Schema::new(vec![
            Field::unnamed(DataType::Int64),
            Field::unnamed(DataType::Int64),
        ])
    }

    fn decode_position_payload_chunk(chunk: StreamChunk) -> Vec<(String, i64)> {
        chunk
            .rows()
            .map(|(op, row)| {
                assert_eq!(op, Op::Insert);
                let file_path = row.datum_at(0).unwrap().into_utf8().to_owned();
                let payload = Payload::decode(row.datum_at(1).unwrap().into_bytea()).unwrap();
                let Payload::Position(position) = payload else {
                    panic!("expected position payload");
                };
                (file_path, position)
            })
            .collect()
    }

    fn test_file_position(position: i64) -> (String, i64) {
        (TEST_FILE_PATH.to_owned(), position)
    }

    struct WriterTestHarness {
        tx: MessageSender,
        executor: BoxedMessageStream,
        written_chunks: Arc<Mutex<Vec<StreamChunk>>>,
    }

    impl WriterTestHarness {
        async fn new() -> Self {
            let store = MemoryStateStore::new();
            let state_table = create_pk_index_state_table(store, TableId::new(1)).await;
            let writer = IcebergWriterMock::new(TEST_FILE_PATH);
            let written_chunks = writer.written_chunks();

            let (tx, source) = MockSource::channel();
            let source = source.into_executor(input_schema(), vec![0]);
            let executor = WriterExecutor::new(
                ActorContext::for_test(123),
                source,
                vec![0],
                state_table,
                writer,
                false,
                CHUNK_SIZE,
                None,
            )
            .boxed()
            .execute();

            Self {
                tx,
                executor,
                written_chunks,
            }
        }

        async fn init(&mut self) {
            self.tx.push_barrier(test_epoch(1), false);
            self.executor.expect_barrier().await;
        }

        fn push_chunk(&mut self, chunk: StreamChunk) {
            self.tx.push_chunk(chunk);
        }

        fn push_pretty_chunk(&mut self, pretty: &str) {
            self.push_chunk(StreamChunk::from_pretty(pretty));
        }

        fn push_barrier(&mut self, epoch: u64) {
            self.tx.push_barrier(test_epoch(epoch), false);
        }

        async fn expect_barrier(&mut self) {
            self.executor.expect_barrier().await;
        }

        async fn expect_position_chunk(&mut self, expected: Vec<(String, i64)>) {
            assert_eq!(
                decode_position_payload_chunk(self.executor.expect_chunk().await),
                expected
            );
        }

        fn written_chunks(&self) -> Vec<StreamChunk> {
            self.written_chunks.lock().unwrap().clone()
        }

        fn compacted_written_chunks(&self) -> Vec<StreamChunk> {
            self.written_chunks()
                .into_iter()
                .map(StreamChunk::compact_vis)
                .collect()
        }
    }

    #[tokio::test]
    async fn test_writer_executor_insert_only() {
        let mut harness = WriterTestHarness::new().await;
        harness.init().await;

        harness.push_pretty_chunk(
            " I I
            + 1 10
            + 2 20
            + 3 30",
        );
        harness.push_barrier(2);

        harness.expect_barrier().await;
        assert_eq!(
            harness.written_chunks(),
            vec![StreamChunk::from_pretty(
                " I I
                + 1 10
                + 2 20
                + 3 30",
            )]
        );
    }

    #[tokio::test]
    async fn test_writer_executor_insert_then_delete() {
        let mut harness = WriterTestHarness::new().await;
        harness.init().await;

        harness.push_pretty_chunk(
            " I I
            + 1 10
            + 2 20
            + 3 30",
        );
        harness.push_barrier(2);
        harness.expect_barrier().await;

        harness.push_pretty_chunk(
            " I I
            - 2 20",
        );
        harness.push_barrier(3);

        harness
            .expect_position_chunk(vec![test_file_position(1)])
            .await;
        harness.expect_barrier().await;
    }

    #[tokio::test]
    async fn test_writer_executor_update_rewrites_position() {
        let mut harness = WriterTestHarness::new().await;
        harness.init().await;

        harness.push_pretty_chunk(
            " I I
            + 1 10",
        );
        harness.push_barrier(2);
        harness.expect_barrier().await;

        harness.push_pretty_chunk(
            " I I
            U- 1 10
            U+ 1 99",
        );
        harness.push_barrier(3);

        harness
            .expect_position_chunk(vec![test_file_position(0)])
            .await;
        harness.expect_barrier().await;

        harness.push_pretty_chunk(
            " I I
            - 1 99",
        );
        harness.push_barrier(4);

        harness
            .expect_position_chunk(vec![test_file_position(1)])
            .await;
        harness.expect_barrier().await;

        assert_eq!(
            harness.written_chunks(),
            vec![
                StreamChunk::from_pretty(
                    " I I
                    + 1 10",
                ),
                StreamChunk::from_pretty(
                    " I I
                    + 1 99",
                ),
            ]
        );
    }

    #[tokio::test]
    async fn test_writer_executor_delete_then_insert_without_existing_row_is_fresh_insert() {
        let mut harness = WriterTestHarness::new().await;
        harness.init().await;

        harness.push_pretty_chunk(
            " I I
            - 1 10
            + 1 99",
        );
        harness.push_barrier(2);

        harness.expect_barrier().await;
        assert_eq!(
            harness.written_chunks(),
            vec![StreamChunk::from_pretty(
                " I I
                + 1 99",
            )]
        );
    }

    #[tokio::test]
    async fn test_writer_executor_delete_then_insert_rewrites_existing_row() {
        let mut harness = WriterTestHarness::new().await;
        harness.init().await;

        harness.push_pretty_chunk(
            " I I
            + 1 10",
        );
        harness.push_barrier(2);
        harness.expect_barrier().await;

        harness.push_pretty_chunk(
            " I I
            - 1 10
            + 1 99",
        );
        harness.push_barrier(3);

        harness
            .expect_position_chunk(vec![test_file_position(0)])
            .await;
        harness.expect_barrier().await;

        assert_eq!(
            harness.written_chunks(),
            vec![
                StreamChunk::from_pretty(
                    " I I
                    + 1 10",
                ),
                StreamChunk::from_pretty(
                    " I I
                    + 1 99",
                ),
            ]
        );
    }

    #[tokio::test]
    async fn test_writer_executor_delete_then_delete_emits_one_position_delete() {
        let mut harness = WriterTestHarness::new().await;
        harness.init().await;

        harness.push_pretty_chunk(
            " I I
            + 1 10",
        );
        harness.push_barrier(2);
        harness.expect_barrier().await;

        harness.push_pretty_chunk(
            " I I
            - 1 10
            - 1 10",
        );
        harness.push_barrier(3);

        harness
            .expect_position_chunk(vec![test_file_position(0)])
            .await;
        harness.expect_barrier().await;
        assert_eq!(
            harness.written_chunks(),
            vec![StreamChunk::from_pretty(
                " I I
                + 1 10",
            )]
        );
    }

    #[tokio::test]
    async fn test_writer_executor_insert_then_delete_in_different_chunks_same_checkpoint() {
        let mut harness = WriterTestHarness::new().await;
        harness.init().await;

        harness.push_pretty_chunk(
            " I I
            + 1 10",
        );
        harness.push_pretty_chunk(
            " I I
            - 1 10",
        );
        harness.push_barrier(2);

        harness
            .expect_position_chunk(vec![test_file_position(0)])
            .await;
        harness.expect_barrier().await;
        assert_eq!(
            harness.written_chunks(),
            vec![StreamChunk::from_pretty(
                " I I
                + 1 10",
            )]
        );
    }

    #[tokio::test]
    async fn test_writer_executor_insert_then_insert_in_same_chunk_keeps_latest_row() {
        let mut harness = WriterTestHarness::new().await;
        harness.init().await;

        harness.push_pretty_chunk(
            " I I
            + 1 10
            + 1 99",
        );
        harness.push_barrier(2);

        harness.expect_barrier().await;
        assert_eq!(
            harness.compacted_written_chunks(),
            vec![StreamChunk::from_pretty(
                " I I
                + 1 99",
            )]
        );

        harness.push_pretty_chunk(
            " I I
            - 1 99",
        );
        harness.push_barrier(3);

        harness
            .expect_position_chunk(vec![test_file_position(0)])
            .await;
        harness.expect_barrier().await;
    }

    #[tokio::test]
    async fn test_writer_executor_insert_then_delete_in_same_chunk_is_cancelled() {
        let mut harness = WriterTestHarness::new().await;
        harness.init().await;

        harness.push_pretty_chunk(
            " I I
            + 1 10
            - 1 10",
        );
        harness.push_barrier(2);

        harness.expect_barrier().await;
        assert!(harness.written_chunks().is_empty());
    }
}

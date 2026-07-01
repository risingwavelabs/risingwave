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
use iceberg::writer::PositionDeleteInput;
use risingwave_common::array::DataChunk;
use risingwave_common::array::stream_record::Record;
use risingwave_common::id::SinkId;
use risingwave_common::row::{Project, RowExt};
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_pb::connector_service::SinkMetadata;
use risingwave_pb::stream_service::PbIcebergV3SinkRole;
use risingwave_storage::StateStore;

use crate::common::change_buffer::output_kind;
use crate::common::compact_chunk::{InconsistencyBehavior, compact_chunk_inline};
use crate::executor::prelude::*;
use crate::task::LocalBarrierManager;

type PkRow<'a> = Project<'a, RowRef<'a>>;

fn new_chunk_builder(chunk_size: usize) -> DataChunkBuilder {
    DataChunkBuilder::new(vec![DataType::Varchar, DataType::Int64], chunk_size)
}

fn append_row(builder: &mut DataChunkBuilder, file_path: &str, position: i64) -> Option<DataChunk> {
    builder.append_one_row([
        Some(ScalarRefImpl::Utf8(file_path)),
        Some(ScalarRefImpl::Int64(position)),
    ])
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

    /// Flush current data files on barrier. Returns serialized commit metadata,
    /// or `None` if no data was written since the last flush.
    async fn flush(&mut self) -> StreamExecutorResult<Option<SinkMetadata>>;
}

/// Writer Executor for Iceberg V3 Sink with PK index
///
/// This stateful executor maintains a PK index that maps primary key values to
/// their position in data files (`file_path`, `position`). It processes change logs
/// from upstream:
///
/// - **Insert**: Writes the row to a data file via [`IcebergWriter`], records the
///   position in the PK index state table.
/// - **Delete**: Looks up the PK index to find the data file position, emits a
///   delete position message downstream to the DV Merger, removes from index.
/// - **Update**: Treated as Delete + Insert. The planner guarantees the old and
///   new rows share the same PK, so the executor can reuse the projected PK from
///   the old row when updating the PK index.
pub struct WriterExecutor<S, W>
where
    S: StateStore,
    W: IcebergWriter,
{
    ctx: ActorContextRef,
    input: Option<Executor>,
    /// Column indices of the primary key in the input schema.
    pk_indices: Vec<usize>,
    /// State table storing the PK index: `pk_columns` -> (`file_path`, `position`).
    /// Schema: [`pk_col_0`, ..., `pk_col_n`, `file_path`: Varchar, `position`: Int64]
    pk_index_state_table: StateTable<S>,
    /// The Iceberg data file writer.
    writer: W,
    /// Buffer for accumulating delete position messages before the next barrier flush.
    delete_position_buffer: Option<DataChunkBuilder>,
    chunk_size: usize,
    sink_id: SinkId,
    local_barrier_manager: LocalBarrierManager,
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
        chunk_size: usize,
        sink_id: SinkId,
        local_barrier_manager: LocalBarrierManager,
    ) -> Self {
        Self {
            ctx,
            input: Some(input),
            pk_indices,
            pk_index_state_table,
            writer,
            delete_position_buffer: None,
            chunk_size,
            sink_id,
            local_barrier_manager,
        }
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
        let chunk = append_row(delete_position_buffer, file_path, position);
        self.pk_index_state_table.delete(index_row);
        Ok(chunk)
    }

    // Process one stream chunk:
    //
    // 1. Compact the chunk by `pk_indices` so each PK appears at most once and any intra-chunk
    //    `+/-` cancellations are absorbed up front. After this step every record is either a
    //    standalone `Insert`, `Delete`, or `Update {old, new}` whose old and new rows share the
    //    same PK.
    // 2. For each record: `Insert` is buffered into a single batched write; `Delete` looks up
    //    `pk_index_state_table` to emit a position delete and clears the entry; `Update` is
    //    handled as a position delete for the old row plus a buffered insert for the new row.
    // 3. After the scan, write all buffered inserts in one `write_chunk` call and persist the
    //    returned Iceberg positions back to `pk_index_state_table`.
    //
    // `pk_index_state_table` and `delete_position_buffer` live until the next barrier, so a later
    // chunk in the same checkpoint observes earlier writes/deletes via the state table.
    #[try_stream(ok = DataChunk, error = StreamExecutorError)]
    async fn process_chunk(&mut self, chunk: StreamChunk) {
        let chunk = compact_chunk_inline::<{ output_kind::RETRACT }>(
            chunk,
            &self.pk_indices,
            InconsistencyBehavior::Panic,
        );

        let mut delete_position_buffer = self
            .delete_position_buffer
            .take()
            .unwrap_or_else(|| new_chunk_builder(self.chunk_size));
        let pk_indices = self.pk_indices.clone();

        // Invariant: every input column is visible and written to Iceberg verbatim. The planner
        // (`promote_iceberg_pk_index_stream_key` in `stream_sink.rs`) enforces this by promoting
        // hidden stream-key columns to visible and by not adding the extra partition column for
        // pk-index sinks, so the writer has no hidden-column projection and writes the whole row.
        // `chunk.capacity() + 1` is an upper bound on appended rows: each surviving record
        // contributes at most one row (Insert / Update::new), and `records()` yields at most
        // `capacity` records.
        let mut insert_chunk =
            DataChunkBuilder::new(chunk.data_chunk().data_types(), chunk.capacity() + 1);
        let mut insert_pks: Vec<PkRow<'_>> = Vec::new();

        for record in chunk.records() {
            match record {
                Record::Insert { new_row } => {
                    let overflow = insert_chunk.append_one_row(new_row);
                    debug_assert!(overflow.is_none(), "insert chunk exceeds capacity");
                    insert_pks.push(new_row.project(&pk_indices));
                }
                Record::Delete { old_row } => {
                    let pk_row = old_row.project(&pk_indices);
                    if let Some(chunk) = self
                        .delete_existing_row(pk_row, &mut delete_position_buffer)
                        .await?
                    {
                        yield chunk;
                    }
                }
                Record::Update { new_row, .. } => {
                    // The compactor groups by `pk_indices`, so old and new share the same PK.
                    let pk_row = new_row.project(&pk_indices);
                    if let Some(chunk) = self
                        .delete_existing_row(pk_row, &mut delete_position_buffer)
                        .await?
                    {
                        yield chunk;
                    }
                    let overflow = insert_chunk.append_one_row(new_row);
                    debug_assert!(overflow.is_none(), "insert chunk exceeds capacity");
                    insert_pks.push(pk_row);
                }
            }
        }

        if !insert_pks.is_empty() {
            let write_chunk = insert_chunk.finish();
            let positions = self.writer.write_chunk(write_chunk).await?;

            for (pk, pos) in insert_pks.into_iter().zip_eq_fast(positions) {
                let mut index_row_data = Vec::with_capacity(pk_indices.len() + 2);
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
                    let mut metadata = None;
                    if barrier.is_checkpoint() {
                        if let Some(chunk) = self
                            .delete_position_buffer
                            .take()
                            .and_then(|mut b| b.consume_all())
                        {
                            yield Message::Chunk(chunk.into());
                        }
                        metadata = self.writer.flush().await?;
                    }

                    let epoch = barrier.epoch;
                    let update_vnode_bitmap = barrier.as_update_vnode_bitmap(self.ctx.id);
                    let post_commit = self.pk_index_state_table.commit(epoch).await?;

                    if let Some(metadata) = metadata
                        && metadata.metadata.is_some()
                    {
                        self.local_barrier_manager.report_iceberg_v3_sink_metadata(
                            epoch,
                            self.sink_id,
                            self.ctx.id,
                            PbIcebergV3SinkRole::Writer,
                            Some(metadata),
                        );
                    }

                    yield Message::Barrier(barrier);

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
    use risingwave_common::id::SinkId;
    use risingwave_common::test_prelude::StreamChunkTestExt;
    use risingwave_common::types::DataType;
    use risingwave_common::util::epoch::test_epoch;
    use risingwave_common::util::sort_util::OrderType;
    use risingwave_storage::memory::MemoryStateStore;

    use super::*;
    use crate::common::table::test_utils::gen_pbtable;
    use crate::executor::test_utils::{MessageSender, MockSource, StreamExecutorTestExt};
    use crate::task::LocalBarrierManager;

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
                file_path: file_path.to_owned(),
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

        async fn flush(&mut self) -> StreamExecutorResult<Option<SinkMetadata>> {
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

    fn decode_chunk(chunk: StreamChunk) -> Vec<(String, i64)> {
        chunk
            .rows()
            .map(|(op, row)| {
                assert_eq!(op, Op::Insert);
                let file_path = row.datum_at(0).unwrap().into_utf8().to_owned();
                let position = row.datum_at(1).unwrap().into_int64();
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
            Self::with_schema(input_schema()).await
        }

        /// Build a harness with a custom input schema. The PK is always the first column (Int64)
        /// so the shared `create_pk_index_state_table` schema applies.
        async fn with_schema(input_schema: Schema) -> Self {
            let store = MemoryStateStore::new();
            let state_table = create_pk_index_state_table(store, TableId::new(1)).await;
            let writer = IcebergWriterMock::new(TEST_FILE_PATH);
            let written_chunks = writer.written_chunks();

            let (tx, source) = MockSource::channel();
            let source = source.into_executor(input_schema, vec![0]);
            let lbm = LocalBarrierManager::for_test();
            let executor = WriterExecutor::new(
                ActorContext::for_test(123),
                source,
                vec![0],
                state_table,
                writer,
                CHUNK_SIZE,
                SinkId::new(0),
                lbm,
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
            assert_eq!(decode_chunk(self.executor.expect_chunk().await), expected);
        }

        fn written_chunks(&self) -> Vec<StreamChunk> {
            self.written_chunks.lock().unwrap().clone()
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

    /// Two deletes for the same PK within one chunk are inconsistent input: the PK is derived from
    /// the upstream stream key, which guarantees uniqueness within a chunk. The writer panics on
    /// compaction rather than silently swallowing the duplicate.
    #[tokio::test]
    #[should_panic(expected = "inconsistency happened")]
    async fn test_writer_executor_duplicate_delete_in_same_chunk_panics() {
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

        // Processing the duplicate-delete chunk panics during compaction.
        harness.expect_barrier().await;
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

    /// Two inserts for the same PK within one chunk are inconsistent input: the upstream stream key
    /// guarantees PK uniqueness within a chunk, so the writer panics on compaction.
    #[tokio::test]
    #[should_panic(expected = "inconsistency happened")]
    async fn test_writer_executor_duplicate_insert_in_same_chunk_panics() {
        let mut harness = WriterTestHarness::new().await;
        harness.init().await;

        harness.push_pretty_chunk(
            " I I
            + 1 10
            + 1 99",
        );
        harness.push_barrier(2);

        // Processing the duplicate-insert chunk panics during compaction.
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

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
use std::ops::Bound;

use anyhow::{Context, anyhow};
use iceberg::writer::PositionDeleteInput;
use risingwave_common::array::DataChunk;
use risingwave_common::array::stream_record::Record;
use risingwave_common::hash::VnodeBitmapExt;
use risingwave_common::id::SinkId;
use risingwave_common::row::{Project, RowExt};
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_connector::sink::iceberg::{IcebergConfig, RowProvenanceEntry};
use risingwave_pb::connector_service::SinkMetadata;
use risingwave_pb::stream_service::PbIcebergPkIndexSinkRole;
use risingwave_storage::StateStore;
use risingwave_storage::store::PrefetchOptions;

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

/// Sequentially scan a pk-index state-table shard over ALL owned vnodes and rewrite, in place,
/// every entry whose `(file_path, position)` value appears in `mapping` to the mapped
/// `(output_file, output_pos)`. Entries absent from the mapping are left untouched.
///
/// The row layout is `[pk.., file_path: Varchar, position: Int64]`, so `file_path` is the
/// second-to-last column and `position` the last. The PK (key columns) is unchanged by a remap, so
/// each rewrite is an in-place `update` of the two value columns.
///
/// A state table cannot be mutated while an iterator borrows it, so matches are buffered per vnode
/// and applied after the vnode's iterator is dropped. Returns the number of entries remapped.
///
/// This is the pure remap core (given a mapping and the state table) and is unit-tested directly;
/// reading the mapping files via `FileIO` is exercised by the B5 SLT.
async fn remap_state_table_shard<S: StateStore>(
    state_table: &mut StateTable<S>,
    mapping: &HashMap<(String, i64), (String, i64)>,
) -> StreamExecutorResult<usize> {
    if mapping.is_empty() {
        return Ok(0);
    }
    let mut remapped = 0usize;
    // Clone the owned-vnode bitmap so iteration does not borrow `state_table` (we mutate it below).
    let vnodes = state_table.vnodes().clone();
    for vnode in vnodes.iter_vnodes() {
        // Buffer `(old_row, output_file, output_pos)` for this vnode; the iterator borrows
        // `state_table`, so collect first, then drop the iterator, then apply the rewrites.
        let mut rewrites: Vec<(OwnedRow, String, i64)> = Vec::new();
        {
            let iter = state_table
                .iter_with_vnode(
                    vnode,
                    &(Bound::<OwnedRow>::Unbounded, Bound::<OwnedRow>::Unbounded),
                    PrefetchOptions::prefetch_for_large_range_scan(),
                )
                .await?;
            pin_mut!(iter);
            while let Some(row) = iter.next().await {
                let row = row?;
                let num_cols = row.len();
                let file_path = row
                    .datum_at(num_cols - 2)
                    .context("pk-index file_path should not be null")?
                    .into_utf8();
                let position = row
                    .datum_at(num_cols - 1)
                    .context("pk-index position should not be null")?
                    .into_int64();
                if let Some((output_file, output_pos)) =
                    mapping.get(&(file_path.to_owned(), position))
                {
                    rewrites.push((row.clone(), output_file.clone(), *output_pos));
                }
            }
        }

        for (old_row, output_file, output_pos) in rewrites {
            let num_cols = old_row.len();
            let mut new_row_data = old_row.as_inner().to_vec();
            new_row_data[num_cols - 2] = Some(ScalarImpl::Utf8(output_file.into()));
            new_row_data[num_cols - 1] = Some(ScalarImpl::Int64(output_pos));
            let new_row = OwnedRow::new(new_row_data);
            state_table.update(old_row, new_row);
            remapped += 1;
        }
    }
    Ok(remapped)
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

/// Writer Executor for iceberg pk-index sink with PK index
///
/// This stateful executor maintains a PK index that maps primary key values to
/// their position in data files (`file_path`, `position`). It processes change logs
/// from upstream:
///
/// - **Insert**: Writes the row to a data file via [`IcebergWriter`], records the
///   position in the PK index state table.
/// - **Delete**: Looks up the PK index to find the data file position, emits a
///   delete position message downstream to the position-delete merger, removes from index.
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
    /// Iceberg connection config, used to lazily build a `FileIO` for reading the row-provenance
    /// mapping files when an [`Mutation::IcebergPkIndexRemap`] barrier mutation arrives.
    iceberg_config: IcebergConfig,
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
        iceberg_config: IcebergConfig,
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
            iceberg_config,
        }
    }

    /// Handle an [`Mutation::IcebergPkIndexRemap`] addressed to this writer: read the spilled
    /// row-provenance mapping files via the table's `FileIO`, then sequentially scan the pk-index
    /// shard and rewrite every entry whose `(file_path, position)` was compacted away to the mapped
    /// `(output_file, output_pos)`.
    ///
    /// Called at the barrier boundary before the state table commits, so the rewrites are persisted
    /// at this barrier's checkpoint. Idempotent: a re-delivered remap re-scans, but entries already
    /// remapped no longer point at the removed input files, so they are absent from the mapping and
    /// left untouched.
    async fn remap_pk_index(&mut self, mapping_paths: &[String]) -> StreamExecutorResult<()> {
        // Lazily load the table to obtain a `FileIO`. The remap is rare (post-compaction), so the
        // load cost is acceptable and avoids threading a `FileIO` through the writer trait.
        let table = self
            .iceberg_config
            .load_table()
            .await
            .map_err(|e| StreamExecutorError::sink_error(e, self.sink_id))?;
        let file_io = table.file_io();

        // Merge all per-plan NDJSON mapping files into a lookup keyed by `(input_file, input_pos)`.
        // Positions are stored as `Int64` in the pk-index state table, so key on `i64` directly.
        let mut mapping: HashMap<(String, i64), (String, i64)> = HashMap::new();
        for path in mapping_paths {
            let input = file_io
                .new_input(path)
                .map_err(|e| anyhow!(e).context(format!("open row-provenance file {}", path)))?;
            let bytes = input
                .read()
                .await
                .map_err(|e| anyhow!(e).context(format!("read row-provenance file {}", path)))?;
            for line in bytes.split(|b| *b == b'\n') {
                if line.is_empty() {
                    continue;
                }
                let entry: RowProvenanceEntry = serde_json::from_slice(line)
                    .map_err(|e| anyhow!(e).context(format!("parse row-provenance in {}", path)))?;
                mapping.insert(
                    (entry.input_file, entry.input_pos as i64),
                    (entry.output_file, entry.output_pos as i64),
                );
            }
        }

        let remapped = remap_state_table_shard(&mut self.pk_index_state_table, &mapping).await?;
        tracing::info!(
            sink_id = self.sink_id.as_raw_id(),
            actor_id = self.ctx.id.as_raw_id(),
            mapping_files = mapping_paths.len(),
            mapping_entries = mapping.len(),
            remapped,
            "applied pk-index remap after coordinated compaction"
        );
        Ok(())
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
                    // Apply an addressed pk-index remap before advancing the state table so the
                    // rewrites are persisted at this barrier's commit. Self-filter by `sink_id`;
                    // other sinks' writers ignore this mutation.
                    if let Some(Mutation::IcebergPkIndexRemap {
                        sink_id,
                        mapping_paths,
                    }) = barrier.mutation.as_deref()
                        && *sink_id == self.sink_id
                    {
                        self.remap_pk_index(mapping_paths).await?;
                    }

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
                        self.local_barrier_manager
                            .report_iceberg_pk_index_sink_metadata(
                                epoch,
                                self.sink_id,
                                self.ctx.id,
                                PbIcebergPkIndexSinkRole::Writer,
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
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};

    use iceberg::writer::PositionDeleteInput;
    use risingwave_common::array::Op;
    use risingwave_common::catalog::{ColumnDesc, ColumnId, Field, Schema, TableId};
    use risingwave_common::id::SinkId;
    use risingwave_common::test_prelude::StreamChunkTestExt;
    use risingwave_common::types::DataType;
    use risingwave_common::util::epoch::{EpochPair, test_epoch};
    use risingwave_common::util::sort_util::OrderType;
    use risingwave_storage::memory::MemoryStateStore;

    use super::*;
    use crate::common::table::test_utils::gen_pbtable;
    use crate::executor::test_utils::{MessageSender, MockSource, StreamExecutorTestExt};
    use crate::task::LocalBarrierManager;

    const CHUNK_SIZE: usize = 1024;
    const TEST_FILE_PATH: &str = "file1.parquet";

    /// A minimal valid `IcebergConfig` for tests. The harness never triggers a remap, so the config
    /// is only constructed, never used to load a table.
    fn test_iceberg_config() -> IcebergConfig {
        let props: BTreeMap<String, String> = [
            ("type", "upsert"),
            ("primary_key", "v1"),
            ("warehouse.path", "s3://test-bucket/warehouse"),
            ("catalog.type", "storage"),
            ("catalog.name", "demo"),
            ("database.name", "demo_db"),
            ("table.name", "demo_table"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_owned(), v.to_owned()))
        .collect();
        IcebergConfig::from_btreemap(props).unwrap()
    }

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
                test_iceberg_config(),
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

    fn index_row(pk: i64, file_path: &str, position: i64) -> OwnedRow {
        OwnedRow::new(vec![
            Some(ScalarImpl::Int64(pk)),
            Some(ScalarImpl::Utf8(file_path.into())),
            Some(ScalarImpl::Int64(position)),
        ])
    }

    async fn read_index_entry(
        state_table: &StateTable<MemoryStateStore>,
        pk: i64,
    ) -> Option<(String, i64)> {
        let row = state_table
            .get_row(&OwnedRow::new(vec![Some(ScalarImpl::Int64(pk))]))
            .await
            .unwrap()?;
        let file_path = row.datum_at(1).unwrap().into_utf8().to_owned();
        let position = row.datum_at(2).unwrap().into_int64();
        Some((file_path, position))
    }

    /// The remap core rewrites only the entries whose `(file_path, position)` appears in the
    /// mapping (i.e. rows in the compacted-away input files); entries pointing at other files are
    /// left untouched. Positions absent from the index but present in the mapping are ignored.
    #[tokio::test]
    async fn test_remap_state_table_shard() {
        let store = MemoryStateStore::new();
        let mut state_table = create_pk_index_state_table(store, TableId::new(7)).await;
        state_table
            .init_epoch(EpochPair::new_test_epoch(test_epoch(1)))
            .await
            .unwrap();

        // pk=1,2 point at a compacted-away input file; pk=3 points at an unrelated file.
        state_table.insert(index_row(1, "input.parquet", 0));
        state_table.insert(index_row(2, "input.parquet", 5));
        state_table.insert(index_row(3, "other.parquet", 9));

        let mapping: HashMap<(String, i64), (String, i64)> = HashMap::from([
            (
                ("input.parquet".to_owned(), 0),
                ("output.parquet".to_owned(), 100),
            ),
            (
                ("input.parquet".to_owned(), 5),
                ("output.parquet".to_owned(), 105),
            ),
            // A mapping entry whose position is not present in this shard's index: must be a no-op.
            (
                ("input.parquet".to_owned(), 42),
                ("output.parquet".to_owned(), 142),
            ),
        ]);

        let remapped = remap_state_table_shard(&mut state_table, &mapping)
            .await
            .unwrap();
        assert_eq!(remapped, 2);

        // Input-file entries rewritten to their mapped output location.
        assert_eq!(
            read_index_entry(&state_table, 1).await,
            Some(("output.parquet".to_owned(), 100))
        );
        assert_eq!(
            read_index_entry(&state_table, 2).await,
            Some(("output.parquet".to_owned(), 105))
        );
        // Non-input entry untouched.
        assert_eq!(
            read_index_entry(&state_table, 3).await,
            Some(("other.parquet".to_owned(), 9))
        );

        // Idempotency: a re-run finds no entries pointing at the removed input files, so it is a
        // no-op (the already-remapped entries now point at output files absent from the mapping).
        let remapped_again = remap_state_table_shard(&mut state_table, &mapping)
            .await
            .unwrap();
        assert_eq!(remapped_again, 0);
    }
}

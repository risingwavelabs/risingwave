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

use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::FutureExt;
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

fn compaction_pair_mutation(
    sink_id: SinkId,
    task_id: IcebergCompactionTaskId,
    phase: IcebergPkIndexBarrierPhase,
) -> Mutation {
    Mutation::IcebergPkIndexBarrier(crate::executor::IcebergPkIndexBarrierMutation {
        sink_id,
        task_id,
        phase,
        gated_actor_ids: HashSet::from([ActorId::new(123)]),
    })
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

/// Test [`PkIndexStateTableFactory`] that rebuilds the pk-index table on the shared in-memory
/// store — the writer uses it to re-open `table_id` on resume, exactly as production does.
struct TestStateTableFactory {
    store: MemoryStateStore,
    table_id: TableId,
}

#[async_trait::async_trait]
impl PkIndexStateTableFactory<MemoryStateStore> for TestStateTableFactory {
    async fn build(&self) -> StateTable<MemoryStateStore> {
        create_pk_index_state_table(self.store.clone(), self.table_id).await
    }
}

struct WriterTestHarness {
    tx: MessageSender,
    executor: BoxedMessageStream,
    writer_control_tx: tokio::sync::mpsc::UnboundedSender<IcebergPkIndexWriterControl>,
    written_chunks: Arc<Mutex<Vec<StreamChunk>>>,
    store: MemoryStateStore,
    table_id: TableId,
}

impl WriterTestHarness {
    async fn new() -> Self {
        Self::with_schema(input_schema()).await
    }

    /// Build a harness with a custom input schema. The PK is always the first column (Int64)
    /// so the shared `create_pk_index_state_table` schema applies.
    async fn with_schema(input_schema: Schema) -> Self {
        Self::build(input_schema).await
    }

    async fn build(input_schema: Schema) -> Self {
        let store = MemoryStateStore::new();
        let table_id = TableId::new(1);
        let state_table = create_pk_index_state_table(store.clone(), table_id).await;
        let state_table_factory = Box::new(TestStateTableFactory {
            store: store.clone(),
            table_id,
        });
        let writer = IcebergWriterMock::new(TEST_FILE_PATH);
        let written_chunks = writer.written_chunks();

        let (tx, source) = MockSource::channel();
        let source = source.into_executor(input_schema, vec![0]);
        let lbm = LocalBarrierManager::for_test();
        let (writer_control_tx, writer_control_rx) = tokio::sync::mpsc::unbounded_channel();
        let executor = WriterExecutor::new(
            ActorContext::for_test(123),
            source,
            vec![0],
            state_table,
            state_table_factory,
            writer,
            CHUNK_SIZE,
            SinkId::new(0),
            lbm,
            writer_control_rx,
        )
        .boxed()
        .execute();

        Self {
            tx,
            executor,
            writer_control_tx,
            written_chunks,
            store,
            table_id,
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

    fn push_barrier_with_mutation(&mut self, epoch: u64, mutation: Mutation) {
        let barrier = Barrier::new_test_barrier(test_epoch(epoch))
            .with_mutation(mutation)
            .project_for_actor(ActorId::new(123));
        self.tx.send_barrier(barrier);
    }

    /// Open a fresh reader handle on the shared store, initialized at `epoch` so it reads
    /// storage committed up to `epoch`'s previous checkpoint.
    async fn open_table_at(&self, epoch: u64) -> StateTable<MemoryStateStore> {
        let mut table = create_pk_index_state_table(self.store.clone(), self.table_id).await;
        table
            .init_epoch(EpochPair::new_test_epoch(test_epoch(epoch)))
            .await
            .unwrap();
        table
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

    fn send_control(&self, control: IcebergPkIndexWriterControl) {
        self.writer_control_tx.send(control).unwrap();
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

/// A paused writer must never receive data. Its sink-scoped barrier domain blocks the input,
/// so accepting a chunk here would hide a broken invariant and re-introduce buffering.
#[tokio::test]
async fn test_writer_executor_compaction_rejects_chunk_between_barriers() {
    let sink_id = SinkId::new(0);
    let mut harness = WriterTestHarness::new().await;
    harness.init().await;

    harness.push_barrier_with_mutation(
        2,
        compaction_pair_mutation(sink_id, 7.into(), IcebergPkIndexBarrierPhase::Pause),
    );
    harness.expect_barrier().await;

    harness.send_control(IcebergPkIndexWriterControl::SealReady {
        task_id: 7.into(),
        epoch: test_epoch(2),
    });
    harness.push_pretty_chunk(
        " I I
            + 1 10",
    );
    harness.push_barrier(3);

    let err = harness.executor.next().await.unwrap().unwrap_err();
    assert!(
        err.to_string().contains("received chunk between B1 and B2"),
        "unexpected error: {err}"
    );
}

/// Drives a writer through the pause/control handoff without processing any data in the
/// blocked window. The notification must reopen the resolved index before input continues.
#[tokio::test]
async fn test_writer_executor_compaction_barrier_pair_reopens_only_after_commit() {
    let sink_id = SinkId::new(0);
    let mut harness = WriterTestHarness::new().await;
    harness.init().await; // barrier E1

    // Run: insert pk=1 -> (file1, 0), committed at E2.
    harness.push_pretty_chunk(
        " I I
            + 1 10",
    );
    harness.push_barrier(2);
    harness.expect_barrier().await;

    // Pause at E3 (checkpoint): commit-then-pause.
    harness.push_barrier_with_mutation(
        3,
        Mutation::IcebergPkIndexBarrier(crate::executor::IcebergPkIndexBarrierMutation {
            sink_id,
            task_id: 7.into(),
            phase: IcebergPkIndexBarrierPhase::Pause,
            gated_actor_ids: HashSet::from([ActorId::new(123)]),
        }),
    );
    harness.expect_barrier().await;

    // The index is durable at E_q: a fresh reader sees {1 -> (file1, 0)}.
    {
        let reader = harness.open_table_at(4).await;
        assert_eq!(
            read_index_entry(&reader, 1).await,
            Some((TEST_FILE_PATH.to_owned(), 0))
        );
    }

    // The compaction-apply executor updates the index in B2's pending epoch E3.
    {
        let mut compaction_apply = harness.open_table_at(3).await;
        compaction_apply.update(
            index_row(1, TEST_FILE_PATH, 0),
            index_row(1, "output.parquet", 100),
        );
        compaction_apply
            .commit_for_test(EpochPair::new_test_epoch(test_epoch(4)))
            .await
            .unwrap();
    }

    harness.send_control(IcebergPkIndexWriterControl::SealReady {
        task_id: 7.into(),
        epoch: test_epoch(3),
    });
    harness.push_barrier_with_mutation(
        4,
        Mutation::IcebergPkIndexBarrier(crate::executor::IcebergPkIndexBarrierMutation {
            sink_id,
            task_id: 7.into(),
            phase: IcebergPkIndexBarrierPhase::Resume,
            gated_actor_ids: HashSet::from([ActorId::new(123)]),
        }),
    );
    tokio::time::timeout(Duration::from_secs(1), harness.executor.expect_barrier())
        .await
        .expect("B2 should be released after SealReady");

    // Post-B2 input may queue, but the writer must not poll it before main B2 commits.
    harness.push_pretty_chunk(
        " I I
            - 1 10",
    );
    harness.push_barrier(5);
    assert!(harness.executor.next().now_or_never().is_none());

    harness.send_control(IcebergPkIndexWriterControl::SealReady {
        task_id: 7.into(),
        epoch: test_epoch(3),
    });
    harness.send_control(IcebergPkIndexWriterControl::Committed {
        task_id: 7.into(),
        epoch: test_epoch(3),
    });
    harness
        .expect_position_chunk(vec![("output.parquet".to_owned(), 100)])
        .await;
    harness.expect_barrier().await;

    // The post-resume delete removed pk=1 from the index, committed at E5.
    {
        let reader = harness.open_table_at(6).await;
        assert_eq!(read_index_entry(&reader, 1).await, None);
    }

    // Only the original insert was written to Iceberg; the delete produces a position delete
    // against the resolved location.
    assert_eq!(
        harness.written_chunks(),
        vec![StreamChunk::from_pretty(
            " I I
                + 1 10",
        )]
    );
}

#[tokio::test]
async fn test_writer_executor_compaction_rejects_committed_before_seal_ready() {
    let sink_id = SinkId::new(0);
    let mut harness = WriterTestHarness::new().await;
    harness.init().await;
    harness.push_barrier_with_mutation(
        2,
        compaction_pair_mutation(sink_id, 7.into(), IcebergPkIndexBarrierPhase::Pause),
    );
    harness.expect_barrier().await;

    harness.send_control(IcebergPkIndexWriterControl::Committed {
        task_id: 7.into(),
        epoch: test_epoch(2),
    });
    let err = harness.executor.next().await.unwrap().unwrap_err();
    assert!(
        err.to_string().contains("before SealReady"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn test_writer_executor_compaction_rejects_wrong_seal_epoch() {
    let sink_id = SinkId::new(0);
    let mut harness = WriterTestHarness::new().await;
    harness.init().await;
    harness.push_barrier_with_mutation(
        2,
        compaction_pair_mutation(sink_id, 7.into(), IcebergPkIndexBarrierPhase::Pause),
    );
    harness.expect_barrier().await;

    harness.send_control(IcebergPkIndexWriterControl::SealReady {
        task_id: 7.into(),
        epoch: test_epoch(3),
    });
    let err = harness.executor.next().await.unwrap().unwrap_err();
    assert!(
        err.to_string().contains("expected SealReady"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn test_writer_executor_compaction_rejects_stale_resolver_control() {
    let sink_id = SinkId::new(0);
    let mut harness = WriterTestHarness::new().await;
    harness.init().await;
    harness.push_barrier_with_mutation(
        2,
        compaction_pair_mutation(sink_id, 7.into(), IcebergPkIndexBarrierPhase::Pause),
    );
    harness.expect_barrier().await;

    harness.send_control(IcebergPkIndexWriterControl::SealReady {
        task_id: 6.into(),
        epoch: test_epoch(2),
    });
    let err = harness.executor.next().await.unwrap().unwrap_err();
    assert!(
        err.to_string().contains("expected SealReady(7"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn test_writer_executor_compaction_rejects_mismatched_b2() {
    let sink_id = SinkId::new(0);
    let mut harness = WriterTestHarness::new().await;
    harness.init().await;
    harness.push_barrier_with_mutation(
        2,
        compaction_pair_mutation(sink_id, 7.into(), IcebergPkIndexBarrierPhase::Pause),
    );
    harness.expect_barrier().await;

    harness.send_control(IcebergPkIndexWriterControl::SealReady {
        task_id: 7.into(),
        epoch: test_epoch(2),
    });
    harness.push_barrier_with_mutation(
        3,
        compaction_pair_mutation(sink_id, 8.into(), IcebergPkIndexBarrierPhase::Resume),
    );
    let err = harness.executor.next().await.unwrap().unwrap_err();
    assert!(
        err.to_string()
            .contains("expected matching Resume mutation on B2"),
        "unexpected error: {err}"
    );
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

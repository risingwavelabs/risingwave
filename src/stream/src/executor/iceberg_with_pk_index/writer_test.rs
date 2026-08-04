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

//! Unit tests for the iceberg pk-index [`super::WriterExecutor`].
//!
//! The tests exercise the writer executor end-to-end with an in-memory state
//! store and a mock iceberg data-file writer: inserts write data files and
//! index rows, deletes emit (`file_path`, `position`) chunks, and updates
//! rewrite the index entry and the underlying data file.

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

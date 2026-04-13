// Copyright 2025 RisingWave Labs
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

use std::collections::BTreeSet;

use anyhow::Context;
use hashbrown::HashMap;
use risingwave_common::util::epoch::EpochPair;
use risingwave_connector::sink::iceberg::common::{DvFileCommitMetadata, IcebergV3CommitPayload};
use risingwave_pb::connector_service::{SinkMetadata, sink_metadata};
use risingwave_rpc_client::CoordinatorStreamHandle;

use super::CoordinatorStreamHandleInit;
use crate::executor::prelude::*;

/// Trait abstracting DV (Deletion Vector) file operations for testability.
///
/// Implementations are responsible for reading existing DVs, merging new
/// delete positions, writing DV files, and returning commit metadata.
/// Serialization into `IcebergV3CommitPayload` is handled by the executor.
#[async_trait::async_trait]
pub trait DvHandler: Send + 'static {
    /// Merge new delete positions with existing DVs and write DV files.
    ///
    /// Accepts all pending deletes for the epoch grouped by data file path.
    /// Returns commit metadata for the written DV files, or `None` if empty.
    async fn append_dv(
        &self,
        pending_deletes: HashMap<String, BTreeSet<i64>>,
    ) -> StreamExecutorResult<Option<DvFileCommitMetadata>>;
}

/// DV Merger Executor for Iceberg V3 Sink without Equality Delete.
///
/// This stateless singleton executor receives delete position messages from the
/// Writer Executor. Each message contains (`file_path`, `row_position`) pairs.
///
/// On each barrier, the DV Merger:
/// 1. Groups accumulated delete positions by `file_path`
/// 2. For each file, reads the existing DV from object storage
/// 3. Merges the new positions with the existing DV
/// 4. Writes the merged DV file
/// 5. Reports the DV file metadata to meta
///
/// Input schema: [`file_path`: Varchar, position: Int64]
/// Output: Only barriers (terminal executor in the stream graph).
pub struct DvMergerExecutor<H>
where
    H: DvHandler,
{
    _ctx: ActorContextRef,
    input: Option<Executor>,
    handler: H,
    /// Handle for sending commit metadata to the coordinator.
    coordinator_handle: Option<CoordinatorStreamHandle>,
    coordinator_handle_init: Option<CoordinatorStreamHandleInit>,
}

impl<H> DvMergerExecutor<H>
where
    H: DvHandler,
{
    pub fn new(
        ctx: ActorContextRef,
        input: Executor,
        handler: H,
        coordinator_handle_init: Option<CoordinatorStreamHandleInit>,
    ) -> Self {
        Self {
            _ctx: ctx,
            input: Some(input),
            handler,
            coordinator_handle: None,
            coordinator_handle_init,
        }
    }

    async fn initialize_coordinator_handle(
        &mut self,
        _first_epoch: EpochPair,
    ) -> StreamExecutorResult<()> {
        let Some(init) = self.coordinator_handle_init.take() else {
            return Ok(());
        };

        let (handle, _log_store_rewind_start_epoch) = init.create_handle().await?;
        self.coordinator_handle = Some(handle);
        Ok(())
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(mut self) {
        let mut input = self.input.take().unwrap().execute();

        // Consume the first barrier.
        let barrier = expect_first_barrier(&mut input).await?;
        let first_epoch = barrier.epoch;
        yield Message::Barrier(barrier);
        self.initialize_coordinator_handle(first_epoch).await?;

        // Accumulate delete positions between barriers.
        let mut pending_deletes: HashMap<String, BTreeSet<i64>> = HashMap::new();

        #[for_await]
        for msg in input {
            match msg? {
                Message::Chunk(chunk) => {
                    for (op, row) in chunk.rows() {
                        debug_assert_eq!(op, risingwave_common::array::Op::Insert);
                        let file_path = row
                            .datum_at(0)
                            .map(|d| d.into_utf8())
                            .context("file_path should not be null")?;
                        let offset = row
                            .datum_at(1)
                            .map(|d| d.into_int64())
                            .context("position should not be null")?;
                        pending_deletes
                            .entry_ref(file_path)
                            .or_default()
                            .insert(offset);
                    }
                }
                Message::Barrier(barrier) => {
                    // Process accumulated delete positions via the handler.
                    let dv_meta = if pending_deletes.is_empty() {
                        None
                    } else {
                        self.handler
                            .append_dv(std::mem::take(&mut pending_deletes))
                            .await?
                    };

                    // Serialize DvFileCommitMetadata into IcebergV3CommitPayload
                    // bytes for the coordinator.
                    let metadata_bytes = match dv_meta {
                        Some(meta) => {
                            let payload = IcebergV3CommitPayload::DvFiles(meta);
                            serde_json::to_vec(&payload).context(
                                "failed to serialize DvFileCommitMetadata for coordinator",
                            )?
                        }
                        None => Vec::new(),
                    };

                    let epoch = barrier.epoch;
                    yield Message::Barrier(barrier);

                    if let Some(handle) = &mut self.coordinator_handle {
                        let sink_metadata = SinkMetadata {
                            metadata: Some(sink_metadata::Metadata::Serialized(
                                risingwave_pb::connector_service::sink_metadata::SerializedMetadata {
                                    metadata: metadata_bytes,
                                },
                            )),
                        };
                        handle
                            .commit(epoch.curr, sink_metadata, None)
                            .await
                            .map_err(|e| e.context("failed to send commit to coordinator"))?;
                    }
                }
                Message::Watermark(w) => {
                    yield Message::Watermark(w);
                }
            }
        }
    }
}

impl<H> Execute for DvMergerExecutor<H>
where
    H: DvHandler,
{
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.execute_inner().boxed()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::{Arc, Mutex};

    use hashbrown::HashMap;
    use risingwave_common::array::{Array, ArrayBuilder, I64ArrayBuilder, Op, Utf8ArrayBuilder};
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::types::DataType;
    use risingwave_common::util::epoch::test_epoch;

    use super::*;
    use crate::executor::test_utils::MockSource;

    /// Build a delete position chunk with the given (file_path, position) pairs.
    fn build_delete_position_chunk(positions: &[(&str, i64)]) -> StreamChunk {
        let len = positions.len();
        let mut file_path_builder = Utf8ArrayBuilder::new(len);
        let mut position_builder = I64ArrayBuilder::new(len);
        let ops = vec![Op::Insert; len];

        for (path, offset) in positions {
            file_path_builder.append(Some(*path));
            position_builder.append(Some(*offset));
        }

        let columns = vec![
            file_path_builder.finish().into_ref(),
            position_builder.finish().into_ref(),
        ];

        StreamChunk::from_parts(ops, risingwave_common::array::DataChunk::new(columns, len))
    }

    #[derive(Clone)]
    struct DvHandlerMock {
        /// Existing DVs stored in "object storage".
        existing_dvs: Arc<Mutex<HashMap<String, BTreeSet<i64>>>>,
        /// Written DVs (for verification).
        written_dvs: Arc<Mutex<HashMap<String, BTreeSet<i64>>>>,
    }

    impl DvHandlerMock {
        fn new() -> Self {
            Self {
                existing_dvs: Arc::new(Mutex::new(HashMap::new())),
                written_dvs: Arc::new(Mutex::new(HashMap::new())),
            }
        }

        fn with_existing_dv(self, file_path: &str, positions: BTreeSet<i64>) -> Self {
            self.existing_dvs
                .lock()
                .unwrap()
                .insert(file_path.to_string(), positions);
            self
        }
    }

    #[async_trait::async_trait]
    impl DvHandler for DvHandlerMock {
        async fn append_dv(
            &self,
            pending_deletes: HashMap<String, BTreeSet<i64>>,
        ) -> StreamExecutorResult<Option<DvFileCommitMetadata>> {
            // For testing, we simply merge new positions with existing ones and store in written_dvs.
            for (file_path, positions) in pending_deletes {
                let mut merged = self
                    .existing_dvs
                    .lock()
                    .unwrap()
                    .get(&file_path)
                    .cloned()
                    .unwrap_or_default();
                merged.extend(&positions);
                self.written_dvs.lock().unwrap().insert(file_path, merged);
            }
            Ok(None)
        }
    }

    #[tokio::test]
    async fn test_dv_merger_basic() {
        let handler = DvHandlerMock::new();
        let written_dvs = handler.written_dvs.clone();

        let schema = Schema::new(vec![
            Field::unnamed(DataType::Varchar), // file_path
            Field::unnamed(DataType::Int64),   // position
        ]);

        let (mut tx, source) = MockSource::channel();
        let source = source.into_executor(schema, vec![]);

        let mut executor =
            DvMergerExecutor::new(ActorContext::for_test(123), source, handler, None)
                .boxed()
                .execute();

        // First barrier
        tx.push_barrier(test_epoch(1), false);
        let msg = executor.next().await.unwrap().unwrap();
        assert!(msg.is_barrier());

        // Send delete positions
        let chunk = build_delete_position_chunk(&[
            ("file1.parquet", 0),
            ("file1.parquet", 3),
            ("file2.parquet", 1),
        ]);
        tx.push_chunk(chunk);

        // Barrier triggers DV merge
        tx.push_barrier(test_epoch(2), false);
        let msg = executor.next().await.unwrap().unwrap();
        assert!(msg.is_barrier());

        // Verify written DVs
        let dvs = written_dvs.lock().unwrap();
        assert_eq!(dvs.get("file1.parquet").unwrap(), &BTreeSet::from([0, 3]));
        assert_eq!(dvs.get("file2.parquet").unwrap(), &BTreeSet::from([1]));
    }

    #[tokio::test]
    async fn test_dv_merger_merge_with_existing() {
        // Pre-populate existing DVs
        let handler =
            DvHandlerMock::new().with_existing_dv("file1.parquet", BTreeSet::from([0, 5, 10]));
        let written_dvs = handler.written_dvs.clone();

        let schema = Schema::new(vec![
            Field::unnamed(DataType::Varchar),
            Field::unnamed(DataType::Int64),
        ]);

        let (mut tx, source) = MockSource::channel();
        let source = source.into_executor(schema, vec![]);

        let mut executor =
            DvMergerExecutor::new(ActorContext::for_test(123), source, handler, None)
                .boxed()
                .execute();

        // First barrier
        tx.push_barrier(test_epoch(1), false);
        executor.next().await.unwrap().unwrap();

        // Send new delete positions for file1 (some overlap with existing)
        let chunk = build_delete_position_chunk(&[
            ("file1.parquet", 3),
            ("file1.parquet", 5), // duplicate with existing
            ("file1.parquet", 7),
        ]);
        tx.push_chunk(chunk);

        // Barrier triggers merge
        tx.push_barrier(test_epoch(2), false);
        let msg = executor.next().await.unwrap().unwrap();
        assert!(msg.is_barrier());

        // Verify: merged DV should contain both old and new positions
        let dvs = written_dvs.lock().unwrap();
        assert_eq!(
            dvs.get("file1.parquet").unwrap(),
            &BTreeSet::from([0, 3, 5, 7, 10])
        );
    }

    #[tokio::test]
    async fn test_dv_merger_no_deletes() {
        let handler = DvHandlerMock::new();
        let written_dvs = handler.written_dvs.clone();

        let schema = Schema::new(vec![
            Field::unnamed(DataType::Varchar),
            Field::unnamed(DataType::Int64),
        ]);

        let (mut tx, source) = MockSource::channel();
        let source = source.into_executor(schema, vec![]);

        let mut executor =
            DvMergerExecutor::new(ActorContext::for_test(123), source, handler, None)
                .boxed()
                .execute();

        // First barrier
        tx.push_barrier(test_epoch(1), false);
        executor.next().await.unwrap().unwrap();

        // No chunks, just barrier
        tx.push_barrier(test_epoch(2), false);
        let msg = executor.next().await.unwrap().unwrap();
        assert!(msg.is_barrier());

        // No DVs should be written
        assert!(written_dvs.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_dv_merger_multiple_epochs() {
        let handler = DvHandlerMock::new();
        let written_dvs = handler.written_dvs.clone();

        let schema = Schema::new(vec![
            Field::unnamed(DataType::Varchar),
            Field::unnamed(DataType::Int64),
        ]);

        let (mut tx, source) = MockSource::channel();
        let source = source.into_executor(schema, vec![]);

        let mut executor =
            DvMergerExecutor::new(ActorContext::for_test(123), source, handler, None)
                .boxed()
                .execute();

        // First barrier
        tx.push_barrier(test_epoch(1), false);
        executor.next().await.unwrap().unwrap();

        // Epoch 1: delete from file1
        let chunk = build_delete_position_chunk(&[("file1.parquet", 0)]);
        tx.push_chunk(chunk);
        tx.push_barrier(test_epoch(2), false);
        executor.next().await.unwrap().unwrap();

        {
            let dvs = written_dvs.lock().unwrap();
            assert_eq!(dvs.get("file1.parquet").unwrap(), &BTreeSet::from([0]));
        }

        // Epoch 2: more deletes from file1
        let chunk = build_delete_position_chunk(&[("file1.parquet", 2)]);
        tx.push_chunk(chunk);
        tx.push_barrier(test_epoch(3), false);
        executor.next().await.unwrap().unwrap();

        // Note: the mock doesn't update existing_dvs across epochs,
        // so only the new position is written. In production, the
        // read_existing_dv would return the latest state.
        let dvs = written_dvs.lock().unwrap();
        assert_eq!(dvs.get("file1.parquet").unwrap(), &BTreeSet::from([2]));
    }
}

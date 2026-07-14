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
use risingwave_common::bitmap::Bitmap;
use risingwave_common::id::SinkId;
use risingwave_connector::sink::Result as SinkResult;
use risingwave_pb::connector_service::SinkMetadata;
use risingwave_pb::stream_service::PbIcebergPkIndexSinkRole;

use crate::executor::prelude::*;
use crate::task::LocalBarrierManager;

/// Trait abstracting position-delete file operations for testability.
///
/// Implementations are responsible for reading existing position deletes
/// (V3 Puffin deletion vectors or V2 Parquet position-delete files),
/// merging new delete positions, writing the resulting delete file, and
/// returning the commit metadata for the current barrier.
#[async_trait::async_trait]
pub trait PositionDeleteHandler: Send + 'static {
    fn write(&mut self, path: &str, pos: i64) -> SinkResult<()>;

    async fn flush(&mut self) -> SinkResult<Option<SinkMetadata>>;

    async fn update_vnode_bitmap(&mut self, vnode_bitmap: Bitmap) -> SinkResult<()>;
}

/// Position-delete merger executor for iceberg pk-index sink without Equality Delete.
///
/// This stateless executor receives [`file_path`, `position`] messages from the Writer Executor,
/// merges them with existing position deletes, and reports the merged delete-file metadata to
/// meta on each barrier. Depending on the table format version, the written delete file is a
/// V3 Puffin deletion vector or a V2 file-scoped Parquet position-delete file.
///
/// The upstream plan shards messages by `file_path`, so each actor only merges delete
/// positions for the files assigned to its shard.
///
/// Input schema: [`file_path`: Varchar, `position`: int64]
/// Output: Barriers and watermarks only; no data chunks (terminal executor in the stream graph).
pub struct PositionDeleteMergerExecutor<H>
where
    H: PositionDeleteHandler,
{
    actor_id: ActorId,
    sink_id: SinkId,
    local_barrier_manager: LocalBarrierManager,
    input: Option<Executor>,
    handler: H,
}

impl<H> PositionDeleteMergerExecutor<H>
where
    H: PositionDeleteHandler,
{
    pub fn new(
        actor_id: ActorId,
        sink_id: SinkId,
        local_barrier_manager: LocalBarrierManager,
        input: Executor,
        handler: H,
    ) -> Self {
        Self {
            actor_id,
            sink_id,
            local_barrier_manager,
            input: Some(input),
            handler,
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(mut self) {
        let mut input = self.input.take().unwrap().execute();

        // Consume the first barrier.
        let barrier = expect_first_barrier(&mut input).await?;
        yield Message::Barrier(barrier);

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
                        let position = row
                            .datum_at(1)
                            .context("position should not be null")?
                            .into_int64();
                        self.handler
                            .write(file_path, position)
                            .map_err(|e| StreamExecutorError::sink_error(e, self.sink_id))?;
                    }
                }
                Message::Barrier(barrier) => {
                    let mut metadata = None;
                    if barrier.is_checkpoint() {
                        metadata = self
                            .handler
                            .flush()
                            .await
                            .map_err(|e| StreamExecutorError::sink_error(e, self.sink_id))?;
                    }

                    if let Some(metadata) = metadata
                        && metadata.metadata.is_some()
                    {
                        self.local_barrier_manager
                            .report_iceberg_pk_index_sink_metadata(
                                barrier.epoch,
                                self.sink_id,
                                self.actor_id,
                                PbIcebergPkIndexSinkRole::PositionDeleteMerger,
                                Some(metadata),
                            );
                    }

                    if let Some(vnode_bitmap) = barrier.as_update_vnode_bitmap(self.actor_id) {
                        self.handler
                            .update_vnode_bitmap(vnode_bitmap.as_ref().clone())
                            .await
                            .map_err(|e| StreamExecutorError::sink_error(e, self.sink_id))?;
                    }

                    yield Message::Barrier(barrier);
                }
                Message::Watermark(w) => {
                    yield Message::Watermark(w);
                }
            }
        }
    }
}

impl<H> Execute for PositionDeleteMergerExecutor<H>
where
    H: PositionDeleteHandler,
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
    use risingwave_common::bitmap::BitmapBuilder;
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::id::SinkId;
    use risingwave_common::types::DataType;
    use risingwave_common::util::epoch::test_epoch;

    use super::*;
    use crate::executor::UpdateMutation;
    use crate::executor::test_utils::MockSource;
    use crate::task::LocalBarrierManager;

    fn build_delete_position_chunk(positions: &[(&str, i64)]) -> StreamChunk {
        let len = positions.len();
        let mut file_path_builder = Utf8ArrayBuilder::new(len);
        let mut position_builder = I64ArrayBuilder::new(len);

        for (path, offset) in positions {
            file_path_builder.append(Some(*path));
            position_builder.append(Some(*offset));
        }

        StreamChunk::from_parts(
            vec![Op::Insert; len],
            risingwave_common::array::DataChunk::new(
                vec![
                    file_path_builder.finish().into_ref(),
                    position_builder.finish().into_ref(),
                ],
                len,
            ),
        )
    }

    #[derive(Clone)]
    struct PositionDeleteHandlerMock {
        existing_dvs: Arc<Mutex<HashMap<String, BTreeSet<i64>>>>,
        pending_dvs: Arc<Mutex<HashMap<String, BTreeSet<i64>>>>,
        written_dvs: Arc<Mutex<HashMap<String, BTreeSet<i64>>>>,
        update_bitmap_calls: Arc<Mutex<Vec<Bitmap>>>,
    }

    impl PositionDeleteHandlerMock {
        fn new() -> Self {
            Self {
                existing_dvs: Arc::new(Mutex::new(HashMap::new())),
                pending_dvs: Arc::new(Mutex::new(HashMap::new())),
                written_dvs: Arc::new(Mutex::new(HashMap::new())),
                update_bitmap_calls: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn with_existing_dv(self, file_path: &str, positions: BTreeSet<i64>) -> Self {
            self.existing_dvs
                .lock()
                .unwrap()
                .insert(file_path.to_owned(), positions);
            self
        }
    }

    #[async_trait::async_trait]
    impl PositionDeleteHandler for PositionDeleteHandlerMock {
        fn write(&mut self, path: &str, pos: i64) -> SinkResult<()> {
            self.pending_dvs
                .lock()
                .unwrap()
                .entry_ref(path)
                .or_default()
                .insert(pos);
            Ok(())
        }

        async fn flush(&mut self) -> SinkResult<Option<SinkMetadata>> {
            let pending = {
                let mut pending_dvs = self.pending_dvs.lock().unwrap();
                std::mem::take(&mut *pending_dvs)
            };
            if pending.is_empty() {
                return Ok(None);
            }

            let mut existing_dvs = self.existing_dvs.lock().unwrap();
            let mut written_dvs = self.written_dvs.lock().unwrap();
            for (file_path, positions) in pending {
                let mut merged = existing_dvs.get(&file_path).cloned().unwrap_or_default();
                merged.extend(positions);
                existing_dvs.insert(file_path.clone(), merged.clone());
                written_dvs.insert(file_path, merged);
            }
            // The mock asserts on side effects (`written_dvs`) rather than emitted
            // metadata, so we still return Ok(None). The report-on-barrier path
            // is exercised by the SLT integration tests.
            // TODO: add unit test for report-on-barrier path once test infra is
            // available to capture `LocalBarrierEvent`s on the receiver side.
            Ok(None)
        }

        async fn update_vnode_bitmap(&mut self, vnode_bitmap: Bitmap) -> SinkResult<()> {
            self.update_bitmap_calls.lock().unwrap().push(vnode_bitmap);
            Ok(())
        }
    }

    fn input_schema() -> Schema {
        Schema::new(vec![
            Field::unnamed(DataType::Varchar),
            Field::unnamed(DataType::Int64),
        ])
    }

    #[tokio::test]
    async fn test_position_delete_merger_basic() {
        let handler = PositionDeleteHandlerMock::new();
        let written_dvs = handler.written_dvs.clone();

        let (mut tx, source) = MockSource::channel();
        let source = source.into_executor(input_schema(), vec![]);

        let lbm = LocalBarrierManager::for_test();
        let mut executor =
            PositionDeleteMergerExecutor::new(123.into(), SinkId::new(0), lbm, source, handler)
                .boxed()
                .execute();

        tx.push_barrier(test_epoch(1), false);
        assert!(executor.next().await.unwrap().unwrap().is_barrier());

        tx.push_chunk(build_delete_position_chunk(&[
            ("file1.parquet", 0),
            ("file1.parquet", 3),
            ("file2.parquet", 1),
        ]));
        tx.push_barrier(test_epoch(2), false);

        assert!(executor.next().await.unwrap().unwrap().is_barrier());

        let dvs = written_dvs.lock().unwrap();
        assert_eq!(dvs.get("file1.parquet").unwrap(), &BTreeSet::from([0, 3]));
        assert_eq!(dvs.get("file2.parquet").unwrap(), &BTreeSet::from([1]));
    }

    #[tokio::test]
    async fn test_position_delete_merger_merge_with_existing() {
        let handler = PositionDeleteHandlerMock::new()
            .with_existing_dv("file1.parquet", BTreeSet::from([0, 5, 10]));
        let written_dvs = handler.written_dvs.clone();

        let (mut tx, source) = MockSource::channel();
        let source = source.into_executor(input_schema(), vec![]);

        let lbm = LocalBarrierManager::for_test();
        let mut executor =
            PositionDeleteMergerExecutor::new(123.into(), SinkId::new(0), lbm, source, handler)
                .boxed()
                .execute();

        tx.push_barrier(test_epoch(1), false);
        assert!(executor.next().await.unwrap().unwrap().is_barrier());

        tx.push_chunk(build_delete_position_chunk(&[
            ("file1.parquet", 3),
            ("file1.parquet", 5),
            ("file1.parquet", 7),
        ]));
        tx.push_barrier(test_epoch(2), false);

        assert!(executor.next().await.unwrap().unwrap().is_barrier());

        let dvs = written_dvs.lock().unwrap();
        assert_eq!(
            dvs.get("file1.parquet").unwrap(),
            &BTreeSet::from([0, 3, 5, 7, 10])
        );
    }

    #[tokio::test]
    async fn test_position_delete_merger_no_deletes() {
        let handler = PositionDeleteHandlerMock::new();
        let written_dvs = handler.written_dvs.clone();

        let (mut tx, source) = MockSource::channel();
        let source = source.into_executor(input_schema(), vec![]);

        let lbm = LocalBarrierManager::for_test();
        let mut executor =
            PositionDeleteMergerExecutor::new(123.into(), SinkId::new(0), lbm, source, handler)
                .boxed()
                .execute();

        tx.push_barrier(test_epoch(1), false);
        assert!(executor.next().await.unwrap().unwrap().is_barrier());

        tx.push_barrier(test_epoch(2), false);
        assert!(executor.next().await.unwrap().unwrap().is_barrier());

        assert!(written_dvs.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_position_delete_merger_multiple_epochs() {
        let handler = PositionDeleteHandlerMock::new();
        let written_dvs = handler.written_dvs.clone();

        let (mut tx, source) = MockSource::channel();
        let source = source.into_executor(input_schema(), vec![]);

        let lbm = LocalBarrierManager::for_test();
        let mut executor =
            PositionDeleteMergerExecutor::new(123.into(), SinkId::new(0), lbm, source, handler)
                .boxed()
                .execute();

        tx.push_barrier(test_epoch(1), false);
        assert!(executor.next().await.unwrap().unwrap().is_barrier());

        tx.push_chunk(build_delete_position_chunk(&[("file1.parquet", 0)]));
        tx.push_barrier(test_epoch(2), false);
        assert!(executor.next().await.unwrap().unwrap().is_barrier());
        assert_eq!(
            written_dvs.lock().unwrap().get("file1.parquet").unwrap(),
            &BTreeSet::from([0])
        );

        tx.push_chunk(build_delete_position_chunk(&[("file1.parquet", 2)]));
        tx.push_barrier(test_epoch(3), false);
        assert!(executor.next().await.unwrap().unwrap().is_barrier());

        assert_eq!(
            written_dvs.lock().unwrap().get("file1.parquet").unwrap(),
            &BTreeSet::from([0, 2])
        );
    }

    #[tokio::test]
    async fn test_position_delete_merger_reseeds_on_vnode_update() {
        let handler = PositionDeleteHandlerMock::new();
        let update_calls = handler.update_bitmap_calls.clone();

        let (mut tx, source) = MockSource::channel();
        let source = source.into_executor(input_schema(), vec![]);
        let lbm = LocalBarrierManager::for_test();
        let actor_id = 123u32;
        let mut executor = PositionDeleteMergerExecutor::new(
            actor_id.into(),
            SinkId::new(0),
            lbm,
            source,
            handler,
        )
        .boxed()
        .execute();

        tx.push_barrier(test_epoch(1), false);
        assert!(executor.next().await.unwrap().unwrap().is_barrier());

        // Build a checkpoint barrier carrying an update-vnode-bitmap for this actor.
        let mut builder = BitmapBuilder::zeroed(256);
        builder.set(7, true);
        let new_bitmap = builder.finish();
        let barrier = Barrier::new_test_barrier(test_epoch(2)).with_mutation(Mutation::Update(
            UpdateMutation {
                vnode_bitmaps: std::collections::HashMap::from([(
                    actor_id.into(),
                    Arc::new(new_bitmap.clone()),
                )]),
                ..Default::default()
            },
        ));
        tx.send_barrier(barrier);
        assert!(executor.next().await.unwrap().unwrap().is_barrier());

        let calls = update_calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0], new_bitmap);
    }
}

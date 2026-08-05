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
use risingwave_common::bail;
use risingwave_common::id::SinkId;
use risingwave_connector::sink::Result as SinkResult;
use risingwave_pb::connector_service::SinkMetadata;
use risingwave_pb::id::IcebergCompactionTaskId;
use risingwave_pb::stream_service::PbIcebergPkIndexSinkRole;

use crate::executor::IcebergPkIndexBarrierPhase;
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
    /// Start (or restart) seeding the resident delete state from an iceberg snapshot that includes
    /// everything committed through `wait_epoch`.
    ///
    /// Called once after the executor's first barrier, and again after every compaction commit (see
    /// [`PositionDeleteMergerExecutor`]). A restart MUST discard all previously seeded and written
    /// state.
    fn start_seed(&mut self, wait_epoch: u64);

    fn write(&mut self, path: &str, pos: i64) -> SinkResult<()>;

    async fn flush(&mut self) -> SinkResult<Option<SinkMetadata>>;
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
/// # Compaction
///
/// Compaction rewrites the very data files this executor's resident delete state is keyed by. On
/// the `Resume` half of a coordinated compaction barrier pair, the merger therefore discards that
/// state and re-seeds from the post-compaction snapshot.
///
/// Unlike the [`WriterExecutor`](super::WriterExecutor), the merger is never a *gated* actor of
/// that pair: gating pauses ingress so the pk-index table has a single writer, which does not
/// concern this executor. It only observes the mutation's context (preserved on every actor's
/// barrier by `Barrier::project_for_actor`) and so must not filter on `gated_actor_ids`.
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

        // Consume the first barrier. Its `prev` epoch is the point the previous actors (if this is a
        // scale) committed up to; seed only from a snapshot that includes it.
        let barrier = expect_first_barrier(&mut input).await?;
        self.handler.start_seed(barrier.epoch.prev);
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
                    barrier.assume_no_update_vnode_bitmap(self.actor_id)?;
                    let compaction_resumed = self.compaction_resume_task_id(&barrier);
                    if compaction_resumed.is_some() && !barrier.is_checkpoint() {
                        bail!(
                            "iceberg pk-index merger {} received a non-checkpoint compaction resume barrier {:?}",
                            self.sink_id,
                            barrier.epoch
                        );
                    }

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

                    if let Some(task_id) = compaction_resumed {
                        tracing::info!(
                            sink_id = self.sink_id.as_raw_id(),
                            actor_id = self.actor_id.as_raw_id(),
                            %task_id,
                            wait_epoch = barrier.epoch.prev,
                            "compaction committed; discarding staged position deletes and re-seeding"
                        );
                        self.handler.start_seed(barrier.epoch.prev);
                    }

                    yield Message::Barrier(barrier);
                }
                Message::Watermark(w) => {
                    yield Message::Watermark(w);
                }
            }
        }
    }

    /// The compaction task id if `barrier` carries the `Resume` half of a coordinated compaction
    /// barrier pair for this sink, meaning the compaction commits under `barrier.epoch.prev`.
    ///
    /// Deliberately does not filter on `gated_actor_ids`: gating targets ingress actors only, and
    /// the merger reacts to the compaction commit rather than to being paused.
    fn compaction_resume_task_id(&self, barrier: &Barrier) -> Option<IcebergCompactionTaskId> {
        match barrier.iceberg_pk_index_barrier() {
            Some(context)
                if context.sink_id == self.sink_id
                    && context.phase == IcebergPkIndexBarrierPhase::Resume =>
            {
                Some(context.task_id)
            }
            _ => None,
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
    use std::collections::{BTreeSet, HashSet};
    use std::sync::{Arc, Mutex};

    use hashbrown::HashMap;
    use risingwave_common::array::{Array, ArrayBuilder, I64ArrayBuilder, Op, Utf8ArrayBuilder};
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::id::SinkId;
    use risingwave_common::types::DataType;
    use risingwave_common::util::epoch::{EpochPair, test_epoch};
    use risingwave_pb::stream_plan::barrier::BarrierKind;

    use super::*;
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

    type Dvs = HashMap<String, BTreeSet<i64>>;

    /// In-memory stand-in for [`PositionDeleteHandlerImpl`](super::PositionDeleteHandlerImpl),
    /// modelling the three layers that matter for the re-seed contract:
    ///
    /// - `table_state`: the committed iceberg snapshot. Tests mutate it directly to simulate an
    ///   external commit (e.g. compaction replacing data files and adding resolver delete files).
    /// - `staged`: the resident per-shard cache. Populated only by `start_seed` (from a *clone* of
    ///   `table_state`, so later external commits are invisible until the next seed) and appended
    ///   to by `flush`.
    /// - `pending`: positions buffered since the last flush.
    #[derive(Clone)]
    struct PositionDeleteHandlerMock {
        table_state: Arc<Mutex<Dvs>>,
        staged: Arc<Mutex<Dvs>>,
        pending: Arc<Mutex<Dvs>>,
        written_dvs: Arc<Mutex<Dvs>>,
        /// Every `wait_epoch` passed to `start_seed`, in call order.
        seed_epochs: Arc<Mutex<Vec<u64>>>,
    }

    impl PositionDeleteHandlerMock {
        fn new() -> Self {
            Self {
                table_state: Arc::new(Mutex::new(HashMap::new())),
                staged: Arc::new(Mutex::new(HashMap::new())),
                pending: Arc::new(Mutex::new(HashMap::new())),
                written_dvs: Arc::new(Mutex::new(HashMap::new())),
                seed_epochs: Arc::new(Mutex::new(Vec::new())),
            }
        }

        /// Pre-commit a delete vector into the table, as if a previous incarnation of this actor
        /// (or the compaction resolver) had written it. Picked up by the next `start_seed`.
        fn with_existing_dv(self, file_path: &str, positions: BTreeSet<i64>) -> Self {
            self.table_state
                .lock()
                .unwrap()
                .insert(file_path.to_owned(), positions);
            self
        }
    }

    #[async_trait::async_trait]
    impl PositionDeleteHandler for PositionDeleteHandlerMock {
        fn start_seed(&mut self, wait_epoch: u64) {
            self.seed_epochs.lock().unwrap().push(wait_epoch);
            // Discard the resident cache wholesale and re-derive it from the (possibly advanced)
            // committed snapshot. This is the eviction + resync the real handler gets by dropping
            // its `SeededState`.
            *self.staged.lock().unwrap() = self.table_state.lock().unwrap().clone();
        }

        fn write(&mut self, path: &str, pos: i64) -> SinkResult<()> {
            self.pending
                .lock()
                .unwrap()
                .entry_ref(path)
                .or_default()
                .insert(pos);
            Ok(())
        }

        async fn flush(&mut self) -> SinkResult<Option<SinkMetadata>> {
            let pending = std::mem::take(&mut *self.pending.lock().unwrap());
            if pending.is_empty() {
                return Ok(None);
            }

            let mut staged = self.staged.lock().unwrap();
            let mut table_state = self.table_state.lock().unwrap();
            let mut written_dvs = self.written_dvs.lock().unwrap();
            for (file_path, positions) in pending {
                // Merge against the resident state only, exactly like the real handler: anything
                // committed to the table but not seeded is invisible and would be clobbered.
                let mut merged = staged.get(&file_path).cloned().unwrap_or_default();
                merged.extend(positions);
                staged.insert(file_path.clone(), merged.clone());
                table_state.insert(file_path.clone(), merged.clone());
                written_dvs.insert(file_path, merged);
            }
            // The mock asserts on side effects (`written_dvs`) rather than emitted
            // metadata, so we still return Ok(None). The report-on-barrier path
            // is exercised by the SLT integration tests.
            // TODO: add unit test for report-on-barrier path once test infra is
            // available to capture `LocalBarrierEvent`s on the receiver side.
            Ok(None)
        }
    }

    /// Build the `Resume` (or `Pause`) half of a coordinated compaction barrier pair, projected for
    /// the merger actor. `gated_actor_ids` deliberately holds only a *writer* actor id: the merger
    /// is never gated, and must react to the context alone.
    fn compaction_barrier(
        epoch: u64,
        sink_id: SinkId,
        phase: IcebergPkIndexBarrierPhase,
    ) -> Barrier {
        Barrier::new_test_barrier(test_epoch(epoch))
            .with_mutation(Mutation::IcebergPkIndexBarrier(
                crate::executor::IcebergPkIndexBarrierMutation {
                    sink_id,
                    task_id: 7.into(),
                    phase,
                    gated_actor_ids: HashSet::from([ActorId::new(999)]),
                },
            ))
            .project_for_actor(MERGER_ACTOR_ID)
    }

    const MERGER_ACTOR_ID: ActorId = ActorId::new(123);

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

    /// Compaction retires the data files the resident delete state is keyed by, and the surviving
    /// output files carry delete files written by the compaction resolver. The `Resume` barrier must
    /// therefore evict the whole cache and re-seed from the post-compaction snapshot, so the next
    /// delete merges with the resolver's delete instead of clobbering it.
    #[tokio::test]
    async fn test_position_delete_merger_compaction_resume_reseeds_from_new_snapshot() {
        let sink_id = SinkId::new(0);
        let handler = PositionDeleteHandlerMock::new();
        let table_state = handler.table_state.clone();
        let staged = handler.staged.clone();
        let written_dvs = handler.written_dvs.clone();
        let seed_epochs = handler.seed_epochs.clone();

        let (mut tx, source) = MockSource::channel();
        let source = source.into_executor(input_schema(), vec![]);

        let lbm = LocalBarrierManager::for_test();
        let mut executor =
            PositionDeleteMergerExecutor::new(MERGER_ACTOR_ID, sink_id, lbm, source, handler)
                .boxed()
                .execute();

        tx.push_barrier(test_epoch(1), false);
        assert!(executor.next().await.unwrap().unwrap().is_barrier());

        // Steady state: delete position 0 of the pre-compaction data file.
        tx.push_chunk(build_delete_position_chunk(&[("input.parquet", 0)]));
        tx.push_barrier(test_epoch(2), false);
        assert!(executor.next().await.unwrap().unwrap().is_barrier());
        assert_eq!(
            written_dvs.lock().unwrap().get("input.parquet").unwrap(),
            &BTreeSet::from([0])
        );

        // Compaction commits under epoch `test_epoch(2)`: `input.parquet` is retired in favour of
        // `output.parquet`, whose position 4 is already deleted by a resolver-written delete file.
        {
            let mut table_state = table_state.lock().unwrap();
            table_state.remove("input.parquet");
            table_state.insert("output.parquet".to_owned(), BTreeSet::from([4]));
        }

        tx.send_barrier(compaction_barrier(
            3,
            sink_id,
            IcebergPkIndexBarrierPhase::Resume,
        ));
        assert!(executor.next().await.unwrap().unwrap().is_barrier());

        {
            let staged = staged.lock().unwrap();
            assert!(
                !staged.contains_key("input.parquet"),
                "the retired data file must be evicted from the resident cache"
            );
            assert_eq!(
                staged.get("output.parquet").unwrap(),
                &BTreeSet::from([4]),
                "the resolver's delete must be resident after the re-seed"
            );
        }

        tx.push_chunk(build_delete_position_chunk(&[("output.parquet", 9)]));
        tx.push_barrier(test_epoch(4), false);
        assert!(executor.next().await.unwrap().unwrap().is_barrier());
        assert_eq!(
            written_dvs.lock().unwrap().get("output.parquet").unwrap(),
            &BTreeSet::from([4, 9])
        );

        // Seeded twice: at the first barrier, and at the epoch the compaction committed under.
        assert_eq!(
            *seed_epochs.lock().unwrap(),
            vec![
                EpochPair::new_test_epoch(test_epoch(1)).prev,
                EpochPair::new_test_epoch(test_epoch(3)).prev,
            ]
        );
    }

    /// Only the `Resume` half signals that a compaction has committed. The `Pause` half arrives
    /// before the resolver has even run, so re-seeding there would just reload the same snapshot.
    #[tokio::test]
    async fn test_position_delete_merger_compaction_pause_does_not_reseed() {
        let sink_id = SinkId::new(0);
        let handler = PositionDeleteHandlerMock::new();
        let seed_epochs = handler.seed_epochs.clone();

        let (mut tx, source) = MockSource::channel();
        let source = source.into_executor(input_schema(), vec![]);

        let lbm = LocalBarrierManager::for_test();
        let mut executor =
            PositionDeleteMergerExecutor::new(MERGER_ACTOR_ID, sink_id, lbm, source, handler)
                .boxed()
                .execute();

        tx.push_barrier(test_epoch(1), false);
        assert!(executor.next().await.unwrap().unwrap().is_barrier());

        tx.send_barrier(compaction_barrier(
            2,
            sink_id,
            IcebergPkIndexBarrierPhase::Pause,
        ));
        assert!(executor.next().await.unwrap().unwrap().is_barrier());

        assert_eq!(seed_epochs.lock().unwrap().len(), 1);
    }

    /// Compaction is coordinated per sink, and one worker can host mergers of several sinks. A
    /// `Resume` for another sink's compaction must not evict this sink's cache.
    #[tokio::test]
    async fn test_position_delete_merger_compaction_resume_for_other_sink_is_ignored() {
        let handler = PositionDeleteHandlerMock::new();
        let seed_epochs = handler.seed_epochs.clone();

        let (mut tx, source) = MockSource::channel();
        let source = source.into_executor(input_schema(), vec![]);

        let lbm = LocalBarrierManager::for_test();
        let mut executor = PositionDeleteMergerExecutor::new(
            MERGER_ACTOR_ID,
            SinkId::new(0),
            lbm,
            source,
            handler,
        )
        .boxed()
        .execute();

        tx.push_barrier(test_epoch(1), false);
        assert!(executor.next().await.unwrap().unwrap().is_barrier());

        tx.send_barrier(compaction_barrier(
            2,
            SinkId::new(1),
            IcebergPkIndexBarrierPhase::Resume,
        ));
        assert!(executor.next().await.unwrap().unwrap().is_barrier());

        assert_eq!(seed_epochs.lock().unwrap().len(), 1);
    }

    /// The re-seed is only safe because the checkpoint flush on the same barrier drains the buffered
    /// positions first. A non-checkpoint `Resume` would re-seed on top of positions accumulated
    /// against the retired data files, so the executor refuses it instead.
    #[tokio::test]
    async fn test_position_delete_merger_non_checkpoint_compaction_resume_fails() {
        let sink_id = SinkId::new(0);
        let handler = PositionDeleteHandlerMock::new();
        let seed_epochs = handler.seed_epochs.clone();

        let (mut tx, source) = MockSource::channel();
        let source = source.into_executor(input_schema(), vec![]);

        let lbm = LocalBarrierManager::for_test();
        let mut executor =
            PositionDeleteMergerExecutor::new(MERGER_ACTOR_ID, sink_id, lbm, source, handler)
                .boxed()
                .execute();

        tx.push_barrier(test_epoch(1), false);
        assert!(executor.next().await.unwrap().unwrap().is_barrier());

        let mut barrier = compaction_barrier(2, sink_id, IcebergPkIndexBarrierPhase::Resume);
        barrier.kind = BarrierKind::Barrier;
        tx.send_barrier(barrier);

        let err = executor.next().await.unwrap().unwrap_err();
        assert!(
            err.to_string()
                .contains("non-checkpoint compaction resume barrier"),
            "unexpected error: {err}"
        );
        assert_eq!(seed_epochs.lock().unwrap().len(), 1);
    }
}

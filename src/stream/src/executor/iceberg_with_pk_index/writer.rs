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
use crate::executor::barrier_align::{AlignedMessage, barrier_align};
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

/// Outcome of classifying a compaction "candidate" row against the PK index.
///
/// During compaction the engine rewrites a set of input data files into output data files. The
/// writer's PK index is the authoritative liveness oracle: for each rewritten output row we must
/// decide whether it is still live (and the index should be repaired to point at the output file)
/// or whether it is dead (and the output copy must be masked with a position delete).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ResolveClassification {
    /// The row is still live; its live copy currently lives in one of the compacted input files,
    /// so the index entry must be repaired to point at the output file.
    Survivor,
    /// The row is no longer live at the output location; the output copy must be masked with a
    /// position delete. This covers two cases: the PK was deleted (no index entry), or the PK was
    /// deleted-then-reinserted so the live copy now lives in a non-compacted file.
    Dead,
}

/// Classify a single compaction candidate using the PK index as the liveness oracle.
///
/// `index_row` is the current `pk_index` entry for the candidate PK (its layout is
/// `[pk.., file_path, position]`), or `None` if no entry exists. `input_file_paths` is the set of
/// data files being compacted away.
///
/// The set-membership test on the stored `file_path` is mandatory: a naive "present -> survivor"
/// would corrupt a deleted-then-reinserted entry whose live copy already moved to a non-compacted
/// file, silently resurrecting a stale duplicate.
///
/// Each candidate PK is produced exactly once per resolve window (every output file is scanned by
/// exactly one resolve actor), so this classifier need not defend against duplicate delivery.
/// Cross-compaction correctness still holds: a PK that a prior compaction remapped to some output
/// file has its index entry's `file_path` equal to that file, which IS in the next compaction's
/// `input_file_paths` (the file is now being compacted away), so it classifies `Survivor`.
fn classify_resolve_candidate(
    index_row: Option<&OwnedRow>,
    input_file_paths: &HashSet<String>,
) -> ResolveClassification {
    let Some(index_row) = index_row else {
        // No index entry: the PK was deleted and not reinserted.
        return ResolveClassification::Dead;
    };

    // `file_path` is the second-to-last column in the index row layout `[pk.., file_path, pos]`.
    let num_cols = index_row.len();
    let file_path = index_row
        .datum_at(num_cols - 2)
        .expect("file_path should not be null")
        .into_utf8();

    if input_file_paths.contains(file_path) {
        // The live copy lives in a compacted input file: a genuine survivor whose index entry must
        // be repaired to point at the output file.
        ResolveClassification::Survivor
    } else {
        // The live copy lives in a non-compacted file: this candidate is a stale duplicate left
        // behind by a delete-then-reinsert.
        ResolveClassification::Dead
    }
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
    /// The transient "remap edge" feeding compaction-resolve candidate rows during a resolve
    /// window. Schema `[pk.., output_file: Varchar, output_pos: Int64]`, hash-distributed by PK so
    /// each candidate lands on the actor owning its vnode. `None` when no resolve window is active
    /// (steady state); the meta attach path sets the upstream at runtime during a
    /// compaction-resolve window.
    remap_input: Option<Executor>,
    metrics: Arc<StreamingMetrics>,
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
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        ctx: ActorContextRef,
        input: Executor,
        remap_input: Option<Executor>,
        metrics: Arc<StreamingMetrics>,
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
            remap_input,
            metrics,
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

    /// Apply one compaction "candidate" row produced by the resolve phase.
    ///
    /// `pk_row` is the candidate's primary key, `output_file`/`output_pos` locate the rewritten
    /// row in the compaction output. `input_file_paths` is the set of data files being compacted
    /// away. The PK index is consulted as the liveness oracle (see
    /// [`classify_resolve_candidate`]):
    ///
    /// - `Survivor`: repair the index entry for this PK to point at the output location.
    /// - `Dead`: emit a position delete for the output location so it is masked downstream, and
    ///   leave the index untouched (any entry already points at the correct live copy).
    async fn apply_resolve_candidate(
        &mut self,
        pk_row: OwnedRow,
        output_file: &str,
        output_pos: i64,
        input_file_paths: &HashSet<String>,
    ) -> StreamExecutorResult<()> {
        let index_row = self.pk_index_state_table.get_row(&pk_row).await?;

        match classify_resolve_candidate(index_row.as_ref(), input_file_paths) {
            ResolveClassification::Survivor => {
                // Repair the index: build the new entry `[pk.., output_file, output_pos]`,
                // mirroring how the insert path constructs index rows.
                let old_row = index_row.expect("survivor must have an index row");

                let mut new_index_row = Vec::with_capacity(pk_row.len() + 2);
                for datum in pk_row.iter() {
                    new_index_row.push(datum);
                }
                new_index_row.push(Some(ScalarRefImpl::Utf8(output_file)));
                new_index_row.push(Some(ScalarRefImpl::Int64(output_pos)));
                self.pk_index_state_table
                    .update(old_row, new_index_row.as_slice());
            }
            ResolveClassification::Dead => {
                // Mask the output copy via a position delete on the shared delete buffer, so it
                // flows downstream to the DV merger just like a regular DELETE.
                let buffer = self
                    .delete_position_buffer
                    .get_or_insert_with(|| new_chunk_builder(self.chunk_size));
                // A single appended row cannot overflow a fresh-or-existing builder past its
                // capacity boundary here; the barrier path drains the buffer regardless.
                let _ = append_row(buffer, output_file, output_pos);
            }
        }
        Ok(())
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

    /// Commit the pk_index repair and (optionally) flush buffered position-deletes at a checkpoint
    /// barrier, then yield the barrier. Shared by both steady-state and resolve checkpoints because
    /// the commit path is identical: the resolve phase repairs the same `pk_index_state_table` and
    /// buffers position-deletes in the same `delete_position_buffer`.
    ///
    /// `flush_delete_buffer` controls whether the buffered position-deletes are drained downstream at
    /// THIS checkpoint. It is `true` everywhere except at intra-window resolve checkpoints: a
    /// resolve-generated position-delete masks a row in a compaction OUTPUT data file, but that output
    /// file only enters the iceberg table when the bracketing-epoch `Resolver`-done report folds the
    /// overwrite in (a LATER epoch). Flushing such a DV early would commit a position-delete that
    /// references a data file not yet present in any table snapshot — an invalid/dangling delete. So
    /// the resolve window defers these DVs and drains them at the `End` checkpoint, which meta injects
    /// only AFTER the fold has committed the output file (see `detach_v3_resolve` /
    /// `try_complete_v3_resolve`), guaranteeing the output file is already in the table by then.
    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn commit_barrier(&mut self, barrier: Barrier, flush_delete_buffer: bool) {
        let mut metadata = None;
        if barrier.is_checkpoint() {
            if flush_delete_buffer
                && let Some(chunk) = self
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

    /// If `barrier` carries an `IcebergV3Resolve` mutation addressed to this sink, return its phase
    /// and (for `Start`) the set of `input_file_paths` being compacted away. Returns `None` for all
    /// other barriers (the steady-state case).
    fn resolve_request(&self, barrier: &Barrier) -> Option<(ResolvePhase, HashSet<String>)> {
        if let Some(Mutation::IcebergV3Resolve {
            sink_id,
            phase,
            input_file_paths,
        }) = barrier.mutation.as_deref()
            && self.sink_id == *sink_id
        {
            return Some((*phase, input_file_paths.iter().cloned().collect()));
        }
        None
    }

    /// Apply one remap-edge candidate chunk during the resolve window. Each row is
    /// `[pk.., output_file: Varchar, output_pos: Int64]` (all `Op::Insert`): the trailing two
    /// columns locate the rewritten row, the leading columns are the PK probed against the index.
    async fn process_remap_chunk(
        &mut self,
        chunk: StreamChunk,
        input_file_paths: &HashSet<String>,
    ) -> StreamExecutorResult<()> {
        let num_cols = chunk.data_chunk().dimension();
        let file_col = num_cols - 2;
        let pos_col = num_cols - 1;
        // The remap-edge candidate row layout is `[pk.., output_file, output_pos]`: the PK occupies
        // the LEADING `pk_len` columns, regardless of where the PK sits in the writer's DATA schema
        // (`self.pk_indices`). Project the leading columns, not `self.pk_indices`, so a non-leading
        // PK is probed correctly against the index.
        let pk_len = self.pk_indices.len();
        let pk_proj: Vec<usize> = (0..pk_len).collect();
        for (op, row) in chunk.rows() {
            // The resolve pipeline emits inserts only; anything else is a producer bug.
            debug_assert!(
                matches!(op, risingwave_common::array::Op::Insert),
                "remap edge must only carry inserts"
            );
            let pk_row = row.project(&pk_proj).to_owned_row();
            let output_file = row
                .datum_at(file_col)
                .context("remap output_file should not be null")?
                .into_utf8();
            let output_pos = row
                .datum_at(pos_col)
                .context("remap output_pos should not be null")?
                .into_int64();
            self.apply_resolve_candidate(pk_row, output_file, output_pos, input_file_paths)
                .await?;
        }
        Ok(())
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(mut self) {
        let input = self.input.take().unwrap().execute();

        // Build the aligned stream over (real = left, remap = right). When the transient resolve
        // pipeline is not wired yet, the remap edge is absent and we adapt the single real input
        // into an aligned stream whose `Right` branch never fires — steady state is byte-identical.
        let metrics = self.metrics.clone();
        let actor_id = self.ctx.id;
        let fragment_id = self.ctx.fragment_id;
        let aligned = if let Some(remap_input) = self.remap_input.take() {
            barrier_align(
                input,
                remap_input.execute(),
                actor_id,
                fragment_id,
                metrics,
                "IcebergWithPkIndexWriter",
            )
            .boxed()
        } else {
            single_input_aligned(input).boxed()
        };
        let mut aligned = aligned;

        // Consume the first barrier.
        let barrier = expect_first_barrier_from_aligned_stream(&mut aligned).await?;
        let first_epoch = barrier.epoch;

        yield Message::Barrier(barrier);
        self.pk_index_state_table.init_epoch(first_epoch).await?;

        while let Some(msg) = aligned.next().await {
            match msg? {
                AlignedMessage::Left(chunk) =>
                {
                    #[for_await]
                    for data_chunk in self.process_chunk(chunk) {
                        yield Message::Chunk(data_chunk?.into());
                    }
                }
                // No remap candidates are expected outside a resolve window; ignore defensively.
                AlignedMessage::Right(_) => {}
                AlignedMessage::Barrier(barrier) => {
                    if let Some((ResolvePhase::Start, input_file_paths)) =
                        self.resolve_request(&barrier)
                    {
                        // The `Start` barrier MUST be a checkpoint: entering resolve-mode here relies on
                        // the opening `commit_barrier` below flushing all pre-resolve in-flight writes to
                        // Iceberg, so the subsequent compaction overwrite is consistent with what the
                        // pk_index reflects. A non-checkpoint `Start` would leave buffered writes
                        // un-flushed and could lose/duplicate them across the overwrite. This holds today
                        // because the `Start` mutation rides a command barrier and `Command::need_checkpoint()`
                        // forces a checkpoint for every command except `Resume`; assert it so a future
                        // change to that policy fails loudly here instead of silently corrupting data.
                        debug_assert!(
                            barrier.is_checkpoint(),
                            "iceberg v3 resolve Start barrier must be a checkpoint"
                        );

                        // Enter resolve-mode: yield this barrier first (it opens the window), then
                        // block the real upstream and drain the remap edge across HOWEVER MANY
                        // checkpoints until the `End` mutation arrives.
                        //
                        // Invariant of the window: the real upstream is BLOCKED (its chunks are
                        // stashed, not applied), so the pk_index is stable while we probe it. We
                        // drain the remap edge applying candidates; at EACH checkpoint barrier we
                        // commit the repair + flush position-deletes (the same `commit_barrier`
                        // path as steady state) and stay in resolve-mode. The window closes only
                        // when an `IcebergV3Resolve { phase: End }` mutation rides a barrier: we
                        // commit that checkpoint normally, then replay the stashed real chunks so
                        // they are applied AFTER the index has been repaired.
                        //
                        // The remap edge is `barrier_align`ed throughout; once meta DETACHES the
                        // transient pipeline the remap Merge goes IDLE (only barriers) rather than
                        // closing, so we cannot rely on stream-end — the explicit `End` mutation is
                        // the sole exit signal.
                        //
                        // Inlined here (rather than a helper method) because the borrowed `aligned`
                        // local and `&mut self` would otherwise carry distinct lifetimes that a
                        // `#[try_stream]` method signature cannot reconcile.
                        //
                        // Commit (not raw-yield) this opening checkpoint: every checkpoint must
                        // advance the pk_index state table's epoch, otherwise a later in-window
                        // commit breaks the consecutive-epoch chain. No real chunks have been
                        // applied yet, so this just flushes any pending steady-state writes.
                        tracing::info!(
                            target: "iceberg_v3_resolve",
                            actor_id = %actor_id,
                            epoch = ?barrier.epoch,
                            input_files = input_file_paths.len(),
                            "writer entering resolve-mode (Start); blocking real upstream, draining remap edge"
                        );
                        #[for_await]
                        for m in self.commit_barrier(barrier, true) {
                            yield m?;
                        }

                        // This accepts the risk that this stash grows unbounded across the resolve
                        // window: compaction is infrequent and windows are short, so the real
                        // upstream's buffered chunks are bounded in practice. Revisit if resolve
                        // windows ever span many checkpoints under heavy ingest.
                        let mut stashed_left: Vec<StreamChunk> = Vec::new();
                        // Surface the unbounded-stash risk operationally: warn once if the stash grows
                        // unexpectedly large (a long resolve window under heavy ingest), so it is
                        // diagnosable before it can OOM the compute node.
                        let mut stash_warned = false;
                        const STASH_WARN_THRESHOLD: usize = 10_000;
                        while let Some(msg) = aligned.next().await {
                            match msg? {
                                // Real upstream is blocked: stash its chunks, do not process.
                                AlignedMessage::Left(chunk) => {
                                    stashed_left.push(chunk);
                                    if !stash_warned && stashed_left.len() >= STASH_WARN_THRESHOLD {
                                        stash_warned = true;
                                        tracing::warn!(
                                            actor_id = %actor_id,
                                            stashed_chunks = stashed_left.len(),
                                            "iceberg v3 resolve window is stashing a large number of \
                                             real-upstream chunks; the window may be spanning many \
                                             checkpoints under heavy ingest"
                                        );
                                    }
                                }
                                // Remap candidates: probe the (stable) index and repair / mask.
                                AlignedMessage::Right(chunk) => {
                                    self.process_remap_chunk(chunk, &input_file_paths).await?;
                                }
                                AlignedMessage::Barrier(barrier) => {
                                    // The `End` mutation rides a checkpoint barrier and closes the
                                    // window; any other barrier is an intra-window checkpoint that
                                    // commits the in-progress repair and keeps draining.
                                    let is_end = matches!(
                                        self.resolve_request(&barrier),
                                        Some((ResolvePhase::End, _))
                                    );
                                    // Defer resolve-generated position-deletes (dead-PK DVs on the
                                    // compaction OUTPUT file) until `End`: the output file only
                                    // enters the table at the bracketing fold epoch, so an in-window
                                    // DV would dangle. `End` is injected after the fold commits, so
                                    // flushing the accumulated DVs here references an already-present
                                    // output file. Intra-window checkpoints still commit the pk_index
                                    // repair to keep the state-table epoch chain intact.
                                    #[for_await]
                                    for m in self.commit_barrier(barrier, is_end) {
                                        yield m?;
                                    }
                                    if is_end {
                                        break;
                                    }
                                }
                                AlignedMessage::WatermarkLeft(w)
                                | AlignedMessage::WatermarkRight(w) => {
                                    yield Message::Watermark(w);
                                }
                            }
                        }

                        // Resume: replay the stashed real-upstream chunks now that the index is
                        // repaired, so a real chunk seen mid-resolve is applied only AFTER commit.
                        tracing::info!(
                            target: "iceberg_v3_resolve",
                            actor_id = %actor_id,
                            stashed_chunks = stashed_left.len(),
                            "writer exiting resolve-mode (End); replaying stashed real chunks"
                        );
                        for chunk in stashed_left {
                            #[for_await]
                            for data_chunk in self.process_chunk(chunk) {
                                yield Message::Chunk(data_chunk?.into());
                            }
                        }
                    } else {
                        #[for_await]
                        for m in self.commit_barrier(barrier, true) {
                            yield m?;
                        }
                    }
                }
                AlignedMessage::WatermarkLeft(w) | AlignedMessage::WatermarkRight(w) => {
                    yield Message::Watermark(w);
                }
            }
        }
    }
}

/// Adapt a single input stream into an [`AlignedMessageStream`] for the no-remap-edge case. Maps
/// chunks to `Left`, watermarks to `WatermarkLeft`, and barriers to `Barrier`, so the unified
/// `execute_inner` loop runs identically whether or not the remap edge is present.
#[try_stream(ok = AlignedMessage, error = StreamExecutorError)]
async fn single_input_aligned(input: BoxedMessageStream) {
    #[for_await]
    for msg in input {
        match msg? {
            Message::Chunk(chunk) => yield AlignedMessage::Left(chunk),
            Message::Watermark(w) => yield AlignedMessage::WatermarkLeft(w),
            Message::Barrier(barrier) => yield AlignedMessage::Barrier(barrier),
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
                None,
                Arc::new(StreamingMetrics::unused()),
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

    /// Harness for the 2-input (real + remap) resolve path. Mirrors `WriterTestHarness` but wires
    /// a second `MockSource` as the remap edge and lets the test drive a resolve barrier.
    struct ResolveTestHarness {
        real_tx: MessageSender,
        remap_tx: MessageSender,
        executor: BoxedMessageStream,
    }

    impl ResolveTestHarness {
        async fn new() -> Self {
            let store = MemoryStateStore::new();
            let state_table = create_pk_index_state_table(store, TableId::new(1)).await;
            let writer = IcebergWriterMock::new(TEST_FILE_PATH);

            let (real_tx, real_source) = MockSource::channel();
            let real_source = real_source.into_executor(input_schema(), vec![0]);

            // Remap-edge schema: `[pk: Int64, output_file: Varchar, output_pos: Int64]`.
            let remap_schema = Schema::new(vec![
                Field::unnamed(DataType::Int64),
                Field::unnamed(DataType::Varchar),
                Field::unnamed(DataType::Int64),
            ]);
            let (remap_tx, remap_source) = MockSource::channel();
            let remap_source = remap_source.into_executor(remap_schema, vec![0]);

            let lbm = LocalBarrierManager::for_test();
            let executor = WriterExecutor::new(
                ActorContext::for_test(123),
                real_source,
                Some(remap_source),
                Arc::new(StreamingMetrics::unused()),
                vec![0],
                state_table,
                vec![0, 1],
                writer,
                CHUNK_SIZE,
                SinkId::new(0),
                lbm,
            )
            .boxed()
            .execute();

            Self {
                real_tx,
                remap_tx,
                executor,
            }
        }

        /// Both inputs must emit aligned barriers for `barrier_align` to progress.
        fn push_barrier_both(&mut self, epoch: u64) {
            self.real_tx.push_barrier(test_epoch(epoch), false);
            self.remap_tx.push_barrier(test_epoch(epoch), false);
        }

        fn push_resolve_barrier_both(
            &mut self,
            epoch: u64,
            phase: ResolvePhase,
            input_file_paths: Vec<String>,
        ) {
            let mutation = Mutation::IcebergV3Resolve {
                sink_id: 0,
                phase,
                input_file_paths,
            };
            self.real_tx.send_barrier(
                Barrier::new_test_barrier(test_epoch(epoch)).with_mutation(mutation.clone()),
            );
            self.remap_tx
                .send_barrier(Barrier::new_test_barrier(test_epoch(epoch)).with_mutation(mutation));
        }
    }

    /// End-to-end of the resolve window over the 2-input path: a survivor candidate repairs the
    /// index, and a real-upstream DELETE that arrives DURING the resolve scan is stashed and only
    /// applied after the window closes — so it observes the REPAIRED index (output location), not
    /// the pre-resolve one. This is the core stash/replay ordering guarantee.
    #[tokio::test]
    async fn test_writer_executor_resolve_stashes_then_replays_real_chunk() {
        let mut harness = ResolveTestHarness::new().await;

        // Init.
        harness.push_barrier_both(1);
        harness.executor.expect_barrier().await;

        // Insert PK 1 -> lands in `TEST_FILE_PATH` at position 0 in the mock writer.
        harness.real_tx.push_chunk(StreamChunk::from_pretty(
            " I I
            + 1 10",
        ));
        // Remap edge is idle in normal mode; only barriers flow on it.
        harness.push_barrier_both(2);
        harness.executor.expect_barrier().await;

        // Open the resolve window with a START phase. `TEST_FILE_PATH` is the file being compacted
        // away, so PK 1's index entry (which points there) classifies as a survivor.
        harness.push_resolve_barrier_both(3, ResolvePhase::Start, vec![TEST_FILE_PATH.to_owned()]);
        // The resolve barrier itself is yielded first (it opens the window).
        harness.executor.expect_barrier().await;

        // A real-upstream DELETE of PK 1 arrives mid-resolve: it must be stashed, NOT applied yet.
        harness.real_tx.push_chunk(StreamChunk::from_pretty(
            " I I
            - 1 10",
        ));
        // Remap candidate: PK 1 was rewritten to `output.parquet` at position 5 (survivor).
        harness.remap_tx.push_chunk(StreamChunk::from_pretty(
            " I T I
            + 1 output.parquet 5",
        ));
        // An intra-window checkpoint commits the in-progress repair but does NOT close the window:
        // the writer stays in resolve-mode and the stashed DELETE is still held back.
        harness.push_barrier_both(4);
        harness.executor.expect_barrier().await;

        // Close the window with an END mutation riding a checkpoint barrier.
        harness.push_resolve_barrier_both(5, ResolvePhase::End, vec![]);

        // The END commit flushes no position delete (the survivor only repaired the index), then
        // yields the closing barrier.
        harness.executor.expect_barrier().await;

        // After END the stashed DELETE is replayed: it probes the REPAIRED index entry and buffers a
        // position delete at the OUTPUT location (output.parquet, 5). The buffer holds a single row,
        // which is below the `chunk_size` flush threshold, so `process_chunk` emits nothing inline —
        // the replayed delete is only DRAINED downstream at the NEXT checkpoint barrier (epoch 6),
        // exactly like a steady-state delete. So we must push that barrier before expecting the
        // chunk; expecting it earlier would deadlock (writer holding the delete, test holding the
        // barrier).
        harness.push_barrier_both(6);

        // The epoch-6 commit flushes the buffered position delete (chunk first), then yields the
        // barrier. The OUTPUT location (output.parquet, 5) proves the replay observed the repaired
        // index and happened only after the window closed.
        assert_eq!(
            decode_chunk(harness.executor.expect_chunk().await),
            vec![("output.parquet".to_owned(), 5)]
        );
        harness.executor.expect_barrier().await;
    }

    /// A PK deleted in the window `(R, N]` is classified Dead by the resolve scan, producing a
    /// position-delete on the compaction OUTPUT file. That output file only enters the iceberg table
    /// at the later fold epoch, so the DV must NOT be flushed at an intra-window checkpoint (it would
    /// dangle); it must be deferred and drained at the `End` checkpoint. This test proves the
    /// intra-window checkpoint emits NO position-delete chunk while the `End` checkpoint emits it.
    #[tokio::test]
    async fn test_writer_executor_resolve_defers_dead_pk_dv_until_end() {
        let mut harness = ResolveTestHarness::new().await;

        // Init.
        harness.push_barrier_both(1);
        harness.executor.expect_barrier().await;

        // No live PK is inserted: PK 1 has NO index entry, so a resolve candidate for it is Dead.

        // Open the resolve window.
        harness.push_resolve_barrier_both(2, ResolvePhase::Start, vec![TEST_FILE_PATH.to_owned()]);
        harness.executor.expect_barrier().await;

        // Dead candidate: PK 1 (no index entry) was rewritten to `output.parquet` at position 5.
        harness.remap_tx.push_chunk(StreamChunk::from_pretty(
            " I T I
            + 1 output.parquet 5",
        ));
        // Intra-window checkpoint: the Dead DV is buffered but MUST NOT be flushed yet (the output
        // file is not in the table). So the only message at this checkpoint is the barrier.
        harness.push_barrier_both(3);
        harness.executor.expect_barrier().await;

        // Close the window. The deferred Dead DV is now flushed at the `End` checkpoint, where the
        // output file is already committed by the fold, so it references a present data file.
        harness.push_resolve_barrier_both(4, ResolvePhase::End, vec![]);
        assert_eq!(
            decode_chunk(harness.executor.expect_chunk().await),
            vec![("output.parquet".to_owned(), 5)]
        );
        harness.executor.expect_barrier().await;
    }

    #[test]
    fn test_classify_resolve_candidate() {
        // Index row layout is `[pk, file_path, position]`.
        let index_row = OwnedRow::new(vec![
            Some(ScalarImpl::Int64(1)),
            Some(ScalarImpl::Utf8("input_a.parquet".into())),
            Some(ScalarImpl::Int64(7)),
        ]);
        let input_file_paths: HashSet<String> =
            ["input_a.parquet".to_owned()].into_iter().collect();

        // Rule 1: absent index entry -> Dead.
        assert_eq!(
            classify_resolve_candidate(None, &input_file_paths),
            ResolveClassification::Dead
        );
        // Rule 2: present and stored file is being compacted away -> Survivor.
        assert_eq!(
            classify_resolve_candidate(Some(&index_row), &input_file_paths),
            ResolveClassification::Survivor
        );
        // Rule 3: present but stored file is NOT compacted (reinserted elsewhere) -> Dead.
        let empty: HashSet<String> = HashSet::new();
        assert_eq!(
            classify_resolve_candidate(Some(&index_row), &empty),
            ResolveClassification::Dead
        );
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

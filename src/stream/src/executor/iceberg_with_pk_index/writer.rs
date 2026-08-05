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
use risingwave_common::bail;
use risingwave_common::id::SinkId;
use risingwave_common::row::{Project, RowExt};
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_common::util::epoch::EpochPair;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_pb::connector_service::SinkMetadata;
use risingwave_pb::id::IcebergCompactionTaskId;
use risingwave_pb::stream_service::PbIcebergPkIndexSinkRole;
use risingwave_storage::StateStore;
use tokio::sync::mpsc::UnboundedReceiver;

use crate::common::change_buffer::output_kind;
use crate::common::compact_chunk::{InconsistencyBehavior, compact_chunk_inline};
use crate::executor::IcebergPkIndexBarrierPhase;
use crate::executor::prelude::*;
use crate::task::{IcebergPkIndexWriterControl, LocalBarrierManager};

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

/// Rebuilds the writer's pk-index [`StateTable`] when it resumes from a pause.
///
/// During a pause the writer relinquishes its own `StateTable` handle while the compaction resolve
/// job updates the same table. On resume the writer needs a *fresh* handle so that `init_epoch`
/// re-reads committed storage, because `init_epoch` can be called only once per handle (it asserts a
/// table is not init'd twice). This factory produces that fresh handle. Abstracting it as a trait
/// lets the writer be unit-tested with an in-memory store.
#[async_trait::async_trait]
pub trait PkIndexStateTableFactory<S: StateStore>: Send + 'static {
    /// Build a new `StateTable` for the writer's `table_id` and this actor's vnode shard, opened at
    /// the latest committed version (the caller `init_epoch`s it afterwards).
    async fn build(&self) -> StateTable<S>;
}

/// Self-targeted pause control signal carried on a barrier mutation.
#[derive(Clone, Copy)]
enum PauseSignal {
    /// The pause half of the pk-index barrier pair addressed to this writer actor.
    Pause(IcebergCompactionTaskId),
    /// No self-targeted signal on this barrier.
    None,
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
    ///
    /// `None` only while the writer is paused: the handle is relinquished so a transient
    /// compaction-apply executor can be the sole writer of `table_id`, and rebuilt on resume.
    pk_index_state_table: Option<StateTable<S>>,
    /// Rebuilds [`Self::pk_index_state_table`] on resume from a pause.
    state_table_factory: Box<dyn PkIndexStateTableFactory<S>>,
    /// The Iceberg data file writer.
    writer: W,
    /// Buffer for accumulating delete position messages before the next barrier flush.
    delete_position_buffer: Option<DataChunkBuilder>,
    /// Whether the writer is paused: its pk-index state table handle is relinquished while the
    /// compaction-resolver job rebuilds the table. Receiving a chunk in this state is an invariant
    /// violation.
    paused: bool,
    writer_control_rx: UnboundedReceiver<IcebergPkIndexWriterControl>,
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
        state_table_factory: Box<dyn PkIndexStateTableFactory<S>>,
        writer: W,
        chunk_size: usize,
        sink_id: SinkId,
        local_barrier_manager: LocalBarrierManager,
        writer_control_rx: UnboundedReceiver<IcebergPkIndexWriterControl>,
    ) -> Self {
        Self {
            ctx,
            input: Some(input),
            pk_indices,
            pk_index_state_table: Some(pk_index_state_table),
            state_table_factory,
            writer,
            delete_position_buffer: None,
            paused: false,
            writer_control_rx,
            chunk_size,
            sink_id,
            local_barrier_manager,
        }
    }

    /// The pk-index state table. Panics if called while paused (the handle is relinquished then),
    /// which never happens: `process_chunk`/commit paths run only while the writer is running.
    fn state_table_mut(&mut self) -> &mut StateTable<S> {
        self.pk_index_state_table
            .as_mut()
            .expect("pk-index state table must be present while the writer is running")
    }

    async fn delete_existing_row(
        &mut self,
        pk_row: PkRow<'_>,
        delete_position_buffer: &mut DataChunkBuilder,
    ) -> StreamExecutorResult<Option<DataChunk>> {
        let Some(index_row) = self.state_table_mut().get_row(pk_row).await? else {
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
        self.state_table_mut().delete(index_row);
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
                self.state_table_mut().insert(index_row_data.as_slice());
            }
        }

        self.delete_position_buffer = Some(delete_position_buffer);
        self.state_table_mut().try_flush().await?;
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(mut self) {
        let mut input = self.input.take().unwrap().execute();

        // Consume the first barrier.
        let barrier = expect_first_barrier(&mut input).await?;
        let first_epoch = barrier.epoch;
        yield Message::Barrier(barrier);
        self.state_table_mut().init_epoch(first_epoch).await?;

        while let Some(msg) = input.next().await {
            match msg? {
                Message::Chunk(chunk) => {
                    if self.paused {
                        bail!(
                            "iceberg pk-index writer {} received chunk while paused",
                            self.sink_id
                        );
                    }
                    #[for_await]
                    for data_chunk in self.process_chunk(chunk) {
                        yield Message::Chunk(data_chunk?.into());
                    }
                }
                Message::Barrier(barrier) => {
                    barrier.assume_no_update_vnode_bitmap(self.ctx.id)?;
                    let epoch = barrier.epoch;
                    let signal = self.pause_signal(&barrier);

                    if self.paused {
                        bail!(
                            "iceberg pk-index writer {} unexpectedly returned to its input loop while paused",
                            self.sink_id
                        );
                    }

                    // Running: the normal per-barrier path. `commit` runs every barrier; the
                    // delete-buffer drain + iceberg flush are checkpoint-gated.
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

                    self.state_table_mut()
                        .commit_assert_no_update_vnode_bitmap(barrier.epoch)
                        .await?;
                    if let Some(metadata) = metadata
                        && metadata.metadata.is_some()
                    {
                        self.local_barrier_manager
                            .report_iceberg_pk_index_sink_metadata(
                                barrier.epoch,
                                self.sink_id,
                                self.ctx.id,
                                PbIcebergPkIndexSinkRole::Writer,
                                Some(metadata),
                            );
                    }
                    yield Message::Barrier(barrier);

                    if let PauseSignal::Pause(task_id) = signal {
                        #[for_await]
                        for msg in self.wait_compaction_resolve(&mut input, task_id, epoch) {
                            yield msg?;
                        }
                    }
                }
                Message::Watermark(w) => {
                    yield Message::Watermark(w);
                }
            }
        }
    }

    fn pause_signal(&self, barrier: &Barrier) -> PauseSignal {
        match barrier.iceberg_pk_index_barrier() {
            Some(context)
                if context.sink_id == self.sink_id
                    && context.phase == IcebergPkIndexBarrierPhase::Pause
                    && context.gated_actor_ids.contains(&self.ctx.id) =>
            {
                PauseSignal::Pause(context.task_id)
            }
            _ => PauseSignal::None,
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn wait_compaction_resolve<'a>(
        &'a mut self,
        input: &'a mut BoxedMessageStream,
        task_id: IcebergCompactionTaskId,
        epoch: EpochPair,
    ) {
        let seal_epoch = epoch.curr;
        self.pk_index_state_table = None;
        self.paused = true;

        let seal_control = self.writer_control_rx.recv().await.ok_or_else(|| {
            StreamExecutorError::channel_closed(
                "pk-index writer control channel closed while waiting for SealReady",
            )
        })?;
        match seal_control {
            IcebergPkIndexWriterControl::SealReady {
                task_id: received_task_id,
                epoch: received_epoch,
            } if received_task_id == task_id && received_epoch == seal_epoch => {}
            _ => bail!(
                "iceberg pk-index writer {} expected SealReady({}, {}) but received {:?}",
                self.sink_id,
                task_id,
                seal_epoch,
                seal_control
            ),
        }

        let next_message = input.next().await.ok_or_else(|| {
            StreamExecutorError::channel_closed(
                "pk-index writer input closed while waiting for the second compaction barrier",
            )
        })??;
        let b2 = match next_message {
            Message::Barrier(barrier) => barrier,
            Message::Chunk(_) => bail!(
                "iceberg pk-index writer {} received chunk between compaction barriers",
                self.sink_id
            ),
            Message::Watermark(_) => bail!(
                "iceberg pk-index writer {} received watermark between compaction barriers",
                self.sink_id
            ),
        };
        if !b2.is_checkpoint() || b2.epoch.prev != seal_epoch {
            bail!(
                "iceberg pk-index writer {} expected checkpoint starting at {}, got {:?}",
                self.sink_id,
                seal_epoch,
                b2
            );
        }
        match b2.iceberg_pk_index_barrier() {
            Some(context)
                if context.sink_id == self.sink_id
                    && context.task_id == task_id
                    && context.phase == IcebergPkIndexBarrierPhase::Resume
                    && context.gated_actor_ids.contains(&self.ctx.id) => {}
            _ => bail!(
                "iceberg pk-index writer {} expected matching Resume mutation on the second compaction barrier, got {:?}",
                self.sink_id,
                b2
            ),
        }
        let b2_epoch = b2.epoch;
        yield Message::Barrier(b2);

        let committed_control = self.writer_control_rx.recv().await.ok_or_else(|| {
            StreamExecutorError::channel_closed(
                "pk-index writer control channel closed while waiting for Committed",
            )
        })?;
        match committed_control {
            IcebergPkIndexWriterControl::Committed {
                task_id: received_task_id,
                epoch: received_epoch,
            } if received_task_id == task_id && received_epoch == seal_epoch => {}
            _ => bail!(
                "iceberg pk-index writer {} received unexpected control {:?} while waiting for Committed({}, {})",
                self.sink_id,
                committed_control,
                task_id,
                seal_epoch
            ),
        }

        let mut state_table = self.state_table_factory.build().await;
        state_table.init_epoch(b2_epoch).await?;
        self.pk_index_state_table = Some(state_table);
        self.paused = false;
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
#[path = "writer_test.rs"]
mod tests;

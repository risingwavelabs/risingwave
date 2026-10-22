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

//! List dispatches bounded pages continuously within a phase. Fetch retires tasks at EOF.
//! Phase transitions wait asynchronously for an empty committed Fetch queue, while ordinary
//! barriers remain responsive. Output and physical cursors recover at the same checkpoint.

use std::ops::Bound;

use anyhow::Context;
use either::Either;
use futures::{TryStreamExt, stream};
use risingwave_common::array::Op;
use risingwave_common::bitmap::Bitmap;
use risingwave_common::catalog::{ColumnCatalog, ColumnId};
use risingwave_common::config::StreamingConfig;
use risingwave_common::hash::{VnodeBitmapExt, VnodeCountCompat};
use risingwave_common::types::{JsonbVal, ScalarRef};
use risingwave_common::{bail, ensure};
use risingwave_connector::sink::iceberg::PositionDeleteReader;
use risingwave_connector::source::ConnectorProperties;
use risingwave_connector::source::iceberg::IcebergProperties;
use risingwave_connector::source::iceberg::update_planner::{
    IcebergUpdateBinding, IcebergUpdatePlanner,
};
use risingwave_connector::source::iceberg::update_reader::read_file_updates;
use risingwave_connector::source::iceberg::update_state::{
    IcebergUpdateAssignment, IcebergUpdateFetchState, IcebergUpdateListState,
};
use risingwave_hummock_sdk::HummockReadEpoch;
use risingwave_pb::plan_common::StorageTableDesc;
use risingwave_storage::store::PrefetchOptions;
use risingwave_storage::table::batch_table::BatchTable;
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::sync::mpsc::UnboundedReceiver;

use super::{SourceStateTableHandler, StreamSourceCore, barrier_to_message_stream};
use crate::executor::prelude::*;
use crate::executor::stream_reader::StreamReaderWithPause;

pub(crate) fn validate_update_state_table(
    table: &risingwave_pb::catalog::Table,
    singleton: bool,
) -> StreamExecutorResult<()> {
    use risingwave_pb::data::data_type::TypeName;
    let types = table
        .columns
        .iter()
        .map(|column| {
            column
                .column_desc
                .as_ref()
                .and_then(|desc| desc.column_type.as_ref())
                .map(|ty| ty.type_name())
        })
        .collect::<Vec<_>>();
    if singleton {
        ensure!(
            types == vec![Some(TypeName::Jsonb)]
                && table.pk.is_empty()
                && table.value_indices == vec![0]
                && table.distribution_key.is_empty(),
            "Iceberg update List state requires a single JSON value without a primary key"
        );
    } else {
        ensure!(
            types == vec![Some(TypeName::Varchar), Some(TypeName::Jsonb)]
                && table.pk.len() == 1
                && table.pk[0].column_index == 0
                && table.value_indices == vec![0, 1]
                && table.distribution_key == vec![0],
            "Iceberg update Fetch state requires a varchar task key and JSON value with matching distribution"
        );
    }
    Ok(())
}

fn encode(value: &impl Serialize) -> StreamExecutorResult<JsonbVal> {
    Ok(serde_json::to_value(value)
        .map_err(anyhow::Error::from)?
        .into())
}

fn decode<T: DeserializeOwned>(row: impl Row) -> StreamExecutorResult<T> {
    let value = row
        .datum_at(1)
        .context("missing Iceberg update state")?
        .into_jsonb();
    decode_value(value.to_owned_scalar())
}

fn decode_value<T: DeserializeOwned>(value: JsonbVal) -> StreamExecutorResult<T> {
    Ok(serde_json::from_value(value.take()).map_err(anyhow::Error::from)?)
}

fn properties(
    core: &mut StreamSourceCore<impl StateStore>,
) -> StreamExecutorResult<IcebergProperties> {
    let builder = core
        .source_desc_builder
        .take()
        .context("missing Iceberg source description")?;
    let ConnectorProperties::Iceberg(properties) =
        ConnectorProperties::extract(builder.with_properties(), false)?
    else {
        bail!("update source requires Iceberg");
    };
    Ok(*properties)
}

pub struct IcebergUpdateListExecutor<S: StateStore> {
    core: StreamSourceCore<S>,
    downstream_columns: Arc<Vec<ColumnCatalog>>,
    fetch_state_table: StorageTableDesc,
    barriers: UnboundedReceiver<Barrier>,
    streaming_config: Arc<StreamingConfig>,
}

impl<S: StateStore> IcebergUpdateListExecutor<S> {
    pub fn new(
        core: StreamSourceCore<S>,
        downstream_columns: Vec<ColumnCatalog>,
        fetch_state_table: StorageTableDesc,
        barriers: UnboundedReceiver<Barrier>,
        streaming_config: Arc<StreamingConfig>,
    ) -> Self {
        Self {
            core,
            downstream_columns: Arc::new(downstream_columns),
            fetch_state_table,
            barriers,
            streaming_config,
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn into_stream(mut self) {
        let batch_size = self.streaming_config.developer.iceberg_fetch_batch_size as usize;
        let desc = &self.fetch_state_table;
        let job_id = desc.table_id.as_raw_id();
        let fetch_reader = Arc::new(BatchTable::new_partial(
            self.core
                .split_state_store
                .state_table()
                .state_store()
                .clone(),
            vec![ColumnId::new(desc.columns[0].column_id)],
            Some(Arc::new(Bitmap::ones(desc.vnode_count()))),
            desc,
        ));
        let properties = properties(&mut self.core)?;
        let state_table = self.core.split_state_store.state_table_mut();
        let mut barriers = barrier_to_message_stream(self.barriers).boxed();
        let first = expect_first_barrier(&mut barriers).await?;
        let first_epoch = first.epoch;
        let paused = first.is_pause_on_startup();
        yield Message::Barrier(first);
        state_table.init_epoch(first_epoch).await?;

        let mut persisted_state = state_table.get_from_one_value_table().await?;
        let mut dirty = false;
        let mut epoch = first_epoch.curr;
        // On recovery the previous epoch is already the recovery checkpoint.
        let mut checkpoint_epoch = first_epoch.prev;
        let mut state = persisted_state
            .as_ref()
            .map(|value| decode_value::<IcebergUpdateListState>(value.as_jsonb().clone()))
            .transpose()?;
        if let Some(state) = &state {
            state.validate_columns(job_id, &self.downstream_columns)?;
        }
        let mut planner = None;
        let mut stream =
            StreamReaderWithPause::<true, ListResult>::new(barriers, stream::pending());
        if paused {
            stream.pause_stream();
        }
        stream.replace_data_stream(list_work(
            properties.clone(),
            (state.clone(), planner.take()),
            self.downstream_columns.clone(),
            batch_size,
            job_id,
            fetch_reader.clone(),
            checkpoint_epoch,
        ));
        let mut busy = true;
        while let Some(message) = stream.next().await {
            match message? {
                Either::Left(Message::Barrier(barrier)) => {
                    match barrier.mutation.as_deref() {
                        Some(Mutation::Pause) => stream.pause_stream(),
                        Some(Mutation::Resume) => stream.resume_stream(),
                        _ => {}
                    }
                    if dirty {
                        let value =
                            ScalarImpl::Jsonb(encode(state.as_ref().expect("dirty List state"))?);
                        if let Some(previous) = persisted_state.replace(value.clone()) {
                            state_table.update([Some(previous)], [Some(value)]);
                        } else {
                            state_table.insert([Some(value)]);
                        }
                        dirty = false;
                    }
                    state_table
                        .commit_assert_no_update_vnode_bitmap(barrier.epoch)
                        .await?;
                    epoch = barrier.epoch.curr;
                    if barrier.is_checkpoint() {
                        checkpoint_epoch = barrier.epoch.prev;
                    }
                    // A committed read must run only after yielding its barrier. Waiting on
                    // the control arm would prevent the very checkpoint it needs to observe.
                    yield Message::Barrier(barrier);
                    if !busy {
                        stream.replace_data_stream(list_work(
                            properties.clone(),
                            (state.clone(), planner.take()),
                            self.downstream_columns.clone(),
                            batch_size,
                            job_id,
                            fetch_reader.clone(),
                            checkpoint_epoch,
                        ));
                        busy = true;
                    }
                }
                Either::Left(_) => bail!("unexpected input to Iceberg update List"),
                Either::Right(mut result) => {
                    planner = result.planner;
                    if result.phase_finished {
                        result.state.record_phase_end(epoch)?;
                    }
                    let continue_listing = !result.assignments.is_empty() && !result.phase_finished;
                    dirty |= state.as_ref() != Some(&result.state);
                    state = Some(result.state);
                    if !result.assignments.is_empty() {
                        let rows = result
                            .assignments
                            .iter()
                            .map(|assignment| {
                                Ok((
                                    Op::Insert,
                                    OwnedRow::new(vec![
                                        Some(ScalarImpl::Utf8(assignment.key()?.into())),
                                        Some(ScalarImpl::Jsonb(encode(assignment)?)),
                                    ]),
                                ))
                            })
                            .collect::<StreamExecutorResult<Vec<_>>>()?;
                        yield Message::Chunk(StreamChunk::from_rows(
                            &rows,
                            &[DataType::Varchar, DataType::Jsonb],
                        ));
                    }
                    busy = continue_listing;
                    if continue_listing {
                        stream.replace_data_stream(list_work(
                            properties.clone(),
                            (state.clone(), planner.take()),
                            self.downstream_columns.clone(),
                            batch_size,
                            job_id,
                            fetch_reader.clone(),
                            checkpoint_epoch,
                        ));
                    } else {
                        stream.replace_data_stream(stream::pending());
                    }
                }
            }
        }
    }
}

struct ListResult {
    state: IcebergUpdateListState,
    planner: Option<IcebergUpdatePlanner>,
    assignments: Vec<IcebergUpdateAssignment>,
    phase_finished: bool,
}

/// Metadata I/O runs on the data arm so it cannot prevent List from forwarding barriers.
#[try_stream(boxed, ok = ListResult, error = StreamExecutorError)]
async fn list_work<S: StateStore>(
    properties: IcebergProperties,
    (state, mut planner): (Option<IcebergUpdateListState>, Option<IcebergUpdatePlanner>),
    downstream_columns: Arc<Vec<ColumnCatalog>>,
    batch_size: usize,
    job_id: u32,
    fetch_reader: Arc<BatchTable<S>>,
    checkpoint_epoch: u64,
) {
    let Some(mut state) = state else {
        // Bind only once, on the pausable data arm. Persist it before enumerating tasks, so
        // their checkpoint always includes the binding even if latest advances on recovery.
        let table = properties.load_table().await?;
        let binding = IcebergUpdateBinding::bind_columns(&table, &downstream_columns)?;
        yield ListResult {
            state: IcebergUpdateListState::new(job_id, binding),
            planner: None,
            assignments: vec![],
            phase_finished: false,
        };
        return Ok(());
    };
    let mut assignments = vec![];
    let mut phase_finished = false;
    if let Some(dispatched_epoch) = state.phase_end_epoch() {
        if checkpoint_epoch >= dispatched_epoch {
            let rows = fetch_reader
                .batch_iter(
                    HummockReadEpoch::Committed(checkpoint_epoch),
                    false,
                    PrefetchOptions::prefetch_for_small_range_scan(),
                )
                .await?;
            pin_mut!(rows);
            if rows.try_next().await?.is_none() {
                state.complete_phase(checkpoint_epoch)?;
            }
        }
    } else {
        let table = properties.load_table().await?;
        (assignments, phase_finished) = state.enumerate(&table, batch_size, &mut planner).await?;
    }
    yield ListResult {
        state,
        planner,
        assignments,
        phase_finished,
    };
}

pub struct IcebergUpdateFetchExecutor<S: StateStore> {
    actor: ActorContextRef,
    core: StreamSourceCore<S>,
    upstream: Executor,
    streaming_config: Arc<StreamingConfig>,
    output_columns: Vec<ColumnCatalog>,
    key_indices: Vec<usize>,
}

impl<S: StateStore> IcebergUpdateFetchExecutor<S> {
    pub fn new(
        actor: ActorContextRef,
        core: StreamSourceCore<S>,
        upstream: Executor,
        streaming_config: Arc<StreamingConfig>,
        output_columns: Vec<ColumnCatalog>,
        key_indices: Vec<usize>,
    ) -> Self {
        Self {
            actor,
            core,
            upstream,
            streaming_config,
            output_columns,
            key_indices,
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn into_stream(mut self) {
        let batch_size = self.streaming_config.developer.iceberg_fetch_batch_size as usize;
        let chunk_size = self.streaming_config.developer.chunk_size;
        ensure!(batch_size > 0, "Iceberg fetch batch size must be positive");
        let job_id = self.core.split_state_store.state_table().table_id();
        let output_types = self
            .output_columns
            .iter()
            .map(|column| column.data_type().clone())
            .collect::<Vec<_>>();
        let properties = properties(&mut self.core)?;
        let mut state_table = self.core.split_state_store;
        let mut upstream = self.upstream.execute();
        let first = expect_first_barrier(&mut upstream).await?;
        let first_epoch = first.epoch;
        let paused = first.is_pause_on_startup();
        yield Message::Barrier(first);
        state_table.init_epoch(first_epoch).await?;
        let mut stream =
            StreamReaderWithPause::<true, FetchResult>::new(upstream, stream::pending());
        if paused {
            stream.pause_stream();
        }
        let tasks = pending_tasks(
            &state_table,
            job_id.as_raw_id(),
            batch_size,
            &self.output_columns,
            &self.key_indices,
        )
        .await?;
        let mut busy = !tasks.is_empty();
        stream.replace_data_stream(fetch_work(properties.clone(), tasks, chunk_size));
        while let Some(message) = stream.next().await {
            match message? {
                Either::Left(Message::Barrier(barrier)) => {
                    match barrier.mutation.as_deref() {
                        Some(Mutation::Pause) => stream.pause_stream(),
                        Some(Mutation::Resume) => stream.resume_stream(),
                        _ => {}
                    }
                    // Rescale restarts the actor and restores its assigned vnodes at startup.
                    barrier.assume_no_update_vnode_bitmap(self.actor.id)?;
                    state_table.commit(barrier.epoch).await?;
                    yield Message::Barrier(barrier);
                    if !busy {
                        let tasks = pending_tasks(
                            &state_table,
                            job_id.as_raw_id(),
                            batch_size,
                            &self.output_columns,
                            &self.key_indices,
                        )
                        .await?;
                        busy = !tasks.is_empty();
                        stream.replace_data_stream(fetch_work(
                            properties.clone(),
                            tasks,
                            chunk_size,
                        ));
                    }
                }
                Either::Left(Message::Chunk(chunk)) => {
                    for (op, row) in chunk.rows() {
                        let key = row
                            .datum_at(0)
                            .context("missing assignment key")?
                            .into_utf8();
                        let assignment: IcebergUpdateAssignment = decode(row)?;
                        ensure!(
                            assignment.job_id == job_id.as_raw_id() && assignment.key()? == key,
                            "Iceberg assignment belongs to another job/generation"
                        );
                        assignment
                            .task
                            .validate_key(&self.output_columns, &self.key_indices)?;
                        match (op, state_table.get(key).await?) {
                            (Op::Insert, None) => {
                                state_table
                                    .set(key, encode(&IcebergUpdateFetchState::new(assignment))?)
                                    .await?
                            }
                            (Op::Insert, Some(old)) => {
                                ensure!(
                                    decode::<IcebergUpdateFetchState>(&old)?.assignment
                                        == assignment,
                                    "conflicting duplicate Iceberg assignment"
                                );
                            }
                            _ => bail!("invalid Iceberg assignment operation"),
                        }
                    }
                    state_table.try_flush().await?;
                    if !busy {
                        let tasks = pending_tasks(
                            &state_table,
                            job_id.as_raw_id(),
                            batch_size,
                            &self.output_columns,
                            &self.key_indices,
                        )
                        .await?;
                        busy = !tasks.is_empty();
                        stream.replace_data_stream(fetch_work(
                            properties.clone(),
                            tasks,
                            chunk_size,
                        ));
                    }
                }
                Either::Left(_) => bail!("unexpected Iceberg Fetch watermark"),
                Either::Right(FetchResult::Batch { state, chunk }) => {
                    let key = state.assignment.key()?;
                    ensure!(
                        chunk
                            .columns()
                            .iter()
                            .map(|column| column.data_type())
                            .collect::<Vec<_>>()
                            == output_types,
                        "Iceberg update output schema differs from its plan"
                    );
                    state_table.set(&key, encode(&state)?).await?;
                    if chunk.cardinality() != 0 {
                        yield Message::Chunk(chunk);
                    }
                }
                Either::Right(FetchResult::Finished(state)) => {
                    let key = state.assignment.key()?;
                    // Retirement and the last output share the same checkpoint. Recovery
                    // restores either the task cursor or its completed downstream effects.
                    state_table.delete(&key).await?;
                }
                Either::Right(FetchResult::BatchFinished) => {
                    let tasks = pending_tasks(
                        &state_table,
                        job_id.as_raw_id(),
                        batch_size,
                        &self.output_columns,
                        &self.key_indices,
                    )
                    .await?;
                    busy = !tasks.is_empty();
                    if busy {
                        stream.replace_data_stream(fetch_work(
                            properties.clone(),
                            tasks,
                            chunk_size,
                        ));
                    } else {
                        stream.replace_data_stream(stream::pending());
                    }
                }
            }
        }
    }
}

async fn pending_tasks<S: StateStore>(
    state: &SourceStateTableHandler<S>,
    job_id: u32,
    limit: usize,
    columns: &[ColumnCatalog],
    key_indices: &[usize],
) -> StreamExecutorResult<Vec<IcebergUpdateFetchState>> {
    ensure!(limit > 0, "Iceberg fetch batch size must be positive");
    let table = state.state_table();
    let mut tasks = vec![];
    'vnodes: for vnode in table.vnodes().iter_vnodes() {
        let iter = table
            .iter_with_vnode(
                vnode,
                &(Bound::<OwnedRow>::Unbounded, Bound::<OwnedRow>::Unbounded),
                PrefetchOptions::prefetch_for_small_range_scan(),
            )
            .await?;
        pin_mut!(iter);
        while let Some(row) = iter.next().await {
            let task: IcebergUpdateFetchState = decode(&row?)?;
            ensure!(
                task.assignment.job_id == job_id,
                "Iceberg task belongs to another ingestion job"
            );
            task.assignment.task.validate_key(columns, key_indices)?;
            ensure!(
                !task.finished && task.next_position <= task.assignment.task.record_count(),
                "invalid persisted Iceberg task cursor"
            );
            tasks.push(task);
            if tasks.len() == limit {
                break 'vnodes;
            }
        }
    }
    Ok(tasks)
}

enum FetchResult {
    Batch {
        state: IcebergUpdateFetchState,
        chunk: StreamChunk,
    },
    Finished(IcebergUpdateFetchState),
    BatchFinished,
}

#[try_stream(boxed, ok = FetchResult, error = StreamExecutorError)]
async fn fetch_work(
    properties: IcebergProperties,
    tasks: Vec<IcebergUpdateFetchState>,
    chunk_size: usize,
) {
    if !tasks.is_empty() {
        let table = properties.load_table().await?;
        let mut reader = PositionDeleteReader::new(table.file_io());
        for mut state in tasks {
            let (scan, mode) = state.assignment.task.prepare_read(&table)?;
            let mut batches = read_file_updates(
                table.clone(),
                &mut reader,
                scan,
                mode,
                chunk_size,
                state.next_position,
            );
            while let Some(batch) = batches.next().await {
                let batch = batch?;
                state.advance(batch.next_position, false)?;
                yield FetchResult::Batch {
                    state: state.clone(),
                    chunk: batch.chunk,
                };
            }
            state.advance(state.assignment.task.record_count(), true)?;
            yield FetchResult::Finished(state);
        }
    }
    yield FetchResult::BatchFinished;
}

impl<S: StateStore> Execute for IcebergUpdateListExecutor<S> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.into_stream().boxed()
    }
}
impl<S: StateStore> Execute for IcebergUpdateFetchExecutor<S> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.into_stream().boxed()
    }
}
impl<S: StateStore> Debug for IcebergUpdateListExecutor<S> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IcebergUpdateListExecutor")
            .finish_non_exhaustive()
    }
}
impl<S: StateStore> Debug for IcebergUpdateFetchExecutor<S> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IcebergUpdateFetchExecutor")
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
#[path = "iceberg_update_executor_test.rs"]
mod tests;

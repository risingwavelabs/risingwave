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

//! Iceberg update List/Fetch emits Insert and Delete rows. Completion is read from the Fetch state
//! table at a globally committed epoch, not a local actor notification or a barrier flush.

use std::ops::Bound;

use anyhow::Context;
use either::Either;
use futures::stream;
use risingwave_common::array::Op;
use risingwave_common::bitmap::Bitmap;
use risingwave_common::catalog::{ColumnCatalog, ColumnId};
use risingwave_common::hash::{VnodeBitmapExt, VnodeCountCompat};
use risingwave_common::types::{JsonbVal, ScalarRef};
use risingwave_common::{bail, ensure};
use risingwave_connector::sink::iceberg::PositionDeleteReader;
use risingwave_connector::source::ConnectorProperties;
use risingwave_connector::source::iceberg::IcebergProperties;
use risingwave_connector::source::iceberg::update_planner::{
    IcebergUpdateBinding, IcebergUpdateLimits,
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

const LIST_KEY: &str = "iceberg-update-list";

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
    ensure!(
        types == vec![Some(TypeName::Varchar), Some(TypeName::Jsonb)]
            && table.pk.len() == 1
            && table.pk[0].column_index == 0
            && table.value_indices == vec![0, 1]
            && table.distribution_key == if singleton { vec![] } else { vec![0] },
        "Iceberg update state requires a varchar task key and JSON value with matching distribution"
    );
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
    Ok(serde_json::from_value(value.to_owned_scalar().take()).map_err(anyhow::Error::from)?)
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

#[derive(Clone)]
struct TableLoader {
    properties: IcebergProperties,
    #[cfg(test)]
    table: Option<tokio::sync::watch::Receiver<iceberg::table::Table>>,
}

impl TableLoader {
    async fn load_table(&self) -> StreamExecutorResult<iceberg::table::Table> {
        #[cfg(test)]
        if let Some(table) = &self.table {
            return Ok(table.borrow().clone());
        }
        Ok(self.properties.load_table().await?)
    }
}

pub struct IcebergUpdateListExecutor<S: StateStore> {
    core: StreamSourceCore<S>,
    downstream_columns: Arc<Vec<ColumnCatalog>>,
    fetch_state_table: StorageTableDesc,
    store: S,
    barriers: UnboundedReceiver<Barrier>,
    #[cfg(test)]
    table: Option<tokio::sync::watch::Receiver<iceberg::table::Table>>,
}

impl<S: StateStore> IcebergUpdateListExecutor<S> {
    pub fn new(
        core: StreamSourceCore<S>,
        downstream_columns: Vec<ColumnCatalog>,
        fetch_state_table: StorageTableDesc,
        store: S,
        barriers: UnboundedReceiver<Barrier>,
    ) -> Self {
        Self {
            core,
            downstream_columns: Arc::new(downstream_columns),
            fetch_state_table,
            store,
            barriers,
            #[cfg(test)]
            table: None,
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn into_stream(mut self) {
        let limits = IcebergUpdateLimits::default();
        let desc = &self.fetch_state_table;
        let job_id = desc.table_id.as_raw_id();
        ensure!(
            desc.columns
                .iter()
                .map(|column| column.column_type.as_ref().map(|ty| ty.type_name()))
                .collect::<Vec<_>>()
                == vec![
                    Some(risingwave_pb::data::data_type::TypeName::Varchar),
                    Some(risingwave_pb::data::data_type::TypeName::Jsonb)
                ]
                && desc.pk.len() == 1
                && desc.pk[0].column_index == 0
                && desc.value_indices == vec![0, 1]
                && desc.dist_key_in_pk_indices == vec![0],
            "Iceberg completion table must be keyed task state"
        );
        ensure!(
            self.core.split_state_store.state_table().table_id() != desc.table_id,
            "Iceberg List and Fetch require distinct state tables"
        );
        let columns = desc
            .columns
            .iter()
            .map(|column| ColumnId::from(column.column_id))
            .collect();
        let completion_table = Arc::new(BatchTable::new_partial(
            self.store,
            columns,
            Some(Arc::new(Bitmap::ones(desc.vnode_count()))),
            desc,
        ));
        let properties = TableLoader {
            properties: properties(&mut self.core)?,
            #[cfg(test)]
            table: self.table,
        };
        let mut state_table = self.core.split_state_store;
        let mut barriers = barrier_to_message_stream(self.barriers).boxed();
        let first = expect_first_barrier(&mut barriers).await?;
        let mut checkpoint = first.epoch.prev;
        let paused = first.is_pause_on_startup();
        state_table.init_epoch(first.epoch).await?;
        let restored = state_table.get(LIST_KEY).await?;
        let mut dirty = false;
        let mut state = restored
            .as_ref()
            .map(decode::<IcebergUpdateListState>)
            .transpose()?;
        if let Some(state) = &state {
            state.validate_columns(job_id, &self.downstream_columns)?;
        }
        yield Message::Barrier(first);
        let mut stream =
            StreamReaderWithPause::<true, ListResult>::new(barriers, stream::pending());
        if paused {
            stream.pause_stream();
        }
        stream.replace_data_stream(list_work(
            properties.clone(),
            state.clone(),
            self.downstream_columns.clone(),
            limits,
            completion_table.clone(),
            checkpoint,
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
                        state_table
                            .set(LIST_KEY, encode(state.as_ref().expect("dirty List state"))?)
                            .await?;
                        dirty = false;
                    }
                    state_table.commit(barrier.epoch).await?;
                    if barrier.is_checkpoint() {
                        checkpoint = barrier.epoch.prev;
                    }
                    yield Message::Barrier(barrier);
                    if !busy {
                        stream.replace_data_stream(list_work(
                            properties.clone(),
                            state.clone(),
                            self.downstream_columns.clone(),
                            limits,
                            completion_table.clone(),
                            checkpoint,
                        ));
                        busy = true;
                    }
                }
                Either::Left(_) => bail!("unexpected input to Iceberg update List"),
                Either::Right(result) => {
                    dirty |= state.as_ref() != Some(&result.state);
                    state = Some(result.state);
                    // GC and the advanced List state share the next checkpoint. If it aborts,
                    // completed Fetch rows and the old pending List page are both recovered.
                    for (op, assignments) in [
                        (Op::Delete, result.completed),
                        (Op::Insert, result.assignments),
                    ] {
                        if !assignments.is_empty() {
                            let rows = assignments
                                .iter()
                                .map(|assignment| {
                                    Ok((
                                        op,
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
                    }
                    busy = false;
                    stream.replace_data_stream(stream::pending());
                }
            }
        }
    }
}

struct ListResult {
    state: IcebergUpdateListState,
    completed: Vec<IcebergUpdateAssignment>,
    assignments: Vec<IcebergUpdateAssignment>,
}

/// Waits run on the data arm, never inline in barrier handling. A slow global checkpoint cannot
/// prevent this List from forwarding the barrier needed to commit the Fetch completion rows.
#[try_stream(boxed, ok = ListResult, error = StreamExecutorError)]
async fn list_work<S: StateStore>(
    properties: TableLoader,
    state: Option<IcebergUpdateListState>,
    downstream_columns: Arc<Vec<ColumnCatalog>>,
    limits: IcebergUpdateLimits,
    completion_table: Arc<BatchTable<S>>,
    checkpoint: u64,
) {
    let Some(mut state) = state else {
        // Bind only once, on the pausable data arm. Persist it before enumerating tasks, so
        // their checkpoint always includes the binding even if latest advances on recovery.
        let table = properties.load_table().await?;
        let binding = IcebergUpdateBinding::bind_columns(&table, &downstream_columns)?;
        yield ListResult {
            state: IcebergUpdateListState::new(completion_table.table_id().as_raw_id(), binding),
            completed: vec![],
            assignments: vec![],
        };
        return Ok(());
    };
    let pending = state.assignments();
    let mut completed = vec![];
    let mut assignments = vec![];
    if pending.is_empty() {
        let table = properties.load_table().await?;
        assignments = state.enumerate(&table, limits).await?;
    } else {
        let mut states = vec![];
        for assignment in &pending {
            let key = OwnedRow::new(vec![Some(ScalarImpl::Utf8(assignment.key()?.into()))]);
            if let Some(row) = completion_table
                .get_row(key, HummockReadEpoch::Committed(checkpoint))
                .await?
            {
                states.push(decode(&row)?);
            }
        }
        if state.acknowledge_committed(&states)? {
            completed = pending;
        }
    }
    yield ListResult {
        state,
        completed,
        assignments,
    };
}

pub struct IcebergUpdateFetchExecutor<S: StateStore> {
    actor: ActorContextRef,
    core: StreamSourceCore<S>,
    upstream: Executor,
    chunk_size: usize,
    output_columns: Vec<ColumnCatalog>,
    #[cfg(test)]
    table: Option<tokio::sync::watch::Receiver<iceberg::table::Table>>,
}

impl<S: StateStore> IcebergUpdateFetchExecutor<S> {
    pub fn new(
        actor: ActorContextRef,
        core: StreamSourceCore<S>,
        upstream: Executor,
        chunk_size: usize,
        output_columns: Vec<ColumnCatalog>,
    ) -> Self {
        Self {
            actor,
            core,
            upstream,
            chunk_size,
            output_columns,
            #[cfg(test)]
            table: None,
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn into_stream(mut self) {
        let limits = IcebergUpdateLimits::default();
        let job_id = self.core.split_state_store.state_table().table_id();
        let output_types = self
            .output_columns
            .iter()
            .map(|column| column.data_type().clone())
            .collect::<Vec<_>>();
        let properties = TableLoader {
            properties: properties(&mut self.core)?,
            #[cfg(test)]
            table: self.table,
        };
        let mut state_table = self.core.split_state_store;
        let mut upstream = self.upstream.execute();
        let first = expect_first_barrier(&mut upstream).await?;
        let paused = first.is_pause_on_startup();
        state_table.init_epoch(first.epoch).await?;
        yield Message::Barrier(first);
        let mut stream =
            StreamReaderWithPause::<true, FetchResult>::new(upstream, stream::pending());
        if paused {
            stream.pause_stream();
        }
        let tasks = pending_tasks(
            &state_table,
            job_id.as_raw_id(),
            limits.page_size,
            &self.output_columns,
        )
        .await?;
        let mut busy = !tasks.is_empty();
        stream.replace_data_stream(fetch_work(properties.clone(), tasks, self.chunk_size));
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
                            limits.page_size,
                            &self.output_columns,
                        )
                        .await?;
                        busy = !tasks.is_empty();
                        stream.replace_data_stream(fetch_work(
                            properties.clone(),
                            tasks,
                            self.chunk_size,
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
                        assignment.task.validate_columns(&self.output_columns)?;
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
                            (Op::Delete, Some(old)) => {
                                let old: IcebergUpdateFetchState = decode(&old)?;
                                ensure!(
                                    old.assignment == assignment && old.finished,
                                    "GC of unfinished Iceberg task"
                                );
                                state_table.delete(key).await?;
                            }
                            (Op::Delete, None) => {}
                            _ => bail!("invalid Iceberg assignment operation"),
                        }
                    }
                    state_table.try_flush().await?;
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
                    state_table.set(&key, encode(&state)?).await?;
                }
                Either::Right(FetchResult::BatchFinished) => {
                    busy = false;
                    stream.replace_data_stream(stream::pending());
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
) -> StreamExecutorResult<Vec<IcebergUpdateFetchState>> {
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
            task.assignment.task.validate_columns(columns)?;
            if !task.finished {
                tasks.push(task);
            }
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
    properties: TableLoader,
    tasks: Vec<IcebergUpdateFetchState>,
    chunk_size: usize,
) {
    if !tasks.is_empty() {
        let table = properties.load_table().await?;
        let mut reader = PositionDeleteReader::new(table.file_io());
        for mut state in tasks {
            let (scan, contract, mode) = state.assignment.task.read_contract(&table)?;
            let mut batches = read_file_updates(
                table.clone(),
                &mut reader,
                scan,
                contract,
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

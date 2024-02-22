// Copyright 2024 RisingWave Labs
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

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::marker::PhantomData;
use std::ops::Deref;
use std::sync::Arc;

use bytes::Bytes;
use futures::{stream, StreamExt};
use futures_async_stream::try_stream;
use itertools::Itertools;
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::{ColumnDesc, ColumnId, ConflictBehavior, Schema, TableId};
use risingwave_common::row::{CompactedRow, RowDeserializer};
use risingwave_common::types::DataType;
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_common::util::iter_util::{ZipEqDebug, ZipEqFast};
use risingwave_common::util::sort_util::ColumnOrder;
use risingwave_common::util::value_encoding::BasicSerde;
use risingwave_pb::catalog::Table;
use risingwave_storage::mem_table::KeyOp;
use risingwave_storage::row_serde::value_serde::ValueRowSerde;
use risingwave_storage::StateStore;

use crate::cache::{new_unbounded, ManagedLruCache};
use crate::common::metrics::MetricsInfo;
use crate::common::table::state_table::StateTableInner;
use crate::executor::error::StreamExecutorError;
use crate::executor::monitor::StreamingMetrics;
use crate::executor::{
    expect_first_barrier, ActorContext, ActorContextRef, AddMutation, BoxedMessageStream, Execute,
    Executor, Message, Mutation, StreamExecutorResult, UpdateMutation,
};
use crate::task::{ActorId, AtomicU64Ref};

/// `MaterializeExecutor` materializes changes in stream into a materialized view on storage.
pub struct MaterializeExecutor<S: StateStore, SD: ValueRowSerde> {
    input: Executor,

    schema: Schema,

    state_table: StateTableInner<S, SD>,

    /// Columns of arrange keys (including pk, group keys, join keys, etc.)
    arrange_key_indices: Vec<usize>,

    actor_context: ActorContextRef,

    materialize_cache: MaterializeCache<SD>,

    conflict_behavior: ConflictBehavior,
}

impl<S: StateStore, SD: ValueRowSerde> MaterializeExecutor<S, SD> {
    /// Create a new `MaterializeExecutor` with distribution specified with `distribution_keys` and
    /// `vnodes`. For singleton distribution, `distribution_keys` should be empty and `vnodes`
    /// should be `None`.
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        input: Executor,
        schema: Schema,
        store: S,
        arrange_key: Vec<ColumnOrder>,
        actor_context: ActorContextRef,
        vnodes: Option<Arc<Bitmap>>,
        table_catalog: &Table,
        watermark_epoch: AtomicU64Ref,
        conflict_behavior: ConflictBehavior,
        metrics: Arc<StreamingMetrics>,
    ) -> Self {
        let arrange_key_indices: Vec<usize> = arrange_key.iter().map(|k| k.column_index).collect();

        // Note: The current implementation could potentially trigger a switch on the inconsistent_op flag. If the storage relies on this flag to perform optimizations, it would be advisable to maintain consistency with it throughout the lifecycle.
        let state_table = if matches!(conflict_behavior, ConflictBehavior::Overwrite)
            && actor_context.dispatch_num == 0
        {
            // Table with overwrite conflict behavior could disable conflict check if no downstream mv depends on it, so we use a inconsistent_op to skip sanity check as well.
            StateTableInner::from_table_catalog_inconsistent_op(table_catalog, store, vnodes).await
        } else {
            StateTableInner::from_table_catalog(table_catalog, store, vnodes).await
        };

        let metrics_info =
            MetricsInfo::new(metrics, table_catalog.id, actor_context.id, "Materialize");

        Self {
            input,
            schema,
            state_table,
            arrange_key_indices,
            actor_context,
            materialize_cache: MaterializeCache::new(watermark_epoch, metrics_info),
            conflict_behavior,
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(mut self) {
        // for metrics
        let table_id_str = self.state_table.table_id().to_string();
        let actor_id_str = self.actor_context.id.to_string();
        let fragment_id_str = self.actor_context.fragment_id.to_string();

        let data_types = self.schema.data_types();
        let mut input = self.input.execute();

        let barrier = expect_first_barrier(&mut input).await?;
        self.state_table.init_epoch(barrier.epoch);

        // The first barrier message should be propagated.
        yield Message::Barrier(barrier);

        #[for_await]
        for msg in input {
            let msg = msg?;
            self.materialize_cache.evict();

            yield match msg {
                Message::Watermark(w) => Message::Watermark(w),
                Message::Chunk(chunk) => {
                    self.actor_context
                        .streaming_metrics
                        .mview_input_row_count
                        .with_label_values(&[&table_id_str, &actor_id_str, &fragment_id_str])
                        .inc_by(chunk.cardinality() as u64);

                    match self.conflict_behavior {
                        ConflictBehavior::Overwrite | ConflictBehavior::IgnoreConflict
                            if self.state_table.is_consistent_op() =>
                        {
                            if chunk.cardinality() == 0 {
                                // empty chunk
                                continue;
                            }
                            let (data_chunk, ops) = chunk.into_parts();

                            if self.state_table.value_indices().is_some() {
                                // TODO(st1page): when materialize partial columns(), we should
                                // construct some columns in the pk
                                panic!("materialize executor with data check can not handle only materialize partial columns")
                            };
                            let values = data_chunk.serialize();

                            let key_chunk = data_chunk.project(self.state_table.pk_indices());

                            let pks = {
                                let mut pks = vec![vec![]; data_chunk.capacity()];
                                key_chunk
                                    .rows_with_holes()
                                    .zip_eq_fast(pks.iter_mut())
                                    .for_each(|(r, vnode_and_pk)| {
                                        if let Some(r) = r {
                                            self.state_table.pk_serde().serialize(r, vnode_and_pk);
                                        }
                                    });
                                pks
                            };
                            let (_, vis) = key_chunk.into_parts();
                            let row_ops = ops
                                .iter()
                                .zip_eq_debug(pks.into_iter())
                                .zip_eq_debug(values.into_iter())
                                .zip_eq_debug(vis.iter())
                                .filter_map(|(((op, k), v), vis)| vis.then_some((*op, k, v)))
                                .collect_vec();

                            let fixed_changes = self
                                .materialize_cache
                                .handle(row_ops, &self.state_table, &self.conflict_behavior)
                                .await?;

                            match generate_output(fixed_changes, data_types.clone())? {
                                Some(output_chunk) => {
                                    self.state_table.write_chunk(output_chunk.clone());
                                    self.state_table.try_flush().await?;
                                    Message::Chunk(output_chunk)
                                }
                                None => continue,
                            }
                        }
                        ConflictBehavior::IgnoreConflict => unreachable!(),
                        ConflictBehavior::NoCheck | ConflictBehavior::Overwrite => {
                            self.state_table.write_chunk(chunk.clone());
                            self.state_table.try_flush().await?;
                            Message::Chunk(chunk)
                        }
                    }
                }
                Message::Barrier(b) => {
                    let mutation = b.mutation.clone();
                    // If a downstream mv depends on the current table, we need to do conflict check again.
                    if !self.state_table.is_consistent_op()
                        && Self::new_downstream_created(mutation, self.actor_context.id)
                    {
                        assert_eq!(self.conflict_behavior, ConflictBehavior::Overwrite);
                        self.state_table
                            .commit_with_switch_consistent_op(b.epoch, Some(true))
                            .await?;
                    } else {
                        self.state_table.commit(b.epoch).await?;
                    }

                    // Update the vnode bitmap for the state table if asked.
                    if let Some(vnode_bitmap) = b.as_update_vnode_bitmap(self.actor_context.id) {
                        let (_, cache_may_stale) =
                            self.state_table.update_vnode_bitmap(vnode_bitmap);

                        if cache_may_stale {
                            self.materialize_cache.data.clear();
                        }
                    }
                    self.materialize_cache.data.update_epoch(b.epoch.curr);
                    Message::Barrier(b)
                }
            }
        }
    }

    fn new_downstream_created(mutation: Option<Arc<Mutation>>, actor_id: ActorId) -> bool {
        let Some(mutation) = mutation.as_deref() else {
            return false;
        };
        match mutation {
            // Add is for mv, index and sink creation.
            Mutation::Add(AddMutation { adds, .. }) => adds.get(&actor_id).is_some(),
            // AddAndUpdate is for sink-into-table.
            Mutation::AddAndUpdate(
                AddMutation { adds, .. },
                UpdateMutation {
                    dispatchers,
                    actor_new_dispatchers: actor_dispatchers,
                    ..
                },
            ) => {
                adds.get(&actor_id).is_some()
                    || actor_dispatchers.get(&actor_id).is_some()
                    || dispatchers.get(&actor_id).is_some()
            }
            Mutation::Update(_)
            | Mutation::Stop(_)
            | Mutation::Pause
            | Mutation::Resume
            | Mutation::SourceChangeSplit(_)
            | Mutation::Throttle(_) => false,
        }
    }
}

impl<S: StateStore> MaterializeExecutor<S, BasicSerde> {
    /// Create a new `MaterializeExecutor` without distribution info for test purpose.
    #[allow(clippy::too_many_arguments)]
    pub async fn for_test(
        input: Executor,
        store: S,
        table_id: TableId,
        keys: Vec<ColumnOrder>,
        column_ids: Vec<ColumnId>,
        watermark_epoch: AtomicU64Ref,
        conflict_behavior: ConflictBehavior,
    ) -> Self {
        let arrange_columns: Vec<usize> = keys.iter().map(|k| k.column_index).collect();
        let arrange_order_types = keys.iter().map(|k| k.order_type).collect();
        let schema = input.schema().clone();
        let columns = column_ids
            .into_iter()
            .zip_eq_fast(schema.fields.iter())
            .map(|(column_id, field)| ColumnDesc::unnamed(column_id, field.data_type()))
            .collect_vec();

        let state_table = StateTableInner::new_without_distribution(
            store,
            table_id,
            columns,
            arrange_order_types,
            arrange_columns.clone(),
        )
        .await;

        Self {
            input,
            schema,
            state_table,
            arrange_key_indices: arrange_columns.clone(),
            actor_context: ActorContext::for_test(0),
            materialize_cache: MaterializeCache::new(watermark_epoch, MetricsInfo::for_test()),
            conflict_behavior,
        }
    }
}

/// Construct output `StreamChunk` from given buffer.
fn generate_output(
    changes: MaterializeBuffer,
    data_types: Vec<DataType>,
) -> StreamExecutorResult<Option<StreamChunk>> {
    // construct output chunk
    // TODO(st1page): when materialize partial columns(), we should construct some columns in the pk
    let mut new_ops: Vec<Op> = vec![];
    let mut new_rows: Vec<Bytes> = vec![];
    let row_deserializer = RowDeserializer::new(data_types.clone());
    for (_, row_op) in changes.into_parts() {
        match row_op {
            KeyOp::Insert(value) => {
                new_ops.push(Op::Insert);
                new_rows.push(value);
            }
            KeyOp::Delete(old_value) => {
                new_ops.push(Op::Delete);
                new_rows.push(old_value);
            }
            KeyOp::Update((old_value, new_value)) => {
                // if old_value == new_value, we don't need to emit updates to downstream.
                if old_value != new_value {
                    new_ops.push(Op::UpdateDelete);
                    new_ops.push(Op::UpdateInsert);
                    new_rows.push(old_value);
                    new_rows.push(new_value);
                }
            }
        }
    }
    let mut data_chunk_builder = DataChunkBuilder::new(data_types, new_rows.len() + 1);

    for row_bytes in new_rows {
        let res =
            data_chunk_builder.append_one_row(row_deserializer.deserialize(row_bytes.as_ref())?);
        debug_assert!(res.is_none());
    }

    if let Some(new_data_chunk) = data_chunk_builder.consume_all() {
        let new_stream_chunk = StreamChunk::new(new_ops, new_data_chunk.columns().to_vec());
        Ok(Some(new_stream_chunk))
    } else {
        Ok(None)
    }
}

/// `MaterializeBuffer` is a buffer to handle chunk into `KeyOp`.
pub struct MaterializeBuffer {
    buffer: HashMap<Vec<u8>, KeyOp>,
}

impl MaterializeBuffer {
    fn new() -> Self {
        Self {
            buffer: HashMap::new(),
        }
    }

    pub fn insert(&mut self, pk: Vec<u8>, value: Bytes) {
        let entry = self.buffer.entry(pk);
        match entry {
            Entry::Vacant(e) => {
                e.insert(KeyOp::Insert(value));
            }
            Entry::Occupied(mut e) => match e.get_mut() {
                KeyOp::Delete(ref mut old_value) => {
                    let old_val = std::mem::take(old_value);
                    e.insert(KeyOp::Update((old_val, value)));
                }
                KeyOp::Insert(_) | KeyOp::Update(_) => {
                    unreachable!();
                }
            },
        }
    }

    pub fn delete(&mut self, pk: Vec<u8>, old_value: Bytes) {
        let entry: Entry<'_, Vec<u8>, KeyOp> = self.buffer.entry(pk);
        match entry {
            Entry::Vacant(e) => {
                e.insert(KeyOp::Delete(old_value));
            }
            Entry::Occupied(mut e) => match e.get_mut() {
                KeyOp::Insert(_) => {
                    e.remove();
                }
                KeyOp::Update((prev, _curr)) => {
                    let prev = std::mem::take(prev);
                    e.insert(KeyOp::Delete(prev));
                }
                KeyOp::Delete(_) => {
                    unreachable!();
                }
            },
        }
    }

    pub fn update(&mut self, pk: Vec<u8>, old_value: Bytes, new_value: Bytes) {
        let entry = self.buffer.entry(pk);
        match entry {
            Entry::Vacant(e) => {
                e.insert(KeyOp::Update((old_value, new_value)));
            }
            Entry::Occupied(mut e) => match e.get_mut() {
                KeyOp::Insert(_) => {
                    e.insert(KeyOp::Insert(new_value));
                }
                KeyOp::Update((_prev, curr)) => {
                    *curr = new_value;
                }
                KeyOp::Delete(_) => {
                    unreachable!()
                }
            },
        }
    }

    pub fn into_parts(self) -> HashMap<Vec<u8>, KeyOp> {
        self.buffer
    }
}
impl<S: StateStore, SD: ValueRowSerde> Execute for MaterializeExecutor<S, SD> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.execute_inner().boxed()
    }
}

// TODO()
impl<S: StateStore, SD: ValueRowSerde> std::fmt::Debug for MaterializeExecutor<S, SD> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MaterializeExecutor")
            // .field("info", &self.info)
            .field("arrange_key_indices", &self.arrange_key_indices)
            .finish()
    }
}

/// A cache for materialize executors.
pub struct MaterializeCache<SD> {
    data: ManagedLruCache<Vec<u8>, CacheValue>,
    metrics_info: MetricsInfo,
    _serde: PhantomData<SD>,
}

type CacheValue = Option<CompactedRow>;

impl<SD: ValueRowSerde> MaterializeCache<SD> {
    pub fn new(watermark_epoch: AtomicU64Ref, metrics_info: MetricsInfo) -> Self {
        let cache: ManagedLruCache<Vec<u8>, CacheValue> =
            new_unbounded(watermark_epoch, metrics_info.clone());
        Self {
            data: cache,
            metrics_info,
            _serde: PhantomData,
        }
    }

    pub async fn handle<'a, S: StateStore>(
        &mut self,
        row_ops: Vec<(Op, Vec<u8>, Bytes)>,
        table: &StateTableInner<S, SD>,
        conflict_behavior: &ConflictBehavior,
    ) -> StreamExecutorResult<MaterializeBuffer> {
        let key_set: HashSet<Box<[u8]>> = row_ops
            .iter()
            .map(|(_, k, _)| k.as_slice().into())
            .collect();

        self.fetch_keys(key_set.iter().map(|v| v.deref()), table, conflict_behavior)
            .await?;

        let mut fixed_changes = MaterializeBuffer::new();
        for (op, key, value) in row_ops {
            let mut update_cache = false;
            let fixed_changes = &mut fixed_changes;
            // When you change the fixed_changes buffer, you must mark `update_cache` to true so the cache and downstream can be consistent.
            let fixed_changes = || {
                update_cache = true;
                fixed_changes
            };
            match op {
                Op::Insert | Op::UpdateInsert => {
                    match conflict_behavior {
                        ConflictBehavior::Overwrite => {
                            match self.force_get(&key) {
                                Some(old_row) => fixed_changes().update(
                                    key.clone(),
                                    old_row.row.clone(),
                                    value.clone(),
                                ),
                                None => fixed_changes().insert(key.clone(), value.clone()),
                            };
                        }
                        ConflictBehavior::IgnoreConflict => {
                            match self.force_get(&key) {
                                Some(_) => (),
                                None => {
                                    fixed_changes().insert(key.clone(), value.clone());
                                }
                            };
                        }
                        _ => unreachable!(),
                    };

                    if update_cache {
                        match conflict_behavior {
                            ConflictBehavior::Overwrite => {
                                self.data.push(key, Some(CompactedRow { row: value }));
                            }
                            ConflictBehavior::IgnoreConflict => {
                                self.data.push(key, Some(CompactedRow { row: value }));
                            }
                            _ => unreachable!(),
                        }
                    }
                }

                Op::Delete | Op::UpdateDelete => {
                    match conflict_behavior {
                        ConflictBehavior::Overwrite => {
                            match self.force_get(&key) {
                                Some(old_row) => {
                                    fixed_changes().delete(key.clone(), old_row.row.clone());
                                }
                                None => (), // delete a nonexistent value
                            };
                        }
                        ConflictBehavior::IgnoreConflict => {
                            match self.force_get(&key) {
                                Some(old_row) => {
                                    if old_row.row == value {
                                        fixed_changes().delete(key.clone(), old_row.row.clone());
                                    }
                                }
                                None => (), // delete a nonexistent value
                            };
                        }
                        _ => unreachable!(),
                    };

                    if update_cache {
                        self.data.push(key, None);
                    }
                }
            }
        }
        Ok(fixed_changes)
    }

    async fn fetch_keys<'a, S: StateStore>(
        &mut self,
        keys: impl Iterator<Item = &'a [u8]>,
        table: &StateTableInner<S, SD>,
        conflict_behavior: &ConflictBehavior,
    ) -> StreamExecutorResult<()> {
        let mut futures = vec![];
        for key in keys {
            self.metrics_info
                .metrics
                .materialize_cache_total_count
                .with_label_values(&[&self.metrics_info.table_id, &self.metrics_info.actor_id])
                .inc();
            if self.data.contains(key) {
                self.metrics_info
                    .metrics
                    .materialize_cache_hit_count
                    .with_label_values(&[&self.metrics_info.table_id, &self.metrics_info.actor_id])
                    .inc();
                continue;
            }
            futures.push(async {
                let key_row = table.pk_serde().deserialize(key).unwrap();
                (key.to_vec(), table.get_compacted_row(&key_row).await)
            });
        }

        let mut buffered = stream::iter(futures).buffer_unordered(10).fuse();
        while let Some(result) = buffered.next().await {
            let (key, value) = result;
            match conflict_behavior {
                ConflictBehavior::Overwrite => self.data.push(key, value?),
                ConflictBehavior::IgnoreConflict => self.data.push(key, value?),
                _ => unreachable!(),
            };
        }

        Ok(())
    }

    pub fn force_get(&mut self, key: &[u8]) -> &CacheValue {
        self.data.get(key).unwrap_or_else(|| {
            panic!(
                "the key {:?} has not been fetched in the materialize executor's cache ",
                key
            )
        })
    }

    fn evict(&mut self) {
        self.data.evict()
    }
}

#[cfg(test)]
mod tests {

    use std::iter;
    use std::sync::atomic::AtomicU64;

    use futures::stream::StreamExt;
    use rand::rngs::SmallRng;
    use rand::{Rng, RngCore, SeedableRng};
    use risingwave_common::array::stream_chunk::{StreamChunkMut, StreamChunkTestExt};
    use risingwave_common::array::stream_chunk_builder::StreamChunkBuilder;
    use risingwave_common::array::Op;
    use risingwave_common::catalog::{ColumnDesc, ConflictBehavior, Field, Schema, TableId};
    use risingwave_common::row::OwnedRow;
    use risingwave_common::types::DataType;
    use risingwave_common::util::sort_util::{ColumnOrder, OrderType};
    use risingwave_hummock_sdk::HummockReadEpoch;
    use risingwave_storage::memory::MemoryStateStore;
    use risingwave_storage::table::batch_table::storage_table::StorageTable;

    use crate::executor::test_utils::prelude::StateTable;
    use crate::executor::test_utils::*;
    use crate::executor::*;

    #[tokio::test]
    async fn test_materialize_executor() {
        // Prepare storage and memtable.
        let memory_state_store = MemoryStateStore::new();
        let table_id = TableId::new(1);
        // Two columns of int32 type, the first column is PK.
        let schema = Schema::new(vec![
            Field::unnamed(DataType::Int32),
            Field::unnamed(DataType::Int32),
        ]);
        let column_ids = vec![0.into(), 1.into()];

        // Prepare source chunks.
        let chunk1 = StreamChunk::from_pretty(
            " i i
            + 1 4
            + 2 5
            + 3 6",
        );
        let chunk2 = StreamChunk::from_pretty(
            " i i
            + 7 8
            - 3 6",
        );

        // Prepare stream executors.
        let source = MockSource::with_messages(vec![
            Message::Barrier(Barrier::new_test_barrier(1)),
            Message::Chunk(chunk1),
            Message::Barrier(Barrier::new_test_barrier(2)),
            Message::Chunk(chunk2),
            Message::Barrier(Barrier::new_test_barrier(3)),
        ])
        .to_executor(schema.clone(), PkIndices::new());

        let order_types = vec![OrderType::ascending()];
        let column_descs = vec![
            ColumnDesc::unnamed(column_ids[0], DataType::Int32),
            ColumnDesc::unnamed(column_ids[1], DataType::Int32),
        ];

        let table = StorageTable::for_test(
            memory_state_store.clone(),
            table_id,
            column_descs,
            order_types,
            vec![0],
            vec![0, 1],
        );

        let mut materialize_executor = MaterializeExecutor::for_test(
            source,
            memory_state_store,
            table_id,
            vec![ColumnOrder::new(0, OrderType::ascending())],
            column_ids,
            Arc::new(AtomicU64::new(0)),
            ConflictBehavior::NoCheck,
        )
        .await
        .boxed()
        .execute();
        materialize_executor.next().await.transpose().unwrap();

        materialize_executor.next().await.transpose().unwrap();

        // First stream chunk. We check the existence of (3) -> (3,6)
        match materialize_executor.next().await.transpose().unwrap() {
            Some(Message::Barrier(_)) => {
                let row = table
                    .get_row(
                        &OwnedRow::new(vec![Some(3_i32.into())]),
                        HummockReadEpoch::NoWait(u64::MAX),
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    row,
                    Some(OwnedRow::new(vec![Some(3_i32.into()), Some(6_i32.into())]))
                );
            }
            _ => unreachable!(),
        }
        materialize_executor.next().await.transpose().unwrap();
        // Second stream chunk. We check the existence of (7) -> (7,8)
        match materialize_executor.next().await.transpose().unwrap() {
            Some(Message::Barrier(_)) => {
                let row = table
                    .get_row(
                        &OwnedRow::new(vec![Some(7_i32.into())]),
                        HummockReadEpoch::NoWait(u64::MAX),
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    row,
                    Some(OwnedRow::new(vec![Some(7_i32.into()), Some(8_i32.into())]))
                );
            }
            _ => unreachable!(),
        }
    }

    // https://github.com/risingwavelabs/risingwave/issues/13346
    #[tokio::test]
    async fn test_upsert_stream() {
        // Prepare storage and memtable.
        let memory_state_store = MemoryStateStore::new();
        let table_id = TableId::new(1);
        // Two columns of int32 type, the first column is PK.
        let schema = Schema::new(vec![
            Field::unnamed(DataType::Int32),
            Field::unnamed(DataType::Int32),
        ]);
        let column_ids = vec![0.into(), 1.into()];

        // test double insert one pk, the latter needs to override the former.
        let chunk1 = StreamChunk::from_pretty(
            " i i
            + 1 1",
        );

        let chunk2 = StreamChunk::from_pretty(
            " i i
            + 1 2
            - 1 2",
        );

        // Prepare stream executors.
        let source = MockSource::with_messages(vec![
            Message::Barrier(Barrier::new_test_barrier(1)),
            Message::Chunk(chunk1),
            Message::Barrier(Barrier::new_test_barrier(2)),
            Message::Chunk(chunk2),
            Message::Barrier(Barrier::new_test_barrier(3)),
        ])
        .to_executor(schema.clone(), PkIndices::new());

        let order_types = vec![OrderType::ascending()];
        let column_descs = vec![
            ColumnDesc::unnamed(column_ids[0], DataType::Int32),
            ColumnDesc::unnamed(column_ids[1], DataType::Int32),
        ];

        let table = StorageTable::for_test(
            memory_state_store.clone(),
            table_id,
            column_descs,
            order_types,
            vec![0],
            vec![0, 1],
        );

        let mut materialize_executor = MaterializeExecutor::for_test(
            source,
            memory_state_store,
            table_id,
            vec![ColumnOrder::new(0, OrderType::ascending())],
            column_ids,
            Arc::new(AtomicU64::new(0)),
            ConflictBehavior::Overwrite,
        )
        .await
        .boxed()
        .execute();
        materialize_executor.next().await.transpose().unwrap();

        materialize_executor.next().await.transpose().unwrap();
        materialize_executor.next().await.transpose().unwrap();
        materialize_executor.next().await.transpose().unwrap();

        match materialize_executor.next().await.transpose().unwrap() {
            Some(Message::Barrier(_)) => {
                let row = table
                    .get_row(
                        &OwnedRow::new(vec![Some(1_i32.into())]),
                        HummockReadEpoch::NoWait(u64::MAX),
                    )
                    .await
                    .unwrap();
                assert!(row.is_none());
            }
            _ => unreachable!(),
        }
    }

    #[tokio::test]
    async fn test_check_insert_conflict() {
        // Prepare storage and memtable.
        let memory_state_store = MemoryStateStore::new();
        let table_id = TableId::new(1);
        // Two columns of int32 type, the first column is PK.
        let schema = Schema::new(vec![
            Field::unnamed(DataType::Int32),
            Field::unnamed(DataType::Int32),
        ]);
        let column_ids = vec![0.into(), 1.into()];

        // test double insert one pk, the latter needs to override the former.
        let chunk1 = StreamChunk::from_pretty(
            " i i
            + 1 3
            + 1 4
            + 2 5
            + 3 6",
        );

        let chunk2 = StreamChunk::from_pretty(
            " i i
            + 1 3
            + 2 6",
        );

        // test delete wrong value, delete inexistent pk
        let chunk3 = StreamChunk::from_pretty(
            " i i
            + 1 4",
        );

        // Prepare stream executors.
        let source = MockSource::with_messages(vec![
            Message::Barrier(Barrier::new_test_barrier(1)),
            Message::Chunk(chunk1),
            Message::Chunk(chunk2),
            Message::Barrier(Barrier::new_test_barrier(2)),
            Message::Chunk(chunk3),
            Message::Barrier(Barrier::new_test_barrier(3)),
        ])
        .to_executor(schema.clone(), PkIndices::new());

        let order_types = vec![OrderType::ascending()];
        let column_descs = vec![
            ColumnDesc::unnamed(column_ids[0], DataType::Int32),
            ColumnDesc::unnamed(column_ids[1], DataType::Int32),
        ];

        let table = StorageTable::for_test(
            memory_state_store.clone(),
            table_id,
            column_descs,
            order_types,
            vec![0],
            vec![0, 1],
        );

        let mut materialize_executor = MaterializeExecutor::for_test(
            source,
            memory_state_store,
            table_id,
            vec![ColumnOrder::new(0, OrderType::ascending())],
            column_ids,
            Arc::new(AtomicU64::new(0)),
            ConflictBehavior::Overwrite,
        )
        .await
        .boxed()
        .execute();
        materialize_executor.next().await.transpose().unwrap();

        materialize_executor.next().await.transpose().unwrap();
        materialize_executor.next().await.transpose().unwrap();

        // First stream chunk. We check the existence of (3) -> (3,6)
        match materialize_executor.next().await.transpose().unwrap() {
            Some(Message::Barrier(_)) => {
                let row = table
                    .get_row(
                        &OwnedRow::new(vec![Some(3_i32.into())]),
                        HummockReadEpoch::NoWait(u64::MAX),
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    row,
                    Some(OwnedRow::new(vec![Some(3_i32.into()), Some(6_i32.into())]))
                );

                let row = table
                    .get_row(
                        &OwnedRow::new(vec![Some(1_i32.into())]),
                        HummockReadEpoch::NoWait(u64::MAX),
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    row,
                    Some(OwnedRow::new(vec![Some(1_i32.into()), Some(3_i32.into())]))
                );

                let row = table
                    .get_row(
                        &OwnedRow::new(vec![Some(2_i32.into())]),
                        HummockReadEpoch::NoWait(u64::MAX),
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    row,
                    Some(OwnedRow::new(vec![Some(2_i32.into()), Some(6_i32.into())]))
                );
            }
            _ => unreachable!(),
        }
    }

    #[tokio::test]
    async fn test_delete_and_update_conflict() {
        // Prepare storage and memtable.
        let memory_state_store = MemoryStateStore::new();
        let table_id = TableId::new(1);
        // Two columns of int32 type, the first column is PK.
        let schema = Schema::new(vec![
            Field::unnamed(DataType::Int32),
            Field::unnamed(DataType::Int32),
        ]);
        let column_ids = vec![0.into(), 1.into()];

        // test double insert one pk, the latter needs to override the former.
        let chunk1 = StreamChunk::from_pretty(
            " i i
            + 1 4
            + 2 5
            + 3 6
            U- 8 1
            U+ 8 2
            + 8 3",
        );

        // test delete wrong value, delete inexistent pk
        let chunk2 = StreamChunk::from_pretty(
            " i i
            + 7 8
            - 3 4
            - 5 0",
        );

        // test delete wrong value, delete inexistent pk
        let chunk3 = StreamChunk::from_pretty(
            " i i
            + 1 5
            U- 2 4
            U+ 2 8
            U- 9 0
            U+ 9 1",
        );

        // Prepare stream executors.
        let source = MockSource::with_messages(vec![
            Message::Barrier(Barrier::new_test_barrier(1)),
            Message::Chunk(chunk1),
            Message::Barrier(Barrier::new_test_barrier(2)),
            Message::Chunk(chunk2),
            Message::Barrier(Barrier::new_test_barrier(3)),
            Message::Chunk(chunk3),
            Message::Barrier(Barrier::new_test_barrier(4)),
        ])
        .to_executor(schema.clone(), PkIndices::new());

        let order_types = vec![OrderType::ascending()];
        let column_descs = vec![
            ColumnDesc::unnamed(column_ids[0], DataType::Int32),
            ColumnDesc::unnamed(column_ids[1], DataType::Int32),
        ];

        let table = StorageTable::for_test(
            memory_state_store.clone(),
            table_id,
            column_descs,
            order_types,
            vec![0],
            vec![0, 1],
        );

        let mut materialize_executor = MaterializeExecutor::for_test(
            source,
            memory_state_store,
            table_id,
            vec![ColumnOrder::new(0, OrderType::ascending())],
            column_ids,
            Arc::new(AtomicU64::new(0)),
            ConflictBehavior::Overwrite,
        )
        .await
        .boxed()
        .execute();
        materialize_executor.next().await.transpose().unwrap();

        materialize_executor.next().await.transpose().unwrap();

        // First stream chunk. We check the existence of (3) -> (3,6)
        match materialize_executor.next().await.transpose().unwrap() {
            Some(Message::Barrier(_)) => {
                // can read (8, 3), check insert after update
                let row = table
                    .get_row(
                        &OwnedRow::new(vec![Some(8_i32.into())]),
                        HummockReadEpoch::NoWait(u64::MAX),
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    row,
                    Some(OwnedRow::new(vec![Some(8_i32.into()), Some(3_i32.into())]))
                );
            }
            _ => unreachable!(),
        }
        materialize_executor.next().await.transpose().unwrap();

        match materialize_executor.next().await.transpose().unwrap() {
            Some(Message::Barrier(_)) => {
                let row = table
                    .get_row(
                        &OwnedRow::new(vec![Some(7_i32.into())]),
                        HummockReadEpoch::NoWait(u64::MAX),
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    row,
                    Some(OwnedRow::new(vec![Some(7_i32.into()), Some(8_i32.into())]))
                );

                // check delete wrong value
                let row = table
                    .get_row(
                        &OwnedRow::new(vec![Some(3_i32.into())]),
                        HummockReadEpoch::NoWait(u64::MAX),
                    )
                    .await
                    .unwrap();
                assert_eq!(row, None);

                // check delete wrong pk
                let row = table
                    .get_row(
                        &OwnedRow::new(vec![Some(5_i32.into())]),
                        HummockReadEpoch::NoWait(u64::MAX),
                    )
                    .await
                    .unwrap();
                assert_eq!(row, None);
            }
            _ => unreachable!(),
        }

        materialize_executor.next().await.transpose().unwrap();
        // Second stream chunk. We check the existence of (7) -> (7,8)
        match materialize_executor.next().await.transpose().unwrap() {
            Some(Message::Barrier(_)) => {
                let row = table
                    .get_row(
                        &OwnedRow::new(vec![Some(1_i32.into())]),
                        HummockReadEpoch::NoWait(u64::MAX),
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    row,
                    Some(OwnedRow::new(vec![Some(1_i32.into()), Some(5_i32.into())]))
                );

                // check update wrong value
                let row = table
                    .get_row(
                        &OwnedRow::new(vec![Some(2_i32.into())]),
                        HummockReadEpoch::NoWait(u64::MAX),
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    row,
                    Some(OwnedRow::new(vec![Some(2_i32.into()), Some(8_i32.into())]))
                );

                // check update wrong pk, should become insert
                let row = table
                    .get_row(
                        &OwnedRow::new(vec![Some(9_i32.into())]),
                        HummockReadEpoch::NoWait(u64::MAX),
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    row,
                    Some(OwnedRow::new(vec![Some(9_i32.into()), Some(1_i32.into())]))
                );
            }
            _ => unreachable!(),
        }
    }

    #[tokio::test]
    async fn test_ignore_insert_conflict() {
        // Prepare storage and memtable.
        let memory_state_store = MemoryStateStore::new();
        let table_id = TableId::new(1);
        // Two columns of int32 type, the first column is PK.
        let schema = Schema::new(vec![
            Field::unnamed(DataType::Int32),
            Field::unnamed(DataType::Int32),
        ]);
        let column_ids = vec![0.into(), 1.into()];

        // test double insert one pk, the latter needs to be ignored.
        let chunk1 = StreamChunk::from_pretty(
            " i i
            + 1 3
            + 1 4
            + 2 5
            + 3 6",
        );

        let chunk2 = StreamChunk::from_pretty(
            " i i
            + 1 5
            + 2 6",
        );

        // test delete wrong value, delete inexistent pk
        let chunk3 = StreamChunk::from_pretty(
            " i i
            + 1 6",
        );

        // Prepare stream executors.
        let source = MockSource::with_messages(vec![
            Message::Barrier(Barrier::new_test_barrier(1)),
            Message::Chunk(chunk1),
            Message::Chunk(chunk2),
            Message::Barrier(Barrier::new_test_barrier(2)),
            Message::Chunk(chunk3),
            Message::Barrier(Barrier::new_test_barrier(3)),
        ])
        .to_executor(schema.clone(), PkIndices::new());

        let order_types = vec![OrderType::ascending()];
        let column_descs = vec![
            ColumnDesc::unnamed(column_ids[0], DataType::Int32),
            ColumnDesc::unnamed(column_ids[1], DataType::Int32),
        ];

        let table = StorageTable::for_test(
            memory_state_store.clone(),
            table_id,
            column_descs,
            order_types,
            vec![0],
            vec![0, 1],
        );

        let mut materialize_executor = MaterializeExecutor::for_test(
            source,
            memory_state_store,
            table_id,
            vec![ColumnOrder::new(0, OrderType::ascending())],
            column_ids,
            Arc::new(AtomicU64::new(0)),
            ConflictBehavior::IgnoreConflict,
        )
        .await
        .boxed()
        .execute();
        materialize_executor.next().await.transpose().unwrap();

        materialize_executor.next().await.transpose().unwrap();
        materialize_executor.next().await.transpose().unwrap();

        // First stream chunk. We check the existence of (3) -> (3,6)
        match materialize_executor.next().await.transpose().unwrap() {
            Some(Message::Barrier(_)) => {
                let row = table
                    .get_row(
                        &OwnedRow::new(vec![Some(3_i32.into())]),
                        HummockReadEpoch::NoWait(u64::MAX),
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    row,
                    Some(OwnedRow::new(vec![Some(3_i32.into()), Some(6_i32.into())]))
                );

                let row = table
                    .get_row(
                        &OwnedRow::new(vec![Some(1_i32.into())]),
                        HummockReadEpoch::NoWait(u64::MAX),
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    row,
                    Some(OwnedRow::new(vec![Some(1_i32.into()), Some(3_i32.into())]))
                );

                let row = table
                    .get_row(
                        &OwnedRow::new(vec![Some(2_i32.into())]),
                        HummockReadEpoch::NoWait(u64::MAX),
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    row,
                    Some(OwnedRow::new(vec![Some(2_i32.into()), Some(5_i32.into())]))
                );
            }
            _ => unreachable!(),
        }
    }

    #[tokio::test]
    async fn test_ignore_delete_then_insert() {
        // Prepare storage and memtable.
        let memory_state_store = MemoryStateStore::new();
        let table_id = TableId::new(1);
        // Two columns of int32 type, the first column is PK.
        let schema = Schema::new(vec![
            Field::unnamed(DataType::Int32),
            Field::unnamed(DataType::Int32),
        ]);
        let column_ids = vec![0.into(), 1.into()];

        // test insert after delete one pk, the latter insert should succeed.
        let chunk1 = StreamChunk::from_pretty(
            " i i
            + 1 3
            - 1 3
            + 1 6",
        );

        // Prepare stream executors.
        let source = MockSource::with_messages(vec![
            Message::Barrier(Barrier::new_test_barrier(1)),
            Message::Chunk(chunk1),
            Message::Barrier(Barrier::new_test_barrier(2)),
        ])
        .to_executor(schema.clone(), PkIndices::new());

        let order_types = vec![OrderType::ascending()];
        let column_descs = vec![
            ColumnDesc::unnamed(column_ids[0], DataType::Int32),
            ColumnDesc::unnamed(column_ids[1], DataType::Int32),
        ];

        let table = StorageTable::for_test(
            memory_state_store.clone(),
            table_id,
            column_descs,
            order_types,
            vec![0],
            vec![0, 1],
        );

        let mut materialize_executor = MaterializeExecutor::for_test(
            source,
            memory_state_store,
            table_id,
            vec![ColumnOrder::new(0, OrderType::ascending())],
            column_ids,
            Arc::new(AtomicU64::new(0)),
            ConflictBehavior::IgnoreConflict,
        )
        .await
        .boxed()
        .execute();
        let _msg1 = materialize_executor
            .next()
            .await
            .transpose()
            .unwrap()
            .unwrap()
            .as_barrier()
            .unwrap();
        let _msg2 = materialize_executor
            .next()
            .await
            .transpose()
            .unwrap()
            .unwrap()
            .as_chunk()
            .unwrap();
        let _msg3 = materialize_executor
            .next()
            .await
            .transpose()
            .unwrap()
            .unwrap()
            .as_barrier()
            .unwrap();

        let row = table
            .get_row(
                &OwnedRow::new(vec![Some(1_i32.into())]),
                HummockReadEpoch::NoWait(u64::MAX),
            )
            .await
            .unwrap();
        assert_eq!(
            row,
            Some(OwnedRow::new(vec![Some(1_i32.into()), Some(6_i32.into())]))
        );
    }

    #[tokio::test]
    async fn test_ignore_delete_and_update_conflict() {
        // Prepare storage and memtable.
        let memory_state_store = MemoryStateStore::new();
        let table_id = TableId::new(1);
        // Two columns of int32 type, the first column is PK.
        let schema = Schema::new(vec![
            Field::unnamed(DataType::Int32),
            Field::unnamed(DataType::Int32),
        ]);
        let column_ids = vec![0.into(), 1.into()];

        // test double insert one pk, the latter should be ignored.
        let chunk1 = StreamChunk::from_pretty(
            " i i
            + 1 4
            + 2 5
            + 3 6
            U- 8 1
            U+ 8 2
            + 8 3",
        );

        // test delete wrong value, delete inexistent pk
        let chunk2 = StreamChunk::from_pretty(
            " i i
            + 7 8
            - 3 4
            - 5 0",
        );

        // test delete wrong value, delete inexistent pk
        let chunk3 = StreamChunk::from_pretty(
            " i i
            + 1 5
            U- 2 4
            U+ 2 8
            U- 9 0
            U+ 9 1",
        );

        // Prepare stream executors.
        let source = MockSource::with_messages(vec![
            Message::Barrier(Barrier::new_test_barrier(1)),
            Message::Chunk(chunk1),
            Message::Barrier(Barrier::new_test_barrier(2)),
            Message::Chunk(chunk2),
            Message::Barrier(Barrier::new_test_barrier(3)),
            Message::Chunk(chunk3),
            Message::Barrier(Barrier::new_test_barrier(4)),
        ])
        .to_executor(schema.clone(), PkIndices::new());

        let order_types = vec![OrderType::ascending()];
        let column_descs = vec![
            ColumnDesc::unnamed(column_ids[0], DataType::Int32),
            ColumnDesc::unnamed(column_ids[1], DataType::Int32),
        ];

        let table = StorageTable::for_test(
            memory_state_store.clone(),
            table_id,
            column_descs,
            order_types,
            vec![0],
            vec![0, 1],
        );

        let mut materialize_executor = MaterializeExecutor::for_test(
            source,
            memory_state_store,
            table_id,
            vec![ColumnOrder::new(0, OrderType::ascending())],
            column_ids,
            Arc::new(AtomicU64::new(0)),
            ConflictBehavior::IgnoreConflict,
        )
        .await
        .boxed()
        .execute();
        materialize_executor.next().await.transpose().unwrap();

        materialize_executor.next().await.transpose().unwrap();

        // First stream chunk. We check the existence of (3) -> (3,6)
        match materialize_executor.next().await.transpose().unwrap() {
            Some(Message::Barrier(_)) => {
                // can read (8, 2), check insert after update
                let row = table
                    .get_row(
                        &OwnedRow::new(vec![Some(8_i32.into())]),
                        HummockReadEpoch::NoWait(u64::MAX),
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    row,
                    Some(OwnedRow::new(vec![Some(8_i32.into()), Some(2_i32.into())]))
                );
            }
            _ => unreachable!(),
        }
        materialize_executor.next().await.transpose().unwrap();

        match materialize_executor.next().await.transpose().unwrap() {
            Some(Message::Barrier(_)) => {
                let row = table
                    .get_row(
                        &OwnedRow::new(vec![Some(7_i32.into())]),
                        HummockReadEpoch::NoWait(u64::MAX),
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    row,
                    Some(OwnedRow::new(vec![Some(7_i32.into()), Some(8_i32.into())]))
                );

                // check delete wrong value
                let row = table
                    .get_row(
                        &OwnedRow::new(vec![Some(3_i32.into())]),
                        HummockReadEpoch::NoWait(u64::MAX),
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    row,
                    Some(OwnedRow::new(vec![Some(3_i32.into()), Some(6_i32.into())]))
                );

                // check delete wrong pk
                let row = table
                    .get_row(
                        &OwnedRow::new(vec![Some(5_i32.into())]),
                        HummockReadEpoch::NoWait(u64::MAX),
                    )
                    .await
                    .unwrap();
                assert_eq!(row, None);
            }
            _ => unreachable!(),
        }

        materialize_executor.next().await.transpose().unwrap();
        // materialize_executor.next().await.transpose().unwrap();
        // Second stream chunk. We check the existence of (7) -> (7,8)
        match materialize_executor.next().await.transpose().unwrap() {
            Some(Message::Barrier(_)) => {
                let row = table
                    .get_row(
                        &OwnedRow::new(vec![Some(1_i32.into())]),
                        HummockReadEpoch::NoWait(u64::MAX),
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    row,
                    Some(OwnedRow::new(vec![Some(1_i32.into()), Some(4_i32.into())]))
                );

                // check update wrong value
                let row = table
                    .get_row(
                        &OwnedRow::new(vec![Some(2_i32.into())]),
                        HummockReadEpoch::NoWait(u64::MAX),
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    row,
                    Some(OwnedRow::new(vec![Some(2_i32.into()), Some(5_i32.into())]))
                );

                // check update wrong pk, should become insert
                let row = table
                    .get_row(
                        &OwnedRow::new(vec![Some(9_i32.into())]),
                        HummockReadEpoch::NoWait(u64::MAX),
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    row,
                    Some(OwnedRow::new(vec![Some(9_i32.into()), Some(1_i32.into())]))
                );
            }
            _ => unreachable!(),
        }
    }

    fn gen_fuzz_data(row_number: usize, chunk_size: usize) -> Vec<StreamChunk> {
        const KN: u32 = 4;
        const SEED: u64 = 998244353;
        let mut ret = vec![];
        let mut builder =
            StreamChunkBuilder::new(chunk_size, vec![DataType::Int32, DataType::Int32]);
        let mut rng = SmallRng::seed_from_u64(SEED);

        let random_vis = |c: StreamChunk, rng: &mut SmallRng| -> StreamChunk {
            let len = c.data_chunk().capacity();
            let mut c = StreamChunkMut::from(c);
            for i in 0..len {
                c.set_vis(i, rng.gen_bool(0.5));
            }
            c.into()
        };
        for _ in 0..row_number {
            let k = (rng.next_u32() % KN) as i32;
            let v = rng.next_u32() as i32;
            let op = if rng.gen_bool(0.5) {
                Op::Insert
            } else {
                Op::Delete
            };
            if let Some(c) =
                builder.append_row(op, OwnedRow::new(vec![Some(k.into()), Some(v.into())]))
            {
                ret.push(random_vis(c, &mut rng));
            }
        }
        if let Some(c) = builder.take() {
            ret.push(random_vis(c, &mut rng));
        }
        ret
    }

    async fn fuzz_test_stream_consistent_inner(conflict_behavior: ConflictBehavior) {
        const N: usize = 100000;

        // Prepare storage and memtable.
        let memory_state_store = MemoryStateStore::new();
        let table_id = TableId::new(1);
        // Two columns of int32 type, the first column is PK.
        let schema = Schema::new(vec![
            Field::unnamed(DataType::Int32),
            Field::unnamed(DataType::Int32),
        ]);
        let column_ids = vec![0.into(), 1.into()];

        let chunks = gen_fuzz_data(N, 128);
        let messages = iter::once(Message::Barrier(Barrier::new_test_barrier(1)))
            .chain(chunks.into_iter().map(Message::Chunk))
            .chain(iter::once(Message::Barrier(Barrier::new_test_barrier(2))))
            .collect();
        // Prepare stream executors.
        let source =
            MockSource::with_messages(messages).to_executor(schema.clone(), PkIndices::new());

        let mut materialize_executor = MaterializeExecutor::for_test(
            source,
            memory_state_store.clone(),
            table_id,
            vec![ColumnOrder::new(0, OrderType::ascending())],
            column_ids,
            Arc::new(AtomicU64::new(0)),
            conflict_behavior,
        )
        .await
        .boxed()
        .execute();
        materialize_executor.expect_barrier().await;

        let order_types = vec![OrderType::ascending()];
        let column_descs = vec![
            ColumnDesc::unnamed(0.into(), DataType::Int32),
            ColumnDesc::unnamed(1.into(), DataType::Int32),
        ];
        let pk_indices = vec![0];

        let mut table = StateTable::new_without_distribution(
            memory_state_store.clone(),
            TableId::from(1002),
            column_descs.clone(),
            order_types,
            pk_indices,
        )
        .await;

        while let Message::Chunk(c) = materialize_executor.next().await.unwrap().unwrap() {
            // check with state table's memtable
            table.write_chunk(c);
        }
    }

    #[tokio::test]
    async fn fuzz_test_stream_consistent_upsert() {
        fuzz_test_stream_consistent_inner(ConflictBehavior::Overwrite).await
    }

    #[tokio::test]
    async fn fuzz_test_stream_consistent_ignore() {
        fuzz_test_stream_consistent_inner(ConflictBehavior::IgnoreConflict).await
    }
}

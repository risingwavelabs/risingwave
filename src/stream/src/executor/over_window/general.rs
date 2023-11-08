// Copyright 2023 RisingWave Labs
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

use std::collections::{btree_map, BTreeMap, HashSet};
use std::marker::PhantomData;
use std::ops::RangeInclusive;
use std::sync::Arc;

use futures::StreamExt;
use futures_async_stream::try_stream;
use itertools::Itertools;
use risingwave_common::array::stream_record::Record;
use risingwave_common::array::{Op, RowRef, StreamChunk};
use risingwave_common::catalog::Field;
use risingwave_common::row::{OwnedRow, Row, RowExt};
use risingwave_common::session_config::OverWindowCachePolicy as CachePolicy;
use risingwave_common::types::{DataType, DefaultOrdered};
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_common::util::memcmp_encoding::{self, MemcmpEncoded};
use risingwave_common::util::sort_util::OrderType;
use risingwave_expr::window_function::{
    create_window_state, StateKey, WindowFuncCall, WindowStates,
};
use risingwave_storage::StateStore;

use super::delta_btree_map::Change;
use super::over_partition::{
    new_empty_partition_cache, shrink_partition_cache, CacheKey, OverPartition, PartitionCache,
    PartitionDelta,
};
use crate::cache::{new_unbounded, ManagedLruCache};
use crate::common::metrics::MetricsInfo;
use crate::common::StreamChunkBuilder;
use crate::executor::monitor::StreamingMetrics;
use crate::executor::over_window::delta_btree_map::PositionType;
use crate::executor::test_utils::prelude::StateTable;
use crate::executor::{
    expect_first_barrier, ActorContextRef, BoxedExecutor, Executor, ExecutorInfo, Message,
    PkIndices, StreamExecutorError, StreamExecutorResult,
};
use crate::task::AtomicU64Ref;

/// [`OverWindowExecutor`] consumes retractable input stream and produces window function outputs.
/// One [`OverWindowExecutor`] can handle one combination of partition key and order key.
///
/// - State table schema = output schema, state table pk = `partition key | order key | input pk`.
/// - Output schema = input schema + window function results.
pub struct OverWindowExecutor<S: StateStore> {
    input: Box<dyn Executor>,
    inner: ExecutorInner<S>,
}

struct ExecutorInner<S: StateStore> {
    actor_ctx: ActorContextRef,
    info: ExecutorInfo,

    calls: Vec<WindowFuncCall>,
    partition_key_indices: Vec<usize>,
    order_key_indices: Vec<usize>,
    order_key_data_types: Vec<DataType>,
    order_key_order_types: Vec<OrderType>,
    input_pk_indices: Vec<usize>,
    input_schema_len: usize,

    state_table: StateTable<S>,
    watermark_epoch: AtomicU64Ref,
    metrics: Arc<StreamingMetrics>,

    /// The maximum size of the chunk produced by executor at a time.
    chunk_size: usize,
    cache_policy: CachePolicy,
}

struct ExecutionVars<S: StateStore> {
    /// partition key => partition range cache.
    cached_partitions: ManagedLruCache<OwnedRow, PartitionCache>,
    /// partition key => recently accessed range.
    recently_accessed_ranges: BTreeMap<DefaultOrdered<OwnedRow>, RangeInclusive<StateKey>>,
    stats: ExecutionStats,
    _phantom: PhantomData<S>,
}

#[derive(Default)]
struct ExecutionStats {
    cache_miss: u64,
    cache_lookup: u64,
}

impl<S: StateStore> Executor for OverWindowExecutor<S> {
    fn execute(self: Box<Self>) -> crate::executor::BoxedMessageStream {
        self.executor_inner().boxed()
    }

    fn schema(&self) -> &risingwave_common::catalog::Schema {
        &self.inner.info.schema
    }

    fn pk_indices(&self) -> crate::executor::PkIndicesRef<'_> {
        &self.inner.info.pk_indices
    }

    fn identity(&self) -> &str {
        &self.inner.info.identity
    }
}

impl<S: StateStore> ExecutorInner<S> {
    fn get_partition_key(&self, full_row: impl Row) -> OwnedRow {
        full_row
            .project(&self.partition_key_indices)
            .into_owned_row()
    }

    fn get_input_pk(&self, full_row: impl Row) -> OwnedRow {
        full_row.project(&self.input_pk_indices).into_owned_row()
    }

    /// `full_row` can be an input row or state table row.
    fn encode_order_key(&self, full_row: impl Row) -> StreamExecutorResult<MemcmpEncoded> {
        Ok(memcmp_encoding::encode_row(
            full_row.project(&self.order_key_indices),
            &self.order_key_order_types,
        )?)
    }

    fn row_to_cache_key(&self, full_row: impl Row + Copy) -> StreamExecutorResult<CacheKey> {
        Ok(CacheKey::Normal(StateKey {
            order_key: self.encode_order_key(full_row)?,
            pk: self.get_input_pk(full_row).into(),
        }))
    }
}

pub struct OverWindowExecutorArgs<S: StateStore> {
    pub input: BoxedExecutor,

    pub actor_ctx: ActorContextRef,
    pub pk_indices: PkIndices,
    pub executor_id: u64,

    pub calls: Vec<WindowFuncCall>,
    pub partition_key_indices: Vec<usize>,
    pub order_key_indices: Vec<usize>,
    pub order_key_order_types: Vec<OrderType>,

    pub state_table: StateTable<S>,
    pub watermark_epoch: AtomicU64Ref,
    pub metrics: Arc<StreamingMetrics>,

    pub chunk_size: usize,
    pub cache_policy: CachePolicy,
}

impl<S: StateStore> OverWindowExecutor<S> {
    pub fn new(args: OverWindowExecutorArgs<S>) -> Self {
        let input_info = args.input.info();

        let schema = {
            let mut schema = input_info.schema.clone();
            args.calls.iter().for_each(|call| {
                schema.fields.push(Field::unnamed(call.return_type.clone()));
            });
            schema
        };

        let has_unbounded_frame = args.calls.iter().any(|call| call.frame.is_unbounded());
        let cache_policy = if has_unbounded_frame {
            // For unbounded frames, we finally need all entries of the partition in the cache,
            // so for simplicity we just use full cache policy for these cases.
            CachePolicy::Full
        } else {
            args.cache_policy
        };

        let order_key_data_types = args
            .order_key_indices
            .iter()
            .map(|i| schema.fields()[*i].data_type.clone())
            .collect();

        Self {
            input: args.input,
            inner: ExecutorInner {
                actor_ctx: args.actor_ctx,
                info: ExecutorInfo {
                    schema,
                    pk_indices: args.pk_indices,
                    identity: format!("OverWindowExecutor {:X}", args.executor_id),
                },
                calls: args.calls,
                partition_key_indices: args.partition_key_indices,
                order_key_indices: args.order_key_indices,
                order_key_data_types,
                order_key_order_types: args.order_key_order_types,
                input_pk_indices: input_info.pk_indices,
                input_schema_len: input_info.schema.len(),
                state_table: args.state_table,
                watermark_epoch: args.watermark_epoch,
                metrics: args.metrics,
                chunk_size: args.chunk_size,
                cache_policy,
            },
        }
    }

    /// Merge changes by input pk in the given chunk, return a change iterator which guarantees that
    /// each pk only appears once. This method also validates the consistency of the input
    /// chunk.
    ///
    /// TODO(rc): We may want to optimize this by handling changes on the same pk during generating
    /// partition [`Change`]s.
    fn merge_changes_in_chunk<'a>(
        this: &'_ ExecutorInner<S>,
        chunk: &'a StreamChunk,
    ) -> impl Iterator<Item = Record<RowRef<'a>>> {
        let mut changes_merged = BTreeMap::new();
        for (op, row) in chunk.rows() {
            let pk = DefaultOrdered(this.get_input_pk(row));
            match op {
                Op::Insert | Op::UpdateInsert => {
                    if let Some(prev_change) = changes_merged.get_mut(&pk) {
                        match prev_change {
                            Record::Delete { old_row } => {
                                *prev_change = Record::Update {
                                    old_row: *old_row,
                                    new_row: row,
                                };
                            }
                            _ => panic!("inconsistent changes in input chunk"),
                        }
                    } else {
                        changes_merged.insert(pk, Record::Insert { new_row: row });
                    }
                }
                Op::Delete | Op::UpdateDelete => {
                    if let Some(prev_change) = changes_merged.get_mut(&pk) {
                        match prev_change {
                            Record::Insert { .. } => {
                                changes_merged.remove(&pk);
                            }
                            Record::Update {
                                old_row: real_old_row,
                                ..
                            } => {
                                *prev_change = Record::Delete {
                                    old_row: *real_old_row,
                                };
                            }
                            _ => panic!("inconsistent changes in input chunk"),
                        }
                    } else {
                        changes_merged.insert(pk, Record::Delete { old_row: row });
                    }
                }
            }
        }
        changes_merged.into_values()
    }

    #[try_stream(ok = StreamChunk, error = StreamExecutorError)]
    async fn apply_chunk<'a>(
        this: &'a mut ExecutorInner<S>,
        vars: &'a mut ExecutionVars<S>,
        chunk: StreamChunk,
    ) {
        // partition key => changes happened in the partition.
        let mut deltas: BTreeMap<DefaultOrdered<OwnedRow>, PartitionDelta> = BTreeMap::new();
        // input pk of update records of which the order key is changed.
        let mut key_change_updated_pks = HashSet::new();

        // Collect changes for each partition.
        for record in Self::merge_changes_in_chunk(this, &chunk) {
            match record {
                Record::Insert { new_row } => {
                    let part_key = this.get_partition_key(new_row).into();
                    let part_delta = deltas.entry(part_key).or_default();
                    part_delta.insert(
                        this.row_to_cache_key(new_row)?,
                        Change::Insert(new_row.into_owned_row()),
                    );
                }
                Record::Delete { old_row } => {
                    let part_key = this.get_partition_key(old_row).into();
                    let part_delta = deltas.entry(part_key).or_default();
                    part_delta.insert(this.row_to_cache_key(old_row)?, Change::Delete);
                }
                Record::Update { old_row, new_row } => {
                    let old_part_key = this.get_partition_key(old_row).into();
                    let new_part_key = this.get_partition_key(new_row).into();
                    let old_state_key = this.row_to_cache_key(old_row)?;
                    let new_state_key = this.row_to_cache_key(new_row)?;
                    if old_part_key == new_part_key && old_state_key == new_state_key {
                        // not a key-change update
                        let part_delta = deltas.entry(old_part_key).or_default();
                        part_delta.insert(old_state_key, Change::Insert(new_row.into_owned_row()));
                    } else if old_part_key == new_part_key {
                        // order-change update, split into delete + insert, will be merged after
                        // building changes
                        key_change_updated_pks.insert(this.get_input_pk(old_row));
                        let part_delta = deltas.entry(old_part_key).or_default();
                        part_delta.insert(old_state_key, Change::Delete);
                        part_delta.insert(new_state_key, Change::Insert(new_row.into_owned_row()));
                    } else {
                        // partition-change update, split into delete + insert
                        // NOTE(rc): Since we append partition key to logical pk, we can't merge the
                        // delete + insert back to update later.
                        // TODO: IMO this behavior is problematic. Deep discussion is needed.
                        let old_part_delta = deltas.entry(old_part_key).or_default();
                        old_part_delta.insert(old_state_key, Change::Delete);
                        let new_part_delta = deltas.entry(new_part_key).or_default();
                        new_part_delta
                            .insert(new_state_key, Change::Insert(new_row.into_owned_row()));
                    }
                }
            }
        }

        // `input pk` => `Record`
        let mut key_change_update_buffer = BTreeMap::new();
        let mut chunk_builder =
            StreamChunkBuilder::new(this.chunk_size, this.info.schema.data_types());

        // Build final changes partition by partition.
        for (part_key, delta) in deltas {
            vars.stats.cache_lookup += 1;
            if !vars.cached_partitions.contains(&part_key.0) {
                vars.stats.cache_miss += 1;
                vars.cached_partitions
                    .put(part_key.0.clone(), new_empty_partition_cache());
            }
            let mut cache = vars.cached_partitions.get_mut(&part_key).unwrap();
            let mut partition = OverPartition::new(
                &part_key,
                &mut cache,
                this.cache_policy,
                &this.calls,
                &this.order_key_data_types,
                &this.order_key_order_types,
                &this.order_key_indices,
                &this.input_pk_indices,
            );

            // Build changes for current partition.
            let (part_changes, accessed_range) =
                Self::build_changes_for_partition(this, &mut partition, delta).await?;

            for (key, record) in part_changes {
                // Build chunk and yield if needed.
                if !key_change_updated_pks.contains(&key.pk) {
                    if let Some(chunk) = chunk_builder.append_record(record.as_ref()) {
                        yield chunk;
                    }
                } else {
                    // For key-change updates, we should wait for both `Delete` and `Insert` changes
                    // and merge them together.
                    let pk = key.pk.clone();
                    let record = record.clone();
                    if let Some(existed) = key_change_update_buffer.remove(&key.pk) {
                        match (existed, record) {
                            (Record::Insert { new_row }, Record::Delete { old_row })
                            | (Record::Delete { old_row }, Record::Insert { new_row }) => {
                                // merge `Delete` and `Insert` into `Update`
                                if let Some(chunk) =
                                    chunk_builder.append_record(Record::Update { old_row, new_row })
                                {
                                    yield chunk;
                                }
                            }
                            _ => panic!("other cases should not exist"),
                        }
                    } else {
                        key_change_update_buffer.insert(pk, record);
                    }
                }

                // Apply the change record.
                partition.write_record(&mut this.state_table, key, record);
            }

            // Update recently accessed range for later shrinking cache.
            if !this.cache_policy.is_full()
                && let Some(accessed_range) = accessed_range
            {
                match vars.recently_accessed_ranges.entry(part_key) {
                    btree_map::Entry::Vacant(vacant) => {
                        vacant.insert(accessed_range);
                    }
                    btree_map::Entry::Occupied(mut occupied) => {
                        let recently_accessed_range = occupied.get_mut();
                        let min_start = accessed_range
                            .start()
                            .min(recently_accessed_range.start())
                            .clone();
                        let max_end = accessed_range
                            .end()
                            .max(recently_accessed_range.end())
                            .clone();
                        *recently_accessed_range = min_start..=max_end;
                    }
                }
            }
        }

        // Yield remaining changes to downstream.
        if let Some(chunk) = chunk_builder.take() {
            yield chunk;
        }
    }

    async fn build_changes_for_partition(
        this: &ExecutorInner<S>,
        partition: &mut OverPartition<'_, S>,
        delta: PartitionDelta,
    ) -> StreamExecutorResult<(
        BTreeMap<StateKey, Record<OwnedRow>>,
        Option<RangeInclusive<StateKey>>,
    )> {
        assert!(!delta.is_empty(), "if there's no delta, we won't be here");

        let mut part_changes = BTreeMap::new();

        // Find affected ranges, this also ensures that all rows in the affected ranges are loaded
        // into the cache.
        let (part_with_delta, affected_ranges) = partition
            .find_affected_ranges(&this.state_table, &delta)
            .await?;

        let snapshot = part_with_delta.snapshot();
        let delta = part_with_delta.delta();

        // Generate delete changes first, because deletes are skipped during iteration over
        // `part_with_delta` in the next step.
        for (key, change) in delta {
            if change.is_delete() {
                part_changes.insert(
                    key.as_normal_expect().clone(),
                    Record::Delete {
                        old_row: snapshot.get(key).unwrap().clone(),
                    },
                );
            }
        }

        let mut accessed_range: Option<RangeInclusive<StateKey>> = None;

        for (first_frame_start, first_curr_key, last_curr_key, last_frame_end) in affected_ranges {
            assert!(first_frame_start <= first_curr_key);
            assert!(first_curr_key <= last_curr_key);
            assert!(last_curr_key <= last_frame_end);
            assert!(first_frame_start.is_normal());
            assert!(first_curr_key.is_normal());
            assert!(last_curr_key.is_normal());
            assert!(last_frame_end.is_normal());

            if let Some(accessed_range) = accessed_range.as_mut() {
                let min_start = first_frame_start
                    .as_normal_expect()
                    .min(accessed_range.start())
                    .clone();
                let max_end = last_frame_end
                    .as_normal_expect()
                    .max(accessed_range.end())
                    .clone();
                *accessed_range = min_start..=max_end;
            } else {
                accessed_range = Some(
                    first_frame_start.as_normal_expect().clone()
                        ..=last_frame_end.as_normal_expect().clone(),
                );
            }

            let mut states =
                WindowStates::new(this.calls.iter().map(create_window_state).try_collect()?);

            // Populate window states with the affected range of rows.
            {
                let mut cursor = part_with_delta
                    .find(first_frame_start)
                    .expect("first frame start key must exist");
                while {
                    let (key, row) = cursor
                        .key_value()
                        .expect("cursor must be valid until `last_frame_end`");

                    for (call, state) in this.calls.iter().zip_eq_fast(states.iter_mut()) {
                        // TODO(rc): batch appending
                        state.append(
                            key.as_normal_expect().clone(),
                            row.project(call.args.val_indices())
                                .into_owned_row()
                                .as_inner()
                                .into(),
                        );
                    }
                    cursor.move_next();

                    key != last_frame_end
                } {}
            }

            // Slide to the first affected key. We can safely compare to `Some(first_curr_key)` here
            // because it must exist in the states, by the definition of affected range.
            while states.curr_key() != Some(first_curr_key.as_normal_expect()) {
                states.just_slide()?;
            }
            let mut curr_key_cursor = part_with_delta.find(first_curr_key).unwrap();
            assert_eq!(
                states.curr_key(),
                curr_key_cursor.key().map(CacheKey::as_normal_expect)
            );

            // Slide and generate changes.
            while {
                let (key, row) = curr_key_cursor
                    .key_value()
                    .expect("cursor must be valid until `last_curr_key`");
                let output = states.slide_no_evict_hint()?;
                let new_row = OwnedRow::new(
                    row.as_inner()
                        .iter()
                        .take(this.input_schema_len)
                        .cloned()
                        .chain(output)
                        .collect(),
                );

                match curr_key_cursor.position() {
                    PositionType::Ghost => unreachable!(),
                    PositionType::Snapshot | PositionType::DeltaUpdate => {
                        // update
                        let old_row = snapshot.get(key).unwrap().clone();
                        if old_row != new_row {
                            part_changes.insert(
                                key.as_normal_expect().clone(),
                                Record::Update { old_row, new_row },
                            );
                        }
                    }
                    PositionType::DeltaInsert => {
                        // insert
                        part_changes
                            .insert(key.as_normal_expect().clone(), Record::Insert { new_row });
                    }
                }

                curr_key_cursor.move_next();

                key != last_curr_key
            } {}
        }

        Ok((part_changes, accessed_range))
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn executor_inner(self) {
        let OverWindowExecutor {
            input,
            inner: mut this,
        } = self;

        let metrics_info = MetricsInfo::new(
            this.actor_ctx.streaming_metrics.clone(),
            this.state_table.table_id(),
            this.actor_ctx.id,
            "OverWindow",
        );

        let mut vars = ExecutionVars {
            cached_partitions: new_unbounded(this.watermark_epoch.clone(), metrics_info),
            recently_accessed_ranges: Default::default(),
            stats: Default::default(),
            _phantom: PhantomData::<S>,
        };

        let mut input = input.execute();
        let barrier = expect_first_barrier(&mut input).await?;
        this.state_table.init_epoch(barrier.epoch);
        vars.cached_partitions.update_epoch(barrier.epoch.curr);

        yield Message::Barrier(barrier);

        #[for_await]
        for msg in input {
            let msg = msg?;
            match msg {
                Message::Watermark(_) => {
                    // TODO(rc): ignore watermark for now, we need to think about watermark for
                    // window functions like `lead` carefully.
                    continue;
                }
                Message::Chunk(chunk) => {
                    #[for_await]
                    for chunk in Self::apply_chunk(&mut this, &mut vars, chunk) {
                        yield Message::Chunk(chunk?);
                    }
                }
                Message::Barrier(barrier) => {
                    this.state_table.commit(barrier.epoch).await?;
                    vars.cached_partitions.evict();

                    {
                        // update metrics
                        let actor_id_str = this.actor_ctx.id.to_string();
                        let fragment_id_str = this.actor_ctx.fragment_id.to_string();
                        let table_id_str = this.state_table.table_id().to_string();
                        this.metrics
                            .over_window_cached_entry_count
                            .with_label_values(&[&table_id_str, &actor_id_str, &fragment_id_str])
                            .set(vars.cached_partitions.len() as _);
                        this.metrics
                            .over_window_cache_lookup_count
                            .with_label_values(&[&table_id_str, &actor_id_str, &fragment_id_str])
                            .inc_by(std::mem::take(&mut vars.stats.cache_lookup));
                        this.metrics
                            .over_window_cache_miss_count
                            .with_label_values(&[&table_id_str, &actor_id_str, &fragment_id_str])
                            .inc_by(std::mem::take(&mut vars.stats.cache_miss));
                    }

                    if let Some(vnode_bitmap) = barrier.as_update_vnode_bitmap(this.actor_ctx.id) {
                        let (_, cache_may_stale) =
                            this.state_table.update_vnode_bitmap(vnode_bitmap);
                        if cache_may_stale {
                            vars.cached_partitions.clear();
                        }
                    }

                    if !this.cache_policy.is_full() {
                        for (part_key, recently_accessed_range) in
                            std::mem::take(&mut vars.recently_accessed_ranges)
                        {
                            if let Some(mut range_cache) =
                                vars.cached_partitions.get_mut(&part_key.0)
                            {
                                shrink_partition_cache(
                                    &part_key.0,
                                    &mut range_cache,
                                    this.cache_policy,
                                    recently_accessed_range,
                                );
                            }
                        }
                    }

                    vars.cached_partitions.update_epoch(barrier.epoch.curr);

                    yield Message::Barrier(barrier);
                }
            }
        }
    }
}

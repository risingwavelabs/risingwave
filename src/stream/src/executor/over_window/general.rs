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

use std::collections::{BTreeMap, HashSet};
use std::marker::PhantomData;
use std::ops::Bound;

use futures::StreamExt;
use futures_async_stream::{for_await, try_stream};
use itertools::Itertools;
use risingwave_common::array::stream_record::Record;
use risingwave_common::array::{RowRef, StreamChunk};
use risingwave_common::catalog::Field;
use risingwave_common::row::{OwnedRow, Row, RowExt};
use risingwave_common::types::DefaultOrdered;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_common::util::memcmp_encoding::{self, MemcmpEncoded};
use risingwave_common::util::sort_util::OrderType;
use risingwave_expr::function::window::{FrameBounds, WindowFuncCall};
use risingwave_storage::store::PrefetchOptions;
use risingwave_storage::StateStore;

use self::private::Partition;
use super::diff_btree_map::{Change, DiffBTreeMap};
use super::state::{create_window_state, StateKey};
use crate::cache::{new_unbounded, ManagedLruCache};
use crate::common::metrics::MetricsInfo;
use crate::executor::aggregation::ChunkBuilder;
use crate::executor::over_window::diff_btree_map::PositionType;
use crate::executor::over_window::window_states::WindowStates;
use crate::executor::test_utils::prelude::StateTable;
use crate::executor::{
    expect_first_barrier, ActorContextRef, BoxedExecutor, Executor, ExecutorInfo, Message,
    PkIndices, StreamExecutorError, StreamExecutorResult,
};
use crate::task::AtomicU64Ref;

mod private {
    use std::collections::BTreeMap;

    use risingwave_common::estimate_size::{EstimateSize, KvSize};
    use risingwave_common::row::OwnedRow;

    use crate::executor::over_window::state::StateKey;

    pub(super) struct Partition {
        /// Fully synced table cache for the partition. `StateKey (order key, input pk)` -> table
        /// row.
        cache: BTreeMap<StateKey, OwnedRow>,
        heap_size: KvSize,
    }

    impl Partition {
        pub fn new(cache: BTreeMap<StateKey, OwnedRow>) -> Self {
            let heap_size = cache.iter().fold(KvSize::new(), |mut x, (k, v)| {
                x.add(k, v);
                x
            });
            Self { cache, heap_size }
        }

        pub fn cache(&self) -> &BTreeMap<StateKey, OwnedRow> {
            &self.cache
        }

        pub fn insert(&mut self, key: StateKey, row: OwnedRow) {
            let key_heap_size = key.estimated_heap_size();
            self.heap_size.add_size(key_heap_size);
            self.heap_size.add_val(&row);
            if let Some(old_row) = self.cache.insert(key, row) {
                self.heap_size.sub_size(key_heap_size);
                self.heap_size.sub_val(&old_row);
            }
        }

        pub fn remove(&mut self, key: &StateKey) {
            if let Some(row) = self.cache.remove(key) {
                self.heap_size.sub(key, &row);
            }
        }
    }

    impl EstimateSize for Partition {
        fn estimated_heap_size(&self) -> usize {
            self.heap_size.size()
        }
    }
}

/// Changes happened in one partition in the chunk. `StateKey (order key, input pk)` => `Change`.
type Diff = BTreeMap<StateKey, Change<OwnedRow>>;

/// `partition key` => `Partition`.
type PartitionCache = ManagedLruCache<OwnedRow, Partition>;

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
    order_key_order_types: Vec<OrderType>,
    input_pk_indices: Vec<usize>,
    input_schema_len: usize,

    state_table: StateTable<S>,
    watermark_epoch: AtomicU64Ref,

    /// The maximum size of the chunk produced by executor at a time.
    chunk_size: usize,
}

struct ExecutionVars<S: StateStore> {
    partitions: PartitionCache,
    _phantom: PhantomData<S>,
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

    fn row_to_state_key(&self, full_row: impl Row + Copy) -> StreamExecutorResult<StateKey> {
        Ok(StateKey {
            order_key: self.encode_order_key(full_row)?,
            pk: self.get_input_pk(full_row).into(),
        })
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

    pub chunk_size: usize,
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
                order_key_order_types: args.order_key_order_types,
                input_pk_indices: input_info.pk_indices,
                input_schema_len: input_info.schema.len(),
                state_table: args.state_table,
                watermark_epoch: args.watermark_epoch,
                chunk_size: args.chunk_size,
            },
        }
    }

    async fn ensure_partition_in_cache(
        this: &mut ExecutorInner<S>,
        cache: &mut PartitionCache,
        partition_key: &OwnedRow,
    ) -> StreamExecutorResult<()> {
        if cache.contains(partition_key) {
            return Ok(());
        }

        let mut cache_for_partition = BTreeMap::new();
        let table_iter = this
            .state_table
            .iter_with_pk_prefix(partition_key, PrefetchOptions::new_for_exhaust_iter())
            .await?;

        #[for_await]
        for row in table_iter {
            let row: OwnedRow = row?;
            cache_for_partition.insert(this.row_to_state_key(&row)?, row);
        }

        cache.put(partition_key.clone(), Partition::new(cache_for_partition));
        Ok(())
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
        for record in chunk.records() {
            match record {
                Record::Insert { new_row } => {
                    let pk = DefaultOrdered(this.get_input_pk(new_row));
                    if let Some(prev_change) = changes_merged.get_mut(&pk) {
                        match prev_change {
                            Record::Delete { old_row } => {
                                *prev_change = Record::Update {
                                    old_row: *old_row,
                                    new_row,
                                };
                            }
                            _ => panic!("inconsistent changes in input chunk"),
                        }
                    } else {
                        changes_merged.insert(pk, record);
                    }
                }
                Record::Delete { old_row } => {
                    let pk = DefaultOrdered(this.get_input_pk(old_row));
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
                        changes_merged.insert(pk, record);
                    }
                }
                Record::Update { old_row, new_row } => {
                    let pk = DefaultOrdered(this.get_input_pk(old_row));
                    if let Some(prev_change) = changes_merged.get_mut(&pk) {
                        match prev_change {
                            Record::Insert { .. } => {
                                *prev_change = Record::Insert { new_row };
                            }
                            Record::Update {
                                old_row: real_old_row,
                                ..
                            } => {
                                *prev_change = Record::Update {
                                    old_row: *real_old_row,
                                    new_row,
                                };
                            }
                            _ => panic!("inconsistent changes in input chunk"),
                        }
                    } else {
                        changes_merged.insert(pk, record);
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
        // `partition key` => `Diff`.
        let mut diffs: BTreeMap<DefaultOrdered<OwnedRow>, Diff> = BTreeMap::new();
        // `input pk` of update records of which the `partition key` or `order key` is changed.
        let mut key_change_updated_pks = HashSet::new();

        // Collect changes for each partition.
        for record in Self::merge_changes_in_chunk(this, &chunk) {
            match record {
                Record::Insert { new_row } => {
                    let part_key = this.get_partition_key(new_row).into();
                    let part_diff = diffs.entry(part_key).or_insert(Diff::new());
                    part_diff.insert(
                        this.row_to_state_key(new_row)?,
                        Change::Insert(new_row.into_owned_row()),
                    );
                }
                Record::Delete { old_row } => {
                    let part_key = this.get_partition_key(old_row).into();
                    let part_diff = diffs.entry(part_key).or_insert(Diff::new());
                    part_diff.insert(this.row_to_state_key(old_row)?, Change::Delete);
                }
                Record::Update { old_row, new_row } => {
                    let old_part_key = this.get_partition_key(old_row).into();
                    let new_part_key = this.get_partition_key(new_row).into();
                    let old_state_key = this.row_to_state_key(old_row)?;
                    let new_state_key = this.row_to_state_key(new_row)?;
                    if old_part_key == new_part_key && old_state_key == new_state_key {
                        // not a key-change update
                        let part_diff = diffs.entry(old_part_key).or_insert(Diff::new());
                        part_diff.insert(old_state_key, Change::Update(new_row.into_owned_row()));
                    } else if old_part_key == new_part_key {
                        // order-change update
                        key_change_updated_pks.insert(this.get_input_pk(old_row));
                        let part_diff = diffs.entry(old_part_key).or_insert(Diff::new());
                        // split into delete + insert, will be merged after building changes
                        part_diff.insert(old_state_key, Change::Delete);
                        part_diff.insert(new_state_key, Change::Insert(new_row.into_owned_row()));
                    } else {
                        // partition-change update
                        key_change_updated_pks.insert(this.get_input_pk(old_row));
                        // split into delete + insert, will be merged after building changes
                        let old_part_diff = diffs.entry(old_part_key).or_insert(Diff::new());
                        old_part_diff.insert(old_state_key, Change::Delete);
                        let new_part_diff = diffs.entry(new_part_key).or_insert(Diff::new());
                        new_part_diff
                            .insert(new_state_key, Change::Insert(new_row.into_owned_row()));
                    }
                }
            }
        }

        // `input pk` => `Record`
        let mut key_change_update_buffer = BTreeMap::new();
        let mut chunk_builder = ChunkBuilder::new(this.chunk_size, &this.info.schema.data_types());

        // Build final changes partition by partition.
        for (part_key, diff) in diffs {
            Self::ensure_partition_in_cache(this, &mut vars.partitions, &part_key).await?;
            let mut partition = vars.partitions.get_mut(&part_key).unwrap();

            // Build changes for current partition.
            let part_changes = Self::build_changes_for_partition(
                this,
                DiffBTreeMap::new(partition.cache(), diff),
            )?;

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

                // Update state table and partition cache.
                this.state_table.write_record(record.as_ref());
                match record {
                    Record::Insert { new_row } | Record::Update { new_row, .. } => {
                        // If `Update`, the update is not a key-change update, so it's safe to just
                        // replace the existing item in the cache.
                        partition.insert(key, new_row);
                    }
                    Record::Delete { .. } => {
                        partition.remove(&key);
                    }
                }
            }
        }

        // Yield remaining changes to downstream.
        if let Some(chunk) = chunk_builder.take() {
            yield chunk;
        }
    }

    fn build_changes_for_partition(
        this: &ExecutorInner<S>,
        part_with_diff: DiffBTreeMap<'_, StateKey, OwnedRow>,
    ) -> StreamExecutorResult<BTreeMap<StateKey, Record<OwnedRow>>> {
        let snapshot = part_with_diff.snapshot();
        let diff = part_with_diff.diff();
        assert!(!diff.is_empty(), "if there's no diff, we won't be here");

        let mut part_changes = BTreeMap::new();

        // Generate delete changes first, because deletes are skipped during iteration over
        // `part_with_diff` in the next step.
        for (key, change) in diff {
            if change.is_delete() {
                part_changes.insert(
                    key.clone(),
                    Record::Delete {
                        old_row: snapshot.get(key).unwrap().clone(),
                    },
                );
            }
        }

        for (first_frame_start, first_curr_key, last_curr_key, last_frame_end) in
            find_affected_ranges(&this.calls, &part_with_diff)
        {
            assert!(first_frame_start <= first_curr_key);
            assert!(first_curr_key <= last_curr_key);
            assert!(last_curr_key <= last_frame_end);

            let mut states =
                WindowStates::new(this.calls.iter().map(create_window_state).try_collect()?);

            // Populate window states with the affected range of rows.
            {
                let mut cursor = part_with_diff
                    .find(&first_frame_start)
                    .expect("first frame start key must exist");
                while {
                    let (key, row) = cursor
                        .key_value()
                        .expect("cursor must be valid until `last_frame_end`");

                    for (call, state) in this.calls.iter().zip_eq_fast(states.iter_mut()) {
                        // TODO(rc): batch appending
                        state.append(
                            key.clone(),
                            row.project(call.args.val_indices())
                                .into_owned_row()
                                .as_inner()
                                .into(),
                        );
                    }
                    cursor.move_next();

                    key != &last_frame_end
                } {}
            }

            // Slide to the first affected key. We can safely compare to `Some(first_curr_key)` here
            // because it must exist in the states, by the definition of affected range.
            while states.curr_key() != Some(&first_curr_key) {
                states.just_slide_forward();
            }
            let mut curr_key_cursor = part_with_diff.find(&first_curr_key).unwrap();
            assert_eq!(states.curr_key(), curr_key_cursor.key());

            // Slide and generate changes.
            while {
                let (key, row) = curr_key_cursor
                    .key_value()
                    .expect("cursor must be valid until `last_curr_key`");
                let output = states.curr_output()?;
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
                    PositionType::Snapshot | PositionType::DiffUpdate => {
                        // update
                        let old_row = snapshot.get(key).unwrap().clone();
                        if old_row != new_row {
                            part_changes.insert(key.clone(), Record::Update { old_row, new_row });
                        }
                    }
                    PositionType::DiffInsert => {
                        // insert
                        part_changes.insert(key.clone(), Record::Insert { new_row });
                    }
                }

                states.just_slide_forward();
                curr_key_cursor.move_next();

                key != &last_curr_key
            } {}
        }

        Ok(part_changes)
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
            partitions: new_unbounded(this.watermark_epoch.clone(), metrics_info),
            _phantom: PhantomData::<S>,
        };

        let mut input = input.execute();
        let barrier = expect_first_barrier(&mut input).await?;
        this.state_table.init_epoch(barrier.epoch);
        vars.partitions.update_epoch(barrier.epoch.curr);

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
                    vars.partitions.evict();

                    if let Some(vnode_bitmap) = barrier.as_update_vnode_bitmap(this.actor_ctx.id) {
                        let (_, cache_may_stale) =
                            this.state_table.update_vnode_bitmap(vnode_bitmap);
                        if cache_may_stale {
                            vars.partitions.clear();
                        }
                    }

                    vars.partitions.update_epoch(barrier.epoch.curr);

                    yield Message::Barrier(barrier);
                }
            }
        }
    }
}

/// Find all affected ranges in the given partition with diff.
///
/// # Returns
///
/// `Vec<(first_frame_start, first_curr_key, last_curr_key, last_frame_end_incl)>`
///
/// Each affected range is a union of many small window frames affected by some adajcent
/// keys in the diff.
///
/// Example:
/// - frame 1: `rows between 2 preceding and current row`
/// - frame 2: `rows between 1 preceding and 2 following`
/// - partition: `[1, 2, 4, 5, 7, 8, 9, 10, 11, 12, 14]`
/// - diff: `[3, 4, 15]`
/// - affected ranges: `[(1, 1, 7, 9), (10, 12, 15, 15)]`
///
/// TODO(rc):
/// Note that, since we assume input chunks have data locality on order key columns, we now only
/// calculate one single affected range. So the affected ranges in the above example will be
/// `(1, 1, 15, 15)`. Later we may optimize this.
fn find_affected_ranges(
    calls: &[WindowFuncCall],
    part_with_diff: &DiffBTreeMap<'_, StateKey, OwnedRow>,
) -> Vec<(StateKey, StateKey, StateKey, StateKey)> {
    let diff = part_with_diff.diff();

    if part_with_diff.first_key().is_none() {
        // all keys are deleted in the diff
        return vec![];
    }

    if part_with_diff.snapshot().is_empty() {
        // all existing keys are inserted in the diff
        return vec![(
            diff.first_key_value().unwrap().0.clone(),
            diff.first_key_value().unwrap().0.clone(),
            diff.last_key_value().unwrap().0.clone(),
            diff.last_key_value().unwrap().0.clone(),
        )];
    }

    let (first_frame_start, first_curr_key) = {
        let first_key = part_with_diff.first_key().unwrap();
        if calls
            .iter()
            .any(|call| call.frame.bounds.end_is_unbounded())
        {
            // If the frame end is unbounded, the frame corresponding to the first key is always
            // affected.
            (first_key.clone(), first_key.clone())
        } else {
            let (a, b) = calls
                .iter()
                .map(|call| match &call.frame.bounds {
                    FrameBounds::Rows(start, end) => {
                        let mut ss_cursor = part_with_diff
                            .lower_bound(Bound::Included(diff.first_key_value().unwrap().0));
                        let n_following_rows = end.to_offset().unwrap().max(0) as usize;
                        for _ in 0..n_following_rows {
                            if ss_cursor.key().is_some() {
                                ss_cursor.move_prev();
                            }
                        }
                        let first_curr_key = ss_cursor.key().unwrap_or(first_key);
                        let first_frame_start = if let Some(offset) = start.to_offset() {
                            let n_preceding_rows = offset.min(0).unsigned_abs();
                            for _ in 0..n_preceding_rows {
                                if ss_cursor.key().is_some() {
                                    ss_cursor.move_prev();
                                }
                            }
                            ss_cursor.key().unwrap_or(first_key)
                        } else {
                            // The frame start is unbounded, so the first affected frame starts
                            // from the beginning.
                            first_key
                        };
                        (first_frame_start, first_curr_key)
                    }
                })
                .reduce(|(x1, y1), (x2, y2)| (x1.min(x2), y1.min(y2)))
                .expect("# of window function calls > 0");
            (a.clone(), b.clone())
        }
    };

    let (last_curr_key, last_frame_end) = {
        let last_key = part_with_diff.last_key().unwrap();
        if calls
            .iter()
            .any(|call| call.frame.bounds.start_is_unbounded())
        {
            // If the frame start is unbounded, the frame corresponding to the last key is
            // always affected.
            (last_key.clone(), last_key.clone())
        } else {
            let (a, b) = calls
                .iter()
                .map(|call| match &call.frame.bounds {
                    FrameBounds::Rows(start, end) => {
                        let mut ss_cursor = part_with_diff
                            .upper_bound(Bound::Included(diff.last_key_value().unwrap().0));
                        let n_preceding_rows = start.to_offset().unwrap().min(0).unsigned_abs();
                        for _ in 0..n_preceding_rows {
                            if ss_cursor.key().is_some() {
                                ss_cursor.move_next();
                            }
                        }
                        let last_curr_key = ss_cursor.key().unwrap_or(last_key);
                        let last_frame_end = if let Some(offset) = end.to_offset() {
                            let n_following_rows = offset.max(0) as usize;
                            for _ in 0..n_following_rows {
                                if ss_cursor.key().is_some() {
                                    ss_cursor.move_next();
                                }
                            }
                            ss_cursor.key().unwrap_or(last_key)
                        } else {
                            // The frame end is unbounded, so the last affected frame ends at
                            // the end.
                            last_key
                        };
                        (last_curr_key, last_frame_end)
                    }
                })
                .reduce(|(x1, y1), (x2, y2)| (x1.max(x2), y1.max(y2)))
                .expect("# of window function calls > 0");
            (a.clone(), b.clone())
        }
    };

    vec![(
        first_frame_start,
        first_curr_key,
        last_curr_key,
        last_frame_end,
    )]
}

#[cfg(test)]
mod tests {
    use risingwave_common::types::DataType;
    use risingwave_expr::agg::{AggArgs, AggKind};
    use risingwave_expr::function::window::{Frame, FrameBound, WindowFuncKind};

    use super::*;

    #[test]
    fn test_find_affected_ranges() {
        fn create_call(frame: Frame) -> WindowFuncCall {
            WindowFuncCall {
                kind: WindowFuncKind::Aggregate(AggKind::Sum),
                args: AggArgs::Unary(DataType::Int32, 0),
                return_type: DataType::Int32,
                frame,
            }
        }

        macro_rules! create_snapshot {
            ($( $pk:literal ),* $(,)?) => {
                {
                    #[allow(unused_mut)]
                    let mut snapshot = BTreeMap::new();
                    $(
                        snapshot.insert(
                            StateKey {
                                // order key doesn't matter here
                                order_key: vec![].into(),
                                pk: OwnedRow::new(vec![Some($pk.into())]).into(),
                            },
                            // value row doesn't matter here
                            OwnedRow::empty(),
                        );
                    )*
                    snapshot
                }
            };
        }

        macro_rules! create_change {
            (Delete) => {
                Change::Delete
            };
            ($other:ident) => {
                Change::$other(OwnedRow::empty())
            };
        }

        macro_rules! create_diff {
            ($(( $pk:literal, $change:ident )),* $(,)?) => {
                {
                    #[allow(unused_mut)]
                    let mut diff = BTreeMap::new();
                    $(
                        diff.insert(
                            StateKey {
                                // order key doesn't matter here
                                order_key: vec![].into(),
                                pk: OwnedRow::new(vec![Some($pk.into())]).into(),
                            },
                            // value row doesn't matter here
                            create_change!( $change ),
                        );
                    )*
                    diff
                }
            };
        }

        {
            // test all empty
            let snapshot = create_snapshot!();
            let diff = create_diff!();
            let part_with_diff = DiffBTreeMap::new(&snapshot, diff);
            let calls = vec![create_call(Frame::rows(
                FrameBound::Preceding(2),
                FrameBound::Preceding(1),
            ))];
            assert!(find_affected_ranges(&calls, &part_with_diff).is_empty());
        }

        {
            // test insert diff only
            let snapshot = create_snapshot!();
            let diff = create_diff!((1, Insert), (2, Insert), (3, Insert));
            let part_with_diff = DiffBTreeMap::new(&snapshot, diff);
            let calls = vec![create_call(Frame::rows(
                FrameBound::Preceding(2),
                FrameBound::Preceding(1),
            ))];
            let affected_ranges = find_affected_ranges(&calls, &part_with_diff);
            assert_eq!(affected_ranges.len(), 1);
            let (first_frame_start, first_curr_key, last_curr_key, last_frame_end) =
                affected_ranges.into_iter().next().unwrap();
            assert_eq!(first_frame_start.pk.0, OwnedRow::new(vec![Some(1.into())]));
            assert_eq!(first_curr_key.pk.0, OwnedRow::new(vec![Some(1.into())]));
            assert_eq!(last_curr_key.pk.0, OwnedRow::new(vec![Some(3.into())]));
            assert_eq!(last_frame_end.pk.0, OwnedRow::new(vec![Some(3.into())]));
        }

        {
            // test simple
            let snapshot = create_snapshot!(1, 2, 3, 4, 5, 6);
            let diff = create_diff!((2, Update), (3, Delete));
            let part_with_diff = DiffBTreeMap::new(&snapshot, diff);

            {
                let calls = vec![create_call(Frame::rows(
                    FrameBound::Preceding(2),
                    FrameBound::Preceding(1),
                ))];
                let (first_frame_start, first_curr_key, last_curr_key, last_frame_end) =
                    find_affected_ranges(&calls, &part_with_diff)
                        .into_iter()
                        .next()
                        .unwrap();
                assert_eq!(first_frame_start.pk.0, OwnedRow::new(vec![Some(1.into())]));
                assert_eq!(first_curr_key.pk.0, OwnedRow::new(vec![Some(2.into())]));
                assert_eq!(last_curr_key.pk.0, OwnedRow::new(vec![Some(5.into())]));
                assert_eq!(last_frame_end.pk.0, OwnedRow::new(vec![Some(5.into())]));
            }

            {
                let calls = vec![create_call(Frame::rows(
                    FrameBound::Preceding(1),
                    FrameBound::Following(2),
                ))];
                let (first_frame_start, first_curr_key, last_curr_key, last_frame_end) =
                    find_affected_ranges(&calls, &part_with_diff)
                        .into_iter()
                        .next()
                        .unwrap();
                assert_eq!(first_frame_start.pk.0, OwnedRow::new(vec![Some(1.into())]));
                assert_eq!(first_curr_key.pk.0, OwnedRow::new(vec![Some(1.into())]));
                assert_eq!(last_curr_key.pk.0, OwnedRow::new(vec![Some(4.into())]));
                assert_eq!(last_frame_end.pk.0, OwnedRow::new(vec![Some(6.into())]));
            }

            {
                let calls = vec![create_call(Frame::rows(
                    FrameBound::CurrentRow,
                    FrameBound::Following(2),
                ))];
                let (first_frame_start, first_curr_key, last_curr_key, last_frame_end) =
                    find_affected_ranges(&calls, &part_with_diff)
                        .into_iter()
                        .next()
                        .unwrap();
                assert_eq!(first_frame_start.pk.0, OwnedRow::new(vec![Some(1.into())]));
                assert_eq!(first_curr_key.pk.0, OwnedRow::new(vec![Some(1.into())]));
                assert_eq!(last_curr_key.pk.0, OwnedRow::new(vec![Some(2.into())]));
                assert_eq!(last_frame_end.pk.0, OwnedRow::new(vec![Some(5.into())]));
            }
        }
    }
}

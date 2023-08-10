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
use futures_async_stream::try_stream;
use itertools::Itertools;
use risingwave_common::array::stream_record::Record;
use risingwave_common::array::{RowRef, StreamChunk};
use risingwave_common::catalog::Field;
use risingwave_common::row::{OwnedRow, Row, RowExt};
use risingwave_common::types::{DataType, DefaultOrdered};
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_common::util::memcmp_encoding::{self, MemcmpEncoded};
use risingwave_common::util::sort_util::OrderType;
use risingwave_expr::window_function::{
    create_window_state, FrameBounds, StateKey, WindowFuncCall, WindowStates,
};
use risingwave_storage::StateStore;

use self::private::{new_empty_partition_cache, OverPartition};
use super::delta_btree_map::{Change, DeltaBTreeMap};
use super::estimated_btree_map::EstimatedBTreeMap;
use super::sentinel::KeyWithSentinel;
use crate::cache::{new_unbounded, ManagedLruCache};
use crate::common::metrics::MetricsInfo;
use crate::executor::aggregation::ChunkBuilder;
use crate::executor::over_window::delta_btree_map::PositionType;
use crate::executor::test_utils::prelude::StateTable;
use crate::executor::{
    expect_first_barrier, ActorContextRef, BoxedExecutor, Executor, ExecutorInfo, Message,
    PkIndices, StreamExecutorError, StreamExecutorResult,
};
use crate::task::AtomicU64Ref;

type CacheKey = KeyWithSentinel<StateKey>;

/// Range cache for one over window partition.
type PartitionCache = EstimatedBTreeMap<CacheKey, OwnedRow>;

/// Changes happened in one over window partition.
type PartitionDelta = BTreeMap<CacheKey, Change<OwnedRow>>;

mod private {
    use std::marker::PhantomData;
    use std::ops::{Bound, RangeInclusive};

    use futures::StreamExt;
    use futures_async_stream::for_await;
    use risingwave_common::array::stream_record::Record;
    use risingwave_common::hash::VnodeBitmapExt;
    use risingwave_common::row::{OwnedRow, Row, RowExt};
    use risingwave_common::types::DataType;
    use risingwave_common::util::memcmp_encoding;
    use risingwave_common::util::sort_util::OrderType;
    use risingwave_expr::window_function::{StateKey, WindowFuncCall};
    use risingwave_storage::store::PrefetchOptions;
    use risingwave_storage::table::merge_sort::merge_sort;
    use risingwave_storage::StateStore;

    use super::{CacheKey, PartitionCache, PartitionDelta};
    use crate::executor::over_window::delta_btree_map::DeltaBTreeMap;
    use crate::executor::test_utils::prelude::StateTable;
    use crate::executor::StreamExecutorResult;

    pub(super) fn new_empty_partition_cache() -> PartitionCache {
        let mut cache = PartitionCache::new();
        cache.insert(CacheKey::Smallest, OwnedRow::empty());
        cache.insert(CacheKey::Largest, OwnedRow::empty());
        cache
    }

    /// A wrapper of [`PartitionCache`] that provides helper methods to manipulate the cache.
    /// By putting this type inside `private` module, we can avoid misuse of the internal fields and
    /// methods.
    pub(super) struct OverPartition<'a, S: StateStore> {
        this_partition_key: &'a OwnedRow,
        range_cache: &'a mut PartitionCache,
        order_key_data_types: &'a [DataType],
        order_key_order_types: &'a [OrderType],
        _phantom: PhantomData<S>,
    }

    const MAGIC_BATCH_SIZE: usize = 1024;

    impl<'a, S: StateStore> OverPartition<'a, S> {
        pub fn new(
            this_partition_key: &'a OwnedRow,
            cache: &'a mut PartitionCache,
            order_key_data_types: &'a [DataType],
            order_key_order_types: &'a [OrderType],
        ) -> Self {
            Self {
                this_partition_key,
                range_cache: cache,
                order_key_data_types,
                order_key_order_types,
                _phantom: PhantomData,
            }
        }

        /// Get the number of cached entries ignoring sentinels.
        pub fn cache_real_len(&self) -> usize {
            let len = self.range_cache.inner().len();
            if len <= 1 {
                debug_assert!(self
                    .range_cache
                    .inner()
                    .first_key_value()
                    .map(|(k, _)| k.is_normal())
                    .unwrap_or(true));
                return len;
            }
            // len >= 2
            let cache_inner = self.range_cache.inner();
            let sentinels = [
                cache_inner.first_key_value().unwrap().0.is_sentinel(),
                cache_inner.last_key_value().unwrap().0.is_sentinel(),
            ];
            len - sentinels.into_iter().filter(|x| *x).count()
        }

        /// Write a change record to state table and cache.
        ///
        /// # Safety
        ///
        /// - Insert/Update is not safe if the key exceeds the range cache and there's a sentinel
        ///   key. The caller must ensure that there's no key in the table that is between the key
        ///   to insert and the side-most cached key besides the sentinel.
        /// - Update must not cross different partitions.
        pub unsafe fn write_record_unchecked(
            &mut self,
            table: &mut StateTable<S>,
            key: StateKey,
            record: Record<OwnedRow>,
        ) {
            table.write_record(record.as_ref());
            match record {
                Record::Insert { new_row } | Record::Update { new_row, .. } => {
                    self.range_cache.insert(CacheKey::from(key), new_row);
                }
                Record::Delete { .. } => {
                    self.range_cache.remove(&CacheKey::from(key));
                }
            }
        }

        pub async fn find_affected_ranges<'l>(
            &'l mut self,
            table: &'_ StateTable<S>,
            calls: &'_ [WindowFuncCall],
            delta: &'l PartitionDelta,
        ) -> StreamExecutorResult<(
            DeltaBTreeMap<'l, CacheKey, OwnedRow>,
            Vec<(CacheKey, CacheKey, CacheKey, CacheKey)>,
        )> {
            // ensure the cache covers all delta (if possible)
            let delta_first = delta.first_key_value().unwrap().0.as_normal_expect();
            let delta_last = delta.last_key_value().unwrap().0.as_normal_expect();
            self.extend_cache_by_range(table, delta_first..=delta_last)
                .await?;

            let mut ranges = vec![];
            while {
                let tmp_ranges = super::find_affected_ranges(
                    calls,
                    DeltaBTreeMap::new(self.range_cache.inner(), &delta),
                );

                let need_retry = if tmp_ranges.is_empty() {
                    false
                } else {
                    // if any range touches the sentinel, we need to extend the cache and retry

                    let left_reached_sentinel = tmp_ranges.first().unwrap().0.is_sentinel();
                    let right_reached_sentinel = tmp_ranges.last().unwrap().3.is_sentinel();

                    if left_reached_sentinel || right_reached_sentinel {
                        if left_reached_sentinel {
                            // TODO(rc): should count cache miss for this, and also the below
                            tracing::info!("partition cache left extension triggered");
                            self.extend_cache_leftward_by_n(table).await?;
                        }
                        if right_reached_sentinel {
                            tracing::info!("partition cache right extension triggered");
                            self.extend_cache_rightward_by_n(table).await?;
                        }
                        true
                    } else {
                        ranges.extend(
                            tmp_ranges
                                .into_iter()
                                .map(|(a, b, c, d)| (a.clone(), b.clone(), c.clone(), d.clone())),
                        );
                        false
                    }
                };

                need_retry
            } {
                tracing::info!("partition cache extended");
            }

            ranges.shrink_to_fit();
            Ok((DeltaBTreeMap::new(self.range_cache.inner(), delta), ranges))
        }

        async fn extend_cache_by_range(
            &mut self,
            table: &StateTable<S>,
            range: RangeInclusive<&StateKey>,
        ) -> StreamExecutorResult<()> {
            // TODO(): load the range of entries and some more into the cache, ensuring there's real
            // entries in cache

            // TODO(): assert must have at least one entry, or, no entry at all

            // TODO(rc): prefetch something before the start of the range;
            self.extend_cache_leftward_by_n(table).await?;

            // prefetch something after the end of the range
            self.extend_cache_rightward_by_n(table).await
        }

        async fn extend_cache_leftward_by_n(
            &mut self,
            table: &StateTable<S>,
        ) -> StreamExecutorResult<()> {
            if self.range_cache.inner().len() <= 1 {
                // empty or only one entry in the table (no sentinel)
                return Ok(());
            }

            let (left_first, left_second) = {
                let mut iter = self.range_cache.inner().iter();
                let first = iter.next().unwrap().0;
                let second = iter.next().unwrap().0;
                (first, second)
            };

            match (left_first, left_second) {
                (CacheKey::Normal(_), _) => {
                    // no sentinel here, meaning we've already cached all of the 2 entries
                    Ok(())
                }
                (CacheKey::Smallest, CacheKey::Normal(key)) => {
                    // For leftward extension, we now must iterate the table in order from the
                    // beginning of this partition and fill only the last n rows
                    // to the cache. This is called `rotate`. TODO(rc): WE NEED
                    // STATE TABLE REVERSE ITERATOR!!
                    let rotate = true;

                    let key = key.clone();
                    self.extend_cache_take_n(
                        table,
                        Bound::Unbounded,
                        Bound::Excluded(&key),
                        MAGIC_BATCH_SIZE,
                        rotate,
                    )
                    .await
                }
                (CacheKey::Smallest, CacheKey::Largest) => {
                    // TODO()
                    panic!("must call `extend_cache_from_table_with_range` before");
                }
                _ => {
                    unreachable!();
                }
            }
        }

        async fn extend_cache_rightward_by_n(
            &mut self,
            table: &StateTable<S>,
        ) -> StreamExecutorResult<()> {
            if self.range_cache.inner().len() <= 1 {
                // empty or only one entry in the table (no sentinel)
                return Ok(());
            }

            // note the order of keys here
            let (right_second, right_first) = {
                let mut iter = self.range_cache.inner().iter();
                let first = iter.next_back().unwrap().0;
                let second = iter.next_back().unwrap().0;
                (second, first)
            };

            match (right_second, right_first) {
                (_, CacheKey::Normal(_)) => {
                    // no sentinel here, meaning we've already cached all of the 2 entries
                    Ok(())
                }
                (CacheKey::Normal(key), CacheKey::Largest) => {
                    let key = key.clone();
                    self.extend_cache_take_n(
                        table,
                        Bound::Excluded(&key),
                        Bound::Unbounded,
                        MAGIC_BATCH_SIZE,
                        false,
                    )
                    .await
                }
                (CacheKey::Smallest, CacheKey::Largest) => {
                    // TODO()
                    panic!("must call `extend_cache_from_table_with_range` before");
                }
                _ => {
                    unreachable!();
                }
            }
        }

        async fn extend_cache_take_n(
            &mut self,
            table: &StateTable<S>,
            start: Bound<&StateKey>,
            end: Bound<&StateKey>,
            n: usize,
            rotate: bool,
        ) -> StreamExecutorResult<()> {
            debug_assert!(
                table.value_indices().is_none(),
                "we must have full row as value here"
            );

            let mut to_extend = Vec::with_capacity(n.min(MAGIC_BATCH_SIZE));

            {
                let range = (self.convert_bound(start)?, self.convert_bound(end)?);
                let streams: Vec<_> = futures::future::try_join_all(
                    table.vnode_bitmap().iter_vnodes().map(|vnode| {
                        table.iter_key_and_val_with_pk_range(
                            &range,
                            vnode,
                            PrefetchOptions {
                                exhaust_iter: n == usize::MAX || rotate,
                            },
                        )
                    }),
                )
                .await?
                .into_iter()
                .map(Box::pin)
                .collect();

                #[for_await]
                for kv in merge_sort(streams).take(n) {
                    let (_, row): (_, OwnedRow) = kv?;
                    // TODO(): fill the cache
                }
            }

            for (key, row) in to_extend {
                self.range_cache.insert(key, row);
            }

            // TODO(): handle the sentinel

            Ok(())
        }

        fn convert_bound<'s, 'k>(
            &'s self,
            bound: Bound<&'k StateKey>,
        ) -> StreamExecutorResult<Bound<impl Row + 'k>>
        where
            's: 'k,
        {
            Ok(match bound {
                Bound::Included(key) => Bound::Included(self.state_key_to_table_pk(key)?),
                Bound::Excluded(key) => Bound::Excluded(self.state_key_to_table_pk(key)?),
                Bound::Unbounded => Bound::Unbounded,
            })
        }

        fn state_key_to_table_pk<'s, 'k>(
            &'s self,
            key: &'k StateKey,
        ) -> StreamExecutorResult<impl Row + 'k>
        where
            's: 'k,
        {
            Ok(self
                .this_partition_key
                .chain(memcmp_encoding::decode_row(
                    &key.order_key,
                    self.order_key_data_types,
                    self.order_key_order_types,
                )?)
                .chain(key.pk.as_inner()))
        }

        pub fn shrink_cache_to_range(&mut self, range: RangeInclusive<&StateKey>) {
            todo!()
        }
    }
}

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

    /// The maximum size of the chunk produced by executor at a time.
    chunk_size: usize,
}

struct ExecutionVars<S: StateStore> {
    /// partition key => partition range cache.
    cached_partitions: ManagedLruCache<OwnedRow, PartitionCache>,
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
                chunk_size: args.chunk_size,
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
        // partition key => changes happened in the partition.
        let mut deltas: BTreeMap<DefaultOrdered<OwnedRow>, PartitionDelta> = BTreeMap::new();
        // input pk of update records of which the order key is changed.
        let mut key_change_updated_pks = HashSet::new();

        // Collect changes for each partition.
        for record in Self::merge_changes_in_chunk(this, &chunk) {
            match record {
                Record::Insert { new_row } => {
                    let part_key = this.get_partition_key(new_row).into();
                    let part_delta = deltas.entry(part_key).or_insert(PartitionDelta::new());
                    part_delta.insert(
                        this.row_to_cache_key(new_row)?,
                        Change::Insert(new_row.into_owned_row()),
                    );
                }
                Record::Delete { old_row } => {
                    let part_key = this.get_partition_key(old_row).into();
                    let part_delta = deltas.entry(part_key).or_insert(PartitionDelta::new());
                    part_delta.insert(this.row_to_cache_key(old_row)?, Change::Delete);
                }
                Record::Update { old_row, new_row } => {
                    let old_part_key = this.get_partition_key(old_row).into();
                    let new_part_key = this.get_partition_key(new_row).into();
                    let old_state_key = this.row_to_cache_key(old_row)?;
                    let new_state_key = this.row_to_cache_key(new_row)?;
                    if old_part_key == new_part_key && old_state_key == new_state_key {
                        // not a key-change update
                        let part_delta =
                            deltas.entry(old_part_key).or_insert(PartitionDelta::new());
                        part_delta.insert(old_state_key, Change::Insert(new_row.into_owned_row()));
                    } else if old_part_key == new_part_key {
                        // order-change update, split into delete + insert, will be merged after
                        // building changes
                        key_change_updated_pks.insert(this.get_input_pk(old_row));
                        let part_delta =
                            deltas.entry(old_part_key).or_insert(PartitionDelta::new());
                        part_delta.insert(old_state_key, Change::Delete);
                        part_delta.insert(new_state_key, Change::Insert(new_row.into_owned_row()));
                    } else {
                        // partition-change update, split into delete + insert
                        // NOTE(rc): Since we append partition key to logical pk, we can't merge the
                        // delete + insert back to update later.
                        // TODO: IMO this behavior is problematic. Deep discussion is needed.
                        let old_part_delta =
                            deltas.entry(old_part_key).or_insert(PartitionDelta::new());
                        old_part_delta.insert(old_state_key, Change::Delete);
                        let new_part_delta =
                            deltas.entry(new_part_key).or_insert(PartitionDelta::new());
                        new_part_delta
                            .insert(new_state_key, Change::Insert(new_row.into_owned_row()));
                    }
                }
            }
        }

        // `input pk` => `Record`
        let mut key_change_update_buffer = BTreeMap::new();
        let mut chunk_builder = ChunkBuilder::new(this.chunk_size, &this.info.schema.data_types());

        // Build final changes partition by partition.
        for (part_key, delta) in deltas {
            if !vars.cached_partitions.contains(&part_key.0) {
                vars.cached_partitions
                    .put(part_key.0.clone(), new_empty_partition_cache());
            }
            let mut cache = vars.cached_partitions.get_mut(&part_key).unwrap();
            let mut partition = OverPartition::new(
                &part_key,
                &mut cache,
                &this.order_key_data_types,
                &this.order_key_order_types,
            );

            // Build changes for current partition.
            let part_changes =
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
                //
                // # Safety
                //
                // - For sentinel keys: We already extended the cache to cover the delta range, so
                //   there's no key in the table that is between `key` and the side-most
                //   non-sentinel key in the cache.
                // - For `Update`: The update is guaranteed not to be a partition-change update.
                //
                // TODO(rc): we can remove the `unsafe` here once we moved the `affected_ranges`
                // block below to `OverPartition`
                unsafe {
                    partition.write_record_unchecked(&mut this.state_table, key, record);
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
    ) -> StreamExecutorResult<BTreeMap<StateKey, Record<OwnedRow>>> {
        assert!(!delta.is_empty(), "if there's no delta, we won't be here");

        let mut part_changes = BTreeMap::new();

        // Find affected ranges, this also ensures that all rows in the affected ranges are loaded
        // into the cache.
        let (part_with_delta, affected_ranges) = partition
            .find_affected_ranges(&this.state_table, &this.calls, &delta)
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

        for (first_frame_start, first_curr_key, last_curr_key, last_frame_end) in affected_ranges {
            assert!(first_frame_start <= first_curr_key);
            assert!(first_curr_key <= last_curr_key);
            assert!(last_curr_key <= last_frame_end);
            assert!(first_frame_start.is_normal());
            assert!(first_curr_key.is_normal());
            assert!(last_curr_key.is_normal());
            assert!(last_frame_end.is_normal());

            let mut states =
                WindowStates::new(this.calls.iter().map(create_window_state).try_collect()?);

            // Populate window states with the affected range of rows.
            {
                let mut cursor = part_with_delta
                    .find(&first_frame_start)
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

                    key != &last_frame_end
                } {}
            }

            // Slide to the first affected key. We can safely compare to `Some(first_curr_key)` here
            // because it must exist in the states, by the definition of affected range.
            while states.curr_key() != Some(first_curr_key.as_normal_expect()) {
                states.just_slide_forward();
            }
            let mut curr_key_cursor = part_with_delta.find(&first_curr_key).unwrap();
            assert_eq!(
                states.curr_key(),
                curr_key_cursor.key().map(CacheKey::as_normal_expect)
            );

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
            cached_partitions: new_unbounded(this.watermark_epoch.clone(), metrics_info),
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

                    if let Some(vnode_bitmap) = barrier.as_update_vnode_bitmap(this.actor_ctx.id) {
                        let (_, cache_may_stale) =
                            this.state_table.update_vnode_bitmap(vnode_bitmap);
                        if cache_may_stale {
                            vars.cached_partitions.clear();
                        }
                    }

                    vars.cached_partitions.update_epoch(barrier.epoch.curr);

                    yield Message::Barrier(barrier);
                }
            }
        }
    }
}

/// Find all affected ranges in the given partition with delta.
///
/// # Returns
///
/// `Vec<(first_frame_start, first_curr_key, last_curr_key, last_frame_end_incl)>`
///
/// Each affected range is a union of many small window frames affected by some adajcent
/// keys in the delta.
///
/// Example:
/// - frame 1: `rows between 2 preceding and current row`
/// - frame 2: `rows between 1 preceding and 2 following`
/// - partition: `[1, 2, 4, 5, 7, 8, 9, 10, 11, 12, 14]`
/// - delta: `[3, 4, 15]`
/// - affected ranges: `[(1, 1, 7, 9), (10, 12, 15, 15)]`
///
/// TODO(rc):
/// Note that, since we assume input chunks have data locality on order key columns, we now only
/// calculate one single affected range. So the affected ranges in the above example will be
/// `(1, 1, 15, 15)`. Later we may optimize this.
fn find_affected_ranges<'cache>(
    calls: &'_ [WindowFuncCall],
    part_with_delta: DeltaBTreeMap<'cache, CacheKey, OwnedRow>,
) -> Vec<(
    &'cache CacheKey,
    &'cache CacheKey,
    &'cache CacheKey,
    &'cache CacheKey,
)> {
    let delta = part_with_delta.delta();

    if part_with_delta.first_key().is_none() {
        // all keys are deleted in the delta
        return vec![];
    }

    if part_with_delta.snapshot().is_empty() {
        // all existing keys are inserted in the delta
        return vec![(
            delta.first_key_value().unwrap().0,
            delta.first_key_value().unwrap().0,
            delta.last_key_value().unwrap().0,
            delta.last_key_value().unwrap().0,
        )];
    }

    let first_key = part_with_delta.first_key().unwrap();
    let last_key = part_with_delta.last_key().unwrap();

    let start_is_unbounded = calls
        .iter()
        .any(|call| call.frame.bounds.start_is_unbounded());
    let end_is_unbounded = calls
        .iter()
        .any(|call| call.frame.bounds.end_is_unbounded());

    let first_curr_key = if end_is_unbounded {
        // If the frame end is unbounded, the frame corresponding to the first key is always
        // affected.
        first_key
    } else {
        calls
            .iter()
            .map(|call| match &call.frame.bounds {
                FrameBounds::Rows(_start, end) => {
                    let mut cursor = part_with_delta
                        .lower_bound(Bound::Included(delta.first_key_value().unwrap().0));
                    for _ in 0..end.n_following_rows().unwrap() {
                        // Note that we have to move before check, to handle situation where the
                        // cursor is at ghost position at first.
                        cursor.move_prev();
                        if cursor.position().is_ghost() {
                            break;
                        }
                    }
                    cursor.key().unwrap_or(first_key)
                }
            })
            .min()
            .expect("# of window function calls > 0")
    };

    let first_frame_start = if start_is_unbounded {
        // If the frame start is unbounded, the first key always need to be included in the affected
        // range.
        first_key
    } else {
        calls
            .iter()
            .map(|call| match &call.frame.bounds {
                FrameBounds::Rows(start, _end) => {
                    let mut cursor = part_with_delta.find(&first_curr_key).unwrap();
                    for _ in 0..start.n_preceding_rows().unwrap() {
                        cursor.move_prev();
                        if cursor.position().is_ghost() {
                            break;
                        }
                    }
                    cursor.key().unwrap_or(first_key)
                }
            })
            .min()
            .expect("# of window function calls > 0")
    };

    let last_curr_key = if start_is_unbounded {
        last_key
    } else {
        calls
            .iter()
            .map(|call| match &call.frame.bounds {
                FrameBounds::Rows(start, _end) => {
                    let mut cursor = part_with_delta
                        .upper_bound(Bound::Included(delta.last_key_value().unwrap().0));
                    for _ in 0..start.n_preceding_rows().unwrap() {
                        cursor.move_next();
                        if cursor.position().is_ghost() {
                            break;
                        }
                    }
                    cursor.key().unwrap_or(last_key)
                }
            })
            .max()
            .expect("# of window function calls > 0")
    };

    let last_frame_end = if end_is_unbounded {
        last_key
    } else {
        calls
            .iter()
            .map(|call| match &call.frame.bounds {
                FrameBounds::Rows(_start, end) => {
                    let mut cursor = part_with_delta.find(&last_curr_key).unwrap();
                    for _ in 0..end.n_following_rows().unwrap() {
                        cursor.move_next();
                        if cursor.position().is_ghost() {
                            break;
                        }
                    }
                    cursor.key().unwrap_or(last_key)
                }
            })
            .max()
            .expect("# of window function calls > 0")
    };

    if first_curr_key > last_curr_key {
        // all affected keys are deleted in the delta
        return vec![];
    }

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
    use risingwave_expr::window_function::{Frame, FrameBound, WindowFuncKind};

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
            ($( $pk:expr ),* $(,)?) => {
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
            (Insert) => {
                Change::Insert(OwnedRow::empty())
            };
        }

        macro_rules! create_delta {
            ($(( $pk:expr, $change:ident )),* $(,)?) => {
                {
                    #[allow(unused_mut)]
                    let mut delta = BTreeMap::new();
                    $(
                        delta.insert(
                            StateKey {
                                // order key doesn't matter here
                                order_key: vec![].into(),
                                pk: OwnedRow::new(vec![Some($pk.into())]).into(),
                            },
                            // value row doesn't matter here
                            create_change!( $change ),
                        );
                    )*
                    delta
                }
            };
        }

        {
            // test all empty
            let snapshot = create_snapshot!();
            let delta = create_delta!();
            let part_with_delta = DeltaBTreeMap::new(&snapshot, &delta);
            let calls = vec![create_call(Frame::rows(
                FrameBound::Preceding(2),
                FrameBound::Preceding(1),
            ))];
            assert!(find_affected_ranges(&calls, &part_with_delta).is_empty());
        }

        {
            // test insert delta only
            let snapshot = create_snapshot!();
            let delta = create_delta!((1, Insert), (2, Insert), (3, Insert));
            let part_with_delta = DeltaBTreeMap::new(&snapshot, &delta);
            let calls = vec![create_call(Frame::rows(
                FrameBound::Preceding(2),
                FrameBound::Preceding(1),
            ))];
            let affected_ranges = find_affected_ranges(&calls, &part_with_delta);
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
            let delta = create_delta!((2, Insert), (3, Delete));
            let part_with_delta = DeltaBTreeMap::new(&snapshot, &delta);

            {
                let calls = vec![create_call(Frame::rows(
                    FrameBound::Preceding(2),
                    FrameBound::Preceding(1),
                ))];
                let (first_frame_start, first_curr_key, last_curr_key, last_frame_end) =
                    find_affected_ranges(&calls, &part_with_delta)
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
                    find_affected_ranges(&calls, &part_with_delta)
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
                    find_affected_ranges(&calls, &part_with_delta)
                        .into_iter()
                        .next()
                        .unwrap();
                assert_eq!(first_frame_start.pk.0, OwnedRow::new(vec![Some(1.into())]));
                assert_eq!(first_curr_key.pk.0, OwnedRow::new(vec![Some(1.into())]));
                assert_eq!(last_curr_key.pk.0, OwnedRow::new(vec![Some(2.into())]));
                assert_eq!(last_frame_end.pk.0, OwnedRow::new(vec![Some(5.into())]));
            }
        }

        {
            // test multiple calls
            let snapshot = create_snapshot!(1, 2, 3, 4, 5, 6);
            let delta = create_delta!((2, Insert), (3, Delete));
            let part_with_delta = DeltaBTreeMap::new(&snapshot, &delta);

            let calls = vec![
                create_call(Frame::rows(
                    FrameBound::Preceding(1),
                    FrameBound::Preceding(1),
                )),
                create_call(Frame::rows(
                    FrameBound::Following(1),
                    FrameBound::Following(1),
                )),
            ];
            let (first_frame_start, first_curr_key, last_curr_key, last_frame_end) =
                find_affected_ranges(&calls, &part_with_delta)
                    .into_iter()
                    .next()
                    .unwrap();
            assert_eq!(first_frame_start.pk.0, OwnedRow::new(vec![Some(1.into())]));
            assert_eq!(first_curr_key.pk.0, OwnedRow::new(vec![Some(1.into())]));
            assert_eq!(last_curr_key.pk.0, OwnedRow::new(vec![Some(4.into())]));
            assert_eq!(last_frame_end.pk.0, OwnedRow::new(vec![Some(5.into())]));
        }

        {
            // test lag corner case
            let snapshot = create_snapshot!(1, 2, 3, 4, 5, 6);
            let delta = create_delta!((1, Delete), (2, Delete), (3, Delete));
            let part_with_delta = DeltaBTreeMap::new(&snapshot, &delta);

            let calls = vec![create_call(Frame::rows(
                FrameBound::Preceding(1),
                FrameBound::Preceding(1),
            ))];
            let (first_frame_start, first_curr_key, last_curr_key, last_frame_end) =
                find_affected_ranges(&calls, &part_with_delta)
                    .into_iter()
                    .next()
                    .unwrap();
            assert_eq!(first_frame_start.pk.0, OwnedRow::new(vec![Some(4.into())]));
            assert_eq!(first_curr_key.pk.0, OwnedRow::new(vec![Some(4.into())]));
            assert_eq!(last_curr_key.pk.0, OwnedRow::new(vec![Some(4.into())]));
            assert_eq!(last_frame_end.pk.0, OwnedRow::new(vec![Some(4.into())]));
        }

        {
            // test lead corner case
            let snapshot = create_snapshot!(1, 2, 3, 4, 5, 6);
            let delta = create_delta!((4, Delete), (5, Delete), (6, Delete));
            let part_with_delta = DeltaBTreeMap::new(&snapshot, &delta);

            let calls = vec![create_call(Frame::rows(
                FrameBound::Following(1),
                FrameBound::Following(1),
            ))];
            let (first_frame_start, first_curr_key, last_curr_key, last_frame_end) =
                find_affected_ranges(&calls, &part_with_delta)
                    .into_iter()
                    .next()
                    .unwrap();
            assert_eq!(first_frame_start.pk.0, OwnedRow::new(vec![Some(3.into())]));
            assert_eq!(first_curr_key.pk.0, OwnedRow::new(vec![Some(3.into())]));
            assert_eq!(last_curr_key.pk.0, OwnedRow::new(vec![Some(3.into())]));
            assert_eq!(last_frame_end.pk.0, OwnedRow::new(vec![Some(3.into())]));
        }

        {
            // test lag/lead(x, 0) corner case
            let snapshot = create_snapshot!(1, 2, 3, 4);
            let delta = create_delta!((2, Delete), (3, Delete));
            let part_with_delta = DeltaBTreeMap::new(&snapshot, &delta);

            let calls = vec![create_call(Frame::rows(
                FrameBound::CurrentRow,
                FrameBound::CurrentRow,
            ))];
            assert!(find_affected_ranges(&calls, &part_with_delta).is_empty());
        }

        {
            // test lag/lead(x, 0) corner case 2
            let snapshot = create_snapshot!(1, 2, 3, 4, 5);
            let delta = create_delta!((2, Delete), (3, Insert), (4, Delete));
            let part_with_delta = DeltaBTreeMap::new(&snapshot, &delta);

            let calls = vec![create_call(Frame::rows(
                FrameBound::CurrentRow,
                FrameBound::CurrentRow,
            ))];
            let (first_frame_start, first_curr_key, last_curr_key, last_frame_end) =
                find_affected_ranges(&calls, &part_with_delta)
                    .into_iter()
                    .next()
                    .unwrap();
            assert_eq!(first_frame_start.pk.0, OwnedRow::new(vec![Some(3.into())]));
            assert_eq!(first_curr_key.pk.0, OwnedRow::new(vec![Some(3.into())]));
            assert_eq!(last_curr_key.pk.0, OwnedRow::new(vec![Some(3.into())]));
            assert_eq!(last_frame_end.pk.0, OwnedRow::new(vec![Some(3.into())]));
        }
    }
}

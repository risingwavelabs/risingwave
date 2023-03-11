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

mod join_entry_state;

use std::alloc::Global;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use fixedbitset::FixedBitSet;
use futures::future::try_join;
use futures::StreamExt;
use futures_async_stream::for_await;
pub(super) use join_entry_state::JoinEntryState;
use local_stats_alloc::{SharedStatsAlloc, StatsAlloc};
use risingwave_common::buffer::Bitmap;
use risingwave_common::collection::estimate_size::EstimateSize;
use risingwave_common::hash::{HashKey, PrecomputedBuildHasher};
use risingwave_common::row;
use risingwave_common::row::{CompactedRow, OwnedRow, Row, RowExt};
use risingwave_common::types::{DataType, ScalarImpl};
use risingwave_common::util::epoch::EpochPair;
use risingwave_common::util::ordered::OrderedRowSerde;
use risingwave_common::util::sort_util::OrderType;
use risingwave_storage::store::PrefetchOptions;
use risingwave_storage::StateStore;

use crate::cache::{cache_may_stale, new_with_hasher_in, ExecutorCache};
use crate::common::table::state_table::StateTable;
use crate::executor::error::StreamExecutorResult;
use crate::executor::monitor::StreamingMetrics;
use crate::task::{ActorId, AtomicU64Ref};

type DegreeType = u64;

fn build_degree_row(order_key: impl Row, degree: DegreeType) -> impl Row {
    order_key.chain(row::once(Some(ScalarImpl::Int64(degree as i64))))
}

/// This is a row with a match degree
#[derive(Clone, Debug)]
pub struct JoinRow<R: Row> {
    pub row: R,
    degree: DegreeType,
}

impl<R: Row> JoinRow<R> {
    pub fn new(row: R, degree: DegreeType) -> Self {
        Self { row, degree }
    }

    pub fn is_zero_degree(&self) -> bool {
        self.degree == 0
    }

    /// Return row and degree in `Row` format. The degree part will be inserted in degree table
    /// later, so a pk prefix will be added.
    ///
    /// * `state_order_key_indices` - the order key of `row`
    pub fn to_table_rows<'a>(
        &'a self,
        state_order_key_indices: &'a [usize],
    ) -> (&'a R, impl Row + 'a) {
        let order_key = (&self.row).project(state_order_key_indices);
        let degree = build_degree_row(order_key, self.degree);
        (&self.row, degree)
    }

    pub fn encode(&self) -> EncodedJoinRow {
        EncodedJoinRow {
            compacted_row: (&self.row).into(),
            degree: self.degree,
        }
    }
}

#[derive(Clone, Debug)]
pub struct EncodedJoinRow {
    pub compacted_row: CompactedRow,
    degree: DegreeType,
}

impl EncodedJoinRow {
    fn decode(&self, data_types: &[DataType]) -> StreamExecutorResult<JoinRow<OwnedRow>> {
        Ok(JoinRow {
            row: self.decode_row(data_types)?,
            degree: self.degree,
        })
    }

    fn decode_row(&self, data_types: &[DataType]) -> StreamExecutorResult<OwnedRow> {
        let row = self.compacted_row.deserialize(data_types)?;
        Ok(row)
    }
}

impl EstimateSize for EncodedJoinRow {
    fn estimated_heap_size(&self) -> usize {
        self.compacted_row.row.len()
    }
}

/// Memcomparable encoding.
type PkType = Vec<u8>;

pub type StateValueType = EncodedJoinRow;
pub type HashValueType = Box<JoinEntryState>;

/// The wrapper for [`JoinEntryState`] which should be `Some` most of the time in the hash table.
///
/// When the executor is operating on the specific entry of the map, it can hold the ownership of
/// the entry by taking the value out of the `Option`, instead of holding a mutable reference to the
/// map, which can make the compiler happy.
struct HashValueWrapper(Option<HashValueType>);

impl HashValueWrapper {
    const MESSAGE: &str = "the state should always be `Some`";

    /// Take the value out of the wrapper. Panic if the value is `None`.
    pub fn take(&mut self) -> HashValueType {
        self.0.take().expect(Self::MESSAGE)
    }
}

impl Deref for HashValueWrapper {
    type Target = HashValueType;

    fn deref(&self) -> &Self::Target {
        self.0.as_ref().expect(Self::MESSAGE)
    }
}

impl DerefMut for HashValueWrapper {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0.as_mut().expect(Self::MESSAGE)
    }
}

type JoinHashMapInner<K> =
    ExecutorCache<K, HashValueWrapper, PrecomputedBuildHasher, SharedStatsAlloc<Global>>;

pub struct JoinHashMapMetrics {
    /// Metrics used by join executor
    metrics: Arc<StreamingMetrics>,
    /// Basic information
    actor_id: String,
    side: &'static str,
    /// How many times have we hit the cache of join executor
    lookup_miss_count: usize,
    total_lookup_count: usize,
    /// How many times have we miss the cache when insert row
    insert_cache_miss_count: usize,
    may_exist_true_count: usize,
}

impl JoinHashMapMetrics {
    pub fn new(metrics: Arc<StreamingMetrics>, actor_id: ActorId, side: &'static str) -> Self {
        Self {
            metrics,
            actor_id: actor_id.to_string(),
            side,
            lookup_miss_count: 0,
            total_lookup_count: 0,
            insert_cache_miss_count: 0,
            may_exist_true_count: 0,
        }
    }

    pub fn flush(&mut self) {
        self.metrics
            .join_lookup_miss_count
            .with_label_values(&[&self.actor_id, self.side])
            .inc_by(self.lookup_miss_count as u64);
        self.metrics
            .join_total_lookup_count
            .with_label_values(&[&self.actor_id, self.side])
            .inc_by(self.total_lookup_count as u64);
        self.metrics
            .join_insert_cache_miss_count
            .with_label_values(&[&self.actor_id, self.side])
            .inc_by(self.insert_cache_miss_count as u64);
        self.metrics
            .join_may_exist_true_count
            .with_label_values(&[&self.actor_id, self.side])
            .inc_by(self.may_exist_true_count as u64);
        self.total_lookup_count = 0;
        self.lookup_miss_count = 0;
        self.insert_cache_miss_count = 0;
        self.may_exist_true_count = 0;
    }
}

pub struct JoinHashMap<K: HashKey, S: StateStore> {
    /// Store the join states.
    inner: JoinHashMapInner<K>,
    /// Data types of the join key columns
    join_key_data_types: Vec<DataType>,
    /// Null safe bitmap for each join pair
    null_matched: FixedBitSet,
    /// The memcomparable serializer of primary key.
    pk_serializer: OrderedRowSerde,
    /// State table. Contains the data from upstream.
    state: TableInner<S>,
    /// Degree table.
    ///
    /// The degree is generated from the hash join executor.
    /// Each row in `state` has a corresponding degree in `degree state`.
    /// A degree value `d` in for a row means the row has `d` matched row in the other join side.
    ///
    /// It will only be used when needed in a side.
    ///
    /// - Full Outer: both side
    /// - Left Outer/Semi/Anti: left side
    /// - Right Outer/Semi/Anti: right side
    /// - Inner: None.
    degree_state: TableInner<S>,
    /// If degree table is need
    need_degree_table: bool,
    /// Pk is part of the join key.
    pk_contained_in_jk: bool,
    /// Metrics of the hash map
    metrics: JoinHashMapMetrics,
}

struct TableInner<S: StateStore> {
    pk_indices: Vec<usize>,
    // This should be identical to the pk in state table.
    order_key_indices: Vec<usize>,
    // This should be identical to the data types in table schema.
    #[expect(dead_code)]
    all_data_types: Vec<DataType>,
    pub(crate) table: StateTable<S>,
}

impl<K: HashKey, S: StateStore> JoinHashMap<K, S> {
    /// Create a [`JoinHashMap`] with the given LRU capacity.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        watermark_epoch: AtomicU64Ref,
        join_key_data_types: Vec<DataType>,
        state_all_data_types: Vec<DataType>,
        state_table: StateTable<S>,
        state_pk_indices: Vec<usize>,
        degree_all_data_types: Vec<DataType>,
        degree_table: StateTable<S>,
        degree_pk_indices: Vec<usize>,
        null_matched: FixedBitSet,
        need_degree_table: bool,
        pk_contained_in_jk: bool,
        metrics: Arc<StreamingMetrics>,
        actor_id: ActorId,
        side: &'static str,
    ) -> Self {
        let alloc = StatsAlloc::new(Global).shared();
        // TODO: unify pk encoding with state table.
        let pk_data_types = state_pk_indices
            .iter()
            .map(|i| state_all_data_types[*i].clone())
            .collect();
        let pk_serializer = OrderedRowSerde::new(
            pk_data_types,
            vec![OrderType::default_ascending(); state_pk_indices.len()],
        );

        let state = TableInner {
            pk_indices: state_pk_indices,
            order_key_indices: state_table.pk_indices().to_vec(),
            all_data_types: state_all_data_types,
            table: state_table,
        };

        let degree_state = TableInner {
            pk_indices: degree_pk_indices,
            order_key_indices: degree_table.pk_indices().to_vec(),
            all_data_types: degree_all_data_types,
            table: degree_table,
        };

        let cache = ExecutorCache::new(new_with_hasher_in(
            watermark_epoch,
            PrecomputedBuildHasher,
            alloc,
        ));

        Self {
            inner: cache,
            join_key_data_types,
            null_matched,
            pk_serializer,
            state,
            degree_state,
            need_degree_table,
            pk_contained_in_jk,
            metrics: JoinHashMapMetrics::new(metrics, actor_id, side),
        }
    }

    pub fn init(&mut self, epoch: EpochPair) {
        self.update_epoch(epoch.curr);
        self.state.table.init_epoch(epoch);
        self.degree_state.table.init_epoch(epoch);
    }

    pub fn update_epoch(&mut self, epoch: u64) {
        // Update the current epoch in `ManagedLruCache`
        self.inner.update_epoch(epoch)
    }

    /// Update the vnode bitmap and manipulate the cache if necessary.
    pub fn update_vnode_bitmap(&mut self, vnode_bitmap: Arc<Bitmap>) {
        let previous_vnode_bitmap = self.state.table.update_vnode_bitmap(vnode_bitmap.clone());
        let _ = self
            .degree_state
            .table
            .update_vnode_bitmap(vnode_bitmap.clone());

        if cache_may_stale(&previous_vnode_bitmap, &vnode_bitmap) {
            self.inner.clear();
        }
    }

    pub fn update_watermark(&mut self, watermark: ScalarImpl) {
        // TODO: remove data in cache.
        self.state.table.update_watermark(watermark.clone());
        self.degree_state.table.update_watermark(watermark);
    }

    /// Take the state for the given `key` out of the hash table and return it. One **MUST** call
    /// `update_state` after some operations to put the state back.
    ///
    /// If the state does not exist in the cache, fetch the remote storage and return. If it still
    /// does not exist in the remote storage, a [`JoinEntryState`] with empty cache will be
    /// returned.
    ///
    /// Note: This will NOT remove anything from remote storage.
    pub async fn take_state<'a>(&mut self, key: &K) -> StreamExecutorResult<HashValueType> {
        // Do not update the LRU statistics here with `peek_mut` since we will put the state back.
        let state = self.inner.peek_mut(key);
        self.metrics.total_lookup_count += 1;
        Ok(match state {
            Some(state) => state.take(),
            None => {
                self.metrics.lookup_miss_count += 1;
                self.fetch_cached_state(key).await?.into()
            }
        })
    }

    /// Fetch cache from the state store. Should only be called if the key does not exist in memory.
    /// Will return a empty `JoinEntryState` even when state does not exist in remote.
    async fn fetch_cached_state(&self, key: &K) -> StreamExecutorResult<JoinEntryState> {
        let key = key.deserialize(&self.join_key_data_types)?;

        let mut entry_state = JoinEntryState::default();

        if self.need_degree_table {
            let table_iter_fut = self.state.table.iter_key_and_val(&key, Default::default());
            let degree_table_iter_fut = self
                .degree_state
                .table
                .iter_key_and_val(&key, Default::default());

            let (table_iter, degree_table_iter) =
                try_join(table_iter_fut, degree_table_iter_fut).await?;

            #[for_await]
            for (row, degree) in table_iter.zip(degree_table_iter) {
                let (pk1, row) = row?;
                let (pk2, degree) = degree?;
                debug_assert_eq!(
                    pk1, pk2,
                    "mismatched pk in degree table: pk1: {pk1:?}, pk2: {pk2:?}",
                );
                let pk = row
                    .as_ref()
                    .project(&self.state.pk_indices)
                    .memcmp_serialize(&self.pk_serializer);
                let degree_i64 = degree
                    .datum_at(degree.len() - 1)
                    .expect("degree should not be NULL");
                entry_state.insert(
                    pk,
                    JoinRow::new(row, degree_i64.into_int64() as u64).encode(),
                );
            }
        } else {
            let table_iter = self
                .state
                .table
                .iter_with_pk_prefix(&key, PrefetchOptions::new_for_exhaust_iter())
                .await?;

            #[for_await]
            for row in table_iter {
                let row: OwnedRow = row?;
                let pk = row
                    .as_ref()
                    .project(&self.state.pk_indices)
                    .memcmp_serialize(&self.pk_serializer);
                entry_state.insert(pk, JoinRow::new(row, 0).encode());
            }
        };

        Ok(entry_state)
    }

    pub async fn flush(&mut self, epoch: EpochPair) -> StreamExecutorResult<()> {
        self.metrics.flush();
        self.state.table.commit(epoch).await?;
        self.degree_state.table.commit(epoch).await?;
        Ok(())
    }

    /// Insert a join row
    #[allow(clippy::unused_async)]
    pub async fn insert(&mut self, key: &K, value: JoinRow<impl Row>) -> StreamExecutorResult<()> {
        let pk = (&value.row)
            .project(&self.state.pk_indices)
            .memcmp_serialize(&self.pk_serializer);
        if let Some(entry) = self.inner.get_mut(key) {
            // Update cache
            entry.insert(pk, value.encode());
        } else if self.pk_contained_in_jk {
            // Refill cache when the join key contains primary key.
            self.metrics.insert_cache_miss_count += 1;
            let mut state = JoinEntryState::default();
            state.insert(pk, value.encode());
            self.update_state(key, state.into());
        } else {
            let prefix = key.deserialize(&self.join_key_data_types)?;
            self.metrics.insert_cache_miss_count += 1;
            // Refill cache when the join key exists in neither cache or storage.
            if !self.state.table.may_exist(&prefix).await? {
                let mut state = JoinEntryState::default();
                state.insert(pk, value.encode());
                self.update_state(key, state.into());
            } else {
                self.metrics.may_exist_true_count += 1;
            }
        }

        // Update the flush buffer.
        let (row, degree) = value.to_table_rows(&self.state.order_key_indices);
        self.state.table.insert(row);
        self.degree_state.table.insert(degree);
        Ok(())
    }

    /// Insert a row.
    /// Used when the side does not need to update degree.
    pub async fn insert_row(&mut self, key: &K, value: impl Row) -> StreamExecutorResult<()> {
        let join_row = JoinRow::new(&value, 0);
        let pk = (&value)
            .project(&self.state.pk_indices)
            .memcmp_serialize(&self.pk_serializer);
        if let Some(entry) = self.inner.get_mut(key) {
            // Update cache
            entry.insert(pk, join_row.encode());
        } else if self.pk_contained_in_jk {
            // Refill cache when the join key contains primary key.
            self.metrics.insert_cache_miss_count += 1;
            let mut state = JoinEntryState::default();
            state.insert(pk, join_row.encode());
            self.update_state(key, state.into());
        } else {
            let prefix = key.deserialize(&self.join_key_data_types)?;
            self.metrics.insert_cache_miss_count += 1;
            // Refill cache when the join key exists in neither cache or storage.
            if !self.state.table.may_exist(&prefix).await? {
                let mut state = JoinEntryState::default();
                state.insert(pk, join_row.encode());
                self.update_state(key, state.into());
            } else {
                self.metrics.may_exist_true_count += 1;
            }
        }

        // Update the flush buffer.
        self.state.table.insert(value);
        Ok(())
    }

    /// Delete a join row
    pub fn delete(&mut self, key: &K, value: JoinRow<impl Row>) {
        if let Some(entry) = self.inner.get_mut(key) {
            let pk = (&value.row)
                .project(&self.state.pk_indices)
                .memcmp_serialize(&self.pk_serializer);
            entry.remove(pk);
        }

        // If no cache maintained, only update the state table.
        let (row, degree) = value.to_table_rows(&self.state.order_key_indices);
        self.state.table.delete(row);
        self.degree_state.table.delete(degree);
    }

    /// Delete a row
    /// Used when the side does not need to update degree.
    pub fn delete_row(&mut self, key: &K, value: impl Row) {
        if let Some(entry) = self.inner.get_mut(key) {
            let pk = (&value)
                .project(&self.state.pk_indices)
                .memcmp_serialize(&self.pk_serializer);
            entry.remove(pk);
        }

        // If no cache maintained, only update the state table.
        self.state.table.delete(value);
    }

    /// Update a [`JoinEntryState`] into the hash table.
    pub fn update_state(&mut self, key: &K, state: HashValueType) {
        self.inner.put(key.clone(), HashValueWrapper(Some(state)));
    }

    /// Manipulate the degree of the given [`JoinRow`] and [`EncodedJoinRow`] with `action`, both in
    /// memory and in the degree table.
    fn manipulate_degree(
        &mut self,
        join_row_ref: &mut StateValueType,
        join_row: &mut JoinRow<OwnedRow>,
        action: impl Fn(&mut DegreeType),
    ) {
        // TODO: no need to `into_owned_row` here due to partial borrow.
        let old_degree = join_row
            .to_table_rows(&self.state.order_key_indices)
            .1
            .into_owned_row();

        action(&mut join_row_ref.degree);
        action(&mut join_row.degree);

        let new_degree = join_row.to_table_rows(&self.state.order_key_indices).1;

        self.degree_state.table.update(old_degree, new_degree);
    }

    /// Increment the degree of the given [`JoinRow`] and [`EncodedJoinRow`] with `action`, both in
    /// memory and in the degree table.
    pub fn inc_degree(
        &mut self,
        join_row_ref: &mut StateValueType,
        join_row: &mut JoinRow<OwnedRow>,
    ) {
        self.manipulate_degree(join_row_ref, join_row, |d| *d += 1)
    }

    /// Decrement the degree of the given [`JoinRow`] and [`EncodedJoinRow`] with `action`, both in
    /// memory and in the degree table.
    pub fn dec_degree(
        &mut self,
        join_row_ref: &mut StateValueType,
        join_row: &mut JoinRow<OwnedRow>,
    ) {
        self.manipulate_degree(join_row_ref, join_row, |d| {
            *d = d
                .checked_sub(1)
                .expect("Tried to decrement zero join row degree")
        })
    }

    /// Evict the cache.
    pub fn evict(&mut self) {
        self.inner.evict();
    }

    /// Cached rows for this hash table.
    #[expect(dead_code)]
    pub fn cached_rows(&self) -> usize {
        self.inner.values().map(|e| e.len()).sum()
    }

    /// Cached entry count for this hash table.
    pub fn entry_count(&self) -> usize {
        self.inner.len()
    }

    pub fn null_matched(&self) -> &FixedBitSet {
        &self.null_matched
    }
}

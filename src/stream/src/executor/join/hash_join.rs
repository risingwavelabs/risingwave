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
use std::alloc::Global;
use std::cmp::Ordering;
use std::ops::{Bound, Deref, DerefMut};
use std::sync::Arc;

use anyhow::Context;
use futures::future::{join, try_join};
use futures::{pin_mut, stream, StreamExt};
use futures_async_stream::for_await;
use local_stats_alloc::{SharedStatsAlloc, StatsAlloc};
use risingwave_common::bitmap::Bitmap;
use risingwave_common::hash::{HashKey, PrecomputedBuildHasher};
use risingwave_common::metrics::LabelGuardedIntCounter;
use risingwave_common::row::{OwnedRow, Row, RowExt};
use risingwave_common::types::{DataType, ScalarImpl};
use risingwave_common::util::epoch::EpochPair;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_common::util::row_serde::OrderedRowSerde;
use risingwave_common::util::sort_util::OrderType;
use risingwave_common_estimate_size::EstimateSize;
use risingwave_storage::store::PrefetchOptions;
use risingwave_storage::StateStore;

use super::row::{DegreeType, EncodedJoinRow};
use crate::cache::ManagedLruCache;
use crate::common::metrics::MetricsInfo;
use crate::common::table::state_table::StateTable;
use crate::consistency::{consistency_error, consistency_panic, enable_strict_consistency};
use crate::executor::error::StreamExecutorResult;
use crate::executor::join::row::JoinRow;
use crate::executor::monitor::StreamingMetrics;
use crate::executor::StreamExecutorError;
use crate::task::{ActorId, AtomicU64Ref, FragmentId};

/// Memcomparable encoding.
type PkType = Vec<u8>;

pub type StateValueType = EncodedJoinRow;
pub type HashValueType = Box<JoinEntryState>;

impl EstimateSize for HashValueType {
    fn estimated_heap_size(&self) -> usize {
        self.as_ref().estimated_heap_size()
    }
}

/// The wrapper for [`JoinEntryState`] which should be `Some` most of the time in the hash table.
///
/// When the executor is operating on the specific entry of the map, it can hold the ownership of
/// the entry by taking the value out of the `Option`, instead of holding a mutable reference to the
/// map, which can make the compiler happy.
struct HashValueWrapper(Option<HashValueType>);

impl EstimateSize for HashValueWrapper {
    fn estimated_heap_size(&self) -> usize {
        self.0.estimated_heap_size()
    }
}

impl HashValueWrapper {
    const MESSAGE: &'static str = "the state should always be `Some`";

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
    ManagedLruCache<K, HashValueWrapper, PrecomputedBuildHasher, SharedStatsAlloc<Global>>;

pub struct JoinHashMapMetrics {
    /// Basic information
    /// How many times have we hit the cache of join executor
    lookup_miss_count: usize,
    total_lookup_count: usize,
    /// How many times have we miss the cache when insert row
    insert_cache_miss_count: usize,

    // Metrics
    join_lookup_total_count_metric: LabelGuardedIntCounter<4>,
    join_lookup_miss_count_metric: LabelGuardedIntCounter<4>,
    join_insert_cache_miss_count_metrics: LabelGuardedIntCounter<4>,
}

impl JoinHashMapMetrics {
    pub fn new(
        metrics: &StreamingMetrics,
        actor_id: ActorId,
        fragment_id: FragmentId,
        side: &'static str,
        join_table_id: u32,
    ) -> Self {
        let actor_id = actor_id.to_string();
        let fragment_id = fragment_id.to_string();
        let join_table_id = join_table_id.to_string();
        let join_lookup_total_count_metric = metrics
            .join_lookup_total_count
            .with_guarded_label_values(&[(side), &join_table_id, &actor_id, &fragment_id]);
        let join_lookup_miss_count_metric = metrics
            .join_lookup_miss_count
            .with_guarded_label_values(&[(side), &join_table_id, &actor_id, &fragment_id]);
        let join_insert_cache_miss_count_metrics = metrics
            .join_insert_cache_miss_count
            .with_guarded_label_values(&[(side), &join_table_id, &actor_id, &fragment_id]);

        Self {
            lookup_miss_count: 0,
            total_lookup_count: 0,
            insert_cache_miss_count: 0,
            join_lookup_total_count_metric,
            join_lookup_miss_count_metric,
            join_insert_cache_miss_count_metrics,
        }
    }

    pub fn flush(&mut self) {
        self.join_lookup_total_count_metric
            .inc_by(self.total_lookup_count as u64);
        self.join_lookup_miss_count_metric
            .inc_by(self.lookup_miss_count as u64);
        self.join_insert_cache_miss_count_metrics
            .inc_by(self.insert_cache_miss_count as u64);
        self.total_lookup_count = 0;
        self.lookup_miss_count = 0;
        self.insert_cache_miss_count = 0;
    }
}

pub struct JoinHashMap<K: HashKey, S: StateStore> {
    /// Store the join states.
    inner: JoinHashMapInner<K>,
    /// Data types of the join key columns
    join_key_data_types: Vec<DataType>,
    /// Null safe bitmap for each join pair
    null_matched: K::Bitmap,
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
    /// Indices of the (cache) pk in a state row
    pk_indices: Vec<usize>,
    /// Indices of the join key in a state row
    join_key_indices: Vec<usize>,
    // This should be identical to the pk in state table.
    order_key_indices: Vec<usize>,
    // This should be identical to the data types in table schema.
    #[expect(dead_code)]
    all_data_types: Vec<DataType>,
    pub(crate) table: StateTable<S>,
}

impl<S: StateStore> TableInner<S> {
    fn error_context(&self, row: &impl Row) -> String {
        let pk = row.project(&self.pk_indices);
        let jk = row.project(&self.join_key_indices);
        format!(
            "join key: {}, pk: {}, row: {}, state_table_id: {}",
            jk.display(),
            pk.display(),
            row.display(),
            self.table.table_id()
        )
    }
}

impl<K: HashKey, S: StateStore> JoinHashMap<K, S> {
    /// Create a [`JoinHashMap`] with the given LRU capacity.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        watermark_sequence: AtomicU64Ref,
        join_key_data_types: Vec<DataType>,
        state_join_key_indices: Vec<usize>,
        state_all_data_types: Vec<DataType>,
        state_table: StateTable<S>,
        state_pk_indices: Vec<usize>,
        degree_join_key_indices: Vec<usize>,
        degree_all_data_types: Vec<DataType>,
        degree_table: StateTable<S>,
        degree_pk_indices: Vec<usize>,
        null_matched: K::Bitmap,
        need_degree_table: bool,
        pk_contained_in_jk: bool,
        metrics: Arc<StreamingMetrics>,
        actor_id: ActorId,
        fragment_id: FragmentId,
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
            vec![OrderType::ascending(); state_pk_indices.len()],
        );

        let join_table_id = state_table.table_id();
        let state = TableInner {
            pk_indices: state_pk_indices,
            join_key_indices: state_join_key_indices,
            order_key_indices: state_table.pk_indices().to_vec(),
            all_data_types: state_all_data_types,
            table: state_table,
        };

        let degree_state = TableInner {
            pk_indices: degree_pk_indices,
            join_key_indices: degree_join_key_indices,
            order_key_indices: degree_table.pk_indices().to_vec(),
            all_data_types: degree_all_data_types,
            table: degree_table,
        };

        let metrics_info = MetricsInfo::new(
            metrics.clone(),
            join_table_id,
            actor_id,
            format!("hash join {}", side),
        );

        let cache = ManagedLruCache::unbounded_with_hasher_in(
            watermark_sequence,
            metrics_info,
            PrecomputedBuildHasher,
            alloc,
        );

        Self {
            inner: cache,
            join_key_data_types,
            null_matched,
            pk_serializer,
            state,
            degree_state,
            need_degree_table,
            pk_contained_in_jk,
            metrics: JoinHashMapMetrics::new(&metrics, actor_id, fragment_id, side, join_table_id),
        }
    }

    pub fn init(&mut self, epoch: EpochPair) {
        self.state.table.init_epoch(epoch);
        self.degree_state.table.init_epoch(epoch);
    }

    /// Update the vnode bitmap and manipulate the cache if necessary.
    pub fn update_vnode_bitmap(&mut self, vnode_bitmap: Arc<Bitmap>) -> bool {
        let (_previous_vnode_bitmap, cache_may_stale) =
            self.state.table.update_vnode_bitmap(vnode_bitmap.clone());
        let _ = self.degree_state.table.update_vnode_bitmap(vnode_bitmap);

        if cache_may_stale {
            self.inner.clear();
        }

        cache_may_stale
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
    pub fn take_state_opt(&mut self, key: &K) -> Option<HashValueType> {
        self.metrics.total_lookup_count += 1;
        if self.inner.contains(key) {
            tracing::info!("hit cache for join key: {:?}", key);
            // Do not update the LRU statistics here with `peek_mut` since we will put the state
            // back.
            let mut state = self.inner.peek_mut(key).unwrap();
            Some(state.take())
        } else {
            tracing::info!("miss cache for join key: {:?}", key);
            None
        }
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
        self.metrics.total_lookup_count += 1;
        let state = if self.inner.contains(key) {
            // Do not update the LRU statistics here with `peek_mut` since we will put the state
            // back.
            let mut state = self.inner.peek_mut(key).unwrap();
            state.take()
        } else {
            self.metrics.lookup_miss_count += 1;
            self.fetch_cached_state(key).await?.into()
        };
        Ok(state)
    }

    #[try_stream(ok = (Vec<u8>, JoinRow<OwnedRow>), error = StreamExecutorError)]
    pub async fn fetch_matched_rows<'a>(&'a self, key: &'a K) {
        tracing::debug!("fetching matched rows for join key: {:?}", key);
        let key = key.deserialize(&self.join_key_data_types)?;
        let sub_range: &(Bound<OwnedRow>, Bound<OwnedRow>) = &(Bound::Unbounded, Bound::Unbounded);
        let table_iter = self
            .state
            .table
            .iter_with_prefix(&key, sub_range, PrefetchOptions::default())
            .await?;

        #[for_await]
        for entry in table_iter {
            let row = entry?;
            let pk = row
                .as_ref()
                .project(&self.state.pk_indices)
                .memcmp_serialize(&self.pk_serializer);
            tracing::debug!(
                "found matching rows for join key: {:?}, row: {:?}",
                key,
                row
            );
            yield (pk, JoinRow::new(row.into_owned_row(), 0));
        }
    }

    /// Fetch cache from the state store. Should only be called if the key does not exist in memory.
    /// Will return a empty `JoinEntryState` even when state does not exist in remote.
    async fn fetch_cached_state(&self, key: &K) -> StreamExecutorResult<JoinEntryState> {
        let key = key.deserialize(&self.join_key_data_types)?;

        let mut entry_state = JoinEntryState::default();

        if self.need_degree_table {
            let sub_range: &(Bound<OwnedRow>, Bound<OwnedRow>) =
                &(Bound::Unbounded, Bound::Unbounded);
            let table_iter_fut =
                self.state
                    .table
                    .iter_with_prefix(&key, sub_range, PrefetchOptions::default());
            let degree_table_iter_fut = self.degree_state.table.iter_with_prefix(
                &key,
                sub_range,
                PrefetchOptions::default(),
            );

            let (table_iter, degree_table_iter) =
                try_join(table_iter_fut, degree_table_iter_fut).await?;

            let mut pinned_table_iter = std::pin::pin!(table_iter);
            let mut pinned_degree_table_iter = std::pin::pin!(degree_table_iter);

            // For better tolerating inconsistent stream, we have to first buffer all rows and
            // degree rows, and check the number of them, then iterate on them.
            let mut rows = vec![];
            let mut degree_rows = vec![];
            let mut inconsistency_happened = false;
            loop {
                let (row, degree_row) =
                    join(pinned_table_iter.next(), pinned_degree_table_iter.next()).await;
                let (row, degree_row) = match (row, degree_row) {
                    (None, None) => break,
                    (None, Some(_)) => {
                        inconsistency_happened = true;
                        consistency_panic!(
                            "mismatched row and degree table of join key: {:?}, degree table has more rows",
                            &key
                        );
                        break;
                    }
                    (Some(_), None) => {
                        inconsistency_happened = true;
                        consistency_panic!(
                            "mismatched row and degree table of join key: {:?}, input table has more rows",
                            &key
                        );
                        break;
                    }
                    (Some(r), Some(d)) => (r, d),
                };

                let row = row?;
                let degree_row = degree_row?;
                rows.push(row);
                degree_rows.push(degree_row);
            }

            if inconsistency_happened {
                // Pk-based row-degree pairing.
                assert_ne!(rows.len(), degree_rows.len());

                let row_iter = stream::iter(rows.into_iter()).peekable();
                let degree_row_iter = stream::iter(degree_rows.into_iter()).peekable();
                pin_mut!(row_iter);
                pin_mut!(degree_row_iter);

                loop {
                    match join(row_iter.as_mut().peek(), degree_row_iter.as_mut().peek()).await {
                        (None, _) | (_, None) => break,
                        (Some(row), Some(degree_row)) => match row.key().cmp(degree_row.key()) {
                            Ordering::Greater => {
                                degree_row_iter.next().await;
                            }
                            Ordering::Less => {
                                row_iter.next().await;
                            }
                            Ordering::Equal => {
                                let row = row_iter.next().await.unwrap();
                                let degree_row = degree_row_iter.next().await.unwrap();

                                let pk = row
                                    .as_ref()
                                    .project(&self.state.pk_indices)
                                    .memcmp_serialize(&self.pk_serializer);
                                let degree_i64 = degree_row
                                    .datum_at(degree_row.len() - 1)
                                    .expect("degree should not be NULL");
                                entry_state
                                    .insert(
                                        pk,
                                        JoinRow::new(row.row(), degree_i64.into_int64() as u64)
                                            .encode(),
                                    )
                                    .with_context(|| self.state.error_context(row.row()))?;
                            }
                        },
                    }
                }
            } else {
                // 1 to 1 row-degree pairing.
                // Actually it's possible that both the input data table and the degree table missed
                // some equal number of rows, but let's ignore this case because it should be rare.

                assert_eq!(rows.len(), degree_rows.len());

                #[for_await]
                for (row, degree_row) in
                    stream::iter(rows.into_iter().zip_eq_fast(degree_rows.into_iter()))
                {
                    let pk1 = row.key();
                    let pk2 = degree_row.key();
                    debug_assert_eq!(
                        pk1, pk2,
                        "mismatched pk in degree table: pk1: {pk1:?}, pk2: {pk2:?}",
                    );
                    let pk = row
                        .as_ref()
                        .project(&self.state.pk_indices)
                        .memcmp_serialize(&self.pk_serializer);
                    let degree_i64 = degree_row
                        .datum_at(degree_row.len() - 1)
                        .expect("degree should not be NULL");
                    entry_state
                        .insert(
                            pk,
                            JoinRow::new(row.row(), degree_i64.into_int64() as u64).encode(),
                        )
                        .with_context(|| self.state.error_context(row.row()))?;
                }
            }
        } else {
            let sub_range: &(Bound<OwnedRow>, Bound<OwnedRow>) =
                &(Bound::Unbounded, Bound::Unbounded);
            let table_iter = self
                .state
                .table
                .iter_with_prefix(&key, sub_range, PrefetchOptions::default())
                .await?;

            #[for_await]
            for entry in table_iter {
                let row = entry?;
                let pk = row
                    .as_ref()
                    .project(&self.state.pk_indices)
                    .memcmp_serialize(&self.pk_serializer);
                entry_state
                    .insert(pk, JoinRow::new(row.row(), 0).encode())
                    .with_context(|| self.state.error_context(row.row()))?;
            }
        };

        Ok(entry_state)
    }

    pub fn error_context(&self, row: &impl Row) -> String {
        self.state.error_context(row)
    }

    pub async fn flush(&mut self, epoch: EpochPair) -> StreamExecutorResult<()> {
        self.metrics.flush();
        self.state.table.commit(epoch).await?;
        self.degree_state.table.commit(epoch).await?;
        Ok(())
    }

    pub async fn try_flush(&mut self) -> StreamExecutorResult<()> {
        self.state.table.try_flush().await?;
        self.degree_state.table.try_flush().await?;
        Ok(())
    }

    /// Insert a join row
    #[allow(clippy::unused_async)]
    pub async fn insert(&mut self, key: &K, value: JoinRow<impl Row>) -> StreamExecutorResult<()> {
        let pk = (&value.row)
            .project(&self.state.pk_indices)
            .memcmp_serialize(&self.pk_serializer);

        // TODO(yuhao): avoid this `contains`.
        // https://github.com/risingwavelabs/risingwave/issues/9233
        if self.inner.contains(key) {
            // Update cache
            let mut entry = self.inner.get_mut(key).unwrap();
            entry
                .insert(pk, value.encode())
                .with_context(|| self.state.error_context(&value.row))?;
        } else if self.pk_contained_in_jk {
            // Refill cache when the join key exist in neither cache or storage.
            self.metrics.insert_cache_miss_count += 1;
            let mut state = JoinEntryState::default();
            state
                .insert(pk, value.encode())
                .with_context(|| self.state.error_context(&value.row))?;
            self.update_state(key, state.into());
        }

        // Update the flush buffer.
        let (row, degree) = value.to_table_rows(&self.state.order_key_indices);
        self.state.table.insert(row);
        self.degree_state.table.insert(degree);
        Ok(())
    }

    /// Insert a row.
    /// Used when the side does not need to update degree.
    #[allow(clippy::unused_async)]
    pub async fn insert_row(&mut self, key: &K, value: impl Row) -> StreamExecutorResult<()> {
        let join_row = JoinRow::new(&value, 0);
        let pk = (&value)
            .project(&self.state.pk_indices)
            .memcmp_serialize(&self.pk_serializer);

        // TODO(yuhao): avoid this `contains`.
        // https://github.com/risingwavelabs/risingwave/issues/9233
        if self.inner.contains(key) {
            // Update cache
            let mut entry = self.inner.get_mut(key).unwrap();
            entry
                .insert(pk, join_row.encode())
                .with_context(|| self.state.error_context(&value))?;
        } else if self.pk_contained_in_jk {
            // Refill cache when the join key exist in neither cache or storage.
            self.metrics.insert_cache_miss_count += 1;
            let mut state = JoinEntryState::default();
            state
                .insert(pk, join_row.encode())
                .with_context(|| self.state.error_context(&value))?;
            self.update_state(key, state.into());
        }

        // Update the flush buffer.
        self.state.table.insert(value);
        Ok(())
    }

    /// Delete a join row
    pub fn delete(&mut self, key: &K, value: JoinRow<impl Row>) -> StreamExecutorResult<()> {
        if let Some(mut entry) = self.inner.get_mut(key) {
            let pk = (&value.row)
                .project(&self.state.pk_indices)
                .memcmp_serialize(&self.pk_serializer);
            entry
                .remove(pk)
                .with_context(|| self.state.error_context(&value.row))?;
        }

        // If no cache maintained, only update the state table.
        let (row, degree) = value.to_table_rows(&self.state.order_key_indices);
        self.state.table.delete(row);
        self.degree_state.table.delete(degree);
        Ok(())
    }

    /// Delete a row
    /// Used when the side does not need to update degree.
    pub fn delete_row(&mut self, key: &K, value: impl Row) -> StreamExecutorResult<()> {
        if let Some(mut entry) = self.inner.get_mut(key) {
            let pk = (&value)
                .project(&self.state.pk_indices)
                .memcmp_serialize(&self.pk_serializer);
            entry
                .remove(pk)
                .with_context(|| self.state.error_context(&value))?;
        }

        // If no cache maintained, only update the state table.
        self.state.table.delete(value);
        Ok(())
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
            *d = d.checked_sub(1).unwrap_or_else(|| {
                consistency_panic!("Tried to decrement zero join row degree");
                0
            });
        })
    }

    /// Evict the cache.
    pub fn evict(&mut self) {
        self.inner.evict();
    }

    /// Cached entry count for this hash table.
    pub fn entry_count(&self) -> usize {
        self.inner.len()
    }

    pub fn null_matched(&self) -> &K::Bitmap {
        &self.null_matched
    }

    pub fn table_id(&self) -> u32 {
        self.state.table.table_id()
    }

    pub fn join_key_data_types(&self) -> &[DataType] {
        &self.join_key_data_types
    }
}

use risingwave_common::array::{Op, RowRef};
use risingwave_common_estimate_size::KvSize;
use risingwave_expr::expr::NonStrictExpression;
use thiserror::Error;

use super::*;
use crate::executor::join::builder::JoinChunkBuilder;
use crate::executor::prelude::try_stream;
use crate::executor::Watermark;

/// We manages a `HashMap` in memory for all entries belonging to a join key.
/// When evicted, `cached` does not hold any entries.
///
/// If a `JoinEntryState` exists for a join key, the all records under this
/// join key will be presented in the cache.
#[derive(Default)]
pub struct JoinEntryState {
    /// The full copy of the state.
    cached: join_row_set::JoinRowSet<PkType, StateValueType>,
    kv_heap_size: KvSize,
}

impl EstimateSize for JoinEntryState {
    fn estimated_heap_size(&self) -> usize {
        // TODO: Add btreemap internal size.
        // https://github.com/risingwavelabs/risingwave/issues/9713
        self.kv_heap_size.size()
    }
}

#[derive(Error, Debug)]
pub enum JoinEntryError {
    #[error("double inserting a join state entry")]
    OccupiedError,
    #[error("removing a join state entry but it is not in the cache")]
    RemoveError,
}

impl JoinEntryState {
    /// Insert into the cache.
    pub fn insert(
        &mut self,
        key: PkType,
        value: StateValueType,
    ) -> Result<&mut StateValueType, JoinEntryError> {
        let mut removed = false;
        if !enable_strict_consistency() {
            // strict consistency is off, let's remove existing (if any) first
            if let Some(old_value) = self.cached.remove(&key) {
                self.kv_heap_size.sub(&key, &old_value);
                removed = true;
            }
        }

        self.kv_heap_size.add(&key, &value);

        let ret = self.cached.try_insert(key.clone(), value);

        if !enable_strict_consistency() {
            assert!(ret.is_ok(), "we have removed existing entry, if any");
            if removed {
                // if not silent, we should log the error
                consistency_error!(?key, "double inserting a join state entry");
            }
        }

        ret.map_err(|_| JoinEntryError::OccupiedError)
    }

    /// Delete from the cache.
    pub fn remove(&mut self, pk: PkType) -> Result<(), JoinEntryError> {
        if let Some(value) = self.cached.remove(&pk) {
            self.kv_heap_size.sub(&pk, &value);
            Ok(())
        } else if enable_strict_consistency() {
            Err(JoinEntryError::RemoveError)
        } else {
            consistency_error!(?pk, "removing a join state entry but it's not in the cache");
            Ok(())
        }
    }

    /// Note: the first item in the tuple is the mutable reference to the value in this entry, while
    /// the second item is the decoded value. To mutate the degree, one **must not** forget to apply
    /// the changes to the first item.
    ///
    /// WARNING: Should not change the heap size of `StateValueType` with the mutable reference.
    pub fn values_mut<'a>(
        &'a mut self,
        data_types: &'a [DataType],
    ) -> impl Iterator<
        Item = (
            &'a mut StateValueType,
            StreamExecutorResult<JoinRow<OwnedRow>>,
        ),
    > + 'a {
        self.cached.values_mut().map(|encoded| {
            let decoded = encoded.decode(data_types);
            (encoded, decoded)
        })
    }

    pub fn len(&self) -> usize {
        self.cached.len()
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use risingwave_common::array::*;
    use risingwave_common::util::iter_util::ZipEqDebug;

    use super::*;

    fn insert_chunk(
        managed_state: &mut JoinEntryState,
        pk_indices: &[usize],
        data_chunk: &DataChunk,
    ) {
        for row_ref in data_chunk.rows() {
            let row: OwnedRow = row_ref.into_owned_row();
            let value_indices = (0..row.len() - 1).collect_vec();
            let pk = pk_indices.iter().map(|idx| row[*idx].clone()).collect_vec();
            // Pk is only a `i64` here, so encoding method does not matter.
            let pk = OwnedRow::new(pk).project(&value_indices).value_serialize();
            let join_row = JoinRow { row, degree: 0 };
            managed_state.insert(pk, join_row.encode()).unwrap();
        }
    }

    fn check(
        managed_state: &mut JoinEntryState,
        col_types: &[DataType],
        col1: &[i64],
        col2: &[i64],
    ) {
        for ((_, matched_row), (d1, d2)) in managed_state
            .values_mut(col_types)
            .zip_eq_debug(col1.iter().zip_eq_debug(col2.iter()))
        {
            let matched_row = matched_row.unwrap();
            assert_eq!(matched_row.row[0], Some(ScalarImpl::Int64(*d1)));
            assert_eq!(matched_row.row[1], Some(ScalarImpl::Int64(*d2)));
            assert_eq!(matched_row.degree, 0);
        }
    }

    #[tokio::test]
    async fn test_managed_all_or_none_state() {
        let mut managed_state = JoinEntryState::default();
        let col_types = vec![DataType::Int64, DataType::Int64];
        let pk_indices = [0];

        let col1 = [3, 2, 1];
        let col2 = [4, 5, 6];
        let data_chunk1 = DataChunk::from_pretty(
            "I I
             3 4
             2 5
             1 6",
        );

        // `Vec` in state
        insert_chunk(&mut managed_state, &pk_indices, &data_chunk1);
        check(&mut managed_state, &col_types, &col1, &col2);

        // `BtreeMap` in state
        let col1 = [1, 2, 3, 4, 5];
        let col2 = [6, 5, 4, 9, 8];
        let data_chunk2 = DataChunk::from_pretty(
            "I I
             5 8
             4 9",
        );
        insert_chunk(&mut managed_state, &pk_indices, &data_chunk2);
        check(&mut managed_state, &col_types, &col1, &col2);
    }
}

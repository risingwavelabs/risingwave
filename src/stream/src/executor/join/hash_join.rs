// Copyright 2025 RisingWave Labs
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
use std::ops::{Bound, Deref, DerefMut, RangeBounds};
use std::sync::Arc;

use anyhow::{Context, anyhow};
use futures::future::{join, try_join};
use futures::{StreamExt, pin_mut, stream};
use futures_async_stream::for_await;
use join_row_set::JoinRowSet;
use local_stats_alloc::{SharedStatsAlloc, StatsAlloc};
use risingwave_common::bitmap::Bitmap;
use risingwave_common::hash::{HashKey, PrecomputedBuildHasher};
use risingwave_common::metrics::LabelGuardedIntCounter;
use risingwave_common::row::{OwnedRow, Row, RowExt, once};
use risingwave_common::types::{DataType, ScalarImpl};
use risingwave_common::util::epoch::EpochPair;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_common::util::row_serde::OrderedRowSerde;
use risingwave_common::util::sort_util::OrderType;
use risingwave_common_estimate_size::EstimateSize;
use risingwave_storage::StateStore;
use risingwave_storage::store::PrefetchOptions;
use thiserror_ext::AsReport;

use super::row::{DegreeType, EncodedJoinRow};
use crate::cache::ManagedLruCache;
use crate::common::metrics::MetricsInfo;
use crate::common::table::state_table::{StateTable, StateTablePostCommit};
use crate::consistency::{consistency_error, consistency_panic, enable_strict_consistency};
use crate::executor::StreamExecutorError;
use crate::executor::error::StreamExecutorResult;
use crate::executor::join::row::JoinRow;
use crate::executor::monitor::StreamingMetrics;
use crate::task::{ActorId, AtomicU64Ref, FragmentId};

/// Memcomparable encoding.
type PkType = Vec<u8>;
type InequalKeyType = Vec<u8>;

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

pub(crate) enum CacheResult {
    NeverMatch,         // Will never match, will not be in cache at all.
    Miss,               // Cache-miss
    Hit(HashValueType), // Cache-hit
}

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
    join_lookup_total_count_metric: LabelGuardedIntCounter,
    join_lookup_miss_count_metric: LabelGuardedIntCounter,
    join_insert_cache_miss_count_metrics: LabelGuardedIntCounter,
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

/// Inequality key description for `AsOf` join.
struct InequalityKeyDesc {
    idx: usize,
    serializer: OrderedRowSerde,
}

impl InequalityKeyDesc {
    /// Serialize the inequality key from a row.
    pub fn serialize_inequal_key_from_row(&self, row: impl Row) -> InequalKeyType {
        let indices = vec![self.idx];
        let inequality_key = row.project(&indices);
        inequality_key.memcmp_serialize(&self.serializer)
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
    /// - Inner: neither side.
    ///
    /// Should be set to `None` if `need_degree_table` was set to `false`.
    ///
    /// The degree of each row will tell us if we need to emit `NULL` for the row.
    /// For instance, given `lhs LEFT JOIN rhs`,
    /// If the degree of a row in `lhs` is 0, it means the row does not have a match in `rhs`.
    /// If the degree of a row in `lhs` is 2, it means the row has two matches in `rhs`.
    /// Now, when emitting the result of the join, we need to emit `NULL` for the row in `lhs` if
    /// the degree is 0.
    ///
    /// Why don't just use a boolean value instead of a degree count?
    /// Consider the case where we delete a matched record from `rhs`.
    /// Since we can delete a record,
    /// there must have been a record in `rhs` that matched the record in `lhs`.
    /// So this value is `true`.
    /// But we don't know how many records are matched after removing this record,
    /// since we only stored a boolean value rather than the count.
    /// Hence we need to store the count of matched records.
    degree_state: Option<TableInner<S>>,
    // TODO(kwannoel): Make this `const` instead.
    /// If degree table is need
    need_degree_table: bool,
    /// Pk is part of the join key.
    pk_contained_in_jk: bool,
    /// Inequality key description for `AsOf` join.
    inequality_key_desc: Option<InequalityKeyDesc>,
    /// Metrics of the hash map
    metrics: JoinHashMapMetrics,
}

impl<K: HashKey, S: StateStore> JoinHashMap<K, S> {
    pub(crate) fn get_degree_state_mut_ref(&mut self) -> (&[usize], &mut Option<TableInner<S>>) {
        (&self.state.order_key_indices, &mut self.degree_state)
    }

    /// NOTE(kwannoel): This allows us to concurrently stream records from the `state_table`,
    /// and update the degree table, without using `unsafe` code.
    ///
    /// This is because we obtain separate references to separate parts of the `JoinHashMap`,
    /// instead of reusing the same reference to `JoinHashMap` for concurrent read access to `state_table`,
    /// and write access to the degree table.
    pub(crate) async fn fetch_matched_rows_and_get_degree_table_ref<'a>(
        &'a mut self,
        key: &'a K,
    ) -> StreamExecutorResult<(
        impl Stream<Item = StreamExecutorResult<(PkType, JoinRow<OwnedRow>)>> + 'a,
        &'a [usize],
        &'a mut Option<TableInner<S>>,
    )> {
        let degree_state = &mut self.degree_state;
        let (order_key_indices, pk_indices, state_table) = (
            &self.state.order_key_indices,
            &self.state.pk_indices,
            &mut self.state.table,
        );
        let degrees = if let Some(degree_state) = degree_state {
            Some(fetch_degrees(key, &self.join_key_data_types, &degree_state.table).await?)
        } else {
            None
        };
        let stream = into_stream(
            &self.join_key_data_types,
            pk_indices,
            &self.pk_serializer,
            state_table,
            key,
            degrees,
        );
        Ok((stream, order_key_indices, &mut self.degree_state))
    }
}

#[try_stream(ok = (PkType, JoinRow<OwnedRow>), error = StreamExecutorError)]
pub(crate) async fn into_stream<'a, K: HashKey, S: StateStore>(
    join_key_data_types: &'a [DataType],
    pk_indices: &'a [usize],
    pk_serializer: &'a OrderedRowSerde,
    state_table: &'a StateTable<S>,
    key: &'a K,
    degrees: Option<Vec<DegreeType>>,
) {
    let sub_range: &(Bound<OwnedRow>, Bound<OwnedRow>) = &(Bound::Unbounded, Bound::Unbounded);
    let decoded_key = key.deserialize(join_key_data_types)?;
    let table_iter = state_table
        .iter_with_prefix(&decoded_key, sub_range, PrefetchOptions::default())
        .await?;

    #[for_await]
    for (i, entry) in table_iter.enumerate() {
        let encoded_row = entry?;
        let encoded_pk = encoded_row
            .as_ref()
            .project(pk_indices)
            .memcmp_serialize(pk_serializer);
        let join_row = JoinRow::new(
            encoded_row.into_owned_row(),
            degrees.as_ref().map_or(0, |d| d[i]),
        );
        yield (encoded_pk, join_row);
    }
}

/// We use this to fetch ALL degrees into memory.
/// We use this instead of a streaming interface.
/// It is necessary because we must update the `degree_state_table` concurrently.
/// If we obtain the degrees in a stream,
/// we will need to hold an immutable reference to the state table for the entire lifetime,
/// preventing us from concurrently updating the state table.
///
/// The cost of fetching all degrees upfront is acceptable. We currently already do so
/// in `fetch_cached_state`.
/// The memory use should be limited since we only store a u64.
///
/// Let's say we have amplification of 1B, we will have 1B * 8 bytes ~= 8GB
///
/// We can also have further optimization, to permit breaking the streaming update,
/// to flush the in-memory degrees, if this is proven to have high memory consumption.
///
/// TODO(kwannoel): Perhaps we can cache these separately from matched rows too.
/// Because matched rows may occupy a larger capacity.
///
/// Argument for this:
/// We only hit this when cache miss. When cache miss, we will have this as one off cost.
/// Keeping this cached separately from matched rows is beneficial.
/// Then we can evict matched rows, without touching the degrees.
async fn fetch_degrees<K: HashKey, S: StateStore>(
    key: &K,
    join_key_data_types: &[DataType],
    degree_state_table: &StateTable<S>,
) -> StreamExecutorResult<Vec<DegreeType>> {
    let key = key.deserialize(join_key_data_types)?;
    let mut degrees = vec![];
    let sub_range: &(Bound<OwnedRow>, Bound<OwnedRow>) = &(Bound::Unbounded, Bound::Unbounded);
    let table_iter = degree_state_table
        .iter_with_prefix(key, sub_range, PrefetchOptions::default())
        .await
        .unwrap();
    #[for_await]
    for entry in table_iter {
        let degree_row = entry?;
        let degree_i64 = degree_row
            .datum_at(degree_row.len() - 1)
            .expect("degree should not be NULL");
        degrees.push(degree_i64.into_int64() as u64);
    }
    Ok(degrees)
}

// NOTE(kwannoel): This is not really specific to `TableInner`.
// A degree table is `TableInner`, a `TableInner` might not be a degree table.
// Hence we don't specify it in its impl block.
pub(crate) fn update_degree<S: StateStore, const INCREMENT: bool>(
    order_key_indices: &[usize],
    degree_state: &mut TableInner<S>,
    matched_row: &mut JoinRow<OwnedRow>,
) {
    let old_degree_row = matched_row
        .row
        .as_ref()
        .project(order_key_indices)
        .chain(once(Some(ScalarImpl::Int64(matched_row.degree as i64))));
    if INCREMENT {
        matched_row.degree += 1;
    } else {
        // DECREMENT
        matched_row.degree -= 1;
    }
    let new_degree_row = matched_row
        .row
        .as_ref()
        .project(order_key_indices)
        .chain(once(Some(ScalarImpl::Int64(matched_row.degree as i64))));
    degree_state.table.update(old_degree_row, new_degree_row);
}

pub struct TableInner<S: StateStore> {
    /// Indices of the (cache) pk in a state row
    pk_indices: Vec<usize>,
    /// Indices of the join key in a state row
    join_key_indices: Vec<usize>,
    /// The order key of the join side has the following format:
    /// | `join_key` ... | pk ... |
    /// Where `join_key` contains all the columns not in the pk.
    /// It should be a superset of the pk.
    order_key_indices: Vec<usize>,
    pub(crate) table: StateTable<S>,
}

impl<S: StateStore> TableInner<S> {
    pub fn new(pk_indices: Vec<usize>, join_key_indices: Vec<usize>, table: StateTable<S>) -> Self {
        let order_key_indices = table.pk_indices().to_vec();
        Self {
            pk_indices,
            join_key_indices,
            order_key_indices,
            table,
        }
    }

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
        degree_state: Option<TableInner<S>>,
        null_matched: K::Bitmap,
        pk_contained_in_jk: bool,
        inequality_key_idx: Option<usize>,
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

        let inequality_key_desc = inequality_key_idx.map(|idx| {
            let serializer = OrderedRowSerde::new(
                vec![state_all_data_types[idx].clone()],
                vec![OrderType::ascending()],
            );
            InequalityKeyDesc { idx, serializer }
        });

        let join_table_id = state_table.table_id();
        let state = TableInner {
            pk_indices: state_pk_indices,
            join_key_indices: state_join_key_indices,
            order_key_indices: state_table.pk_indices().to_vec(),
            table: state_table,
        };

        let need_degree_table = degree_state.is_some();

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
            inequality_key_desc,
            metrics: JoinHashMapMetrics::new(&metrics, actor_id, fragment_id, side, join_table_id),
        }
    }

    pub async fn init(&mut self, epoch: EpochPair) -> StreamExecutorResult<()> {
        self.state.table.init_epoch(epoch).await?;
        if let Some(degree_state) = &mut self.degree_state {
            degree_state.table.init_epoch(epoch).await?;
        }
        Ok(())
    }
}

impl<K: HashKey, S: StateStore> JoinHashMapPostCommit<'_, K, S> {
    pub async fn post_yield_barrier(
        self,
        vnode_bitmap: Option<Arc<Bitmap>>,
    ) -> StreamExecutorResult<Option<bool>> {
        let cache_may_stale = self.state.post_yield_barrier(vnode_bitmap.clone()).await?;
        if let Some(degree_state) = self.degree_state {
            let _ = degree_state.post_yield_barrier(vnode_bitmap).await?;
        }
        let cache_may_stale = cache_may_stale.map(|(_, cache_may_stale)| cache_may_stale);
        if cache_may_stale.unwrap_or(false) {
            self.inner.clear();
        }
        Ok(cache_may_stale)
    }
}
impl<K: HashKey, S: StateStore> JoinHashMap<K, S> {
    pub fn update_watermark(&mut self, watermark: ScalarImpl) {
        // TODO: remove data in cache.
        self.state.table.update_watermark(watermark.clone());
        if let Some(degree_state) = &mut self.degree_state {
            degree_state.table.update_watermark(watermark);
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
    pub fn take_state_opt(&mut self, key: &K) -> CacheResult {
        self.metrics.total_lookup_count += 1;
        if self.inner.contains(key) {
            tracing::trace!("hit cache for join key: {:?}", key);
            // Do not update the LRU statistics here with `peek_mut` since we will put the state
            // back.
            let mut state = self.inner.peek_mut(key).unwrap();
            CacheResult::Hit(state.take())
        } else {
            tracing::trace!("miss cache for join key: {:?}", key);
            CacheResult::Miss
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
    pub async fn take_state(&mut self, key: &K) -> StreamExecutorResult<HashValueType> {
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

    /// Fetch cache from the state store. Should only be called if the key does not exist in memory.
    /// Will return a empty `JoinEntryState` even when state does not exist in remote.
    async fn fetch_cached_state(&self, key: &K) -> StreamExecutorResult<JoinEntryState> {
        let key = key.deserialize(&self.join_key_data_types)?;

        let mut entry_state = JoinEntryState::default();

        if self.need_degree_table {
            let sub_range: &(Bound<OwnedRow>, Bound<OwnedRow>) =
                &(Bound::Unbounded, Bound::Unbounded);
            let table_iter_fut = self.state.table.iter_keyed_row_with_prefix(
                &key,
                sub_range,
                PrefetchOptions::default(),
            );
            let degree_state = self.degree_state.as_ref().unwrap();
            let degree_table_iter_fut = degree_state.table.iter_keyed_row_with_prefix(
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
                                let inequality_key = self
                                    .inequality_key_desc
                                    .as_ref()
                                    .map(|desc| desc.serialize_inequal_key_from_row(row.row()));
                                entry_state
                                    .insert(
                                        pk,
                                        JoinRow::new(row.row(), degree_i64.into_int64() as u64)
                                            .encode(),
                                        inequality_key,
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
                    let inequality_key = self
                        .inequality_key_desc
                        .as_ref()
                        .map(|desc| desc.serialize_inequal_key_from_row(row.row()));
                    let degree_i64 = degree_row
                        .datum_at(degree_row.len() - 1)
                        .expect("degree should not be NULL");
                    entry_state
                        .insert(
                            pk,
                            JoinRow::new(row.row(), degree_i64.into_int64() as u64).encode(),
                            inequality_key,
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
                .iter_keyed_row_with_prefix(&key, sub_range, PrefetchOptions::default())
                .await?;

            #[for_await]
            for entry in table_iter {
                let row = entry?;
                let pk = row
                    .as_ref()
                    .project(&self.state.pk_indices)
                    .memcmp_serialize(&self.pk_serializer);
                let inequality_key = self
                    .inequality_key_desc
                    .as_ref()
                    .map(|desc| desc.serialize_inequal_key_from_row(row.row()));
                entry_state
                    .insert(pk, JoinRow::new(row.row(), 0).encode(), inequality_key)
                    .with_context(|| self.state.error_context(row.row()))?;
            }
        };

        Ok(entry_state)
    }

    pub async fn flush(
        &mut self,
        epoch: EpochPair,
    ) -> StreamExecutorResult<JoinHashMapPostCommit<'_, K, S>> {
        self.metrics.flush();
        let state_post_commit = self.state.table.commit(epoch).await?;
        let degree_state_post_commit = if let Some(degree_state) = &mut self.degree_state {
            Some(degree_state.table.commit(epoch).await?)
        } else {
            None
        };
        Ok(JoinHashMapPostCommit {
            state: state_post_commit,
            degree_state: degree_state_post_commit,
            inner: &mut self.inner,
        })
    }

    pub async fn try_flush(&mut self) -> StreamExecutorResult<()> {
        self.state.table.try_flush().await?;
        if let Some(degree_state) = &mut self.degree_state {
            degree_state.table.try_flush().await?;
        }
        Ok(())
    }

    pub fn insert_handle_degree(
        &mut self,
        key: &K,
        value: JoinRow<impl Row>,
    ) -> StreamExecutorResult<()> {
        if self.need_degree_table {
            self.insert(key, value)
        } else {
            self.insert_row(key, value.row)
        }
    }

    /// Insert a join row
    pub fn insert(&mut self, key: &K, value: JoinRow<impl Row>) -> StreamExecutorResult<()> {
        let pk = self.serialize_pk_from_row(&value.row);

        let inequality_key = self
            .inequality_key_desc
            .as_ref()
            .map(|desc| desc.serialize_inequal_key_from_row(&value.row));

        // TODO(yuhao): avoid this `contains`.
        // https://github.com/risingwavelabs/risingwave/issues/9233
        if self.inner.contains(key) {
            // Update cache
            let mut entry = self.inner.get_mut(key).unwrap();
            entry
                .insert(pk, value.encode(), inequality_key)
                .with_context(|| self.state.error_context(&value.row))?;
        } else if self.pk_contained_in_jk {
            // Refill cache when the join key exist in neither cache or storage.
            self.metrics.insert_cache_miss_count += 1;
            let mut entry = JoinEntryState::default();
            entry
                .insert(pk, value.encode(), inequality_key)
                .with_context(|| self.state.error_context(&value.row))?;
            self.update_state(key, entry.into());
        }

        // Update the flush buffer.
        if let Some(degree_state) = self.degree_state.as_mut() {
            let (row, degree) = value.to_table_rows(&self.state.order_key_indices);
            self.state.table.insert(row);
            degree_state.table.insert(degree);
        } else {
            self.state.table.insert(value.row);
        }
        Ok(())
    }

    /// Insert a row.
    /// Used when the side does not need to update degree.
    pub fn insert_row(&mut self, key: &K, value: impl Row) -> StreamExecutorResult<()> {
        let join_row = JoinRow::new(&value, 0);
        self.insert(key, join_row)?;
        Ok(())
    }

    pub fn delete_handle_degree(
        &mut self,
        key: &K,
        value: JoinRow<impl Row>,
    ) -> StreamExecutorResult<()> {
        if self.need_degree_table {
            self.delete(key, value)
        } else {
            self.delete_row(key, value.row)
        }
    }

    /// Delete a join row
    pub fn delete(&mut self, key: &K, value: JoinRow<impl Row>) -> StreamExecutorResult<()> {
        if let Some(mut entry) = self.inner.get_mut(key) {
            let pk = (&value.row)
                .project(&self.state.pk_indices)
                .memcmp_serialize(&self.pk_serializer);
            let inequality_key = self
                .inequality_key_desc
                .as_ref()
                .map(|desc| desc.serialize_inequal_key_from_row(&value.row));
            entry
                .remove(pk, inequality_key.as_ref())
                .with_context(|| self.state.error_context(&value.row))?;
        }

        // If no cache maintained, only update the state table.
        let (row, degree) = value.to_table_rows(&self.state.order_key_indices);
        self.state.table.delete(row);
        let degree_state = self.degree_state.as_mut().unwrap();
        degree_state.table.delete(degree);
        Ok(())
    }

    /// Delete a row
    /// Used when the side does not need to update degree.
    pub fn delete_row(&mut self, key: &K, value: impl Row) -> StreamExecutorResult<()> {
        if let Some(mut entry) = self.inner.get_mut(key) {
            let pk = (&value)
                .project(&self.state.pk_indices)
                .memcmp_serialize(&self.pk_serializer);

            let inequality_key = self
                .inequality_key_desc
                .as_ref()
                .map(|desc| desc.serialize_inequal_key_from_row(&value));
            entry
                .remove(pk, inequality_key.as_ref())
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

    /// Return true if the inequality key is null.
    /// # Panics
    /// Panics if the inequality key is not set.
    pub fn check_inequal_key_null(&self, row: &impl Row) -> bool {
        let desc = self.inequality_key_desc.as_ref().unwrap();
        row.datum_at(desc.idx).is_none()
    }

    /// Serialize the inequality key from a row.
    /// # Panics
    /// Panics if the inequality key is not set.
    pub fn serialize_inequal_key_from_row(&self, row: impl Row) -> InequalKeyType {
        self.inequality_key_desc
            .as_ref()
            .unwrap()
            .serialize_inequal_key_from_row(&row)
    }

    pub fn serialize_pk_from_row(&self, row: impl Row) -> PkType {
        row.project(&self.state.pk_indices)
            .memcmp_serialize(&self.pk_serializer)
    }
}

#[must_use]
pub struct JoinHashMapPostCommit<'a, K: HashKey, S: StateStore> {
    state: StateTablePostCommit<'a, S>,
    degree_state: Option<StateTablePostCommit<'a, S>>,
    inner: &'a mut JoinHashMapInner<K>,
}

use risingwave_common_estimate_size::KvSize;
use thiserror::Error;

use super::*;
use crate::executor::prelude::{Stream, try_stream};

/// We manages a `HashMap` in memory for all entries belonging to a join key.
/// When evicted, `cached` does not hold any entries.
///
/// If a `JoinEntryState` exists for a join key, the all records under this
/// join key will be presented in the cache.
#[derive(Default)]
pub struct JoinEntryState {
    /// The full copy of the state.
    cached: JoinRowSet<PkType, StateValueType>,
    /// Index used for AS OF join. The key is inequal column value. The value is the primary key in `cached`.
    inequality_index: JoinRowSet<InequalKeyType, JoinRowSet<PkType, ()>>,
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
    Occupied,
    #[error("removing a join state entry but it is not in the cache")]
    Remove,
    #[error("retrieving a pk from the inequality index but it is not in the cache")]
    InequalIndex,
}

impl JoinEntryState {
    /// Insert into the cache.
    pub fn insert(
        &mut self,
        key: PkType,
        value: StateValueType,
        inequality_key: Option<InequalKeyType>,
    ) -> Result<&mut StateValueType, JoinEntryError> {
        let mut removed = false;
        if !enable_strict_consistency() {
            // strict consistency is off, let's remove existing (if any) first
            if let Some(old_value) = self.cached.remove(&key) {
                if let Some(inequality_key) = inequality_key.as_ref() {
                    self.remove_pk_from_inequality_index(&key, inequality_key);
                }
                self.kv_heap_size.sub(&key, &old_value);
                removed = true;
            }
        }

        self.kv_heap_size.add(&key, &value);

        if let Some(inequality_key) = inequality_key {
            self.insert_pk_to_inequality_index(key.clone(), inequality_key);
        }
        let ret = self.cached.try_insert(key.clone(), value);

        if !enable_strict_consistency() {
            assert!(ret.is_ok(), "we have removed existing entry, if any");
            if removed {
                // if not silent, we should log the error
                consistency_error!(?key, "double inserting a join state entry");
            }
        }

        ret.map_err(|_| JoinEntryError::Occupied)
    }

    /// Delete from the cache.
    pub fn remove(
        &mut self,
        pk: PkType,
        inequality_key: Option<&InequalKeyType>,
    ) -> Result<(), JoinEntryError> {
        if let Some(value) = self.cached.remove(&pk) {
            self.kv_heap_size.sub(&pk, &value);
            if let Some(inequality_key) = inequality_key {
                self.remove_pk_from_inequality_index(&pk, inequality_key);
            }
            Ok(())
        } else if enable_strict_consistency() {
            Err(JoinEntryError::Remove)
        } else {
            consistency_error!(?pk, "removing a join state entry but it's not in the cache");
            Ok(())
        }
    }

    fn remove_pk_from_inequality_index(&mut self, pk: &PkType, inequality_key: &InequalKeyType) {
        if let Some(pk_set) = self.inequality_index.get_mut(inequality_key) {
            if pk_set.remove(pk).is_none() {
                if enable_strict_consistency() {
                    panic!("removing a pk that it not in the inequality index");
                } else {
                    consistency_error!(?pk, "removing a pk that it not in the inequality index");
                };
            } else {
                self.kv_heap_size.sub(pk, &());
            }
            if pk_set.is_empty() {
                self.inequality_index.remove(inequality_key);
            }
        }
    }

    fn insert_pk_to_inequality_index(&mut self, pk: PkType, inequality_key: InequalKeyType) {
        if let Some(pk_set) = self.inequality_index.get_mut(&inequality_key) {
            let pk_size = pk.estimated_size();
            if pk_set.try_insert(pk, ()).is_err() {
                if enable_strict_consistency() {
                    panic!("inserting a pk that it already in the inequality index");
                } else {
                    consistency_error!("inserting a pk that it already in the inequality index");
                };
            } else {
                self.kv_heap_size.add_size(pk_size);
            }
        } else {
            let mut pk_set = JoinRowSet::default();
            pk_set.try_insert(pk, ()).unwrap();
            self.inequality_index
                .try_insert(inequality_key, pk_set)
                .unwrap();
        }
    }

    pub fn get(
        &self,
        pk: &PkType,
        data_types: &[DataType],
    ) -> Option<StreamExecutorResult<JoinRow<OwnedRow>>> {
        self.cached
            .get(pk)
            .map(|encoded| encoded.decode(data_types))
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

    /// Range scan the cache using the inequality index.
    pub fn range_by_inequality<'a, R>(
        &'a self,
        range: R,
        data_types: &'a [DataType],
    ) -> impl Iterator<Item = StreamExecutorResult<JoinRow<OwnedRow>>> + 'a
    where
        R: RangeBounds<InequalKeyType> + 'a,
    {
        self.inequality_index.range(range).flat_map(|(_, pk_set)| {
            pk_set
                .keys()
                .flat_map(|pk| self.get_by_indexed_pk(pk, data_types))
        })
    }

    /// Get the records whose inequality key upper bound satisfy the given bound.
    pub fn upper_bound_by_inequality<'a>(
        &'a self,
        bound: Bound<&InequalKeyType>,
        data_types: &'a [DataType],
    ) -> Option<StreamExecutorResult<JoinRow<OwnedRow>>> {
        if let Some((_, pk_set)) = self.inequality_index.upper_bound(bound) {
            if let Some(pk) = pk_set.first_key_sorted() {
                self.get_by_indexed_pk(pk, data_types)
            } else {
                panic!("pk set for a index record must has at least one element");
            }
        } else {
            None
        }
    }

    pub fn get_by_indexed_pk(
        &self,
        pk: &PkType,
        data_types: &[DataType],
    ) -> Option<StreamExecutorResult<JoinRow<OwnedRow>>>
where {
        if let Some(value) = self.cached.get(pk) {
            Some(value.decode(data_types))
        } else if enable_strict_consistency() {
            Some(Err(anyhow!(JoinEntryError::InequalIndex).into()))
        } else {
            consistency_error!(?pk, "{}", JoinEntryError::InequalIndex.as_report());
            None
        }
    }

    /// Get the records whose inequality key lower bound satisfy the given bound.
    pub fn lower_bound_by_inequality<'a>(
        &'a self,
        bound: Bound<&InequalKeyType>,
        data_types: &'a [DataType],
    ) -> Option<StreamExecutorResult<JoinRow<OwnedRow>>> {
        if let Some((_, pk_set)) = self.inequality_index.lower_bound(bound) {
            if let Some(pk) = pk_set.first_key_sorted() {
                self.get_by_indexed_pk(pk, data_types)
            } else {
                panic!("pk set for a index record must has at least one element");
            }
        } else {
            None
        }
    }

    pub fn get_first_by_inequality<'a>(
        &'a self,
        inequality_key: &InequalKeyType,
        data_types: &'a [DataType],
    ) -> Option<StreamExecutorResult<JoinRow<OwnedRow>>> {
        if let Some(pk_set) = self.inequality_index.get(inequality_key) {
            if let Some(pk) = pk_set.first_key_sorted() {
                self.get_by_indexed_pk(pk, data_types)
            } else {
                panic!("pk set for a index record must has at least one element");
            }
        } else {
            None
        }
    }

    pub fn inequality_index(&self) -> &JoinRowSet<InequalKeyType, JoinRowSet<PkType, ()>> {
        &self.inequality_index
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
        col_types: &[DataType],
        inequality_key_idx: Option<usize>,
        data_chunk: &DataChunk,
    ) {
        let pk_col_type = pk_indices
            .iter()
            .map(|idx| col_types[*idx].clone())
            .collect_vec();
        let pk_serializer =
            OrderedRowSerde::new(pk_col_type, vec![OrderType::ascending(); pk_indices.len()]);
        let inequality_key_type = inequality_key_idx.map(|idx| col_types[idx].clone());
        let inequality_key_serializer = inequality_key_type
            .map(|data_type| OrderedRowSerde::new(vec![data_type], vec![OrderType::ascending()]));
        for row_ref in data_chunk.rows() {
            let row: OwnedRow = row_ref.into_owned_row();
            let value_indices = (0..row.len() - 1).collect_vec();
            let pk = pk_indices.iter().map(|idx| row[*idx].clone()).collect_vec();
            // Pk is only a `i64` here, so encoding method does not matter.
            let pk = OwnedRow::new(pk)
                .project(&value_indices)
                .memcmp_serialize(&pk_serializer);
            let inequality_key = inequality_key_idx.map(|idx| {
                (&row)
                    .project(&[idx])
                    .memcmp_serialize(inequality_key_serializer.as_ref().unwrap())
            });
            let join_row = JoinRow { row, degree: 0 };
            managed_state
                .insert(pk, join_row.encode(), inequality_key)
                .unwrap();
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
    async fn test_managed_join_state() {
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
        insert_chunk(
            &mut managed_state,
            &pk_indices,
            &col_types,
            None,
            &data_chunk1,
        );
        check(&mut managed_state, &col_types, &col1, &col2);

        // `BtreeMap` in state
        let col1 = [1, 2, 3, 4, 5];
        let col2 = [6, 5, 4, 9, 8];
        let data_chunk2 = DataChunk::from_pretty(
            "I I
             5 8
             4 9",
        );
        insert_chunk(
            &mut managed_state,
            &pk_indices,
            &col_types,
            None,
            &data_chunk2,
        );
        check(&mut managed_state, &col_types, &col1, &col2);
    }

    #[tokio::test]
    async fn test_managed_join_state_w_inequality_index() {
        let mut managed_state = JoinEntryState::default();
        let col_types = vec![DataType::Int64, DataType::Int64];
        let pk_indices = [0];
        let inequality_key_idx = Some(1);
        let inequality_key_serializer =
            OrderedRowSerde::new(vec![DataType::Int64], vec![OrderType::ascending()]);

        let col1 = [3, 2, 1];
        let col2 = [4, 5, 5];
        let data_chunk1 = DataChunk::from_pretty(
            "I I
             3 4
             2 5
             1 5",
        );

        // `Vec` in state
        insert_chunk(
            &mut managed_state,
            &pk_indices,
            &col_types,
            inequality_key_idx,
            &data_chunk1,
        );
        check(&mut managed_state, &col_types, &col1, &col2);
        let bound = OwnedRow::new(vec![Some(ScalarImpl::Int64(5))])
            .memcmp_serialize(&inequality_key_serializer);
        let row = managed_state
            .upper_bound_by_inequality(Bound::Included(&bound), &col_types)
            .unwrap()
            .unwrap();
        assert_eq!(row.row[0], Some(ScalarImpl::Int64(1)));
        let row = managed_state
            .upper_bound_by_inequality(Bound::Excluded(&bound), &col_types)
            .unwrap()
            .unwrap();
        assert_eq!(row.row[0], Some(ScalarImpl::Int64(3)));

        // `BtreeMap` in state
        let col1 = [1, 2, 3, 4, 5];
        let col2 = [5, 5, 4, 4, 8];
        let data_chunk2 = DataChunk::from_pretty(
            "I I
             5 8
             4 4",
        );
        insert_chunk(
            &mut managed_state,
            &pk_indices,
            &col_types,
            inequality_key_idx,
            &data_chunk2,
        );
        check(&mut managed_state, &col_types, &col1, &col2);

        let bound = OwnedRow::new(vec![Some(ScalarImpl::Int64(8))])
            .memcmp_serialize(&inequality_key_serializer);
        let row = managed_state.lower_bound_by_inequality(Bound::Excluded(&bound), &col_types);
        assert!(row.is_none());
    }
}

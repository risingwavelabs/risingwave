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

//! Hash map for `AsOf` join with optional LRU cache support.
//!
//! When cache is disabled, the hash map directly queries the state table.
//! When cache is enabled, it maintains a `ManagedLruCache` that caches all rows for
//! each join key, indexed by (`inequality_key`, `pk_suffix`) for efficient range queries.

use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::Arc;

use anyhow::Context;
use futures::StreamExt;
use risingwave_common::bitmap::Bitmap;
use risingwave_common::catalog::TableId;
use risingwave_common::row::{CompactedRow, OwnedRow, Row, RowExt};
use risingwave_common::types::{DataType, ScalarImpl};
use risingwave_common::util::epoch::EpochPair;
use risingwave_common::util::row_serde::OrderedRowSerde;
use risingwave_common::util::sort_util::OrderType;
use risingwave_common_estimate_size::{EstimateSize, KvSize};
use risingwave_storage::StateStore;
use risingwave_storage::store::PrefetchOptions;

use super::hash_join::{JoinEntryError, JoinHashMapMetrics, TableInner};
use super::join_row_set::JoinRowSet;
use crate::cache::ManagedLruCache;
use crate::common::metrics::MetricsInfo;
use crate::common::table::state_table::{RowStream, StateTable, StateTablePostCommit};
use crate::consistency::{consistency_error, enable_strict_consistency};
use crate::executor::error::StreamExecutorResult;
use crate::executor::monitor::StreamingMetrics;
use crate::task::{ActorId, AtomicU64Ref, FragmentId};

/// Memcomparable encoding for inequality key and pk suffix.
type InequalKeyBytes = Vec<u8>;
type PkSuffixBytes = Vec<u8>;

/// Trait for encoding/decoding cached `AsOf` join rows.
/// Unlike `JoinEncoding`, this doesn't carry degree since `AsOf` join has no degree tracking.
pub trait AsOfRowEncoding: 'static + Send + Sync {
    type Encoded: EstimateSize + Clone + Send + Sync;
    fn encode(row: &OwnedRow) -> Self::Encoded;
    /// Decode an encoded value back to `OwnedRow`.
    fn decode(encoded: &Self::Encoded, data_types: &[DataType]) -> OwnedRow;
}

/// CPU-optimized encoding: stores full `OwnedRow` in cache (no encode/decode cost).
pub struct AsOfCpuEncoding;

impl AsOfRowEncoding for AsOfCpuEncoding {
    type Encoded = OwnedRow;

    fn encode(row: &OwnedRow) -> OwnedRow {
        // TODO: Avoid this clone
        row.clone()
    }

    fn decode(encoded: &OwnedRow, _data_types: &[DataType]) -> OwnedRow {
        // TODO: Avoid this clone
        encoded.clone()
    }
}

/// Memory-optimized encoding: stores `CompactedRow` (serialized bytes) in cache.
pub struct AsOfMemoryEncoding;

impl AsOfRowEncoding for AsOfMemoryEncoding {
    type Encoded = CompactedRow;

    fn encode(row: &OwnedRow) -> CompactedRow {
        row.into()
    }

    fn decode(encoded: &CompactedRow, data_types: &[DataType]) -> OwnedRow {
        encoded
            .deserialize(data_types)
            .expect("failed to decode compacted row in AsOf join cache")
    }
}

/// A cache entry for a single join key, storing all encoded rows indexed by (`inequality_key`, `pk_suffix`).
/// The outer `BTreeMap` gives efficient range queries on inequality keys;
/// the inner `JoinRowSet` adaptively uses `Vec` for small pk sets and `BTreeMap` for larger ones.
///
/// `V` is the encoded row type: `OwnedRow` for CPU encoding, `CompactedRow` for memory encoding.
pub struct AsOfJoinCacheEntry<V: EstimateSize + Clone> {
    /// `inequality_key_bytes` -> { `pk_suffix_bytes` -> `encoded_row` }
    inner: BTreeMap<InequalKeyBytes, JoinRowSet<PkSuffixBytes, V>>,
    /// Incrementally tracked heap size of all keys and values.
    kv_heap_size: KvSize,
}

impl<V: EstimateSize + Clone> Default for AsOfJoinCacheEntry<V> {
    fn default() -> Self {
        Self {
            inner: BTreeMap::new(),
            kv_heap_size: KvSize::new(),
        }
    }
}

impl<V: EstimateSize + Clone> EstimateSize for AsOfJoinCacheEntry<V> {
    fn estimated_heap_size(&self) -> usize {
        // TODO: Add btreemap internal size.
        // https://github.com/risingwavelabs/risingwave/issues/9713
        self.kv_heap_size.size()
    }
}

impl<V: EstimateSize + Clone> AsOfJoinCacheEntry<V> {
    /// Find the encoded row with the greatest inequality key <= bound (upper bound query).
    /// Returns the first entry by pk order in the pk sub-map for that inequality key.
    fn upper_bound(&self, bound: Bound<&[u8]>) -> Option<&V> {
        self.inner
            .range::<[u8], _>((Bound::Unbounded, bound))
            .next_back()
            .and_then(|(_, pk_map)| pk_map.first_key_sorted().and_then(|k| pk_map.get(k)))
    }

    /// Find the encoded row with the smallest inequality key >= bound (lower bound query).
    /// Returns the first entry by pk order in the pk sub-map for that inequality key.
    fn lower_bound(&self, bound: Bound<&[u8]>) -> Option<&V> {
        self.inner
            .range::<[u8], _>((bound, Bound::Unbounded))
            .next()
            .and_then(|(_, pk_map)| pk_map.first_key_sorted().and_then(|k| pk_map.get(k)))
    }

    /// Iterate all encoded rows within an inequality key range.
    fn range(
        &self,
        range: (Bound<&[u8]>, Bound<&[u8]>),
    ) -> impl Iterator<Item = &V> + '_ + use<'_, V> {
        self.inner
            .range::<[u8], _>(range)
            .flat_map(|(_, pk_map)| pk_map.values())
    }

    /// Find the first encoded row (by pk order) with exact inequality key match.
    fn first_by_inequality(&self, ineq_key_bytes: &[u8]) -> Option<&V> {
        self.inner
            .get(ineq_key_bytes)
            .and_then(|pk_map| pk_map.first_key_sorted().and_then(|k| pk_map.get(k)))
    }

    /// Find the first two encoded rows (by PK order) with exact inequality key match.
    fn first_two_by_inequality(&self, ineq_key_bytes: &[u8]) -> (Option<&V>, Option<&V>) {
        if let Some(pk_map) = self.inner.get(ineq_key_bytes) {
            let (first_key, second_key) = pk_map.first_two_key_sorted();
            let first = first_key.and_then(|k| pk_map.get(k));
            let second = second_key.and_then(|k| pk_map.get(k));
            (first, second)
        } else {
            (None, None)
        }
    }

    /// Insert an encoded row into the cache entry.
    fn insert(
        &mut self,
        ineq_key_bytes: InequalKeyBytes,
        pk_suffix_bytes: PkSuffixBytes,
        val: V,
    ) -> Result<(), JoinEntryError> {
        use std::collections::btree_map::Entry;
        let pk_map = match self.inner.entry(ineq_key_bytes) {
            Entry::Vacant(e) => {
                self.kv_heap_size.add_val(e.key());
                e.insert(JoinRowSet::default())
            }
            Entry::Occupied(e) => e.into_mut(),
        };
        if !enable_strict_consistency()
            && let Some(old_val) = pk_map.remove(&pk_suffix_bytes)
        {
            self.kv_heap_size.sub(&pk_suffix_bytes, &old_val);
        }
        self.kv_heap_size.add(&pk_suffix_bytes, &val);
        let ret = pk_map.try_insert(pk_suffix_bytes.clone(), val);
        if !enable_strict_consistency() {
            assert!(ret.is_ok(), "we have removed existing entry, if any");
        }
        ret.map(|_| ()).map_err(|_| JoinEntryError::Occupied)
    }

    /// Delete an encoded row from the cache entry.
    fn delete(
        &mut self,
        ineq_key_bytes: &InequalKeyBytes,
        pk_suffix_bytes: &PkSuffixBytes,
    ) -> Result<(), JoinEntryError> {
        if let Some(pk_map) = self.inner.get_mut(ineq_key_bytes) {
            if let Some(old_val) = pk_map.remove(pk_suffix_bytes) {
                self.kv_heap_size.sub(pk_suffix_bytes, &old_val);
                if pk_map.is_empty() {
                    self.kv_heap_size.sub_val(ineq_key_bytes);
                    self.inner.remove(ineq_key_bytes);
                }
                Ok(())
            } else if enable_strict_consistency() {
                Err(JoinEntryError::Remove)
            } else {
                consistency_error!(
                    ?pk_suffix_bytes,
                    "removing an asof join cache entry but it's not in the cache"
                );
                Ok(())
            }
        } else if enable_strict_consistency() {
            Err(JoinEntryError::Remove)
        } else {
            consistency_error!(
                ?pk_suffix_bytes,
                "removing an asof join cache entry but it's not in the cache"
            );
            Ok(())
        }
    }
}

/// A hash map for `AsOf` join with optional LRU cache support.
///
/// When `cache` is `None`, it directly queries the state table (no-cache mode).
/// When `cache` is `Some`, it maintains a `ManagedLruCache` keyed by join key,
/// caching all rows for each join key indexed by (`inequality_key`, `pk_suffix`).
///
/// `E` controls row encoding in the cache: `AsOfCpuEncoding` (full `OwnedRow`) or
/// `AsOfMemoryEncoding` (compact `CompactedRow`).
pub struct AsOfJoinHashMap<S: StateStore, E: AsOfRowEncoding> {
    /// State table. Contains the data from upstream.
    state: TableInner<S>,
    /// The index of the inequality key column.
    inequality_key_idx: usize,
    /// Serializer for inequality key (single column value -> ordered bytes).
    inequality_key_serializer: OrderedRowSerde,
    /// Serializer for pk suffix (deduped pk columns -> ordered bytes).
    pk_suffix_serializer: OrderedRowSerde,
    /// The indices of the deduped pk columns in the input row (for projecting pk suffix).
    pk_suffix_indices: Vec<usize>,
    /// LRU cache: `join_key` (`OwnedRow`) -> encoded cache entry. None if cache is disabled.
    cache: Option<ManagedLruCache<OwnedRow, AsOfJoinCacheEntry<E::Encoded>>>,
    /// The indices of join key columns in the input row (for projecting join key).
    join_key_indices: Vec<usize>,
    /// All data types of the state table rows, needed for decoding `CompactedRow`.
    state_all_data_types: Vec<DataType>,
    /// Metrics of the hash map
    metrics: JoinHashMapMetrics,
}

impl<S: StateStore, E: AsOfRowEncoding> AsOfJoinHashMap<S, E> {
    /// Create a [`AsOfJoinHashMap`] for `AsOf` join with optional cache.
    ///
    /// If `watermark_epoch` is `Some`, an LRU cache is enabled.
    /// If `watermark_epoch` is `None`, the hash map directly queries the state table.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        state_join_key_indices: Vec<usize>,
        state_table: StateTable<S>,
        state_pk_indices: Vec<usize>,
        inequality_key_idx: usize,
        state_all_data_types: Vec<DataType>,
        watermark_epoch: Option<AtomicU64Ref>,
        metrics: Arc<StreamingMetrics>,
        actor_id: ActorId,
        fragment_id: FragmentId,
        side: &'static str,
    ) -> Self {
        let join_table_id = state_table.table_id();

        // Build serializer for inequality key (single column).
        let inequality_key_serializer = OrderedRowSerde::new(
            vec![state_all_data_types[inequality_key_idx].clone()],
            vec![OrderType::ascending()],
        );

        // Build serializer for pk suffix (deduped pk columns).
        let pk_suffix_serializer = OrderedRowSerde::new(
            state_pk_indices
                .iter()
                .map(|idx| state_all_data_types[*idx].clone())
                .collect(),
            vec![OrderType::ascending(); state_pk_indices.len()],
        );

        let pk_suffix_indices = state_pk_indices.clone();
        let join_key_indices = state_join_key_indices.clone();

        let cache = watermark_epoch.map(|epoch_ref| {
            let metrics_info = MetricsInfo::new(
                metrics.clone(),
                join_table_id,
                actor_id,
                format!("asof join {}", side),
            );
            ManagedLruCache::unbounded(epoch_ref, metrics_info)
        });

        let state = TableInner::new(state_pk_indices, state_join_key_indices, state_table, None);

        Self {
            state,
            inequality_key_idx,
            inequality_key_serializer,
            pk_suffix_serializer,
            pk_suffix_indices,
            cache,
            join_key_indices,
            state_all_data_types,
            metrics: JoinHashMapMetrics::new(&metrics, actor_id, fragment_id, side, join_table_id),
        }
    }

    pub async fn init(&mut self, epoch: EpochPair) -> StreamExecutorResult<()> {
        self.state.table.init_epoch(epoch).await?;
        Ok(())
    }

    pub fn update_watermark(&mut self, watermark: ScalarImpl) {
        self.state.table.update_watermark(watermark);
    }

    pub fn table_id(&self) -> TableId {
        self.state.table.table_id()
    }

    /// Cached entry count (number of distinct join keys in the LRU cache).
    /// Returns 0 if cache is disabled.
    pub fn entry_count(&self) -> usize {
        self.cache.as_ref().map_or(0, |c| c.len())
    }

    /// Serialize a row as an inequality key for cache lookup.
    /// The row should already be the inequality column(s) (e.g. a projected single-column row).
    fn serialize_inequal_key(&self, row: &impl Row) -> Vec<u8> {
        row.memcmp_serialize(&self.inequality_key_serializer)
    }

    /// Serialize the inequality key from a full state table row for cache key.
    fn serialize_inequal_key_from_state_row(&self, row: &impl Row) -> Vec<u8> {
        row.project(&[self.inequality_key_idx])
            .memcmp_serialize(&self.inequality_key_serializer)
    }

    /// Serialize the pk suffix from a row for cache key.
    fn serialize_pk_suffix(&self, row: &impl Row) -> Vec<u8> {
        row.project(&self.pk_suffix_indices)
            .memcmp_serialize(&self.pk_suffix_serializer)
    }

    /// Ensure the cache entry for the given join key is populated.
    /// Does nothing if cache is disabled or the entry is already present.
    async fn ensure_cache_populated(&mut self, join_key: &OwnedRow) -> StreamExecutorResult<()> {
        let needs_populate = match &self.cache {
            Some(cache) => !cache.contains(join_key),
            None => return Ok(()),
        };
        self.metrics.inc_lookup();
        if !needs_populate {
            return Ok(());
        }
        self.metrics.inc_lookup_miss();

        // Cache miss - load all rows for this join key from state table.
        let sub_range: (Bound<OwnedRow>, Bound<OwnedRow>) = (Bound::Unbounded, Bound::Unbounded);
        let table_iter = self
            .state
            .table
            .iter_with_prefix(join_key, &sub_range, PrefetchOptions::default())
            .await?;
        let mut pinned = std::pin::pin!(table_iter);
        let mut entry = AsOfJoinCacheEntry::<E::Encoded>::default();
        while let Some(row) = pinned.next().await.transpose()? {
            let ineq_key = self.serialize_inequal_key_from_state_row(&row);
            let pk_suffix = self.serialize_pk_suffix(&row);
            entry
                .insert(ineq_key, pk_suffix, E::encode(&row))
                .with_context(|| format!("row: {}", row.display()))?;
        }
        self.cache.as_mut().unwrap().put(join_key.clone(), entry);
        Ok(())
    }

    /// Insert a row into the state table and optionally update the cache.
    pub fn insert(&mut self, value: impl Row) -> StreamExecutorResult<()> {
        if self.cache.is_some() {
            // Convert to OwnedRow first to avoid move issues with `impl Row`.
            let owned_row = value.to_owned_row();
            let ineq_key = self.serialize_inequal_key_from_state_row(&owned_row);
            let pk_suffix = self.serialize_pk_suffix(&owned_row);
            // Use reference to avoid moving owned_row.
            let join_key = (&owned_row).project(&self.join_key_indices).to_owned_row();
            // Update cache if the join key is cached.
            if let Some(cache) = &mut self.cache
                && let Some(mut entry) = cache.get_mut(&join_key)
            {
                entry
                    .insert(ineq_key, pk_suffix, E::encode(&owned_row))
                    .with_context(|| format!("row: {}", owned_row.display()))?;
            } else {
                self.metrics.inc_insert_cache_miss();
            }
            self.state.table.insert(owned_row);
        } else {
            self.state.table.insert(value);
        }
        Ok(())
    }

    /// Delete a row from the state table and optionally update the cache.
    pub fn delete(&mut self, value: impl Row) -> StreamExecutorResult<()> {
        if self.cache.is_some() {
            // Convert to OwnedRow first to avoid move issues with `impl Row`.
            let owned_row = value.to_owned_row();
            let ineq_key = self.serialize_inequal_key_from_state_row(&owned_row);
            let pk_suffix = self.serialize_pk_suffix(&owned_row);
            // Use reference to avoid moving owned_row.
            let join_key = (&owned_row).project(&self.join_key_indices).to_owned_row();
            // Update cache if the join key is cached.
            if let Some(cache) = &mut self.cache
                && let Some(mut entry) = cache.get_mut(&join_key)
            {
                entry
                    .delete(&ineq_key, &pk_suffix)
                    .with_context(|| format!("row: {}", owned_row.display()))?;
            }
            self.state.table.delete(owned_row);
        } else {
            self.state.table.delete(value);
        }
        Ok(())
    }

    /// Evict the LRU cache. No-op if cache is not enabled.
    pub fn evict_cache(&mut self) {
        if let Some(cache) = &mut self.cache {
            cache.evict();
        }
    }

    /// Flush the state table.
    pub async fn flush(
        &mut self,
        epoch: EpochPair,
    ) -> StreamExecutorResult<AsOfJoinHashMapPostCommit<'_, S, E>> {
        self.metrics.flush();
        if let Some(cache) = &mut self.cache {
            cache.evict();
        }
        let state_post_commit = self.state.table.commit(epoch).await?;
        Ok(AsOfJoinHashMapPostCommit {
            state: state_post_commit,
            cache: self.cache.as_mut(),
        })
    }

    pub async fn try_flush(&mut self) -> StreamExecutorResult<()> {
        self.state.table.try_flush().await?;
        Ok(())
    }

    pub async fn upper_bound_by_inequality_with_jk_prefix(
        &mut self,
        join_key: impl Row,
        bound: Bound<&impl Row>,
    ) -> StreamExecutorResult<Option<OwnedRow>> {
        if self.cache.is_some() {
            let join_key_owned = join_key.to_owned_row();
            // Serialize bound bytes before borrowing cache to avoid borrow conflicts.
            let bound_bytes = match &bound {
                Bound::Included(r) => Bound::Included(self.serialize_inequal_key(r)),
                Bound::Excluded(r) => Bound::Excluded(self.serialize_inequal_key(r)),
                Bound::Unbounded => Bound::Unbounded,
            };
            self.ensure_cache_populated(&join_key_owned).await?;
            let cache = self.cache.as_mut().unwrap();
            if let Some(entry) = cache.get(&join_key_owned) {
                let bound_ref = match &bound_bytes {
                    Bound::Included(b) => Bound::Included(b.as_slice()),
                    Bound::Excluded(b) => Bound::Excluded(b.as_slice()),
                    Bound::Unbounded => Bound::Unbounded,
                };
                return Ok(entry
                    .upper_bound(bound_ref)
                    .map(|v| E::decode(v, &self.state_all_data_types)));
            }
            // Not in cache (shouldn't happen after ensure_cache_populated, but fallback)
        }
        // Non-cached path: use rev_iter to find the largest inequality key within the
        // bound, then forward-scan for the smallest PK at that inequality key. This
        // matches the cached path's behavior (smallest PK wins on tie), which is
        // required for consistency with eq_join_right's first_two_by_inequality logic.
        let sub_range = (Bound::<OwnedRow>::Unbounded, bound);
        let table_iter = self
            .state
            .table
            .rev_iter_with_prefix(&join_key, &sub_range, PrefetchOptions::default())
            .await?;
        let mut pinned_table_iter = std::pin::pin!(table_iter);
        let Some(rev_row) = pinned_table_iter.next().await.transpose()? else {
            return Ok(None);
        };
        // Extract the inequality key from the found row and forward-scan for smallest PK.
        let found_ineq_key = (&rev_row)
            .project(&[self.inequality_key_idx])
            .to_owned_row();
        let exact_range = (
            Bound::Included(&found_ineq_key),
            Bound::Included(&found_ineq_key),
        );
        let fwd_iter = self
            .state
            .table
            .iter_with_prefix(&join_key, &exact_range, PrefetchOptions::default())
            .await?;
        let mut pinned_fwd = std::pin::pin!(fwd_iter);
        // Forward scan returns smallest PK first; fall back to rev_row if empty (shouldn't happen).
        let row = pinned_fwd.next().await.transpose()?;
        Ok(row.or(Some(rev_row)))
    }

    pub async fn lower_bound_by_inequality_with_jk_prefix(
        &mut self,
        join_key: &impl Row,
        bound: Bound<&impl Row>,
    ) -> StreamExecutorResult<Option<OwnedRow>> {
        if self.cache.is_some() {
            let join_key_owned = join_key.to_owned_row();
            // Serialize bound bytes before borrowing cache to avoid borrow conflicts.
            let bound_bytes = match &bound {
                Bound::Included(r) => Bound::Included(self.serialize_inequal_key(r)),
                Bound::Excluded(r) => Bound::Excluded(self.serialize_inequal_key(r)),
                Bound::Unbounded => Bound::Unbounded,
            };
            self.ensure_cache_populated(&join_key_owned).await?;
            let cache = self.cache.as_mut().unwrap();
            if let Some(entry) = cache.get(&join_key_owned) {
                let bound_ref = match &bound_bytes {
                    Bound::Included(b) => Bound::Included(b.as_slice()),
                    Bound::Excluded(b) => Bound::Excluded(b.as_slice()),
                    Bound::Unbounded => Bound::Unbounded,
                };
                return Ok(entry
                    .lower_bound(bound_ref)
                    .map(|v| E::decode(v, &self.state_all_data_types)));
            }
        }
        // Non-cached path
        let sub_range = (bound, Bound::<OwnedRow>::Unbounded);
        let table_iter = self
            .state
            .table
            .iter_with_prefix(join_key, &sub_range, PrefetchOptions::default())
            .await?;
        let mut pinned_table_iter = std::pin::pin!(table_iter);
        let row = pinned_table_iter.next().await.transpose()?;
        Ok(row)
    }

    async fn table_iter_by_inequality_with_jk_prefix<'a>(
        &'a self,
        join_key: &'a impl Row,
        range: &'a (Bound<impl Row>, Bound<impl Row>),
    ) -> StreamExecutorResult<impl RowStream<'a>> {
        self.state
            .table
            .iter_with_prefix(join_key, range, PrefetchOptions::default())
            .await
    }

    pub async fn first_by_inequality_with_jk_prefix(
        &mut self,
        join_key: &impl Row,
        inequality_key: &impl Row,
    ) -> StreamExecutorResult<Option<OwnedRow>> {
        if self.cache.is_some() {
            let join_key_owned = join_key.to_owned_row();
            // Serialize before borrowing cache to avoid borrow conflicts.
            let ineq_key_bytes = self.serialize_inequal_key(inequality_key);
            self.ensure_cache_populated(&join_key_owned).await?;
            let cache = self.cache.as_mut().unwrap();
            if let Some(entry) = cache.get(&join_key_owned) {
                return Ok(entry
                    .first_by_inequality(&ineq_key_bytes)
                    .map(|v| E::decode(v, &self.state_all_data_types)));
            }
        }
        // Non-cached path
        let range = (
            Bound::Included(inequality_key),
            Bound::Included(inequality_key),
        );
        let table_iter = self
            .table_iter_by_inequality_with_jk_prefix(join_key, &range)
            .await?;
        let mut pinned_table_iter = std::pin::pin!(table_iter);
        let row = pinned_table_iter.next().await.transpose()?;
        Ok(row)
    }

    /// Return the first two rows (by PK order) with exact inequality key match.
    /// Used by `eq_join_right` to determine replacement: Insert needs first row
    /// for comparison, Delete needs first + second for fallback.
    pub async fn first_two_by_inequality_with_jk_prefix(
        &mut self,
        join_key: &impl Row,
        inequality_key: &impl Row,
    ) -> StreamExecutorResult<(Option<OwnedRow>, Option<OwnedRow>)> {
        if self.cache.is_some() {
            let join_key_owned = join_key.to_owned_row();
            let ineq_key_bytes = self.serialize_inequal_key(inequality_key);
            self.ensure_cache_populated(&join_key_owned).await?;
            let cache = self.cache.as_mut().unwrap();
            if let Some(entry) = cache.get(&join_key_owned) {
                let (first, second) = entry.first_two_by_inequality(&ineq_key_bytes);
                return Ok((
                    first.map(|v| E::decode(v, &self.state_all_data_types)),
                    second.map(|v| E::decode(v, &self.state_all_data_types)),
                ));
            }
        }
        // Non-cached path
        let range = (
            Bound::Included(inequality_key),
            Bound::Included(inequality_key),
        );
        let table_iter = self
            .table_iter_by_inequality_with_jk_prefix(join_key, &range)
            .await?;
        let mut pinned_table_iter = std::pin::pin!(table_iter);
        let first = pinned_table_iter.next().await.transpose()?;
        let second = if first.is_some() {
            pinned_table_iter.next().await.transpose()?
        } else {
            None
        };
        Ok((first, second))
    }

    pub async fn range_by_inequality_with_jk_prefix<'a>(
        &'a mut self,
        join_key: &'a impl Row,
        inequality_key_range: &'a (Bound<impl Row>, Bound<impl Row>),
    ) -> StreamExecutorResult<impl RowStream<'a>> {
        if self.cache.is_some() {
            let join_key_owned = join_key.to_owned_row();
            // Serialize bound bytes before borrowing cache.
            let bound_lo = match &inequality_key_range.0 {
                Bound::Included(r) => Bound::Included(self.serialize_inequal_key(r)),
                Bound::Excluded(r) => Bound::Excluded(self.serialize_inequal_key(r)),
                Bound::Unbounded => Bound::Unbounded,
            };
            let bound_hi = match &inequality_key_range.1 {
                Bound::Included(r) => Bound::Included(self.serialize_inequal_key(r)),
                Bound::Excluded(r) => Bound::Excluded(self.serialize_inequal_key(r)),
                Bound::Unbounded => Bound::Unbounded,
            };
            self.ensure_cache_populated(&join_key_owned).await?;
            let cache = self.cache.as_mut().unwrap();
            if let Some(entry) = cache.get(&join_key_owned) {
                let range = (
                    bound_lo.as_ref().map(|b| b.as_slice()),
                    bound_hi.as_ref().map(|b| b.as_slice()),
                );
                // Cached: lazily decode cloned rows from cache and wrap as stream.
                let data_types = &self.state_all_data_types;
                return Ok(futures::future::Either::Left(futures::stream::iter(
                    entry.range(range).map(|v| Ok(E::decode(v, data_types))),
                )));
            }
        }
        // Non-cached path: stream directly from state table without collecting.
        let table_iter = self
            .state
            .table
            .iter_with_prefix(join_key, inequality_key_range, PrefetchOptions::default())
            .await?;
        Ok(futures::future::Either::Right(table_iter))
    }

    /// Return true if the inequality key is null.
    pub fn check_inequal_key_null(&self, row: &impl Row) -> bool {
        row.datum_at(self.inequality_key_idx).is_none()
    }

    pub fn get_pk_from_row<'a>(&'a self, row: impl Row + 'a) -> impl Row + 'a {
        row.project(&self.state.pk_indices)
    }
}

#[must_use]
pub struct AsOfJoinHashMapPostCommit<'a, S: StateStore, E: AsOfRowEncoding> {
    state: StateTablePostCommit<'a, S>,
    cache: Option<&'a mut ManagedLruCache<OwnedRow, AsOfJoinCacheEntry<E::Encoded>>>,
}

impl<S: StateStore, E: AsOfRowEncoding> AsOfJoinHashMapPostCommit<'_, S, E> {
    pub async fn post_yield_barrier(
        self,
        vnode_bitmap: Option<Arc<Bitmap>>,
    ) -> StreamExecutorResult<Option<bool>> {
        let cache_may_stale = self.state.post_yield_barrier(vnode_bitmap.clone()).await?;
        let keyed_cache_may_stale = cache_may_stale.map(|(_, changed)| changed);
        // Clear cache if the keyed cache may be stale after the barrier update.
        if keyed_cache_may_stale.unwrap_or(false)
            && let Some(cache) = self.cache
        {
            cache.clear();
        }
        Ok(keyed_cache_may_stale)
    }
}

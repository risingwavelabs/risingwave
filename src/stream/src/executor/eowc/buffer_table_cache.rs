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

use std::collections::BTreeSet;
use std::ops::Bound;

use anyhow::Context;
use bytes::Bytes;
use futures::StreamExt;
use futures_async_stream::for_await;
use risingwave_common::hash::VnodeBitmapExt;
use risingwave_common::row::{self, OwnedRow};
use risingwave_common_estimate_size::EstimateSize;
use risingwave_storage::StateStore;
use risingwave_storage::store::PrefetchOptions;
use risingwave_storage::table::KeyedRow;
use risingwave_storage::table::merge_sort::merge_sort;

use crate::common::state_cache::{StateCache, StateCacheFiller, TopNStateCache};
use crate::common::table::state_table::StateTable;
use crate::executor::StreamExecutorResult;

// TODO(rc): need to make this configurable?
const CACHE_CAPACITY: usize = 2048;

/// Caches the smallest rows of a buffer table, whose rows are consumed in the pk order by
/// watermark, so that consuming doesn't scan the table every time. The cache key must be ordered
/// as the pk of the table, and the writes to the table must be applied to the cache too.
pub struct BufferTableCache<K: Ord + EstimateSize, V: EstimateSize> {
    cache: TopNStateCache<K, V>,
}

impl<K: Ord + EstimateSize + Clone, V: EstimateSize> BufferTableCache<K, V> {
    pub fn new() -> Self {
        Self {
            cache: TopNStateCache::new(CACHE_CAPACITY),
        }
    }

    pub fn is_synced(&self) -> bool {
        self.cache.is_synced()
    }

    pub fn insert(&mut self, key: K, value: V) {
        self.cache.insert(key, value);
    }

    pub fn delete(&mut self, key: &K) -> Option<V> {
        self.cache.delete(key)
    }

    /// Pop the first cached entry if `pred` holds for it. `None` is also returned if the cache is
    /// not synced, e.g., after it runs out of entries, when it should be refilled after the last
    /// popped entry.
    pub fn pop_first_if(&mut self, pred: impl FnOnce(&K, &V) -> bool) -> Option<(K, V)> {
        if !self.cache.is_synced() {
            return None;
        }
        let (key, value) = self.cache.first_key_value()?;
        if !pred(key, value) {
            return None;
        }
        let key = key.clone();
        let value = self.cache.delete(&key).expect("the first key must exist");
        Some((key, value))
    }

    /// The table row count is known when a refill reads all rows, after which the cache stays
    /// synced as long as the count is maintained by the writes.
    pub fn set_table_row_count(&mut self, table_row_count: usize) {
        self.cache.set_table_row_count(table_row_count);
    }

    pub fn clear(&mut self) {
        self.cache.clear();
    }

    /// Clear the cache and fill it with the smallest rows of `table` after `start_after`, converted
    /// by `entry`. Returns the number of rows read if they are all the rows after `start_after`.
    pub async fn refill<S: StateStore>(
        &mut self,
        table: &StateTable<S>,
        start_after: Option<OwnedRow>,
        mut entry: impl FnMut(OwnedRow) -> StreamExecutorResult<(K, V)>,
    ) -> StreamExecutorResult<Option<usize>> {
        // Start over, as the row count of the table is known only if the refill reads all rows.
        self.cache = TopNStateCache::new(CACHE_CAPACITY);
        let mut filler = self.cache.begin_syncing();

        let pk_range = (
            start_after.map(Bound::Excluded).unwrap_or(Bound::Unbounded),
            Bound::<row::Empty>::Unbounded,
        );

        let streams: Vec<_> =
            futures::future::try_join_all(table.vnodes().iter_vnodes().map(|vnode| {
                table.iter_keyed_row_with_vnode(
                    vnode,
                    &pk_range,
                    PrefetchOptions::new(filler.capacity().is_none(), false),
                )
            }))
            .await?
            .into_iter()
            .map(Box::pin)
            .collect();

        let capacity = filler.capacity().unwrap_or(usize::MAX);
        let mut count = 0;
        #[for_await]
        for kv in merge_sort(streams).take(capacity) {
            let (key, value) = entry(key_value_to_full_row(kv?, table)?)?;
            filler.insert_unchecked(key, value);
            count += 1;
        }

        filler.finish();
        Ok((count < capacity).then_some(count))
    }
}

impl<K: Ord + EstimateSize + Clone, V: EstimateSize> Default for BufferTableCache<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

/// Merge the key part and value part of a row into a full row. This is needed for state table with
/// non-None value indices.
fn key_value_to_full_row<S: StateStore>(
    keyed_row: KeyedRow<Bytes>,
    table: &StateTable<S>,
) -> StreamExecutorResult<OwnedRow> {
    let Some(val_indices) = table.value_indices() else {
        return Ok(keyed_row.into_owned_row());
    };
    let pk_indices = table.pk_indices();
    let indices: BTreeSet<_> = val_indices
        .iter()
        .chain(pk_indices.iter())
        .copied()
        .collect();
    let len = indices.iter().max().unwrap() + 1;
    assert!(indices.iter().copied().eq(0..len));

    let mut row = vec![None; len];
    let key = table
        .pk_serde()
        .deserialize(keyed_row.key())
        .context("failed to deserialize pk")?;
    for (i, v) in key.into_iter().enumerate() {
        row[pk_indices[i]] = v;
    }
    for (i, v) in keyed_row.into_owned_row().into_iter().enumerate() {
        row[val_indices[i]] = v;
    }
    Ok(OwnedRow::new(row))
}

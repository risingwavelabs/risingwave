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

use std::alloc::Global;
use std::collections::btree_map::{DrainFilter, OccupiedEntry, Range};
use std::collections::BTreeMap;
use std::ops::RangeBounds;

use risingwave_common::estimate_size::EstimateSize;
use risingwave_common::row::CompactedRow;

/// `CacheKey` is composed of `(order_by, remaining columns of pk)`.
pub type CacheKey = (Vec<u8>, Vec<u8>);

#[derive(Default)]
pub struct TopNCacheState {
    /// The full copy of the state.
    inner: BTreeMap<CacheKey, CompactedRow>,
    kv_heap_size: usize,
}

impl EstimateSize for TopNCacheState {
    fn estimated_heap_size(&self) -> usize {
        // TODO: Add btreemap internal size.
        // https://github.com/risingwavelabs/risingwave/issues/9713
        self.kv_heap_size
    }
}

impl TopNCacheState {
    pub fn new() -> Self {
        Default::default()
    }

    /// Insert into the cache.
    pub fn insert(&mut self, key: CacheKey, value: CompactedRow) -> Option<CompactedRow> {
        self.kv_heap_size = self
            .kv_heap_size
            .saturating_add(key.estimated_heap_size() + value.estimated_heap_size());
        self.inner.insert(key, value)
    }

    /// Delete from the cache.
    pub fn remove(&mut self, key: &CacheKey) {
        if let Some(value) = self.inner.remove(key) {
            self.kv_heap_size = self
                .kv_heap_size
                .saturating_sub(key.estimated_heap_size() + value.estimated_heap_size());
        }
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn last_key_value(&self) -> Option<(&CacheKey, &CompactedRow)> {
        self.inner.last_key_value()
    }

    pub fn first_key_value(&self) -> Option<(&CacheKey, &CompactedRow)> {
        self.inner.first_key_value()
    }

    pub fn clear(&mut self) {
        self.inner.clear()
    }

    pub fn pop_first(&mut self) -> Option<(CacheKey, CompactedRow)> {
        self.inner.pop_first().inspect(|(k, v)| {
            self.kv_heap_size = self
                .kv_heap_size
                .saturating_sub(k.estimated_heap_size() + v.estimated_heap_size())
        })
    }

    pub fn pop_last(&mut self) -> Option<(CacheKey, CompactedRow)> {
        self.inner.pop_last().inspect(|(k, v)| {
            self.kv_heap_size = self
                .kv_heap_size
                .saturating_sub(k.estimated_heap_size() + v.estimated_heap_size())
        })
    }

    pub fn last_entry(&mut self) -> Option<TopNCacheOccupiedEntry<'_>> {
        self.inner
            .last_entry()
            .map(|entry| TopNCacheOccupiedEntry::new(entry, &mut self.kv_heap_size))
    }

    pub fn inner(&self) -> &BTreeMap<CacheKey, CompactedRow> {
        &self.inner
    }

    pub fn range<R>(&self, range: R) -> Range<'_, CacheKey, CompactedRow>
    where
        R: RangeBounds<CacheKey>,
    {
        self.inner.range(range)
    }

    pub fn drain_filter<F>(&mut self, pred: F) -> DrainFilter<'_, CacheKey, CompactedRow, F, Global>
    where
        F: FnMut(&CacheKey, &mut CompactedRow) -> bool,
    {
        self.inner.drain_filter(pred)
    }
}

pub struct TopNCacheOccupiedEntry<'a> {
    inner: OccupiedEntry<'a, CacheKey, CompactedRow>,
    /// The total size of the `TopNCacheState`
    kv_heap_size: &'a mut usize,
}

impl<'a> TopNCacheOccupiedEntry<'a> {
    pub fn new(entry: OccupiedEntry<'a, CacheKey, CompactedRow>, size: &'a mut usize) -> Self {
        Self {
            inner: entry,
            kv_heap_size: size,
        }
    }

    pub fn remove_entry(self) -> (CacheKey, CompactedRow) {
        let (k, v) = self.inner.remove_entry();
        *self.kv_heap_size = self
            .kv_heap_size
            .saturating_sub(k.estimated_heap_size() + v.estimated_heap_size());
        (k, v)
    }

    pub fn key(&self) -> &CacheKey {
        self.inner.key()
    }
}

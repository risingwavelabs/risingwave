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

use risingwave_common::array::Op;
use risingwave_common::row::RowExt;
use risingwave_common_estimate_size::collections::{EstimatedHashSet, EstimatedVec};

use crate::cache::ManagedLruCache;
use crate::common::metrics::MetricsInfo;
use crate::consistency::consistency_panic;
use crate::executor::prelude::*;

pub type LookupEntryState = EstimatedHashSet<OwnedRow>;

/// A cache for lookup's arrangement side.
pub struct LookupCache {
    data: ManagedLruCache<OwnedRow, LookupEntryState>,
}

impl LookupCache {
    /// Lookup a row in cache. If not found, return `None`.
    pub fn lookup(&mut self, key: &OwnedRow) -> Option<&LookupEntryState> {
        self.data.get(key)
    }

    /// Update a key after lookup cache misses.
    pub fn batch_update(&mut self, key: OwnedRow, value: EstimatedVec<OwnedRow>) {
        self.data.put(key, LookupEntryState::from_vec(value));
    }

    /// Apply a batch from the arrangement side
    pub fn apply_batch(&mut self, chunk: StreamChunk, arrange_join_keys: &[usize]) {
        for (op, row) in chunk.rows() {
            let key = row.project(arrange_join_keys).into_owned_row();
            if let Some(mut values) = self.data.get_mut(&key) {
                // the item is in cache, update it
                let row = row.into_owned_row();
                match op {
                    Op::Insert | Op::UpdateInsert => {
                        if !values.insert(row) {
                            consistency_panic!("inserting a duplicated value");
                        }
                    }
                    Op::Delete | Op::UpdateDelete => {
                        if !values.remove(&row) {
                            consistency_panic!("row {:?} should be in the cache", row);
                        }
                    }
                }
            }
        }
    }

    pub fn evict(&mut self) {
        self.data.evict()
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Clear the cache.
    pub fn clear(&mut self) {
        self.data.clear();
    }

    pub fn new(watermark_sequence: AtomicU64Ref, metrics_info: MetricsInfo) -> Self {
        let cache = ManagedLruCache::unbounded(watermark_sequence, metrics_info);
        Self { data: cache }
    }
}

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

use std::collections::HashSet;

use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::row::{OwnedRow, Row, RowExt};

use crate::cache::{new_unbounded, ManagedLruCache};
use crate::task::AtomicU64Ref;

/// A cache for lookup's arrangement side.
pub struct LookupCache {
    data: ManagedLruCache<OwnedRow, HashSet<OwnedRow>>,
}

impl LookupCache {
    /// Lookup a row in cache. If not found, return `None`.
    pub fn lookup(&mut self, key: &OwnedRow) -> Option<&HashSet<OwnedRow>> {
        self.data.get(key)
    }

    /// Update a key after lookup cache misses.
    pub fn batch_update(&mut self, key: OwnedRow, value: impl Iterator<Item = OwnedRow>) {
        self.data.push(key, value.collect());
    }

    /// Apply a batch from the arrangement side
    pub fn apply_batch(&mut self, chunk: StreamChunk, arrange_join_keys: &[usize]) {
        for (op, row) in chunk.rows() {
            let key = row.project(arrange_join_keys).into_owned_row();
            if let Some(mut values) = self.data.get_mut(&key) {
                // the item is in cache, update it
                match op {
                    Op::Insert | Op::UpdateInsert => {
                        values.insert(row.into_owned_row());
                    }
                    Op::Delete | Op::UpdateDelete => {
                        values.remove(&row.into_owned_row());
                    }
                }
            }
        }
    }

    /// Flush the cache and evict the items.
    pub fn flush(&mut self) {
        self.data.evict();
    }

    /// Update the current epoch.
    pub fn update_epoch(&mut self, epoch: u64) {
        self.data.update_epoch(epoch);
    }

    /// Clear the cache.
    pub fn clear(&mut self) {
        self.data.clear();
    }

    pub fn new(watermark_epoch: AtomicU64Ref) -> Self {
        let cache = new_unbounded(watermark_epoch);
        Self { data: cache }
    }
}

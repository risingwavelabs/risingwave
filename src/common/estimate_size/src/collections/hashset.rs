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

use std::collections::HashSet;
use std::hash::Hash;

use super::EstimatedVec;
use crate::{EstimateSize, KvSize};

#[derive(Default)]
pub struct EstimatedHashSet<T: EstimateSize> {
    inner: HashSet<T>,
    heap_size: KvSize,
}

impl<T: EstimateSize> EstimateSize for EstimatedHashSet<T> {
    fn estimated_heap_size(&self) -> usize {
        // TODO: Add hashset internal size.
        // https://github.com/risingwavelabs/risingwave/issues/9713
        self.heap_size.size()
    }
}

impl<T: EstimateSize> EstimatedHashSet<T>
where
    T: Eq + Hash,
{
    /// Insert into the cache.
    pub fn insert(&mut self, value: T) -> bool {
        let heap_size = self.heap_size.add_val(&value);
        let inserted = self.inner.insert(value);
        if inserted {
            self.heap_size.set(heap_size);
        }
        inserted
    }

    /// Delete from the cache.
    pub fn remove(&mut self, value: &T) -> bool {
        let removed = self.inner.remove(value);
        if removed {
            self.heap_size.sub_val(value);
        }
        removed
    }

    /// Convert an [`EstimatedVec`] to a [`EstimatedHashSet`]. Do not need to recalculate the
    /// heap size.
    pub fn from_vec(v: EstimatedVec<T>) -> Self {
        let heap_size = v.estimated_heap_size();
        Self {
            inner: HashSet::from_iter(v),
            heap_size: KvSize::with_size(heap_size),
        }
    }
}

impl<T: EstimateSize> EstimatedHashSet<T> {
    pub fn iter(&self) -> impl Iterator<Item = &T> {
        self.inner.iter()
    }
}

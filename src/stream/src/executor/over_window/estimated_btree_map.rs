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

use std::collections::BTreeMap;

use risingwave_common::estimate_size::{EstimateSize, KvSize};

pub struct EstimatedBTreeMap<K, V> {
    inner: BTreeMap<K, V>,
    heap_size: KvSize,
}

impl<K, V> EstimatedBTreeMap<K, V> {
    pub fn new() -> Self {
        Self {
            inner: BTreeMap::new(),
            heap_size: KvSize::new(),
        }
    }

    pub fn inner(&self) -> &BTreeMap<K, V> {
        &self.inner
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    #[expect(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

impl<K, V> Default for EstimatedBTreeMap<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> EstimatedBTreeMap<K, V>
where
    K: EstimateSize + Ord,
    V: EstimateSize,
{
    pub fn first_key_value(&self) -> Option<(&K, &V)> {
        self.inner.first_key_value()
    }

    pub fn last_key_value(&self) -> Option<(&K, &V)> {
        self.inner.last_key_value()
    }

    pub fn insert(&mut self, key: K, row: V) {
        let key_size = self.heap_size.add_val(&key);
        self.heap_size.add_val(&row);
        if let Some(old_row) = self.inner.insert(key, row) {
            self.heap_size.sub_size(key_size);
            self.heap_size.sub_val(&old_row);
        }
    }

    pub fn remove(&mut self, key: &K) {
        if let Some(row) = self.inner.remove(key) {
            self.heap_size.sub(key, &row);
        }
    }

    #[expect(dead_code)]
    pub fn clear(&mut self) {
        self.inner.clear();
        self.heap_size.set(0);
    }
}

impl<K, V> EstimateSize for EstimatedBTreeMap<K, V>
where
    K: EstimateSize,
    V: EstimateSize,
{
    fn estimated_heap_size(&self) -> usize {
        self.heap_size.size()
    }
}

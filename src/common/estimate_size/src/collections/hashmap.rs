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

use std::collections::HashMap;
use std::ops::Deref;

use super::{AtomicMutGuard, MutGuard};
use crate::{EstimateSize, KvSize};

pub struct EstimatedHashMap<K, V> {
    inner: HashMap<K, V>,
    heap_size: KvSize,
}

impl<K, V> EstimateSize for EstimatedHashMap<K, V> {
    fn estimated_heap_size(&self) -> usize {
        self.heap_size.size()
    }
}

impl<K, V> Deref for EstimatedHashMap<K, V> {
    type Target = HashMap<K, V>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<K, V> Default for EstimatedHashMap<K, V> {
    fn default() -> Self {
        Self {
            inner: HashMap::default(),
            heap_size: KvSize::default(),
        }
    }
}

impl<K, V> EstimatedHashMap<K, V>
where
    K: EstimateSize + Eq + std::hash::Hash,
    V: EstimateSize,
{
    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        self.heap_size.add_val(&key);
        self.heap_size.add_val(&value);
        self.inner.insert(key, value)
    }

    pub fn get_mut(&mut self, key: &K) -> Option<MutGuard<'_, V>> {
        self.inner
            .get_mut(key)
            .map(|v| MutGuard::new(v, &mut self.heap_size))
    }

    pub fn remove(&mut self, key: &K) -> Option<V> {
        if let Some(v) = self.inner.remove(key) {
            self.heap_size.sub_val(key);
            self.heap_size.sub_val(&v);
            Some(v)
        } else {
            None
        }
    }

    pub fn values_mut(&mut self) -> impl Iterator<Item = AtomicMutGuard<'_, V>> + '_ {
        let heap_size = &self.heap_size;
        self.inner
            .values_mut()
            .map(move |v| AtomicMutGuard::new(v, heap_size))
    }

    pub fn drain(&mut self) -> impl Iterator<Item = (K, V)> + '_ {
        self.heap_size.set(0);
        self.inner.drain()
    }
}

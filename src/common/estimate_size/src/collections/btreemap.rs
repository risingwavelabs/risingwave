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

use core::fmt;
use std::collections::BTreeMap;
use std::ops::{Bound, RangeBounds, RangeFull, RangeInclusive};

use crate::{EstimateSize, KvSize};

#[derive(Clone)]
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

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn iter(&self) -> impl DoubleEndedIterator<Item = (&K, &V)> {
        self.inner.iter()
    }

    pub fn range<R>(&self, range: R) -> std::collections::btree_map::Range<'_, K, V>
    where
        K: Ord,
        R: std::ops::RangeBounds<K>,
    {
        self.inner.range(range)
    }

    pub fn values(&self) -> impl Iterator<Item = &V> {
        self.inner.values()
    }
}

impl<K, V> EstimatedBTreeMap<K, V>
where
    K: Ord,
{
    pub fn first_key_value(&self) -> Option<(&K, &V)> {
        self.inner.first_key_value()
    }

    pub fn first_key(&self) -> Option<&K> {
        self.first_key_value().map(|(k, _)| k)
    }

    pub fn first_value(&self) -> Option<&V> {
        self.first_key_value().map(|(_, v)| v)
    }

    pub fn last_key_value(&self) -> Option<(&K, &V)> {
        self.inner.last_key_value()
    }

    pub fn last_key(&self) -> Option<&K> {
        self.last_key_value().map(|(k, _)| k)
    }

    pub fn last_value(&self) -> Option<&V> {
        self.last_key_value().map(|(_, v)| v)
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
    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        let key_size = self.heap_size.add_val(&key);
        self.heap_size.add_val(&value);
        let old_value = self.inner.insert(key, value);
        if let Some(old_value) = &old_value {
            self.heap_size.sub_size(key_size);
            self.heap_size.sub_val(old_value);
        }
        old_value
    }

    pub fn remove(&mut self, key: &K) -> Option<V> {
        let old_value = self.inner.remove(key);
        if let Some(old_value) = &old_value {
            self.heap_size.sub(key, old_value);
        }
        old_value
    }

    pub fn clear(&mut self) {
        self.inner.clear();
        self.heap_size.set(0);
    }

    pub fn pop_first(&mut self) -> Option<(K, V)> {
        let (key, value) = self.inner.pop_first()?;
        self.heap_size.sub(&key, &value);
        Some((key, value))
    }

    pub fn pop_last(&mut self) -> Option<(K, V)> {
        let (key, value) = self.inner.pop_last()?;
        self.heap_size.sub(&key, &value);
        Some((key, value))
    }

    pub fn last_entry(&mut self) -> Option<OccupiedEntry<'_, K, V>> {
        self.inner.last_entry().map(|inner| OccupiedEntry {
            inner,
            heap_size: &mut self.heap_size,
        })
    }

    /// Retain the given range of entries in the map, removing others.
    pub fn retain_range(&mut self, range: RangeInclusive<&K>) -> (BTreeMap<K, V>, BTreeMap<K, V>)
    where
        K: Clone,
    {
        let start = *range.start();
        let end = *range.end();

        // [ left, [mid], right ]
        let mut mid_right = self.inner.split_off(start);
        let mid_right_split_key = mid_right
            .lower_bound(Bound::Excluded(end))
            .peek_next()
            .map(|(k, _)| k)
            .cloned();
        let right = if let Some(ref mid_right_split_key) = mid_right_split_key {
            mid_right.split_off(mid_right_split_key)
        } else {
            Default::default()
        };
        let mid = mid_right;
        let left = std::mem::replace(&mut self.inner, mid);

        for (k, v) in &left {
            self.heap_size.sub(k, v);
        }
        for (k, v) in &right {
            self.heap_size.sub(k, v);
        }

        (left, right)
    }

    pub fn extract_if<'a, F>(
        &'a mut self,
        mut pred: F,
    ) -> ExtractIf<'a, K, V, RangeFull, impl FnMut(&K, &mut V) -> bool + use<F, K, V>>
    where
        F: 'a + FnMut(&K, &V) -> bool,
    {
        let pred_immut = move |key: &K, value: &mut V| pred(key, value);
        ExtractIf {
            inner: self.inner.extract_if(.., pred_immut),
            heap_size: &mut self.heap_size,
        }
    }

    pub fn retain<F>(&mut self, mut f: F)
    where
        F: FnMut(&K, &V) -> bool,
    {
        self.extract_if(|k, v| !f(k, v)).for_each(drop);
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

impl<K, V> fmt::Debug for EstimatedBTreeMap<K, V>
where
    K: fmt::Debug,
    V: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.fmt(f)
    }
}

pub struct OccupiedEntry<'a, K, V> {
    inner: std::collections::btree_map::OccupiedEntry<'a, K, V>,
    heap_size: &'a mut KvSize,
}

impl<K, V> OccupiedEntry<'_, K, V>
where
    K: EstimateSize + Ord,
    V: EstimateSize,
{
    pub fn key(&self) -> &K {
        self.inner.key()
    }

    pub fn remove_entry(self) -> (K, V) {
        let (key, value) = self.inner.remove_entry();
        self.heap_size.sub(&key, &value);
        (key, value)
    }
}

pub struct ExtractIf<'a, K, V, R, F>
where
    F: FnMut(&K, &mut V) -> bool,
{
    inner: std::collections::btree_map::ExtractIf<'a, K, V, R, F>,
    heap_size: &'a mut KvSize,
}

impl<K, V, R, F> Iterator for ExtractIf<'_, K, V, R, F>
where
    K: EstimateSize + PartialOrd,
    V: EstimateSize,
    F: FnMut(&K, &mut V) -> bool,
    R: RangeBounds<K>,
{
    type Item = (K, V);

    fn next(&mut self) -> Option<Self::Item> {
        let (key, value) = self.inner.next()?;
        self.heap_size.sub(&key, &value);
        Some((key, value))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

#[cfg(test)]
mod tests {
    use super::EstimatedBTreeMap;

    #[test]
    fn test_retain_range() {
        let mut map = EstimatedBTreeMap::new();

        let (left, right) = map.retain_range(&1..=&10);
        assert!(left.is_empty());
        assert!(right.is_empty());

        map.insert(1, "hello".to_owned());
        map.insert(6, "world".to_owned());
        let (left, right) = map.retain_range(&6..=&6);
        assert_eq!(map.len(), 1);
        assert_eq!(map.inner[&6], "world".to_owned());
        assert_eq!(left.len(), 1);
        assert_eq!(left[&1], "hello".to_owned());
        assert!(right.is_empty());

        map.insert(8, "risingwave".to_owned());
        map.insert(3, "great".to_owned());
        map.insert(0, "wooow".to_owned());
        let (left, right) = map.retain_range(&2..=&7);
        assert_eq!(map.len(), 2);
        assert_eq!(map.inner[&3], "great".to_owned());
        assert_eq!(map.inner[&6], "world".to_owned());
        assert_eq!(left.len(), 1);
        assert_eq!(left[&0], "wooow".to_owned());
        assert_eq!(right.len(), 1);
        assert_eq!(right[&8], "risingwave".to_owned());
    }
}

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

use std::hash::{BuildHasher, Hash, RandomState};

use hashbrown::HashTable;

/// A hash map that iterates in insertion order like `IndexMap`, but removes a key in O(1): the
/// key leaves a hole instead of shifting the entries after it, and goes to the end if inserted
/// again.
#[derive(Debug)]
pub(super) struct InsertionOrderedMap<K, V> {
    /// Positions in `entries` of the keys in the map.
    index: HashTable<usize>,
    /// `None` marks a removed entry.
    entries: Vec<Option<(K, V)>>,
    hasher: RandomState,
}

pub(super) enum Entry<'a, K, V> {
    Occupied(OccupiedEntry<'a, K, V>),
    Vacant(VacantEntry<'a, K, V>),
}

pub(super) struct OccupiedEntry<'a, K, V> {
    map: &'a mut InsertionOrderedMap<K, V>,
    hash: u64,
    pos: usize,
}

pub(super) struct VacantEntry<'a, K, V> {
    map: &'a mut InsertionOrderedMap<K, V>,
    hash: u64,
    key: K,
}

impl<K, V> InsertionOrderedMap<K, V> {
    pub(super) fn with_capacity(capacity: usize) -> Self {
        Self {
            index: HashTable::with_capacity(capacity),
            entries: Vec::with_capacity(capacity),
            hasher: RandomState::new(),
        }
    }

    pub(super) fn len(&self) -> usize {
        self.index.len()
    }

    pub(super) fn is_empty(&self) -> bool {
        self.index.is_empty()
    }

    pub(super) fn into_values(self) -> impl Iterator<Item = V> {
        self.entries.into_iter().flatten().map(|(_, value)| value)
    }
}

impl<K: Hash + Eq, V> InsertionOrderedMap<K, V> {
    pub(super) fn entry(&mut self, key: K) -> Entry<'_, K, V> {
        let hash = self.hasher.hash_one(&key);
        match self
            .index
            .find(hash, |&i| {
                self.entries[i].as_ref().is_some_and(|(k, _)| *k == key)
            })
            .copied()
        {
            Some(pos) => Entry::Occupied(OccupiedEntry {
                map: self,
                hash,
                pos,
            }),
            None => Entry::Vacant(VacantEntry {
                map: self,
                hash,
                key,
            }),
        }
    }

    /// Insert the value, replacing the value of an existing key in place.
    pub(super) fn insert(&mut self, key: K, value: V) {
        match self.entry(key) {
            Entry::Occupied(mut e) => *e.get_mut() = value,
            Entry::Vacant(e) => e.insert(value),
        }
    }
}

impl<K, V> OccupiedEntry<'_, K, V> {
    pub(super) fn get_mut(&mut self) -> &mut V {
        &mut self.map.entries[self.pos]
            .as_mut()
            .expect("indexed entry should exist")
            .1
    }

    pub(super) fn remove(self) {
        let entry = self.map.index.find_entry(self.hash, |&i| i == self.pos);
        entry.expect("entry should be indexed").remove();
        self.map.entries[self.pos] = None;
    }
}

impl<K: Hash, V> VacantEntry<'_, K, V> {
    pub(super) fn insert(self, value: V) {
        let InsertionOrderedMap {
            index,
            entries,
            hasher,
        } = self.map;
        index.insert_unique(self.hash, entries.len(), |&i| {
            hasher.hash_one(&entries[i].as_ref().expect("indexed entry should exist").0)
        });
        entries.push(Some((self.key, value)));
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;

    use super::*;

    fn remove(map: &mut InsertionOrderedMap<i32, i32>, key: i32) {
        let Entry::Occupied(e) = map.entry(key) else {
            panic!("key {key} should be in the map");
        };
        e.remove();
    }

    #[test]
    fn test_removed_key_goes_to_the_end_when_inserted_again() {
        let mut map = InsertionOrderedMap::with_capacity(0);
        map.insert(1, 10);
        map.insert(2, 20);
        map.insert(3, 30);
        remove(&mut map, 1);
        assert_eq!(map.len(), 2);

        map.insert(1, 11);
        map.insert(4, 40);
        assert_eq!(map.len(), 4);
        assert_eq!(map.into_values().collect_vec(), [20, 30, 11, 40]);
    }

    #[test]
    fn test_replaced_key_keeps_its_position() {
        let mut map = InsertionOrderedMap::with_capacity(0);
        map.insert(1, 10);
        map.insert(2, 20);
        map.insert(1, 11);
        assert_eq!(map.len(), 2);
        assert_eq!(map.into_values().collect_vec(), [11, 20]);
    }
}

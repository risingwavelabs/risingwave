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

#![feature(btree_cursors)]

use std::cmp::Ordering;
use std::collections::{btree_map, BTreeMap};
use std::ops::Bound;

use educe::Educe;
use enum_as_inner::EnumAsInner;

/// [`DeltaBTreeMap`] wraps two [`BTreeMap`] references respectively as snapshot and delta,
/// providing cursor that can iterate over the updated version of the snapshot.
#[derive(Debug, Educe)]
#[educe(Clone, Copy)]
pub struct DeltaBTreeMap<'a, K: Ord, V> {
    snapshot: &'a BTreeMap<K, V>,
    delta: &'a BTreeMap<K, Change<V>>,

    first_key: Option<&'a K>,
    last_key: Option<&'a K>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, EnumAsInner)]
pub enum Change<V> {
    Insert(V),
    Delete,
}

impl<'a, K: Ord, V> DeltaBTreeMap<'a, K, V> {
    /// Create a new [`DeltaBTreeMap`] from the given snapshot and delta.
    /// Best case time complexity: O(1), worst case time complexity: O(m), where m is `delta.len()`.
    pub fn new(snapshot: &'a BTreeMap<K, V>, delta: &'a BTreeMap<K, Change<V>>) -> Self {
        let first_key = {
            let cursor = CursorWithDelta2 {
                ss_cursor: snapshot.lower_bound(Bound::Unbounded),
                dt_cursor: delta.lower_bound(Bound::Unbounded),
            };
            cursor.peek_next().map(|(key, _)| key)
        };
        let last_key = {
            let cursor = CursorWithDelta2 {
                ss_cursor: snapshot.upper_bound(Bound::Unbounded),
                dt_cursor: delta.upper_bound(Bound::Unbounded),
            };
            cursor.peek_prev().map(|(key, _)| key)
        };
        Self {
            snapshot,
            delta,
            first_key,
            last_key,
        }
    }

    /// Get a reference to the snapshot.
    pub fn snapshot(&self) -> &'a BTreeMap<K, V> {
        self.snapshot
    }

    /// Get a reference to the delta.
    pub fn delta(&self) -> &'a BTreeMap<K, Change<V>> {
        self.delta
    }

    /// Get the first key in the updated version of the snapshot. Complexity: O(1).
    pub fn first_key(&self) -> Option<&'a K> {
        self.first_key
    }

    /// Get the last key in the updated version of the snapshot. Complexity: O(1).
    pub fn last_key(&self) -> Option<&'a K> {
        self.last_key
    }

    /// Get a [`CursorWithDelta`] pointing to the element corresponding to the given key.
    /// If the given key is not found in either the snapshot or the delta, `None` is returned.
    pub fn find(&self, key: &K) -> Option<CursorWithDelta<'a, K, V>> {
        let ss_cursor = self.snapshot.lower_bound(Bound::Included(key));
        let dt_cursor = self.delta.lower_bound(Bound::Included(key));
        let ss_cursor_kv = ss_cursor.peek_next();
        let dt_cursor_kv = dt_cursor.peek_next();
        let curr_key_value = if dt_cursor_kv.map(|(k, _)| k) == Some(key) {
            match dt_cursor_kv.unwrap() {
                (key, Change::Insert(value)) => (key, value),
                (_key, Change::Delete) => {
                    // the key is deleted
                    return None;
                }
            }
        } else if ss_cursor_kv.map(|(k, _)| k) == Some(key) {
            ss_cursor_kv.unwrap()
        } else {
            // the key doesn't exist
            return None;
        };
        Some(CursorWithDelta {
            snapshot: self.snapshot,
            delta: self.delta,
            curr_key_value: Some(curr_key_value),
        })
    }

    /// Get a [`CursorWithDelta`] pointing to the first element that is above the given bound.
    pub fn lower_bound(&self, bound: Bound<&K>) -> CursorWithDelta<'a, K, V> {
        // the implementation is very similar to `CursorWithDelta::peek_next`
        let mut ss_cursor = self.snapshot.lower_bound(bound);
        let mut dt_cursor = self.delta.lower_bound(bound);
        let next_ss_entry = || ss_cursor.next();
        let next_dt_entry = || dt_cursor.next();
        let curr_key_value =
            CursorWithDelta::peek_impl(PeekDirection::Next, next_ss_entry, next_dt_entry);
        CursorWithDelta {
            snapshot: self.snapshot,
            delta: self.delta,
            curr_key_value,
        }
    }

    /// Get a [`CursorWithDelta`] pointing to the first element that is below the given bound.
    pub fn upper_bound(&self, bound: Bound<&K>) -> CursorWithDelta<'a, K, V> {
        // the implementation is very similar to `CursorWithDelta::peek_prev`
        let mut ss_cursor = self.snapshot.upper_bound(bound);
        let mut dt_cursor = self.delta.upper_bound(bound);
        let prev_ss_entry = || ss_cursor.prev();
        let prev_dt_entry = || dt_cursor.prev();
        let curr_key_value =
            CursorWithDelta::peek_impl(PeekDirection::Prev, prev_ss_entry, prev_dt_entry);
        CursorWithDelta {
            snapshot: self.snapshot,
            delta: self.delta,
            curr_key_value,
        }
    }
}

/// Cursor that can iterate back and forth over the updated version of the snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CursorWithDelta<'a, K: Ord, V> {
    snapshot: &'a BTreeMap<K, V>,
    delta: &'a BTreeMap<K, Change<V>>,
    curr_key_value: Option<(&'a K, &'a V)>,
}

/// Type of cursor position. [`PositionType::Ghost`] is a special position between the first and
/// the last item, where the key and value are `None`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, EnumAsInner)]
pub enum PositionType {
    Ghost,
    Snapshot,
    DeltaUpdate,
    DeltaInsert,
}

#[derive(PartialEq, Eq)]
enum PeekDirection {
    Next,
    Prev,
}

impl<'a, K: Ord, V> CursorWithDelta<'a, K, V> {
    /// Get the cursor position type.
    pub fn position(&self) -> PositionType {
        let Some((key, _)) = self.curr_key_value else {
            return PositionType::Ghost;
        };
        if self.delta.contains_key(key) {
            assert!(matches!(self.delta.get(key).unwrap(), Change::Insert(_)));
            if self.snapshot.contains_key(key) {
                PositionType::DeltaUpdate
            } else {
                PositionType::DeltaInsert
            }
        } else {
            assert!(self.snapshot.contains_key(key));
            PositionType::Snapshot
        }
    }

    /// Get the key pointed by the cursor.
    pub fn key(&self) -> Option<&'a K> {
        self.curr_key_value.map(|(k, _)| k)
    }

    /// Get the value pointed by the cursor.
    pub fn value(&self) -> Option<&'a V> {
        self.curr_key_value.map(|(_, v)| v)
    }

    /// Get the key-value pair pointed by the cursor.
    pub fn key_value(&self) -> Option<(&'a K, &'a V)> {
        self.curr_key_value
    }

    fn peek_impl(
        direction: PeekDirection,
        mut next_ss_entry: impl FnMut() -> Option<(&'a K, &'a V)>,
        mut next_dt_entry: impl FnMut() -> Option<(&'a K, &'a Change<V>)>,
    ) -> Option<(&'a K, &'a V)> {
        loop {
            match (next_ss_entry(), next_dt_entry()) {
                (None, None) => return None,
                (None, Some((key, change))) => return Some((key, change.as_insert().unwrap())),
                (Some((key, value)), None) => return Some((key, value)),
                (Some((ss_key, ss_value)), Some((dt_key, dt_change))) => match ss_key.cmp(dt_key) {
                    Ordering::Less => {
                        if direction == PeekDirection::Next {
                            return Some((ss_key, ss_value));
                        } else {
                            return Some((dt_key, dt_change.as_insert().unwrap()));
                        }
                    }
                    Ordering::Greater => {
                        if direction == PeekDirection::Next {
                            return Some((dt_key, dt_change.as_insert().unwrap()));
                        } else {
                            return Some((ss_key, ss_value));
                        }
                    }
                    Ordering::Equal => match dt_change {
                        Change::Insert(v) => return Some((ss_key, v)),
                        Change::Delete => continue,
                    },
                },
            }
        }
    }

    /// Peek the next key-value pair.
    pub fn peek_next(&self) -> Option<(&'a K, &'a V)> {
        if let Some(key) = self.key() {
            let mut ss_cursor = self.snapshot.lower_bound(Bound::Included(key));
            let mut dt_cursor = self.delta.lower_bound(Bound::Included(key));
            // either one of `ss_cursor.key()` and `dt_cursor.key()` == `Some(key)`, or both are
            if ss_cursor.peek_next().map(|(k, _)| k) == Some(key) {
                ss_cursor.next();
            }
            if dt_cursor.peek_next().map(|(k, _)| k) == Some(key) {
                dt_cursor.next();
            }
            let next_ss_entry = || ss_cursor.next();
            let next_dt_entry = || dt_cursor.next();
            Self::peek_impl(PeekDirection::Next, next_ss_entry, next_dt_entry)
        } else {
            // we are at the ghost position, now let's go back to the beginning
            let mut ss_iter = self.snapshot.iter();
            let mut dt_iter = self.delta.iter();
            Self::peek_impl(PeekDirection::Next, || ss_iter.next(), || dt_iter.next())
        }
    }

    /// Peek the previous key-value pair.
    pub fn peek_prev(&self) -> Option<(&'a K, &'a V)> {
        if let Some(key) = self.key() {
            let mut ss_cursor = self.snapshot.upper_bound(Bound::Included(key));
            let mut dt_cursor = self.delta.upper_bound(Bound::Included(key));
            // either one of `ss_cursor.key()` and `dt_cursor.key()` == `Some(key)`, or both are
            if ss_cursor.peek_prev().map(|(k, _)| k) == Some(key) {
                ss_cursor.prev();
            }
            if dt_cursor.peek_prev().map(|(k, _)| k) == Some(key) {
                dt_cursor.prev();
            }
            let next_ss_entry = || ss_cursor.prev();
            let next_dt_entry = || dt_cursor.prev();
            Self::peek_impl(PeekDirection::Prev, next_ss_entry, next_dt_entry)
        } else {
            // we are at the ghost position, now let's go back to the end
            let mut ss_iter = self.snapshot.iter();
            let mut dt_iter = self.delta.iter();
            Self::peek_impl(
                PeekDirection::Prev,
                || ss_iter.next_back(),
                || dt_iter.next_back(),
            )
        }
    }

    /// Move the cursor to the next position.
    pub fn move_next(&mut self) {
        self.curr_key_value = self.peek_next();
    }

    /// Move the cursor to the previous position.
    pub fn move_prev(&mut self) {
        self.curr_key_value = self.peek_prev();
    }
}

/// Cursor that can iterate back and forth over the updated version of the snapshot.
///
/// A cursor always points to the boundary of items in the map. For example:
///
///     | Foo | Bar |
///     ^     ^     ^
///     1     2     3
///
/// The cursor can be at position 1, 2, or 3.
/// If it's at position 1, `peek_prev` will return `None`, and `peek_next` will return `Foo`.
/// If it's at position 3, `peek_prev` will return `Bar`, and `peek_next` will return `None`.
#[derive(Debug, Clone)]
pub struct CursorWithDelta2<'a, K: Ord, V> {
    // NOTE: `btree_map::Cursor` always points to the boundary of items in the map.
    ss_cursor: btree_map::Cursor<'a, K, V>,
    dt_cursor: btree_map::Cursor<'a, K, Change<V>>,
}

impl<'a, K: Ord, V> CursorWithDelta2<'a, K, V> {
    pub fn peek_prev(&self) -> Option<(&'a K, &'a V)> {
        self.peek::<false /* PREV */>()
    }

    pub fn peek_next(&self) -> Option<(&'a K, &'a V)> {
        self.peek::<true /* NEXT */>()
    }

    pub fn prev(&mut self) -> Option<(&'a K, &'a V)> {
        self.r#move::<false /* PREV */>()
    }

    pub fn next(&mut self) -> Option<(&'a K, &'a V)> {
        self.r#move::<true /* NEXT */>()
    }

    fn peek<const NEXT: bool>(&self) -> Option<(&'a K, &'a V)> {
        let mut ss_cursor = self.ss_cursor.clone();
        let mut dt_cursor = self.dt_cursor.clone();
        let res = Self::move_impl::<NEXT>(&mut ss_cursor, &mut dt_cursor);
        res
    }

    fn r#move<const NEXT: bool>(&mut self) -> Option<(&'a K, &'a V)> {
        let mut ss_cursor = self.ss_cursor.clone();
        let mut dt_cursor = self.dt_cursor.clone();
        let res = Self::move_impl::<NEXT>(&mut ss_cursor, &mut dt_cursor);
        self.ss_cursor = ss_cursor;
        self.dt_cursor = dt_cursor;
        res
    }

    fn move_impl<const NEXT: bool>(
        ss_cursor: &mut btree_map::Cursor<'a, K, V>,
        dt_cursor: &mut btree_map::Cursor<'a, K, Change<V>>,
    ) -> Option<(&'a K, &'a V)> {
        loop {
            let ss_peek = if NEXT {
                ss_cursor.peek_next()
            } else {
                ss_cursor.peek_prev()
            };
            let dt_peek = if NEXT {
                dt_cursor.peek_next()
            } else {
                dt_cursor.peek_prev()
            };

            let in_delta = match (ss_peek, dt_peek) {
                (None, None) => return None,
                (None, Some(_)) => true,
                (Some(_), None) => false,
                (Some((ss_key, _)), Some((dt_key, dt_change))) => match ss_key.cmp(dt_key) {
                    Ordering::Less => !NEXT,   // if NEXT { in snapshot } else { in delta }
                    Ordering::Greater => NEXT, // if NEXT { in delta } else { in snapshot }
                    Ordering::Equal => {
                        if NEXT {
                            ss_cursor.next().unwrap();
                        } else {
                            ss_cursor.prev().unwrap();
                        }
                        match dt_change {
                            Change::Insert(_) => true, // in delta
                            Change::Delete => {
                                if NEXT {
                                    dt_cursor.next().unwrap();
                                } else {
                                    dt_cursor.prev().unwrap();
                                }
                                continue;
                            }
                        }
                    }
                },
            };

            if in_delta {
                let (key, change) = if NEXT {
                    dt_cursor.next().unwrap()
                } else {
                    dt_cursor.prev().unwrap()
                };
                return Some((key, change.as_insert().unwrap()));
            } else {
                return if NEXT {
                    ss_cursor.next()
                } else {
                    ss_cursor.prev()
                };
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty() {
        let map: BTreeMap<i32, &str> = BTreeMap::new();
        let delta = BTreeMap::new();
        let delta_map = DeltaBTreeMap::new(&map, &delta);

        assert_eq!(delta_map.first_key(), None);
        assert_eq!(delta_map.last_key(), None);
        assert_eq!(delta_map.find(&1), None);
        assert_eq!(delta_map.lower_bound(Bound::Included(&1)).key(), None);
        assert_eq!(delta_map.upper_bound(Bound::Included(&1)).key(), None);

        let mut map = BTreeMap::new();
        map.insert(1, "1");
        map.insert(2, "2");
        let mut delta = BTreeMap::new();
        delta.insert(1, Change::Delete);
        delta.insert(2, Change::Delete);
        let delta_map = DeltaBTreeMap::new(&map, &delta);
        assert_eq!(delta_map.first_key(), None);
        assert_eq!(delta_map.last_key(), None);
        assert_eq!(delta_map.find(&1), None);
        assert_eq!(delta_map.find(&2), None);
        assert_eq!(delta_map.find(&3), None);
    }

    #[test]
    fn test_empty_delta() {
        let mut map = BTreeMap::new();
        map.insert(1, "1");
        map.insert(2, "2");
        map.insert(5, "5");
        let delta = BTreeMap::new();
        let delta_map = DeltaBTreeMap::new(&map, &delta);

        assert_eq!(delta_map.first_key(), Some(&1));
        assert_eq!(delta_map.last_key(), Some(&5));
        assert_eq!(delta_map.find(&100), None);
        assert_eq!(delta_map.lower_bound(Bound::Included(&1)).key(), Some(&1));
        assert_eq!(delta_map.lower_bound(Bound::Excluded(&3)).key(), Some(&5));
        assert_eq!(delta_map.upper_bound(Bound::Included(&1)).key(), Some(&1));
        assert_eq!(delta_map.upper_bound(Bound::Excluded(&3)).key(), Some(&2));

        let mut cursor = delta_map.find(&2).unwrap();
        assert_eq!(cursor.position(), PositionType::Snapshot);
        assert_eq!(cursor.key(), Some(&2));
        assert_eq!(cursor.value(), Some(&"2"));
        assert_eq!(cursor.key_value(), Some((&2, &"2")));
        assert_eq!(cursor.peek_next(), Some((&5, &"5")));
        assert_eq!(cursor.peek_prev(), Some((&1, &"1")));
        cursor.move_next();
        assert_eq!(cursor.key(), Some(&5));
        assert_eq!(cursor.value(), Some(&"5"));
        cursor.move_next();
        assert_eq!(cursor.position(), PositionType::Ghost);
        assert_eq!(cursor.key(), None);
        assert_eq!(cursor.value(), None);
        cursor.move_prev();
        assert_eq!(cursor.key(), Some(&5));
        assert_eq!(cursor.value(), Some(&"5"));
        cursor.move_prev();
        cursor.move_prev();
        assert_eq!(cursor.key(), Some(&1));
        assert_eq!(cursor.value(), Some(&"1"));
        assert_eq!(cursor.peek_prev(), None);
        assert_eq!(cursor.peek_next(), Some((&2, &"2")));
        cursor.move_prev();
        assert_eq!(cursor.key(), None);
        assert_eq!(cursor.value(), None);
        assert_eq!(cursor.peek_prev(), Some((&5, &"5")));
        assert_eq!(cursor.peek_next(), Some((&1, &"1")));
    }

    #[test]
    fn test_empty_snapshot() {
        let map: BTreeMap<i32, &str> = BTreeMap::new();
        let mut delta = BTreeMap::new();
        delta.insert(1, Change::Insert("1"));
        delta.insert(2, Change::Insert("2"));
        let delta_map = DeltaBTreeMap::new(&map, &delta);

        assert_eq!(delta_map.first_key(), Some(&1));
        assert_eq!(delta_map.last_key(), Some(&2));
        assert_eq!(delta_map.find(&100), None);
        assert_eq!(delta_map.lower_bound(Bound::Included(&1)).key(), Some(&1));
        assert_eq!(delta_map.lower_bound(Bound::Excluded(&1)).key(), Some(&2));
        assert_eq!(delta_map.upper_bound(Bound::Included(&1)).key(), Some(&1));
        assert_eq!(delta_map.upper_bound(Bound::Excluded(&10)).key(), Some(&2));

        let mut cursor = delta_map.find(&2).unwrap();
        assert_eq!(cursor.position(), PositionType::DeltaInsert);
        assert_eq!(cursor.key(), Some(&2));
        assert_eq!(cursor.value(), Some(&"2"));
        assert_eq!(cursor.key_value(), Some((&2, &"2")));
        assert_eq!(cursor.peek_next(), None);
        assert_eq!(cursor.peek_prev(), Some((&1, &"1")));
        cursor.move_next();
        assert_eq!(cursor.key(), None);
        assert_eq!(cursor.value(), None);
        cursor.move_prev();
        assert_eq!(cursor.key(), Some(&2));
        assert_eq!(cursor.value(), Some(&"2"));
        cursor.move_prev();
        assert_eq!(cursor.key(), Some(&1));
        assert_eq!(cursor.value(), Some(&"1"));
        assert_eq!(cursor.peek_prev(), None);
        assert_eq!(cursor.peek_next(), Some((&2, &"2")));
        cursor.move_prev();
        assert_eq!(cursor.key(), None);
        assert_eq!(cursor.value(), None);
        assert_eq!(cursor.peek_prev(), Some((&2, &"2")));
        assert_eq!(cursor.peek_next(), Some((&1, &"1")));
    }

    #[test]
    fn test_delete_first() {
        let mut map = BTreeMap::new();
        map.insert(1, "1");
        map.insert(3, "3");
        let mut delta = BTreeMap::new();
        delta.insert(1, Change::Delete);
        let delta_map = DeltaBTreeMap::new(&map, &delta);

        assert_eq!(delta_map.first_key(), Some(&3));
        assert_eq!(delta_map.last_key(), Some(&3));
        assert_eq!(delta_map.find(&1), None);
        assert_eq!(delta_map.find(&2), None);
        assert_eq!(delta_map.lower_bound(Bound::Included(&1)).key(), Some(&3));
        assert_eq!(delta_map.lower_bound(Bound::Excluded(&0)).key(), Some(&3));
        assert_eq!(delta_map.upper_bound(Bound::Included(&1)).key(), None);
        assert_eq!(delta_map.upper_bound(Bound::Excluded(&10)).key(), Some(&3));

        let mut cursor = delta_map.find(&3).unwrap();
        assert_eq!(cursor.position(), PositionType::Snapshot);
        assert_eq!(cursor.key(), Some(&3));
        assert_eq!(cursor.value(), Some(&"3"));
        assert_eq!(cursor.key_value(), Some((&3, &"3")));
        assert_eq!(cursor.peek_next(), None);
        assert_eq!(cursor.peek_prev(), None);
        cursor.move_next();
        assert_eq!(cursor.key(), None);
        assert_eq!(cursor.value(), None);
        cursor.move_prev();
        assert_eq!(cursor.key(), Some(&3));
        assert_eq!(cursor.value(), Some(&"3"));
    }

    #[test]
    fn test_delete_last() {
        let mut map = BTreeMap::new();
        map.insert(1, "1");
        map.insert(3, "3");
        let mut delta = BTreeMap::new();
        delta.insert(3, Change::Delete);
        let delta_map = DeltaBTreeMap::new(&map, &delta);

        assert_eq!(delta_map.first_key(), Some(&1));
        assert_eq!(delta_map.last_key(), Some(&1));
        assert_eq!(delta_map.find(&2), None);
        assert_eq!(delta_map.find(&3), None);
        assert_eq!(delta_map.lower_bound(Bound::Included(&1)).key(), Some(&1));
        assert_eq!(delta_map.lower_bound(Bound::Excluded(&1)).key(), None);
        assert_eq!(delta_map.upper_bound(Bound::Included(&3)).key(), Some(&1));
        assert_eq!(delta_map.upper_bound(Bound::Excluded(&3)).key(), Some(&1));

        let cursor = delta_map.find(&1).unwrap();
        assert_eq!(cursor.position(), PositionType::Snapshot);
    }

    #[test]
    fn test_delete_all() {
        let mut map = BTreeMap::new();
        map.insert(1, "1");
        map.insert(3, "3");
        let mut delta = BTreeMap::new();
        delta.insert(1, Change::Delete);
        delta.insert(3, Change::Delete);
        let delta_map = DeltaBTreeMap::new(&map, &delta);

        assert_eq!(delta_map.first_key(), None);
        assert_eq!(delta_map.last_key(), None);
        assert_eq!(delta_map.find(&1), None);
        assert_eq!(delta_map.find(&2), None);
        assert_eq!(delta_map.find(&3), None);
        assert_eq!(delta_map.lower_bound(Bound::Included(&1)).key(), None);
        assert_eq!(delta_map.upper_bound(Bound::Excluded(&3)).key(), None);
    }

    #[test]
    fn test_insert_middle() {
        let mut map = BTreeMap::new();
        map.insert(1, "1");
        map.insert(3, "3");
        let mut delta = BTreeMap::new();
        delta.insert(2, Change::Insert("2"));
        let delta_map = DeltaBTreeMap::new(&map, &delta);

        assert_eq!(delta_map.first_key(), Some(&1));
        assert_eq!(delta_map.last_key(), Some(&3));
        assert_eq!(delta_map.find(&10), None);
        assert_eq!(delta_map.lower_bound(Bound::Included(&1)).key(), Some(&1));
        assert_eq!(delta_map.lower_bound(Bound::Excluded(&1)).key(), Some(&2));
        assert_eq!(delta_map.upper_bound(Bound::Included(&2)).key(), Some(&2));
        assert_eq!(delta_map.upper_bound(Bound::Excluded(&2)).key(), Some(&1));

        let mut cursor = delta_map.find(&2).unwrap();
        assert_eq!(cursor.position(), PositionType::DeltaInsert);
        assert_eq!(cursor.key(), Some(&2));
        assert_eq!(cursor.value(), Some(&"2"));
        assert_eq!(cursor.key_value(), Some((&2, &"2")));
        assert_eq!(cursor.peek_next(), Some((&3, &"3")));
        assert_eq!(cursor.peek_prev(), Some((&1, &"1")));
        cursor.move_next();
        assert_eq!(cursor.key(), Some(&3));
        assert_eq!(cursor.value(), Some(&"3"));
    }

    #[test]
    fn test_update_first() {
        let mut map = BTreeMap::new();
        map.insert(1, "1");
        map.insert(3, "3");
        let mut delta = BTreeMap::new();
        delta.insert(1, Change::Insert("1 new"));
        let delta_map = DeltaBTreeMap::new(&map, &delta);

        assert_eq!(delta_map.first_key(), Some(&1));
        assert_eq!(delta_map.last_key(), Some(&3));

        let mut cursor = delta_map.find(&1).unwrap();
        assert_eq!(cursor.position(), PositionType::DeltaUpdate);
        assert_eq!(cursor.key(), Some(&1));
        assert_eq!(cursor.value(), Some(&"1 new"));
        assert_eq!(cursor.key_value(), Some((&1, &"1 new")));
        assert_eq!(cursor.peek_next(), Some((&3, &"3")));
        assert_eq!(cursor.peek_prev(), None);
        cursor.move_next();
        assert_eq!(cursor.key(), Some(&3));
        assert_eq!(cursor.value(), Some(&"3"));
        cursor.move_prev();
        assert_eq!(cursor.key(), Some(&1));
        assert_eq!(cursor.value(), Some(&"1 new"));
    }

    #[test]
    fn test_update_last() {
        let mut map = BTreeMap::new();
        map.insert(1, "1");
        map.insert(3, "3");
        let mut delta = BTreeMap::new();
        delta.insert(3, Change::Insert("3 new"));
        let delta_map = DeltaBTreeMap::new(&map, &delta);

        assert_eq!(delta_map.first_key(), Some(&1));
        assert_eq!(delta_map.last_key(), Some(&3));

        let mut cursor = delta_map.find(&3).unwrap();
        assert_eq!(cursor.position(), PositionType::DeltaUpdate);
        assert_eq!(cursor.key(), Some(&3));
        assert_eq!(cursor.value(), Some(&"3 new"));
        assert_eq!(cursor.key_value(), Some((&3, &"3 new")));
        assert_eq!(cursor.peek_next(), None);
        assert_eq!(cursor.peek_prev(), Some((&1, &"1")));
        cursor.move_next();
        assert_eq!(cursor.key(), None);
        assert_eq!(cursor.value(), None);
        cursor.move_prev();
        assert_eq!(cursor.key(), Some(&3));
        assert_eq!(cursor.value(), Some(&"3 new"));
    }

    #[test]
    fn test_mixed() {
        let mut map = BTreeMap::new();
        map.insert(1, "1");
        map.insert(2, "2");
        map.insert(3, "3");
        let mut delta = BTreeMap::new();
        delta.insert(0, Change::Insert("0"));
        delta.insert(1, Change::Insert("1 new"));
        delta.insert(3, Change::Delete);
        delta.insert(4, Change::Insert("4"));
        let delta_map = DeltaBTreeMap::new(&map, &delta);

        assert_eq!(delta_map.first_key(), Some(&0));
        assert_eq!(delta_map.last_key(), Some(&4));
        assert_eq!(delta_map.find(&-1), None);
        assert_eq!(delta_map.find(&3), None);
        assert_eq!(delta_map.find(&10), None);
        assert_eq!(delta_map.lower_bound(Bound::Included(&0)).key(), Some(&0));
        assert_eq!(delta_map.lower_bound(Bound::Excluded(&0)).key(), Some(&1));
        assert_eq!(delta_map.lower_bound(Bound::Included(&3)).key(), Some(&4));
        assert_eq!(delta_map.upper_bound(Bound::Included(&5)).key(), Some(&4));
        assert_eq!(delta_map.upper_bound(Bound::Excluded(&4)).key(), Some(&2));
        assert_eq!(delta_map.upper_bound(Bound::Excluded(&2)).key(), Some(&1));

        let mut cursor = delta_map.find(&0).unwrap();
        assert_eq!(cursor.position(), PositionType::DeltaInsert);
        assert_eq!(cursor.key_value(), Some((&0, &"0")));
        cursor.move_next();
        assert_eq!(cursor.position(), PositionType::DeltaUpdate);
        assert_eq!(cursor.key_value(), Some((&1, &"1 new")));
        cursor.move_next();
        assert_eq!(cursor.position(), PositionType::Snapshot);
        assert_eq!(cursor.key_value(), Some((&2, &"2")));
        cursor.move_next();
        assert_eq!(cursor.position(), PositionType::DeltaInsert);
        assert_eq!(cursor.key_value(), Some((&4, &"4")));
        cursor.move_next();
        assert_eq!(cursor.position(), PositionType::Ghost);
        assert_eq!(cursor.key_value(), None);
        cursor.move_next();
        assert_eq!(cursor.position(), PositionType::DeltaInsert);
        assert_eq!(cursor.key_value(), Some((&0, &"0")));
        cursor.move_prev();
        assert_eq!(cursor.position(), PositionType::Ghost);
        cursor.move_prev();
        assert_eq!(cursor.position(), PositionType::DeltaInsert);
        assert_eq!(cursor.key_value(), Some((&4, &"4")));
        cursor.move_prev();
        assert_eq!(cursor.position(), PositionType::Snapshot);
        assert_eq!(cursor.key_value(), Some((&2, &"2")));
        cursor.move_prev();
        assert_eq!(cursor.position(), PositionType::DeltaUpdate);
        assert_eq!(cursor.key_value(), Some((&1, &"1 new")));
        cursor.move_prev();
        assert_eq!(cursor.position(), PositionType::DeltaInsert);
        assert_eq!(cursor.key_value(), Some((&0, &"0")));
        cursor.move_prev();
        assert_eq!(cursor.position(), PositionType::Ghost);
    }

    #[test]
    fn test_mixed_complex() {
        let mut map = BTreeMap::new();
        map.insert(1, "1");
        map.insert(3, "3");
        map.insert(5, "5");
        map.insert(7, "7");
        map.insert(9, "9");
        let mut delta = BTreeMap::new();
        delta.insert(0, Change::Insert("0"));
        delta.insert(1, Change::Insert("1 new"));
        delta.insert(5, Change::Delete);
        delta.insert(7, Change::Delete);
        delta.insert(9, Change::Delete);
        let delta_map = DeltaBTreeMap::new(&map, &delta);

        assert_eq!(delta_map.first_key(), Some(&0));
        assert_eq!(delta_map.last_key(), Some(&3));

        let mut cursor = delta_map.find(&0).unwrap();
        let mut res = vec![];
        while let Some((k, v)) = cursor.key_value() {
            res.push((*k, *v));
            cursor.move_next();
        }
        assert_eq!(res, vec![(0, "0"), (1, "1 new"), (3, "3")]);

        let mut cursor = delta_map.find(&3).unwrap();
        let mut res = vec![];
        while let Some((k, v)) = cursor.key_value() {
            res.push((*k, *v));
            cursor.move_prev();
        }
        assert_eq!(res, vec![(3, "3"), (1, "1 new"), (0, "0")]);
    }
}

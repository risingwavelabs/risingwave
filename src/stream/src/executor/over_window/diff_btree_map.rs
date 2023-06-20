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

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::ops::Bound;

use enum_as_inner::EnumAsInner;

/// [`DiffBTreeMap`] wraps a [`BTreeMap`] reference as a snapshot and an owned diff [`BTreeMap`],
/// providing cursor that can iterate over the updated version of the snapshot.
pub(super) struct DiffBTreeMap<'part, K: Ord, V> {
    snapshot: &'part BTreeMap<K, V>,
    diff: BTreeMap<K, Change<V>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, EnumAsInner)]
pub(super) enum Change<V> {
    Update(V),
    Insert(V),
    Delete,
}

impl<'part, K: Ord, V> DiffBTreeMap<'part, K, V> {
    pub fn new(snapshot: &'part BTreeMap<K, V>, diff: BTreeMap<K, Change<V>>) -> Self {
        Self { snapshot, diff }
    }

    /// Get a reference to the snapshot.
    pub fn snapshot(&self) -> &'part BTreeMap<K, V> {
        self.snapshot
    }

    /// Get a reference to the diff.
    pub fn diff(&self) -> &BTreeMap<K, Change<V>> {
        &self.diff
    }

    /// Get the first key in the updated version of the snapshot.
    pub fn first_key(&self) -> Option<&K> {
        let cursor = CursorWithDiff {
            snapshot: self.snapshot,
            diff: &self.diff,
            curr_key_value: None,
        };
        cursor.peek_next().map(|(key, _)| key)
    }

    /// Get the last key in the updated version of the snapshot.
    pub fn last_key(&self) -> Option<&K> {
        let cursor = CursorWithDiff {
            snapshot: self.snapshot,
            diff: &self.diff,
            curr_key_value: None,
        };
        cursor.peek_prev().map(|(key, _)| key)
    }

    /// Get a [`CursorWithDiff`] pointing to the element corresponding to the given key.
    /// If the given key is not found in either the snapshot or the diff, `None` is returned.
    pub fn find(&self, key: &K) -> Option<CursorWithDiff<'_, K, V>> {
        let ss_cursor = self.snapshot.lower_bound(Bound::Included(key));
        let df_cursor = self.diff.lower_bound(Bound::Included(key));
        let curr_key_value = if df_cursor.key() == Some(key) {
            match df_cursor.key_value().unwrap() {
                (key, Change::Update(value)) => {
                    assert!(ss_cursor.key() == Some(key));
                    (key, value)
                }
                (key, Change::Insert(value)) => {
                    assert!(ss_cursor.key() != Some(key));
                    (key, value)
                }
                (_key, Change::Delete) => {
                    // the key is deleted
                    return None;
                }
            }
        } else if ss_cursor.key() == Some(key) {
            ss_cursor.key_value().unwrap()
        } else {
            // the key doesn't exist
            return None;
        };
        Some(CursorWithDiff {
            snapshot: self.snapshot,
            diff: &self.diff,
            curr_key_value: Some(curr_key_value),
        })
    }

    /// Get a [`CursorWithDiff`] pointing to the first element that is above the given bound.
    pub fn lower_bound(&self, bound: Bound<&K>) -> CursorWithDiff<'_, K, V> {
        // the implementation is very similar to `CursorWithDiff::peek_next`
        let mut ss_cursor = self.snapshot.lower_bound(bound);
        let mut df_cursor = self.diff.lower_bound(bound);
        let next_ss_entry = || {
            let tmp = ss_cursor.key_value();
            ss_cursor.move_next();
            tmp
        };
        let next_df_entry = || {
            let tmp = df_cursor.key_value();
            df_cursor.move_next();
            tmp
        };
        let curr_key_value =
            CursorWithDiff::peek_impl(PeekDirection::Next, next_ss_entry, next_df_entry);
        CursorWithDiff {
            snapshot: self.snapshot,
            diff: &self.diff,
            curr_key_value,
        }
    }

    /// Get a [`CursorWithDiff`] pointing to the first element that is below the given bound.
    pub fn upper_bound(&self, bound: Bound<&K>) -> CursorWithDiff<'_, K, V> {
        // the implementation is very similar to `CursorWithDiff::peek_prev`
        let mut ss_cursor = self.snapshot.upper_bound(bound);
        let mut df_cursor = self.diff.upper_bound(bound);
        let prev_ss_entry = || {
            let tmp = ss_cursor.key_value();
            ss_cursor.move_prev();
            tmp
        };
        let prev_df_entry = || {
            let tmp = df_cursor.key_value();
            df_cursor.move_prev();
            tmp
        };
        let curr_key_value =
            CursorWithDiff::peek_impl(PeekDirection::Prev, prev_ss_entry, prev_df_entry);
        CursorWithDiff {
            snapshot: self.snapshot,
            diff: &self.diff,
            curr_key_value,
        }
    }
}

/// Cursor that can iterate back and forth over the updated version of the snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct CursorWithDiff<'a, K: Ord, V> {
    snapshot: &'a BTreeMap<K, V>,
    diff: &'a BTreeMap<K, Change<V>>,
    curr_key_value: Option<(&'a K, &'a V)>,
}

/// Type of cursor position. [`PositionType::Ghost`] is a special position between the first and
/// the last item, where the key and value are `None`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, EnumAsInner)]
pub(super) enum PositionType {
    Ghost,
    Snapshot,
    DiffUpdate,
    DiffInsert,
}

#[derive(PartialEq, Eq)]
enum PeekDirection {
    Next,
    Prev,
}

impl<'a, K: Ord, V> CursorWithDiff<'a, K, V> {
    /// Get the cursor position type.
    pub fn position(&self) -> PositionType {
        let Some((key, _)) = self.curr_key_value else { return PositionType::Ghost; };
        if self.diff.contains_key(key) {
            if matches!(self.diff.get(key).unwrap(), Change::Update(_)) {
                assert!(self.snapshot.contains_key(key));
                PositionType::DiffUpdate
            } else {
                assert!(!self.snapshot.contains_key(key));
                PositionType::DiffInsert
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
    #[cfg_attr(not(test), expect(dead_code))]
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
        mut next_df_entry: impl FnMut() -> Option<(&'a K, &'a Change<V>)>,
    ) -> Option<(&'a K, &'a V)> {
        loop {
            match (next_ss_entry(), next_df_entry()) {
                (None, None) => return None,
                (None, Some((key, change))) => return Some((key, change.as_insert().unwrap())),
                (Some((key, value)), None) => return Some((key, value)),
                (Some((ss_key, ss_value)), Some((df_key, df_change))) => match ss_key.cmp(df_key) {
                    Ordering::Less => {
                        if direction == PeekDirection::Next {
                            return Some((ss_key, ss_value));
                        } else {
                            return Some((df_key, df_change.as_insert().unwrap()));
                        }
                    }
                    Ordering::Greater => {
                        if direction == PeekDirection::Next {
                            return Some((df_key, df_change.as_insert().unwrap()));
                        } else {
                            return Some((ss_key, ss_value));
                        }
                    }
                    Ordering::Equal => match df_change {
                        Change::Update(v) => return Some((ss_key, v)),
                        Change::Insert(_) => panic!("bad diff"),
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
            let mut df_cursor = self.diff.lower_bound(Bound::Included(key));
            // either one of `ss_cursor.key()` and `df_cursor.key()` == `Some(key)`, or both are
            if ss_cursor.key() == Some(key) {
                ss_cursor.move_next();
            }
            if df_cursor.key() == Some(key) {
                df_cursor.move_next();
            }
            let next_ss_entry = || {
                let tmp = ss_cursor.key_value();
                ss_cursor.move_next();
                tmp
            };
            let next_df_entry = || {
                let tmp = df_cursor.key_value();
                df_cursor.move_next();
                tmp
            };
            Self::peek_impl(PeekDirection::Next, next_ss_entry, next_df_entry)
        } else {
            // we are at the ghost position, now let's go back to the beginning
            let mut ss_iter = self.snapshot.iter();
            let mut df_iter = self.diff.iter();
            Self::peek_impl(PeekDirection::Next, || ss_iter.next(), || df_iter.next())
        }
    }

    /// Peek the previous key-value pair.
    pub fn peek_prev(&self) -> Option<(&'a K, &'a V)> {
        if let Some(key) = self.key() {
            let mut ss_cursor = self.snapshot.upper_bound(Bound::Included(key));
            let mut df_cursor = self.diff.upper_bound(Bound::Included(key));
            // either one of `ss_cursor.key()` and `df_cursor.key()` == `Some(key)`, or both are
            if ss_cursor.key() == Some(key) {
                ss_cursor.move_prev();
            }
            if df_cursor.key() == Some(key) {
                df_cursor.move_prev();
            }
            let next_ss_entry = || {
                let tmp = ss_cursor.key_value();
                ss_cursor.move_prev();
                tmp
            };
            let next_df_entry = || {
                let tmp = df_cursor.key_value();
                df_cursor.move_prev();
                tmp
            };
            Self::peek_impl(PeekDirection::Prev, next_ss_entry, next_df_entry)
        } else {
            // we are at the ghost position, now let's go back to the end
            let mut ss_iter = self.snapshot.iter();
            let mut df_iter = self.diff.iter();
            Self::peek_impl(
                PeekDirection::Prev,
                || ss_iter.next_back(),
                || df_iter.next_back(),
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    #[test]
    fn test_empty() {
        let map: BTreeMap<i32, &str> = BTreeMap::new();
        let diff = BTreeMap::new();
        let diff_map = DiffBTreeMap::new(&map, diff);

        assert_eq!(diff_map.first_key(), None);
        assert_eq!(diff_map.last_key(), None);
        assert_eq!(diff_map.find(&1), None);
        assert_eq!(diff_map.lower_bound(Bound::Included(&1)).key(), None);
        assert_eq!(diff_map.upper_bound(Bound::Included(&1)).key(), None);

        let mut map = BTreeMap::new();
        map.insert(1, "1");
        map.insert(2, "2");
        let mut diff = BTreeMap::new();
        diff.insert(1, Change::Delete);
        diff.insert(2, Change::Delete);
        let diff_map = DiffBTreeMap::new(&map, diff);
        assert_eq!(diff_map.first_key(), None);
        assert_eq!(diff_map.last_key(), None);
        assert_eq!(diff_map.find(&1), None);
        assert_eq!(diff_map.find(&2), None);
        assert_eq!(diff_map.find(&3), None);
    }

    #[test]
    fn test_empty_diff() {
        let mut map = BTreeMap::new();
        map.insert(1, "1");
        map.insert(2, "2");
        map.insert(5, "5");
        let diff = BTreeMap::new();
        let diff_map = DiffBTreeMap::new(&map, diff);

        assert_eq!(diff_map.first_key(), Some(&1));
        assert_eq!(diff_map.last_key(), Some(&5));
        assert_eq!(diff_map.find(&100), None);
        assert_eq!(diff_map.lower_bound(Bound::Included(&1)).key(), Some(&1));
        assert_eq!(diff_map.lower_bound(Bound::Excluded(&3)).key(), Some(&5));
        assert_eq!(diff_map.upper_bound(Bound::Included(&1)).key(), Some(&1));
        assert_eq!(diff_map.upper_bound(Bound::Excluded(&3)).key(), Some(&2));

        let mut cursor = diff_map.find(&2).unwrap();
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
        let mut diff = BTreeMap::new();
        diff.insert(1, Change::Insert("1"));
        diff.insert(2, Change::Insert("2"));
        let diff_map = DiffBTreeMap::new(&map, diff);

        assert_eq!(diff_map.first_key(), Some(&1));
        assert_eq!(diff_map.last_key(), Some(&2));
        assert_eq!(diff_map.find(&100), None);
        assert_eq!(diff_map.lower_bound(Bound::Included(&1)).key(), Some(&1));
        assert_eq!(diff_map.lower_bound(Bound::Excluded(&1)).key(), Some(&2));
        assert_eq!(diff_map.upper_bound(Bound::Included(&1)).key(), Some(&1));
        assert_eq!(diff_map.upper_bound(Bound::Excluded(&10)).key(), Some(&2));

        let mut cursor = diff_map.find(&2).unwrap();
        assert_eq!(cursor.position(), PositionType::DiffInsert);
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
        let mut diff = BTreeMap::new();
        diff.insert(1, Change::Delete);
        let diff_map = DiffBTreeMap::new(&map, diff);

        assert_eq!(diff_map.first_key(), Some(&3));
        assert_eq!(diff_map.last_key(), Some(&3));
        assert_eq!(diff_map.find(&1), None);
        assert_eq!(diff_map.find(&2), None);
        assert_eq!(diff_map.lower_bound(Bound::Included(&1)).key(), Some(&3));
        assert_eq!(diff_map.lower_bound(Bound::Excluded(&0)).key(), Some(&3));
        assert_eq!(diff_map.upper_bound(Bound::Included(&1)).key(), None);
        assert_eq!(diff_map.upper_bound(Bound::Excluded(&10)).key(), Some(&3));

        let mut cursor = diff_map.find(&3).unwrap();
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
        let mut diff = BTreeMap::new();
        diff.insert(3, Change::Delete);
        let diff_map = DiffBTreeMap::new(&map, diff);

        assert_eq!(diff_map.first_key(), Some(&1));
        assert_eq!(diff_map.last_key(), Some(&1));
        assert_eq!(diff_map.find(&2), None);
        assert_eq!(diff_map.find(&3), None);
        assert_eq!(diff_map.lower_bound(Bound::Included(&1)).key(), Some(&1));
        assert_eq!(diff_map.lower_bound(Bound::Excluded(&1)).key(), None);
        assert_eq!(diff_map.upper_bound(Bound::Included(&3)).key(), Some(&1));
        assert_eq!(diff_map.upper_bound(Bound::Excluded(&3)).key(), Some(&1));

        let cursor = diff_map.find(&1).unwrap();
        assert_eq!(cursor.position(), PositionType::Snapshot);
    }

    #[test]
    fn test_insert_middle() {
        let mut map = BTreeMap::new();
        map.insert(1, "1");
        map.insert(3, "3");
        let mut diff = BTreeMap::new();
        diff.insert(2, Change::Insert("2"));
        let diff_map = DiffBTreeMap::new(&map, diff);

        assert_eq!(diff_map.first_key(), Some(&1));
        assert_eq!(diff_map.last_key(), Some(&3));
        assert_eq!(diff_map.find(&10), None);
        assert_eq!(diff_map.lower_bound(Bound::Included(&1)).key(), Some(&1));
        assert_eq!(diff_map.lower_bound(Bound::Excluded(&1)).key(), Some(&2));
        assert_eq!(diff_map.upper_bound(Bound::Included(&2)).key(), Some(&2));
        assert_eq!(diff_map.upper_bound(Bound::Excluded(&2)).key(), Some(&1));

        let mut cursor = diff_map.find(&2).unwrap();
        assert_eq!(cursor.position(), PositionType::DiffInsert);
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
        let mut diff = BTreeMap::new();
        diff.insert(1, Change::Update("1 new"));
        let diff_map = DiffBTreeMap::new(&map, diff);

        assert_eq!(diff_map.first_key(), Some(&1));
        assert_eq!(diff_map.last_key(), Some(&3));

        let mut cursor = diff_map.find(&1).unwrap();
        assert_eq!(cursor.position(), PositionType::DiffUpdate);
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
        let mut diff = BTreeMap::new();
        diff.insert(3, Change::Update("3 new"));
        let diff_map = DiffBTreeMap::new(&map, diff);

        assert_eq!(diff_map.first_key(), Some(&1));
        assert_eq!(diff_map.last_key(), Some(&3));

        let mut cursor = diff_map.find(&3).unwrap();
        assert_eq!(cursor.position(), PositionType::DiffUpdate);
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
        let mut diff = BTreeMap::new();
        diff.insert(0, Change::Insert("0"));
        diff.insert(1, Change::Update("1 new"));
        diff.insert(3, Change::Delete);
        diff.insert(4, Change::Insert("4"));
        let diff_map = DiffBTreeMap::new(&map, diff);

        assert_eq!(diff_map.first_key(), Some(&0));
        assert_eq!(diff_map.last_key(), Some(&4));
        assert_eq!(diff_map.find(&-1), None);
        assert_eq!(diff_map.find(&3), None);
        assert_eq!(diff_map.find(&10), None);
        assert_eq!(diff_map.lower_bound(Bound::Included(&0)).key(), Some(&0));
        assert_eq!(diff_map.lower_bound(Bound::Excluded(&0)).key(), Some(&1));
        assert_eq!(diff_map.lower_bound(Bound::Included(&3)).key(), Some(&4));
        assert_eq!(diff_map.upper_bound(Bound::Included(&5)).key(), Some(&4));
        assert_eq!(diff_map.upper_bound(Bound::Excluded(&4)).key(), Some(&2));
        assert_eq!(diff_map.upper_bound(Bound::Excluded(&2)).key(), Some(&1));

        let mut cursor = diff_map.find(&0).unwrap();
        assert_eq!(cursor.position(), PositionType::DiffInsert);
        assert_eq!(cursor.key_value(), Some((&0, &"0")));
        cursor.move_next();
        assert_eq!(cursor.position(), PositionType::DiffUpdate);
        assert_eq!(cursor.key_value(), Some((&1, &"1 new")));
        cursor.move_next();
        assert_eq!(cursor.position(), PositionType::Snapshot);
        assert_eq!(cursor.key_value(), Some((&2, &"2")));
        cursor.move_next();
        assert_eq!(cursor.position(), PositionType::DiffInsert);
        assert_eq!(cursor.key_value(), Some((&4, &"4")));
        cursor.move_next();
        assert_eq!(cursor.position(), PositionType::Ghost);
        assert_eq!(cursor.key_value(), None);
        cursor.move_next();
        assert_eq!(cursor.position(), PositionType::DiffInsert);
        assert_eq!(cursor.key_value(), Some((&0, &"0")));
        cursor.move_prev();
        assert_eq!(cursor.position(), PositionType::Ghost);
        cursor.move_prev();
        assert_eq!(cursor.position(), PositionType::DiffInsert);
        assert_eq!(cursor.key_value(), Some((&4, &"4")));
        cursor.move_prev();
        assert_eq!(cursor.position(), PositionType::Snapshot);
        assert_eq!(cursor.key_value(), Some((&2, &"2")));
        cursor.move_prev();
        assert_eq!(cursor.position(), PositionType::DiffUpdate);
        assert_eq!(cursor.key_value(), Some((&1, &"1 new")));
        cursor.move_prev();
        assert_eq!(cursor.position(), PositionType::DiffInsert);
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
        let mut diff = BTreeMap::new();
        diff.insert(0, Change::Insert("0"));
        diff.insert(1, Change::Update("1 new"));
        diff.insert(5, Change::Delete);
        diff.insert(7, Change::Delete);
        diff.insert(9, Change::Delete);
        let diff_map = DiffBTreeMap::new(&map, diff);

        assert_eq!(diff_map.first_key(), Some(&0));
        assert_eq!(diff_map.last_key(), Some(&3));

        let mut cursor = diff_map.find(&0).unwrap();
        let mut res = vec![];
        while let Some((k, v)) = cursor.key_value() {
            res.push((*k, *v));
            cursor.move_next();
        }
        assert_eq!(res, vec![(0, "0"), (1, "1 new"), (3, "3")]);

        let mut cursor = diff_map.find(&3).unwrap();
        let mut res = vec![];
        while let Some((k, v)) = cursor.key_value() {
            res.push((*k, *v));
            cursor.move_prev();
        }
        assert_eq!(res, vec![(3, "3"), (1, "1 new"), (0, "0")]);
    }
}

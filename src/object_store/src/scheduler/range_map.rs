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
use std::collections::btree_map::{BTreeMap, CursorMut, Iter};
use std::fmt::Debug;
use std::ops::{Bound, Range};

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct OrdRange<Idx: Ord + Copy + Debug> {
    inner: Range<Idx>,
}

impl<Idx: Ord + Copy + Debug> Ord for OrdRange<Idx> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // // only compare `start`
        // self.inner.start.cmp(&other.inner.start)
        match self.inner.start.cmp(&other.inner.start) {
            Ordering::Equal => self.inner.end.cmp(&other.inner.end),
            cmp => cmp,
        }
    }
}

impl<Idx: Ord + Copy + Debug> PartialOrd for OrdRange<Idx> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<Idx: Ord + Copy + Debug> From<Range<Idx>> for OrdRange<Idx> {
    fn from(range: Range<Idx>) -> Self {
        Self { inner: range }
    }
}

impl<Idx: Ord + Copy + Debug> From<OrdRange<Idx>> for Range<Idx> {
    fn from(range: OrdRange<Idx>) -> Self {
        range.inner
    }
}

pub trait RangeExt: Sized {
    type Idx: Ord + Copy + Debug;

    fn new(start: Self::Idx, end: Self::Idx) -> Option<Self>;
    fn is_contiguous(lhs: &Self, rhs: &Self) -> bool;
    fn is_overlapping(lhs: &Self, rhs: &Self) -> bool;
    fn mergeable(lhs: &Self, rhs: &Self) -> bool;
    fn overlaps(lhs: &Self, rhs: &Self) -> Option<Self>;
    fn covers(lhs: &Self, rhs: &Self) -> bool;
    fn merge(lhs: &Self, rhs: &Self) -> Option<Self>;
    fn split(lhs: &Self, rhs: &Self) -> (Option<Self>, Option<Self>);
}

impl<Idx: Ord + Copy + Debug> RangeExt for Range<Idx> {
    type Idx = Idx;

    fn new(start: Self::Idx, end: Self::Idx) -> Option<Self> {
        if start >= end {
            None
        } else {
            Some(start..end)
        }
    }

    fn is_contiguous(lhs: &Self, rhs: &Self) -> bool {
        lhs.start == rhs.end || rhs.start == lhs.end
    }

    fn is_overlapping(lhs: &Self, rhs: &Self) -> bool {
        std::cmp::max(lhs.start, rhs.start) < std::cmp::min(lhs.end, rhs.end)
    }

    fn mergeable(lhs: &Self, rhs: &Self) -> bool {
        Self::is_contiguous(lhs, rhs) || Self::is_overlapping(lhs, rhs)
    }

    fn covers(lhs: &Self, rhs: &Self) -> bool {
        lhs.start <= rhs.start && lhs.end >= rhs.end
    }

    fn merge(lhs: &Self, rhs: &Self) -> Option<Self> {
        if !Self::mergeable(lhs, rhs) {
            return None;
        }
        Some(std::cmp::min(lhs.start, rhs.start)..std::cmp::max(lhs.end, rhs.end))
    }

    fn overlaps(lhs: &Self, rhs: &Self) -> Option<Self> {
        Self::new(
            std::cmp::max(lhs.start, rhs.start),
            std::cmp::min(lhs.end, rhs.end),
        )
    }

    /// Split `lhs` by `rhs`. Returns the left part and the right part.
    ///
    /// e.g.
    ///
    /// 1.
    /// [          lhs          )
    /// [   l   )[ rhs )[   r   )
    ///
    /// 2.
    ///    [          lhs          )
    /// [ rhs )[         r         )
    ///
    /// 3.
    /// [          lhs          )
    /// [         l         )[ rhs )
    ///
    /// 4.
    ///          [ lhs )
    /// [          rhs          )
    fn split(lhs: &Self, rhs: &Self) -> (Option<Self>, Option<Self>) {
        let l = Self::new(lhs.start, rhs.start);
        let r = Self::new(rhs.end, lhs.end);
        (l, r)
    }
}

/// [`RangeMap`] maintains [`Range`]s and related values.
///
/// [`Range`]s never overlaps with each other.
///
/// # Relations
///
/// Relations between two [`Range`] can be described as:
///
/// 1. `contiguous`: [0, 5) [5, 10)
/// 2. `overlaps`: [0, 5) [3, 8)
/// 3. `mergeable`: `contiguous` or `overlaps`
#[derive(Debug)]
pub struct RangeMap<Idx: Ord + Copy + Debug, T> {
    ranges: BTreeMap<OrdRange<Idx>, T>,
}

impl<Idx: Ord + Copy + Debug, T> Default for RangeMap<Idx, T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<Idx: Ord + Copy + Debug, T> RangeMap<Idx, T> {
    pub fn new() -> Self {
        Self {
            ranges: BTreeMap::new(),
        }
    }

    /// Insert a range with related value into [`RangeMap`].
    ///
    /// # Panics
    ///
    /// This function panics if the given `range` overlaps with other ranges.
    pub fn insert(&mut self, range: Range<Idx>, value: T) {
        let range = range.into();
        let cursor = self.ranges.lower_bound(Bound::Included(&range));
        if let Some(r) = cursor.key() && RangeExt::is_overlapping(&range.inner, &r.inner) {
            panic!("{:?} overlaps {:?}, cannot insert", range.inner, r.inner);
        }
        self.ranges.insert(range, value);
    }

    pub fn get(&self, range: Range<Idx>) -> Option<&T> {
        let range = range.into();
        self.ranges.get(&range)
    }

    pub fn remove(&mut self, range: Range<Idx>) -> Option<T> {
        let range = range.into();
        self.ranges.remove(&range)
    }

    pub fn merge<F>(&mut self, range: Range<Idx>, f: F)
    where
        F: FnOnce(Range<Idx>, Vec<(Range<Idx>, T)>) -> T,
    {
        let range = range.into();
        let mut cursor = self.ranges.lower_bound_mut(Bound::Included(&range));

        if let Some((r, _)) = cursor.peek_prev() && RangeExt::mergeable(&range.inner, &r.inner) {
            cursor.move_prev();
        }

        let mut merged = vec![];
        let mut new_range = range.clone();

        while let Some(r) = cursor.key() && RangeExt::mergeable(&range.inner, &r.inner) {
            let (r, v) = cursor.remove_current().unwrap();
            new_range = RangeExt::merge(&new_range.inner, &r.inner).unwrap().into();
            merged.push((r.into(),v));
        }

        let value = f(new_range.clone().into(), merged);
        self.ranges.insert(new_range, value);
    }

    pub fn split(&mut self, range: Range<Idx>) -> RangeMapSplitMut<'_, Idx, T> {
        RangeMapSplitMut::new(self, range)
    }

    pub fn covers(&mut self, range: Range<Idx>) -> Option<RangeMapCoversMut<'_, Idx, T>> {
        let range = range.into();
        let mut cursor = self.ranges.lower_bound_mut(Bound::Included(&range));
        if let Some((r, _)) = cursor.peek_prev() && RangeExt::covers(&r.inner, &range.inner) {
            cursor.move_prev();
            return Some(RangeMapCoversMut::new(cursor));
        }
        if let Some(r) = cursor.key() && RangeExt::covers(&r.inner, &range.inner) {
            return Some(RangeMapCoversMut::new(cursor));
        }
        None
    }

    pub fn len(&self) -> usize {
        self.ranges.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn iter(&self) -> RangeMapIter<'_, Idx, T> {
        RangeMapIter {
            iter: self.ranges.iter(),
        }
    }

    pub fn keys(&self) -> RangeMapKeyIter<'_, Idx, T> {
        RangeMapKeyIter {
            iter: self.ranges.iter(),
        }
    }

    pub fn values(&self) -> RangeMapValueIter<'_, Idx, T> {
        RangeMapValueIter {
            iter: self.ranges.iter(),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Entry<Idx: Ord + Copy + Debug> {
    Range(Range<Idx>),
    Gap(Range<Idx>),
}

pub struct RangeMapSplitMut<'a, Idx: Ord + Copy + Debug, T> {
    map: &'a mut RangeMap<Idx, T>,

    range: Range<Idx>,

    cursor: CursorMut<'a, OrdRange<Idx>, T>,

    entry: Option<Entry<Idx>>,

    buffer: Vec<(OrdRange<Idx>, T)>,
}

impl<'a, Idx: Ord + Copy + Debug, T> RangeMapSplitMut<'a, Idx, T> {
    pub fn new(map: &'a mut RangeMap<Idx, T>, range: Range<Idx>) -> Self {
        let ptr = map as *mut _;
        let mut cursor = map
            .ranges
            .lower_bound_mut(Bound::Included(&range.clone().into()));
        if let Some((r, _)) = cursor.peek_prev() && RangeExt::is_overlapping(&range, &r.inner) {
            cursor.move_prev();
        }
        let mut res = Self {
            map: unsafe { &mut *ptr },
            range,
            cursor,
            entry: None,
            buffer: vec![],
        };
        res.update();
        res
    }

    pub fn entry(&self) -> Option<&Entry<Idx>> {
        self.entry.as_ref()
    }

    pub fn skip(&mut self) {
        self.update();
    }

    pub fn value_mut(&mut self) -> Option<&mut T> {
        match self.entry {
            Some(Entry::Range(..)) => self.cursor.value_mut(),
            _ => None,
        }
    }

    pub fn insert(&mut self, value: T) {
        let Some(Entry::Gap(range)) = &self.entry else {
            panic!("only gap can be inserted, entry: {:?}", self.entry);
        };
        self.buffer.push((range.clone().into(), value));
        self.update();
    }

    fn update(&mut self) {
        if matches!(self.entry, Some(Entry::Range(..))) {
            self.cursor.move_next();
        }

        // Finish if `range` is empty.
        if self.range.is_empty() {
            self.entry = None;
            return;
        }

        // Set full if cursor is empty.
        let Some(r) = self.cursor.key() else {
            self.entry = Some(Entry::Gap(self.range.clone()));
            self.range = self.range.end..self.range.end;
            debug_assert!(self.range.is_empty());
            return;
        };

        if r.inner.start <= self.range.start {
            // 1.
            //   [ range ]
            // [  r  ]
            //
            // 2.
            //   [ range ]
            // [     r     ]
            self.entry = Some(Entry::Range(r.clone().into()));
            self.range = r.inner.end..self.range.end;
        } else {
            // 1.
            // [ range ]
            //   [ r ]
            //
            // 2.
            // [ range ]
            //      [  r  ]
            self.entry = Some(Entry::Gap(self.range.start..r.inner.start));
            self.range = r.inner.start..self.range.end;
        }
    }
}

impl<'a, Idx: Ord + Copy + Debug, T> Drop for RangeMapSplitMut<'a, Idx, T> {
    fn drop(&mut self) {
        for (range, value) in self.buffer.drain(..) {
            self.map.insert(range.into(), value);
        }
    }
}

pub struct RangeMapCoversMut<'a, Idx: Ord + Copy + Debug, T> {
    cursor: CursorMut<'a, OrdRange<Idx>, T>,
}

impl<'a, Idx: Ord + Copy + Debug, T> RangeMapCoversMut<'a, Idx, T> {
    pub fn new(cursor: CursorMut<'a, OrdRange<Idx>, T>) -> Self {
        assert!(cursor.key().is_some());
        Self { cursor }
    }

    pub fn range(&self) -> Range<Idx> {
        self.cursor.key().cloned().unwrap().into()
    }

    pub fn value(&self) -> &T {
        self.cursor.value().unwrap()
    }

    pub fn value_mut(&mut self) -> &mut T {
        self.cursor.value_mut().unwrap()
    }

    pub fn range_value_mut(&mut self) -> (Range<Idx>, &mut T) {
        (self.range(), self.value_mut())
    }
}

pub struct RangeMapIter<'a, Idx: Ord + Copy + Debug, T> {
    iter: Iter<'a, OrdRange<Idx>, T>,
}

impl<'a, Idx: Ord + Copy + Debug, T> Iterator for RangeMapIter<'a, Idx, T> {
    type Item = (Range<Idx>, &'a T);

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|(or, v)| (or.clone().into(), v))
    }
}

pub struct RangeMapKeyIter<'a, Idx: Ord + Copy + Debug, T> {
    iter: Iter<'a, OrdRange<Idx>, T>,
}

impl<'a, Idx: Ord + Copy + Debug, T> Iterator for RangeMapKeyIter<'a, Idx, T> {
    type Item = Range<Idx>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|(or, _)| or.clone().into())
    }
}

pub struct RangeMapValueIter<'a, Idx: Ord + Copy + Debug, T> {
    iter: Iter<'a, OrdRange<Idx>, T>,
}

impl<'a, Idx: Ord + Copy + Debug, T> Iterator for RangeMapValueIter<'a, Idx, T> {
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        match self.iter.next() {
            None => None,
            Some((_, v)) => Some(v),
        }
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;

    use super::*;

    #[test]
    fn test_range_map_simple() {
        let mut m = RangeMap::new();

        m.insert(0..10, 0);
        m.insert(20..30, 2);
        m.insert(10..20, 1);

        assert_eq!(m.keys().collect_vec(), vec![0..10, 10..20, 20..30]);
        assert_eq!(m.values().copied().collect_vec(), vec![0, 1, 2]);

        assert_eq!(m.get(10..20), Some(&1));
        assert_eq!(m.remove(10..20), Some(1));
    }

    #[test]
    #[should_panic]
    fn test_range_map_panic() {
        let mut m = RangeMap::new();

        m.insert(0..10, 0);
        m.insert(20..30, 2);
        m.insert(5..25, 1);
    }

    #[test]
    fn test_range_map_merge() {
        let mut m = RangeMap::new();

        // { 0..10 => 0 }
        m.merge(0..10, |range, merged| {
            assert_eq!(range, 0..10);
            assert_eq!(merged, vec![]);
            0
        });

        // { 0..20 => 1 }
        m.merge(10..20, |range, merged| {
            assert_eq!(range, 0..20);
            assert_eq!(merged, vec![(0..10, 0)]);
            1
        });

        // { 0..20 => 2 }
        m.merge(5..15, |range, merged| {
            assert_eq!(range, 0..20);
            assert_eq!(merged, vec![(0..20, 1)]);
            2
        });

        // { 0..20 => 2, 30..40 => 3 }
        m.merge(30..40, |range, merged| {
            assert_eq!(range, 30..40);
            assert_eq!(merged, vec![]);
            3
        });

        // { 0..20 => 2, 30..40 => 3, 50..60 => 4 }
        m.merge(50..60, |range, merged| {
            assert_eq!(range, 50..60);
            assert_eq!(merged, vec![]);
            4
        });

        // { 0..40 => 5, 50..60 => 4 }
        m.merge(20..30, |range, merged| {
            assert_eq!(range, 0..40);
            assert_eq!(merged, vec![(0..20, 2), (30..40, 3)]);
            5
        });

        // { 0..60 => 6 }
        m.merge(35..50, |range, merged| {
            assert_eq!(range, 0..60);
            assert_eq!(merged, vec![(0..40, 5), (50..60, 4)]);
            6
        });

        // { 0..60 => 6, 60..70 => 7, 80..90 => 8, 90..100 => 9 }
        m.insert(60..70, 7);
        m.insert(80..90, 8);
        m.insert(90..100, 9);

        // { 0..60 => 6, 60..90 => 10, 90..100 => 9 }
        m.merge(70..80, |range, merged| {
            assert_eq!(range, 60..90);
            assert_eq!(merged, vec![(60..70, 7), (80..90, 8)]);
            10
        });

        assert_eq!(m.keys().collect_vec(), vec![0..60, 60..90, 90..100]);
        assert_eq!(m.values().copied().collect_vec(), vec![6, 10, 9]);
    }

    #[test]
    fn test_range_map_split() {
        let mut m = RangeMap::new();

        // { 0..10 => 0, 10..20 => 1, 20..30 => 2 }
        let mut s = m.split(0..10);
        assert_eq!(s.entry(), Some(&Entry::Gap(0..10)));
        s.insert(0);
        assert_eq!(s.entry(), None);
        drop(s);
        let mut s = m.split(10..20);
        assert_eq!(s.entry(), Some(&Entry::Gap(10..20)));
        s.insert(1);
        assert_eq!(s.entry(), None);
        drop(s);
        let mut s = m.split(20..30);
        assert_eq!(s.entry(), Some(&Entry::Gap(20..30)));
        s.insert(2);
        assert_eq!(s.entry(), None);
        drop(s);
        assert_eq!(m.keys().collect_vec(), vec![0..10, 10..20, 20..30]);
        assert_eq!(m.values().copied().collect_vec(), vec![0, 1, 2]);

        let mut s = m.split(10..20);
        assert_eq!(s.entry(), Some(&Entry::Range(10..20)));
        assert_eq!(s.value_mut().unwrap(), &mut 1);
        s.skip();
        assert_eq!(s.entry(), None);
        drop(s);

        let mut s = m.split(0..30);
        assert_eq!(s.entry(), Some(&Entry::Range(0..10)));
        assert_eq!(s.value_mut().unwrap(), &mut 0);
        s.skip();
        assert_eq!(s.entry(), Some(&Entry::Range(10..20)));
        assert_eq!(s.value_mut().unwrap(), &mut 1);
        s.skip();
        assert_eq!(s.entry(), Some(&Entry::Range(20..30)));
        assert_eq!(s.value_mut().unwrap(), &mut 2);
        s.skip();
        assert_eq!(s.entry(), None);
        drop(s);

        let mut m = RangeMap::new();
        m.insert(10..20, 0);

        let mut s = m.split(15..30);
        assert_eq!(s.entry(), Some(&Entry::Range(10..20)));
        assert_eq!(s.value_mut().unwrap(), &mut 0);
        s.skip();
        assert_eq!(s.entry(), Some(&Entry::Gap(20..30)));
        s.insert(1);
        assert_eq!(s.entry(), None);
        drop(s);
        assert_eq!(m.keys().collect_vec(), vec![10..20, 20..30]);
        assert_eq!(m.values().copied().collect_vec(), vec![0, 1]);

        let mut m = RangeMap::new();
        m.insert(0..30, 0);
        let mut s = m.split(10..20);
        assert_eq!(s.entry(), Some(&Entry::Range(0..30)));
        assert_eq!(s.value_mut().unwrap(), &mut 0);
        *s.value_mut().unwrap() = 1;
        s.skip();
        assert_eq!(s.entry(), None);
        drop(s);
        assert_eq!(m.keys().collect_vec(), vec![0..30]);
        assert_eq!(m.values().copied().collect_vec(), vec![1]);

        let mut m = RangeMap::new();
        m.insert(0..10, 0);
        m.insert(20..30, 1);
        m.insert(30..40, 2);
        m.insert(50..60, 3);

        let mut s = m.split(0..70);
        assert_eq!(s.entry(), Some(&Entry::Range(0..10)));
        assert_eq!(s.value_mut().unwrap(), &mut 0);
        s.skip();
        assert_eq!(s.entry(), Some(&Entry::Gap(10..20)));
        s.insert(4);
        assert_eq!(s.entry(), Some(&Entry::Range(20..30)));
        assert_eq!(s.value_mut().unwrap(), &mut 1);
        s.skip();
        assert_eq!(s.entry(), Some(&Entry::Range(30..40)));
        assert_eq!(s.value_mut().unwrap(), &mut 2);
        *s.value_mut().unwrap() = 7;
        s.skip();
        assert_eq!(s.entry(), Some(&Entry::Gap(40..50)));
        s.insert(5);
        assert_eq!(s.entry(), Some(&Entry::Range(50..60)));
        assert_eq!(s.value_mut().unwrap(), &mut 3);
        s.skip();
        assert_eq!(s.entry(), Some(&Entry::Gap(60..70)));
        s.insert(6);
        assert_eq!(s.entry(), None);
        drop(s);
        assert_eq!(
            m.keys().collect_vec(),
            vec![0..10, 10..20, 20..30, 30..40, 40..50, 50..60, 60..70]
        );
        assert_eq!(m.values().copied().collect_vec(), vec![0, 4, 1, 7, 5, 3, 6]);
    }

    #[test]
    fn test_range_map_covers() {
        let mut m = RangeMap::new();

        m.insert(10..20, 0);

        assert_eq!(
            m.covers(10..20).unwrap().range_value_mut(),
            (10..20, &mut 0)
        );
        assert_eq!(
            m.covers(12..18).unwrap().range_value_mut(),
            (10..20, &mut 0)
        );
        assert!(m.covers(0..30).is_none());
    }
}

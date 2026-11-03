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

use std::cmp::Ordering;
use std::ops::Bound;

use crate::types::{Datum, ScalarImpl};
use crate::util::scan_range::ScanRange;
use crate::util::sort_util::{OrderType, cmp_datum};

/// A position in the key order of a table or an index: right before or right after all the keys
/// starting with `prefix`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cut {
    prefix: Vec<Datum>,
    after: bool,
}

impl Cut {
    fn before(prefix: Vec<Datum>) -> Self {
        Self {
            prefix,
            after: false,
        }
    }

    fn after(prefix: Vec<Datum>) -> Self {
        Self {
            prefix,
            after: true,
        }
    }

    /// Compares positions in key order. `order` has the order type of each column of the prefixes.
    fn cmp(&self, other: &Self, order: &[OrderType]) -> Ordering {
        let common_len = self.prefix.len().min(other.prefix.len());
        if let Some(ordering) = (0..common_len)
            .map(|i| cmp_datum(&self.prefix[i], &other.prefix[i], order[i]))
            .find(|ordering| ordering.is_ne())
        {
            return ordering;
        }
        match self.prefix.len().cmp(&other.prefix.len()) {
            Ordering::Equal => self.after.cmp(&other.after),
            Ordering::Less if self.after => Ordering::Greater,
            Ordering::Less => Ordering::Less,
            Ordering::Greater if other.after => Ordering::Less,
            Ordering::Greater => Ordering::Greater,
        }
    }
}

/// A set of keys in key order, as ranges between two [`Cut`]s. Unlike a list of [`ScanRange`]s, it
/// has one form for each set, an empty set is different from a full one, and every position means
/// the same thing wherever it is used, so unions and intersections are exact.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeySet {
    /// Non-empty, sorted and disjoint. Ranges that touch are merged.
    spans: Vec<(Cut, Cut)>,
}

impl KeySet {
    pub fn empty() -> Self {
        Self { spans: vec![] }
    }

    pub fn full() -> Self {
        Self {
            spans: vec![(Cut::before(vec![]), Cut::after(vec![]))],
        }
    }

    pub fn is_empty(&self) -> bool {
        self.spans.is_empty()
    }

    pub fn is_full(&self) -> bool {
        *self == Self::full()
    }

    /// The number of disjoint ranges.
    pub fn len(&self) -> usize {
        self.spans.len()
    }

    /// The keys whose first column is one of `values`. NULL is a value here, for `IS NULL`.
    pub fn any_of(values: impl IntoIterator<Item = Datum>, order_type: OrderType) -> Self {
        Self::from_spans(
            values
                .into_iter()
                .map(|value| (Cut::before(vec![value.clone()]), Cut::after(vec![value])))
                .collect(),
            &[order_type],
        )
    }

    /// The keys whose first column is between the given values. A comparison is never true for
    /// NULL, so NULL is not in the set.
    pub fn between(
        lower: Bound<ScalarImpl>,
        upper: Bound<ScalarImpl>,
        order_type: OrderType,
    ) -> Self {
        fn start(bound: Bound<ScalarImpl>) -> Cut {
            match bound {
                Bound::Included(value) => Cut::before(vec![Some(value)]),
                Bound::Excluded(value) => Cut::after(vec![Some(value)]),
                Bound::Unbounded => Cut::before(vec![]),
            }
        }
        fn end(bound: Bound<ScalarImpl>) -> Cut {
            match bound {
                Bound::Included(value) => Cut::after(vec![Some(value)]),
                Bound::Excluded(value) => Cut::before(vec![Some(value)]),
                Bound::Unbounded => Cut::after(vec![]),
            }
        }
        let order = [order_type];
        let values = if order_type.is_ascending() {
            (start(lower), end(upper))
        } else {
            (start(upper), end(lower))
        };
        let not_null = if order_type.nulls_are_first() {
            (Cut::after(vec![None]), Cut::after(vec![]))
        } else {
            (Cut::before(vec![]), Cut::before(vec![None]))
        };
        Self::from_spans(vec![values], &order)
            .intersect(&Self::from_spans(vec![not_null], &order), &order)
    }

    /// The keys that come after `key` in key order.
    pub fn after_key(key: Vec<Datum>, order: &[OrderType]) -> Self {
        Self::from_spans(vec![(Cut::after(key), Cut::after(vec![]))], order)
    }

    fn from_spans(mut spans: Vec<(Cut, Cut)>, order: &[OrderType]) -> Self {
        spans.retain(|(start, end)| start.cmp(end, order).is_lt());
        spans.sort_by(|(l, _), (r, _)| l.cmp(r, order));
        let mut merged: Vec<(Cut, Cut)> = Vec::with_capacity(spans.len());
        for (start, end) in spans {
            match merged.last_mut() {
                Some((_, last_end)) if start.cmp(last_end, order).is_le() => {
                    if end.cmp(last_end, order).is_gt() {
                        *last_end = end;
                    }
                }
                _ => merged.push((start, end)),
            }
        }
        Self { spans: merged }
    }

    pub fn union_all(sets: impl IntoIterator<Item = Self>, order: &[OrderType]) -> Self {
        Self::from_spans(sets.into_iter().flat_map(|set| set.spans).collect(), order)
    }

    pub fn intersect(&self, other: &Self, order: &[OrderType]) -> Self {
        let mut spans = vec![];
        let (mut i, mut j) = (0, 0);
        while let (Some((l_start, l_end)), Some((r_start, r_end))) =
            (self.spans.get(i), other.spans.get(j))
        {
            let start = if l_start.cmp(r_start, order).is_ge() {
                l_start
            } else {
                r_start
            };
            let l_ends_first = l_end.cmp(r_end, order).is_lt();
            let end = if l_ends_first { l_end } else { r_end };
            if start.cmp(end, order).is_lt() {
                spans.push((start.clone(), end.clone()));
            }
            if l_ends_first {
                i += 1;
            } else {
                j += 1;
            }
        }
        Self { spans }
    }

    /// Whether `key`, a full key or a prefix of one, is in the set.
    pub fn contains(&self, key: &[Datum], order: &[OrderType]) -> bool {
        let position = Cut::before(key.to_vec());
        self.spans.iter().any(|(start, end)| {
            start.cmp(&position, order).is_le() && position.cmp(end, order).is_lt()
        })
    }

    /// Whether every range is exactly one value of the first column, so that the next column can
    /// narrow the set further with [`Self::refine`].
    pub fn is_single_values(&self) -> bool {
        self.spans.iter().all(|(start, end)| {
            start.prefix.len() == 1 && !start.after && end.after && start.prefix == end.prefix
        })
    }

    /// Narrows each value of the first column with `rest`, a set over the columns after it.
    ///
    /// # Panics
    /// Panics if `self` is not [`Self::is_single_values`].
    pub fn refine(&self, rest: &Self) -> Self {
        assert!(self.is_single_values());
        let with_value = |value: &Datum, cut: &Cut| Cut {
            prefix: std::iter::once(value.clone())
                .chain(cut.prefix.iter().cloned())
                .collect(),
            after: cut.after,
        };
        let mut spans = Vec::with_capacity(self.spans.len() * rest.spans.len());
        for (start, _) in &self.spans {
            let value = &start.prefix[0];
            for (rest_start, rest_end) in &rest.spans {
                spans.push((with_value(value, rest_start), with_value(value, rest_end)));
            }
        }
        Self { spans }
    }

    /// Converts to [`ScanRange`]s that select exactly this set, in key order.
    ///
    /// An empty list of scan ranges reads the whole table, so check [`Self::is_empty`] first.
    pub fn into_scan_ranges(self, order: &[OrderType], max_split_range_gap: u64) -> Vec<ScanRange> {
        if self.is_full() {
            return vec![];
        }
        self.spans
            .into_iter()
            .map(|(start, end)| span_to_scan_range(start, end, order))
            .flat_map(|scan_range| {
                match scan_range.split_small_range(max_split_range_gap) {
                    // These are in the order of the values, and only split the first column.
                    Some(mut scan_ranges) => {
                        if order[0].is_descending() {
                            scan_ranges.reverse();
                        }
                        scan_ranges
                    }
                    None => vec![scan_range],
                }
            })
            .collect()
    }

    /// The one [`ScanRange`] from the start of this set to its end, for a reader of a single range.
    /// It selects this set and what is between its ranges. `None` if the set is empty, or if that is
    /// the whole table.
    pub fn covering_scan_range(&self, order: &[OrderType]) -> Option<ScanRange> {
        let start = self.spans.first()?.0.clone();
        let end = self.spans.last()?.1.clone();
        let scan_range = span_to_scan_range(start, end, order);
        (!scan_range.is_full_table_scan()).then_some(scan_range)
    }

    /// The values of a key prefix that this set consists of, if every range is exactly one of them.
    pub fn as_values(&self) -> Option<Vec<Vec<Datum>>> {
        self.spans
            .iter()
            .map(|(start, end)| {
                let is_value = !start.prefix.is_empty()
                    && !start.after
                    && end.after
                    && start.prefix == end.prefix;
                is_value.then(|| start.prefix.clone())
            })
            .collect()
    }
}

fn common_prefix_len(l: &Cut, r: &Cut) -> usize {
    let max_len = l.prefix.len().min(r.prefix.len());
    (0..max_len)
        .find(|&i| l.prefix[i] != r.prefix[i])
        .unwrap_or(max_len)
}

/// See `PkScanRange::convert_to_range_bounds` and `serialize_pk_bound` in the storage crate for how
/// a [`ScanRange`] is read: bounds are swapped when its first range column is descending, and an
/// unbounded side skips the NULLs of that column if they are on that side and the other side is
/// bounded.
fn span_to_scan_range(start: Cut, end: Cut, order: &[OrderType]) -> ScanRange {
    let eq_len = common_prefix_len(&start, &end);

    let eq_conds = start.prefix[..eq_len].to_vec();
    let bound = |rest: &[Datum], included: bool| match (rest.is_empty(), included) {
        (true, _) => Bound::Unbounded,
        (false, true) => Bound::Included(rest.to_vec()),
        (false, false) => Bound::Excluded(rest.to_vec()),
    };
    let first = bound(&start.prefix[eq_len..], !start.after);
    let last = bound(&end.prefix[eq_len..], end.after);

    let Some(order_type) = order.get(eq_len) else {
        return ScanRange {
            eq_conds,
            range: (first, last),
        };
    };
    let excludes_null =
        |bound: &Bound<Vec<Datum>>| matches!(bound, Bound::Excluded(rest) if rest[..] == [None]);
    let (first, last) = match (first, last) {
        (Bound::Unbounded, last @ (Bound::Included(_) | Bound::Excluded(_)))
            if order_type.nulls_are_first() =>
        {
            (Bound::Included(vec![None]), last)
        }
        (first @ (Bound::Included(_) | Bound::Excluded(_)), Bound::Unbounded)
            if order_type.nulls_are_last() =>
        {
            (first, Bound::Included(vec![None]))
        }
        // Spelled the same way as a plain comparison when that selects the same keys.
        (first, last)
            if excludes_null(&first)
                && order_type.nulls_are_first()
                && !matches!(last, Bound::Unbounded) =>
        {
            (Bound::Unbounded, last)
        }
        (first, last)
            if excludes_null(&last)
                && order_type.nulls_are_last()
                && !matches!(first, Bound::Unbounded) =>
        {
            (first, Bound::Unbounded)
        }
        bounds => bounds,
    };
    ScanRange {
        eq_conds,
        range: if order_type.is_ascending() {
            (first, last)
        } else {
            (last, first)
        },
    }
}

#[cfg(test)]
mod tests {
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};

    use super::*;

    fn int(v: i32) -> Datum {
        Some(ScalarImpl::Int32(v))
    }

    const NUM_COLUMNS: usize = 3;

    /// Every key of three columns over a few values, in no particular order.
    fn all_keys() -> Vec<Vec<Datum>> {
        let values = [None, int(0), int(1), int(2), int(3)];
        let mut keys = vec![];
        for a in &values {
            for b in &values {
                for c in &values {
                    keys.push(vec![a.clone(), b.clone(), c.clone()]);
                }
            }
        }
        keys
    }

    fn assert_canonical(set: &KeySet, order: &[OrderType]) {
        for (start, end) in &set.spans {
            assert!(start.cmp(end, order).is_lt(), "empty range in {set:?}");
        }
        for pair in set.spans.windows(2) {
            assert!(
                pair[0].1.cmp(&pair[1].0, order).is_lt(),
                "not disjoint: {set:?}"
            );
        }
    }

    fn random_order(rng: &mut StdRng) -> Vec<OrderType> {
        let all = OrderType::all();
        (0..NUM_COLUMNS)
            .map(|_| all[rng.random_range(0..all.len())])
            .collect()
    }

    fn random_cut(rng: &mut StdRng) -> Cut {
        let values = [None, int(0), int(1), int(2), int(3)];
        let len = rng.random_range(0..=NUM_COLUMNS);
        Cut {
            prefix: (0..len)
                .map(|_| values[rng.random_range(0..values.len())].clone())
                .collect(),
            after: rng.random_bool(0.5),
        }
    }

    fn random_set(rng: &mut StdRng, order: &[OrderType]) -> KeySet {
        let spans = (0..rng.random_range(0..4))
            .map(|_| (random_cut(rng), random_cut(rng)))
            .collect();
        KeySet::from_spans(spans, order)
    }

    /// Reads a [`ScanRange`] back the way the storage crate does.
    fn read_back(scan_range: &ScanRange, order: &[OrderType]) -> (Cut, Cut) {
        let eq_len = scan_range.eq_conds.len();
        let (lower, upper) = scan_range.range.clone();
        let (mut first, mut last) = match order.get(eq_len) {
            Some(order_type) if order_type.is_descending() => (upper, lower),
            _ => (lower, upper),
        };
        if let Some(order_type) = order.get(eq_len) {
            let null = || Bound::Excluded(vec![None]);
            if matches!(first, Bound::Unbounded)
                && !matches!(last, Bound::Unbounded)
                && order_type.nulls_are_first()
            {
                first = null();
            } else if matches!(last, Bound::Unbounded)
                && !matches!(first, Bound::Unbounded)
                && order_type.nulls_are_last()
            {
                last = null();
            }
        }
        let with_eq = |rest: &[Datum]| {
            scan_range
                .eq_conds
                .iter()
                .chain(rest)
                .cloned()
                .collect::<Vec<_>>()
        };
        let start = match &first {
            Bound::Included(rest) => Cut::before(with_eq(rest)),
            Bound::Excluded(rest) => Cut::after(with_eq(rest)),
            Bound::Unbounded => Cut::before(with_eq(&[])),
        };
        let end = match &last {
            Bound::Included(rest) => Cut::after(with_eq(rest)),
            Bound::Excluded(rest) => Cut::before(with_eq(rest)),
            Bound::Unbounded => Cut::after(with_eq(&[])),
        };
        (start, end)
    }

    #[test]
    fn test_algebra_against_membership() {
        let keys = all_keys();
        let mut rng = StdRng::seed_from_u64(27783);
        for _ in 0..3000 {
            let order = random_order(&mut rng);
            let (a, b) = (random_set(&mut rng, &order), random_set(&mut rng, &order));
            assert_canonical(&a, &order);
            let union = KeySet::union_all([a.clone(), b.clone()], &order);
            let intersection = a.intersect(&b, &order);
            assert_canonical(&union, &order);
            assert_canonical(&intersection, &order);
            for key in &keys {
                let (in_a, in_b) = (a.contains(key, &order), b.contains(key, &order));
                assert_eq!(
                    union.contains(key, &order),
                    in_a || in_b,
                    "{a:?} {b:?} {key:?}"
                );
                assert_eq!(
                    intersection.contains(key, &order),
                    in_a && in_b,
                    "{a:?} {b:?} {key:?}"
                );
            }
        }
    }

    #[test]
    fn test_scan_ranges_read_back_as_the_same_keys() {
        let keys = all_keys();
        let mut rng = StdRng::seed_from_u64(27767);
        for _ in 0..3000 {
            let order = random_order(&mut rng);
            let set = random_set(&mut rng, &order);
            if set.is_empty() {
                continue;
            }
            let scan_ranges = set.clone().into_scan_ranges(&order, rng.random_range(0..4));
            let spans: Vec<_> = scan_ranges.iter().map(|r| read_back(r, &order)).collect();
            for pair in spans.windows(2) {
                assert!(
                    pair[0].1.cmp(&pair[1].0, &order).is_le(),
                    "not in key order: {set:?} {scan_ranges:?}"
                );
            }
            let read = if spans.is_empty() {
                KeySet::full()
            } else {
                KeySet::from_spans(spans, &order)
            };
            let covering = set
                .covering_scan_range(&order)
                .map_or_else(KeySet::full, |r| {
                    KeySet::from_spans(vec![read_back(&r, &order)], &order)
                });
            for key in &keys {
                let in_set = set.contains(key, &order);
                assert_eq!(
                    read.contains(key, &order),
                    in_set,
                    "{set:?} {scan_ranges:?} {key:?}"
                );
                assert!(!in_set || covering.contains(key, &order), "{set:?} {key:?}");
            }
        }
    }

    #[test]
    fn test_between_is_the_same_as_comparing_values() {
        let values = [None, int(0), int(1), int(2), int(3)];
        let bounds = |v: i32| {
            [
                Bound::Included(ScalarImpl::Int32(v)),
                Bound::Excluded(ScalarImpl::Int32(v)),
                Bound::Unbounded,
            ]
        };
        let in_bounds = |value: i32, lower: &Bound<ScalarImpl>, upper: &Bound<ScalarImpl>| {
            (match lower {
                Bound::Included(l) => value >= *l.as_int32(),
                Bound::Excluded(l) => value > *l.as_int32(),
                Bound::Unbounded => true,
            }) && (match upper {
                Bound::Included(u) => value <= *u.as_int32(),
                Bound::Excluded(u) => value < *u.as_int32(),
                Bound::Unbounded => true,
            })
        };
        for order_type in OrderType::all() {
            let order = [order_type];
            for lower in bounds(1) {
                for upper in bounds(2) {
                    let set = KeySet::between(lower.clone(), upper.clone(), order_type);
                    assert_canonical(&set, &order);
                    for value in &values {
                        let expected = match value {
                            Some(ScalarImpl::Int32(v)) => in_bounds(*v, &lower, &upper),
                            _ => false,
                        };
                        assert_eq!(
                            set.contains(std::slice::from_ref(value), &order),
                            expected,
                            "{order_type:?} {lower:?} {upper:?} {value:?}"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn test_row_comparison_ranges() {
        // `(a, b) > (1, 5)` on two ascending NULLS LAST columns.
        let order = [OrderType::ascending(), OrderType::ascending()];
        let set = KeySet::from_spans(
            vec![
                (
                    Cut::after(vec![int(1), int(5)]),
                    Cut::before(vec![int(1), None]),
                ),
                (Cut::after(vec![int(1)]), Cut::before(vec![None])),
            ],
            &order,
        );
        assert_eq!(
            set.covering_scan_range(&order),
            Some(ScanRange {
                eq_conds: vec![],
                range: (Bound::Excluded(vec![int(1), int(5)]), Bound::Unbounded),
            })
        );
        assert_eq!(
            set.into_scan_ranges(&order, 0),
            vec![
                ScanRange {
                    eq_conds: vec![int(1)],
                    range: (Bound::Excluded(vec![int(5)]), Bound::Unbounded),
                },
                ScanRange {
                    eq_conds: vec![],
                    range: (Bound::Excluded(vec![int(1)]), Bound::Unbounded),
                },
            ]
        );
    }

    #[test]
    fn test_covering_scan_range_of_nothing_to_narrow() {
        let order = [OrderType::ascending()];
        assert_eq!(KeySet::empty().covering_scan_range(&order), None);
        assert_eq!(KeySet::full().covering_scan_range(&order), None);
    }

    #[test]
    fn test_as_values() {
        let order = [OrderType::ascending(), OrderType::ascending()];
        let a = KeySet::any_of([None, int(1)], order[0]);
        assert_eq!(a.as_values(), Some(vec![vec![int(1)], vec![None]]));
        let b = KeySet::any_of([int(3)], order[1]);
        assert_eq!(
            a.refine(&b).as_values(),
            Some(vec![vec![int(1), int(3)], vec![None, int(3)]])
        );
        let greater = KeySet::between(
            Bound::Excluded(ScalarImpl::Int32(5)),
            Bound::Unbounded,
            order[1],
        );
        assert_eq!(a.refine(&greater).as_values(), None);
        assert_eq!(KeySet::full().as_values(), None);
    }

    #[test]
    fn test_plain_comparisons_keep_their_spelling() {
        for (order_type, lower, upper) in [
            (
                OrderType::ascending(),
                Bound::Excluded(int(5).unwrap()),
                Bound::Unbounded,
            ),
            (
                OrderType::ascending(),
                Bound::Unbounded,
                Bound::Excluded(int(5).unwrap()),
            ),
            (
                OrderType::descending(),
                Bound::Excluded(int(5).unwrap()),
                Bound::Unbounded,
            ),
            (
                OrderType::descending(),
                Bound::Unbounded,
                Bound::Excluded(int(5).unwrap()),
            ),
            (
                OrderType::ascending_nulls_first(),
                Bound::Unbounded,
                Bound::Excluded(int(5).unwrap()),
            ),
            (
                OrderType::descending_nulls_last(),
                Bound::Excluded(int(5).unwrap()),
                Bound::Unbounded,
            ),
        ] {
            let order = [order_type];
            let set = KeySet::between(lower.clone(), upper.clone(), order_type);
            let scan_ranges = set.into_scan_ranges(&order, 0);
            let as_row = |bound: Bound<ScalarImpl>| match bound {
                Bound::Included(v) => Bound::Included(vec![Some(v)]),
                Bound::Excluded(v) => Bound::Excluded(vec![Some(v)]),
                Bound::Unbounded => Bound::Unbounded,
            };
            assert_eq!(
                scan_ranges,
                vec![ScanRange {
                    eq_conds: vec![],
                    range: (as_row(lower), as_row(upper)),
                }],
                "{order_type:?}"
            );
        }
    }

    #[test]
    fn test_refine_and_small_ranges() {
        let order = [OrderType::ascending(), OrderType::ascending()];
        let a = KeySet::any_of([int(2), int(1), int(2)], order[0]);
        assert_eq!(a.len(), 2);
        assert!(a.is_single_values());
        let b = KeySet::between(
            Bound::Excluded(ScalarImpl::Int32(5)),
            Bound::Unbounded,
            order[1],
        );
        assert!(!b.is_single_values());
        let in_a = |v| ScanRange {
            eq_conds: vec![int(v)],
            range: (Bound::Excluded(vec![int(5)]), Bound::Unbounded),
        };
        assert_eq!(
            a.refine(&b).into_scan_ranges(&order, 0),
            vec![in_a(1), in_a(2)]
        );

        let small = KeySet::between(
            Bound::Included(ScalarImpl::Int32(1)),
            Bound::Included(ScalarImpl::Int32(2)),
            order[0],
        );
        let point = |v| ScanRange {
            eq_conds: vec![int(v)],
            range: (Bound::Unbounded, Bound::Unbounded),
        };
        assert_eq!(small.into_scan_ranges(&order, 8), vec![point(1), point(2)]);

        // In key order on a descending column too.
        let descending = [OrderType::descending()];
        let small = KeySet::between(
            Bound::Included(ScalarImpl::Int32(1)),
            Bound::Included(ScalarImpl::Int32(2)),
            descending[0],
        );
        assert_eq!(
            small.into_scan_ranges(&descending, 8),
            vec![point(2), point(1)]
        );
    }

    #[test]
    fn test_after_key_with_null() {
        // A key with NULL still has a position, and NULLs after it stay in the set.
        let order = [OrderType::ascending(), OrderType::ascending()];
        let set = KeySet::after_key(vec![int(1), None], &order);
        assert!(!set.contains(&[int(1), int(5)], &order));
        assert!(set.contains(&[int(2), int(0)], &order));
        assert!(set.contains(&[None, int(0)], &order));
        let scan_range = set.covering_scan_range(&order).unwrap();
        let read = KeySet::from_spans(vec![read_back(&scan_range, &order)], &order);
        assert!(read.contains(&[None, int(0)], &order));
        assert!(!read.contains(&[int(1), int(5)], &order));
    }
}

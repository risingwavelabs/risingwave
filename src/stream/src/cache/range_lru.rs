// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::BTreeMap;
use std::ops::Bound::{self, *};

use risingwave_common::types::ScalarImpl;

type ScalarRange = (Bound<ScalarImpl>, Bound<ScalarImpl>);

#[derive(Default)]
struct RangeLru {
    // We should be aware of the existing range to reason about continuity and boundary conditions
    existing_range: Option<ScalarRange>,
    // Due to the path of range changes being continuous
    // Every continuous range of epochs is associated with a continuous range.
    last_accessed_at: BTreeMap<u64, ScalarRange>,
    // The secondary index allows us to update `last_accessed_at` in amortized constant time.
    // It allows us to search for epochs whose associated range overlaps with that of this epoch.
    // `false`/`true` in the bool value of the key indicates `Inclusive`/`Exclusive` respectively.
    // If the value is `None`, it indicates that the range bounded on the left by the key is not
    // contained in the `RangeLRU`.
    secondary_index: BTreeMap<(ScalarImpl, bool), Option<u64>>,
    // Since we cannot represent the lowest range (Unbounded, x) in our `BTreeMap`,
    // we represent its last-accessed epoch with this field.
    lower_unbounded_epoch: Option<u64>,
    last_accessed_epoch: u64,
}

impl RangeLru {
    /// Evicts ranges that are last accessed prior to the given epoch
    /// Due to continuity in the range changes, this range is always contiguous.
    ///
    /// returns: If some range is evicted, Some((the range to evict, the new `existing_range`)),
    /// else `None`
    pub(crate) fn evict_before(
        &mut self,
        epoch: u64,
    ) -> Option<(ScalarRange, Option<ScalarRange>)> {
        let mut range = None;
        let new = self.last_accessed_at.split_off(&epoch);
        let old = std::mem::replace(&mut self.last_accessed_at, new);

        // TODO: Can use fold for this.
        for r in old.values() {
            if let Some(r2) = range {
                range = Some(merge_continuous_ranges(r.clone(), r2));
            } else {
                range = Some(r.clone())
            }
        }
        if let Some(range) = range {
            // If there is a range, there is an existing range
            let existing_range = self.existing_range.as_ref().unwrap();
            if range == *existing_range {
                // Reset the LRU
                assert!(self.last_accessed_at.is_empty());
                self.lower_unbounded_epoch = None;
                self.secondary_index.clear();
                return Some((range, None));
            }
            let mut iter = old.iter().rev().peekable();
            let (from_bottom, new_existing_range) = if range.0 == existing_range.0 {
                // Evict from the bottom
                let new_existing_range = match range.1.clone() {
                    Unbounded => None,
                    Included(x) => Some((Excluded(x), existing_range.1.clone())),
                    Excluded(x) => Some((Included(x), existing_range.1.clone())),
                };
                self.existing_range = new_existing_range.clone();
                (true, new_existing_range)
            } else if range.1 == existing_range.1 {
                // Evict from the top
                let new_existing_range = match range.0.clone() {
                    Unbounded => None,
                    Included(x) => Some((existing_range.0.clone(), Excluded(x))),
                    Excluded(x) => Some((existing_range.0.clone(), Included(x))),
                };
                self.existing_range = new_existing_range.clone();
                (false, new_existing_range)
            } else {
                panic!("The evicted range {range:?} does not share a bound with the existing range {existing_range:?}");
            };

            // If from top, delete top part of the range
            if !from_bottom {
                // If from top remove the top marker as well.
                match range.1.clone() {
                    Included(x) => {
                        assert!(self.secondary_index.remove(&(x, true)).unwrap().is_none())
                    }
                    Excluded(x) => {
                        assert!(self.secondary_index.remove(&(x, false)).unwrap().is_none())
                    }
                    Unbounded => (),
                }
            }
            while let Some((k, v)) = iter.next() {
                match translate_lower_bound(v.0.clone()) {
                    Included(x) => {
                        if from_bottom || iter.peek().is_some() {
                            self.secondary_index.remove(&x).unwrap(); // TODO: assert the epochs are
                                                                      // the same?
                        } else {
                            // From top and last element, we should simply convert it to `None`
                            // instead of removing
                            let old = self.secondary_index.get_mut(&x).unwrap();
                            if let Some(e) = old {
                                assert_eq!(*e, *k);
                            }
                            *old = None;
                        }
                    }
                    Unbounded => assert_eq!(self.lower_unbounded_epoch.take().unwrap(), *k),
                    _ => unreachable!(),
                }
            }
            Some((range, new_existing_range))
        } else {
            None
        }
    }

    /// `access` is called when a range is accessed during an epoch. `access` assumes that
    /// the ranges provided form a continuous path as a function of the epochs.
    pub(crate) fn access(&mut self, range: ScalarRange, epoch: u64) {
        assert_ne!(epoch, 0);
        assert!(self.last_accessed_epoch < epoch);
        self.last_accessed_epoch = epoch;
        let new_existing_range = self
            .existing_range
            .take()
            .map_or(Some(range.clone()), |er| {
                Some(merge_continuous_ranges(range.clone(), er))
            });
        self.existing_range = new_existing_range;

        let mut last_deleted_epoch = None;
        let mut delete_upper_range = false;
        let mut remove_keys = vec![];
        {
            let encoded_range = (
                translate_lower_bound(range.0.clone()),
                translate_upper_bound(range.1.clone()),
            );

            let mut iter = self.secondary_index.range(encoded_range).peekable();

            while let Some((k, v)) = iter.next() {
                remove_keys.push(k.clone());
                // Delete all of these, replace it with our range. Keep track of the last deleted.
                // Exceptions:
                if iter.peek().is_some() {
                    // Not the last, so the epoch must be `Some`
                    self.last_accessed_at.remove(v.as_ref().unwrap());
                } else {
                    if let Some(e) = v {
                        // Our secondary index contains this epoch, so it must be `Some`
                        {
                            let split_range = self.last_accessed_at.get_mut(e).unwrap();
                            if split_range.1 == range.1 {
                                delete_upper_range = true;
                            } else {
                                split_range.0 = match range.1.clone() {
                                    Excluded(x) => Included(x),
                                    Included(x) => Excluded(x),
                                    _ => unreachable!(),
                                };
                            }
                        }
                        if delete_upper_range {
                            self.last_accessed_at.remove(e).unwrap();
                        }
                    }
                    last_deleted_epoch = *v;
                }
            }
            // Deal with the precursor to the range (by shortening it).
            if !matches!(range.0, Unbounded) {
                let upper = match translate_lower_bound(range.0.clone()) {
                    Included(x) => Excluded(x),
                    _ => unreachable!(),
                };
                let lower_range = (Unbounded, upper);
                let mut rev_iter = self.secondary_index.range(lower_range).rev();

                let e = rev_iter
                    .next()
                    .map_or(self.lower_unbounded_epoch, |(_, &x)| x);
                if let Some(e) = e {
                    let split_range = self.last_accessed_at.get_mut(&e).unwrap();
                    split_range.1 = match range.0.clone() {
                        Excluded(x) => Included(x),
                        Included(x) => Excluded(x),
                        _ => unreachable!(),
                    };
                }
            }
        }
        for key in remove_keys {
            self.secondary_index.remove(&key).unwrap();
        }
        self.last_accessed_at.insert(epoch, range.clone());

        // Insert lower bound of our range into secondary index
        if matches!(range.0, Unbounded) {
            self.lower_unbounded_epoch = Some(epoch);
        } else {
            let (k, v) = match translate_lower_bound(range.0) {
                Included(k) => (k, Some(epoch)),
                _ => unreachable!(),
            };
            self.secondary_index.insert(k, v);
        }

        // Insert upper bound of our range into secondary index
        if delete_upper_range || matches!(range.1, Unbounded) { // do nothing
        } else {
            let k = match range.1 {
                Included(x) => (x, true),
                Excluded(x) => (x, false),
                _ => unreachable!(),
            };
            // If is `Some`, we hit a boundary condition.
            if self.secondary_index.get(&k).is_none() {
                self.secondary_index.insert(k, last_deleted_epoch);
            }
        }
    }
}

fn merge_continuous_ranges(r1: ScalarRange, r2: ScalarRange) -> ScalarRange {
    // We take the extreme values
    let lower = if matches!(r1.0, Unbounded) | matches!(r2.0, Unbounded) {
        Unbounded
    } else {
        match (r1.0, r2.0) {
            (Excluded(x), Excluded(y)) => {
                if x < y {
                    Excluded(x)
                } else {
                    Excluded(y)
                }
            }
            (Included(x), Included(y)) => {
                if x < y {
                    Included(x)
                } else {
                    Included(y)
                }
            }
            (Included(x), Excluded(y)) => {
                if x <= y {
                    Included(x)
                } else {
                    Excluded(y)
                }
            }
            (Excluded(x), Included(y)) => {
                if y <= x {
                    Included(y)
                } else {
                    Excluded(x)
                }
            }
            _ => unreachable!(),
        }
    };

    let upper = if matches!(r1.1, Unbounded) | matches!(r2.1, Unbounded) {
        Unbounded
    } else {
        match (r1.1, r2.1) {
            (Excluded(x), Excluded(y)) => {
                if x > y {
                    Excluded(x)
                } else {
                    Excluded(y)
                }
            }
            (Included(x), Included(y)) => {
                if x > y {
                    Included(x)
                } else {
                    Included(y)
                }
            }
            (Included(x), Excluded(y)) => {
                if x >= y {
                    Included(x)
                } else {
                    Excluded(y)
                }
            }
            (Excluded(x), Included(y)) => {
                if y >= x {
                    Included(y)
                } else {
                    Excluded(x)
                }
            }
            _ => unreachable!(),
        }
    };
    (lower, upper)
}

fn translate_lower_bound(bound: Bound<ScalarImpl>) -> Bound<(ScalarImpl, bool)> {
    match bound {
        Included(x) => Included((x, false)),
        Excluded(x) => Included((x, true)),
        Unbounded => Unbounded,
    }
}

fn translate_upper_bound(bound: Bound<ScalarImpl>) -> Bound<(ScalarImpl, bool)> {
    match bound {
        Excluded(x) => Excluded((x, false)), // Everything before (x, false)
        Included(x) => Included((x, false)), // Everything including (x, false)
        Unbounded => Unbounded,
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_range_lru_access_and_evict() {
        // Range of behaviours tested:
        // 1. Accessing with (Unbounded, x), (x, Unbounded), (Included, Included), (Excluded,
        // Excluded), (Included, Excluded), (Excluded, Included) 2. Evicting unbounded upper
        // range [(x, y) (y, Unbounded)]

        // Testing the `access` correctness
        let mut range_lru = RangeLru::default();
        range_lru.access((Unbounded, Included(ScalarImpl::Int64(50))), 1);
        range_lru.access(
            (
                Excluded(ScalarImpl::Int64(50)),
                Excluded(ScalarImpl::Int64(135)),
            ),
            2,
        );
        range_lru.access(
            (
                Included(ScalarImpl::Int64(135)),
                Excluded(ScalarImpl::Int64(150)),
            ),
            3,
        );

        assert_eq!(
            *range_lru.last_accessed_at.get(&1).unwrap(),
            (Unbounded, Included(ScalarImpl::Int64(50)))
        );
        assert_eq!(
            *range_lru.last_accessed_at.get(&2).unwrap(),
            (
                Excluded(ScalarImpl::Int64(50)),
                Excluded(ScalarImpl::Int64(135))
            )
        );
        assert_eq!(
            *range_lru.last_accessed_at.get(&3).unwrap(),
            (
                Included(ScalarImpl::Int64(135)),
                Excluded(ScalarImpl::Int64(150))
            )
        );

        range_lru.access(
            (
                Included(ScalarImpl::Int64(100)),
                Excluded(ScalarImpl::Int64(150)),
            ),
            4,
        );
        assert_eq!(
            *range_lru.last_accessed_at.get(&2).unwrap(),
            (
                Excluded(ScalarImpl::Int64(50)),
                Excluded(ScalarImpl::Int64(100))
            )
        );
        assert_eq!(
            *range_lru.last_accessed_at.get(&4).unwrap(),
            (
                Included(ScalarImpl::Int64(100)),
                Excluded(ScalarImpl::Int64(150))
            )
        );

        range_lru.access(
            (
                Included(ScalarImpl::Int64(100)),
                Included(ScalarImpl::Int64(125)),
            ),
            5,
        );

        assert_eq!(
            *range_lru.last_accessed_at.get(&2).unwrap(),
            (
                Excluded(ScalarImpl::Int64(50)),
                Excluded(ScalarImpl::Int64(100))
            )
        );
        assert_eq!(
            *range_lru.last_accessed_at.get(&4).unwrap(),
            (
                Excluded(ScalarImpl::Int64(125)),
                Excluded(ScalarImpl::Int64(150))
            )
        );
        assert_eq!(
            *range_lru.last_accessed_at.get(&5).unwrap(),
            (
                Included(ScalarImpl::Int64(100)),
                Included(ScalarImpl::Int64(125))
            )
        );

        // Testing `evict_before` correctness
        assert_eq!(
            range_lru.evict_before(2).unwrap(),
            (
                (Unbounded, Included(ScalarImpl::Int64(50))),
                Some((
                    Excluded(ScalarImpl::Int64(50)),
                    Excluded(ScalarImpl::Int64(150))
                ))
            )
        );
        assert_eq!(
            range_lru.evict_before(4).unwrap(),
            (
                (
                    Excluded(ScalarImpl::Int64(50)),
                    Excluded(ScalarImpl::Int64(100))
                ),
                Some((
                    Included(ScalarImpl::Int64(100)),
                    Excluded(ScalarImpl::Int64(150))
                ))
            )
        );

        range_lru.access(
            (
                Excluded(ScalarImpl::Int64(99)),
                Included(ScalarImpl::Int64(100)),
            ),
            6,
        );
        range_lru.access(
            (
                Excluded(ScalarImpl::Int64(99)),
                Included(ScalarImpl::Int64(105)),
            ),
            7,
        );

        assert_eq!(
            range_lru.evict_before(5).unwrap(),
            (
                (
                    Excluded(ScalarImpl::Int64(125)),
                    Excluded(ScalarImpl::Int64(150))
                ),
                Some((
                    Excluded(ScalarImpl::Int64(99)),
                    Included(ScalarImpl::Int64(125))
                ))
            )
        );

        assert_eq!(
            range_lru.evict_before(6).unwrap(),
            (
                (
                    Excluded(ScalarImpl::Int64(105)),
                    Included(ScalarImpl::Int64(125))
                ),
                Some((
                    Excluded(ScalarImpl::Int64(99)),
                    Included(ScalarImpl::Int64(105))
                ))
            )
        );

        range_lru.access((Excluded(ScalarImpl::Int64(105)), Unbounded), 8);

        assert_eq!(
            range_lru.evict_before(8).unwrap(),
            (
                (
                    Excluded(ScalarImpl::Int64(99)),
                    Included(ScalarImpl::Int64(105))
                ),
                Some((Excluded(ScalarImpl::Int64(105)), Unbounded))
            )
        );

        assert_eq!(
            range_lru.evict_before(9).unwrap(),
            ((Excluded(ScalarImpl::Int64(105)), Unbounded), None,)
        );

        // Evicting unbounded upper range [(x, y) (y, Unbounded)]:
        range_lru.access((Included(ScalarImpl::Int64(150)), Unbounded), 9);
        range_lru.access(
            (
                Included(ScalarImpl::Int64(50)),
                Excluded(ScalarImpl::Int64(150)),
            ),
            10,
        );

        assert_eq!(
            range_lru.evict_before(10).unwrap(),
            (
                (Included(ScalarImpl::Int64(150)), Unbounded),
                Some((
                    Included(ScalarImpl::Int64(50)),
                    Excluded(ScalarImpl::Int64(150))
                )),
            )
        );
    }
}

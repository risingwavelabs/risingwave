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

use std::ops::Range;

use smallvec::{SmallVec, smallvec};

/// Calculate range (A - B), the result might be the union of two ranges when B is totally included
/// in the A.
pub(super) fn range_except(a: Range<usize>, b: Range<usize>) -> (Range<usize>, Range<usize>) {
    #[allow(clippy::if_same_then_else)] // for better readability
    if a.is_empty() {
        (0..0, 0..0)
    } else if b.is_empty() {
        (a, 0..0)
    } else if a.end <= b.start || b.end <= a.start {
        // a: [   )
        // b:        [   )
        // or
        // a:        [   )
        // b: [   )
        (a, 0..0)
    } else if b.start <= a.start && a.end <= b.end {
        // a:  [   )
        // b: [       )
        (0..0, 0..0)
    } else if a.start < b.start && b.end < a.end {
        // a: [       )
        // b:   [   )
        (a.start..b.start, b.end..a.end)
    } else if a.end <= b.end {
        // a: [   )
        // b:   [   )
        (a.start..b.start, 0..0)
    } else if b.start <= a.start {
        // a:   [   )
        // b: [   )
        (b.end..a.end, 0..0)
    } else {
        unreachable!()
    }
}

/// Calculate the difference of two ranges A and B, return (removed ranges, added ranges).
/// Note this is quite different from [`range_except`].
#[allow(clippy::type_complexity)] // looks complex but it's not
pub(super) fn range_diff(
    a: Range<usize>,
    b: Range<usize>,
) -> (SmallVec<[Range<usize>; 2]>, SmallVec<[Range<usize>; 2]>) {
    if a.start == b.start {
        match a.end.cmp(&b.end) {
            std::cmp::Ordering::Equal => {
                // a: [   )
                // b: [   )
                (smallvec![], smallvec![])
            }
            std::cmp::Ordering::Less => {
                // a: [   )
                // b: [     )
                (smallvec![], smallvec![a.end..b.end])
            }
            std::cmp::Ordering::Greater => {
                // a: [     )
                // b: [   )
                (smallvec![b.end..a.end], smallvec![])
            }
        }
    } else if a.end == b.end {
        debug_assert!(a.start != b.start);
        if a.start < b.start {
            // a: [     )
            // b:   [   )
            (smallvec![a.start..b.start], smallvec![])
        } else {
            // a:   [   )
            // b: [     )
            (smallvec![], smallvec![b.start..a.start])
        }
    } else {
        debug_assert!(a.start != b.start && a.end != b.end);
        if a.end <= b.start || b.end <= a.start {
            // a: [   )
            // b:     [  [   )
            // or
            // a:       [   )
            // b: [   ) )
            (smallvec![a], smallvec![b])
        } else if b.start < a.start && a.end < b.end {
            // a:  [   )
            // b: [       )
            (smallvec![], smallvec![b.start..a.start, a.end..b.end])
        } else if a.start < b.start && b.end < a.end {
            // a: [       )
            // b:   [   )
            (smallvec![a.start..b.start, b.end..a.end], smallvec![])
        } else if a.end < b.end {
            // a: [   )
            // b:   [   )
            (smallvec![a.start..b.start], smallvec![a.end..b.end])
        } else {
            // a:   [   )
            // b: [   )
            (smallvec![b.end..a.end], smallvec![b.start..a.start])
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    #[test]
    fn test_range_except() {
        fn test(a: Range<usize>, b: Range<usize>, expected: impl IntoIterator<Item = usize>) {
            let (l, r) = range_except(a, b);
            let set = l.into_iter().chain(r).collect::<HashSet<_>>();
            assert_eq!(set, expected.into_iter().collect())
        }

        test(0..0, 0..0, []);
        test(0..1, 0..1, []);
        test(0..1, 0..2, []);
        test(1..2, 0..2, []);
        test(0..2, 0..1, [1]);
        test(0..2, 1..2, [0]);
        test(0..5, 2..3, [0, 1, 3, 4]);
        test(2..5, 1..3, [3, 4]);
        test(2..5, 4..5, [2, 3]);
    }

    #[test]
    fn test_range_diff() {
        fn test(
            a: Range<usize>,
            b: Range<usize>,
            expected_removed: impl IntoIterator<Item = usize>,
            expected_added: impl IntoIterator<Item = usize>,
        ) {
            let (removed, added) = range_diff(a, b);
            let removed_set = removed.into_iter().flatten().collect::<HashSet<_>>();
            let added_set = added.into_iter().flatten().collect::<HashSet<_>>();
            let expected_removed_set = expected_removed.into_iter().collect::<HashSet<_>>();
            let expected_added_set = expected_added.into_iter().collect::<HashSet<_>>();
            assert_eq!(removed_set, expected_removed_set);
            assert_eq!(added_set, expected_added_set);
        }

        test(0..0, 0..0, [], []);
        test(0..1, 0..1, [], []);
        test(0..1, 0..2, [], [1]);
        test(0..2, 0..1, [1], []);
        test(0..2, 1..2, [0], []);
        test(1..2, 0..2, [], [0]);
        test(0..1, 1..2, [0], [1]);
        test(0..1, 2..3, [0], [2]);
        test(1..2, 0..1, [1], [0]);
        test(2..3, 0..1, [2], [0]);
        test(0..3, 1..2, [0, 2], []);
        test(1..2, 0..3, [], [0, 2]);
        test(0..3, 2..4, [0, 1], [3]);
        test(2..4, 0..3, [3], [0, 1]);
    }
}

// Copyright 2022 RisingWave Labs
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
use std::ops::{Bound, RangeBounds};

use paste::paste;
use risingwave_pb::batch_plan::ScanRange as PbScanRange;
use risingwave_pb::batch_plan::scan_range::Bound as PbBound;

use super::sort_util::{OrderType, cmp_rows};
use crate::hash::VirtualNode;
use crate::hash::table_distribution::TableDistribution;
use crate::types::{Datum, ScalarImpl};
use crate::util::value_encoding::serialize_datum_into;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ScanRange {
    pub eq_conds: Vec<Datum>,
    pub range: (Bound<Vec<Datum>>, Bound<Vec<Datum>>),
}

fn bound_vec_datum_to_proto(bound: &Bound<Vec<Datum>>) -> Option<PbBound> {
    match bound {
        Bound::Included(literal) => Some(PbBound {
            value: literal
                .iter()
                .map(|datum| {
                    let mut encoded = vec![];
                    serialize_datum_into(datum, &mut encoded);
                    encoded
                })
                .collect(),
            inclusive: true,
        }),
        Bound::Excluded(literal) => Some(PbBound {
            value: literal
                .iter()
                .map(|datum| {
                    let mut encoded = vec![];
                    serialize_datum_into(datum, &mut encoded);
                    encoded
                })
                .collect(),
            inclusive: false,
        }),
        Bound::Unbounded => None,
    }
}

impl ScanRange {
    pub fn to_protobuf(&self) -> PbScanRange {
        PbScanRange {
            eq_conds: self
                .eq_conds
                .iter()
                .map(|datum| {
                    let mut encoded = vec![];
                    serialize_datum_into(datum, &mut encoded);
                    encoded
                })
                .collect(),
            lower_bound: bound_vec_datum_to_proto(&self.range.0),
            upper_bound: bound_vec_datum_to_proto(&self.range.1),
        }
    }

    pub fn is_full_table_scan(&self) -> bool {
        self.eq_conds.is_empty() && self.range == full_range()
    }

    pub fn has_eq_conds(&self) -> bool {
        !self.eq_conds.is_empty()
    }

    pub fn two_side_bound(&self) -> bool {
        let bounds = &self.range;
        !matches!(bounds.start_bound(), Bound::Unbounded)
            && !matches!(bounds.end_bound(), Bound::Unbounded)
    }

    pub fn try_compute_vnode(&self, table_distribution: &TableDistribution) -> Option<VirtualNode> {
        table_distribution.try_compute_vnode_by_pk_prefix(self.eq_conds.as_slice())
    }

    pub const fn full_table_scan() -> Self {
        Self {
            eq_conds: vec![],
            range: full_range(),
        }
    }

    pub fn convert_to_range(&self) -> (Bound<Vec<Datum>>, Bound<Vec<Datum>>) {
        fn handle_bound(eq_conds: &Vec<Datum>, bound: &Bound<Vec<Datum>>) -> Bound<Vec<Datum>> {
            match bound {
                Bound::Included(literal) => {
                    let mut prefix = eq_conds.clone();
                    prefix.extend_from_slice(literal);
                    Bound::Included(prefix)
                }
                Bound::Excluded(literal) => {
                    let mut prefix = eq_conds.clone();
                    prefix.extend_from_slice(literal);
                    Bound::Excluded(prefix)
                }
                Bound::Unbounded => {
                    if eq_conds.is_empty() {
                        Bound::Unbounded
                    } else {
                        Bound::Included(eq_conds.clone())
                    }
                }
            }
        }

        let new_left = handle_bound(&self.eq_conds, &self.range.0);
        let new_right = handle_bound(&self.eq_conds, &self.range.1);
        (new_left, new_right)
    }

    /// Returns whether the two ranges may contain a common row.
    ///
    /// `order_types` are the pk order types with descending ones reversed, i.e. the order in which
    /// the range bounds are compared.
    pub fn is_overlap(left: &ScanRange, right: &ScanRange, order_types: &[OrderType]) -> bool {
        cmp_cut(
            &left.start_cut(order_types),
            &right.end_cut(order_types),
            order_types,
        )
        .is_lt()
            && cmp_cut(
                &right.start_cut(order_types),
                &left.end_cut(order_types),
                order_types,
            )
            .is_lt()
    }

    /// Compares where the two ranges start. See [`Self::is_overlap`] for `order_types`.
    pub fn cmp_start(left: &ScanRange, right: &ScanRange, order_types: &[OrderType]) -> Ordering {
        cmp_cut(
            &left.start_cut(order_types),
            &right.start_cut(order_types),
            order_types,
        )
    }

    /// Returns a range covering two overlapping ranges. It is exact if one range contains the other
    /// or both have the same `eq_conds`. Otherwise, it is widened to the whole common prefix of
    /// their `eq_conds`. See [`Self::is_overlap`] for `order_types`.
    pub fn union(self, other: ScanRange, order_types: &[OrderType]) -> ScanRange {
        let cmp_start = Self::cmp_start(&self, &other, order_types);
        let cmp_end = cmp_cut(
            &self.end_cut(order_types),
            &other.end_cut(order_types),
            order_types,
        );
        if cmp_start.is_le() && cmp_end.is_ge() {
            return self;
        }
        if cmp_start.is_ge() && cmp_end.is_le() {
            return other;
        }
        if self.eq_conds == other.eq_conds {
            let start = if cmp_start.is_le() {
                self.range.0
            } else {
                other.range.0
            };
            let end = if cmp_end.is_ge() {
                self.range.1
            } else {
                other.range.1
            };
            ScanRange {
                eq_conds: self.eq_conds,
                range: (start, end),
            }
        } else {
            let common_len = (0..self.eq_conds.len().min(other.eq_conds.len()))
                .take_while(|&i| self.eq_conds[i] == other.eq_conds[i])
                .count();
            let mut eq_conds = self.eq_conds;
            eq_conds.truncate(common_len);
            ScanRange {
                eq_conds,
                range: full_range(),
            }
        }
    }

    /// Where the range starts in pk order. See [`Cut`].
    fn start_cut(&self, order_types: &[OrderType]) -> Cut {
        let mut prefix = self.eq_conds.clone();
        let after = match &self.range.0 {
            Bound::Included(literal) => {
                prefix.extend_from_slice(literal);
                false
            }
            Bound::Excluded(literal) => {
                prefix.extend_from_slice(literal);
                true
            }
            Bound::Unbounded => {
                if self.skipped_nulls_are_largest(order_types) == Some(false) {
                    prefix.push(None);
                    true
                } else {
                    false
                }
            }
        };
        (prefix, after)
    }

    /// Where the range ends in pk order. See [`Cut`].
    fn end_cut(&self, order_types: &[OrderType]) -> Cut {
        let mut prefix = self.eq_conds.clone();
        let after = match &self.range.1 {
            Bound::Included(literal) => {
                prefix.extend_from_slice(literal);
                true
            }
            Bound::Excluded(literal) => {
                prefix.extend_from_slice(literal);
                false
            }
            Bound::Unbounded => {
                if self.skipped_nulls_are_largest(order_types) == Some(true) {
                    prefix.push(None);
                    false
                } else {
                    true
                }
            }
        };
        (prefix, after)
    }

    /// When only one side of the range is bounded, storage skips the NULLs of the first range
    /// column (see `PkScanRange::convert_to_range_bounds`). Returns whether those NULLs sort after
    /// the other values, or `None` if no NULLs are skipped.
    fn skipped_nulls_are_largest(&self, order_types: &[OrderType]) -> Option<bool> {
        let half_bounded =
            matches!(self.range.0, Bound::Unbounded) != matches!(self.range.1, Bound::Unbounded);
        let order_type = order_types.get(self.eq_conds.len())?;
        half_bounded.then(|| order_type.nulls_are_largest())
    }
}

/// A position in pk order: right before (`false`) or right after (`true`) all the keys starting
/// with the prefix.
type Cut = (Vec<Datum>, bool);

/// Compares two cuts. When one prefix is shorter, it is compared as if extended with -inf or +inf.
fn cmp_cut(
    (left, left_after): &Cut,
    (right, right_after): &Cut,
    order_types: &[OrderType],
) -> Ordering {
    let len = left.len().min(right.len());
    cmp_rows(&left[..len], &right[..len], &order_types[..len]).then_with(|| {
        match left.len().cmp(&right.len()) {
            Ordering::Equal => left_after.cmp(right_after),
            Ordering::Less if *left_after => Ordering::Greater,
            Ordering::Less => Ordering::Less,
            Ordering::Greater if *right_after => Ordering::Less,
            Ordering::Greater => Ordering::Greater,
        }
    })
}

pub const fn full_range<T>() -> (Bound<T>, Bound<T>) {
    (Bound::Unbounded, Bound::Unbounded)
}

pub fn is_full_range<T>(bounds: &impl RangeBounds<T>) -> bool {
    matches!(bounds.start_bound(), Bound::Unbounded)
        && matches!(bounds.end_bound(), Bound::Unbounded)
}

macro_rules! for_all_scalar_int_variants {
    ($macro:ident) => {
        $macro! {
            { Int16 },
            { Int32 },
            { Int64 }
        }
    };
}

macro_rules! impl_split_small_range {
    ($( { $type_name:ident} ),*) => {
        paste! {
            impl ScanRange {
                /// `Precondition`: make sure the first order key is int type if you call this method.
                /// Optimize small range scan. It turns x between 0 and 5 into x in (0, 1, 2, 3, 4, 5).s
                pub fn split_small_range(&self, max_gap: u64) -> Option<Vec<Self>> {
                    if self.eq_conds.is_empty() {
                        if let (Bound::Included(left),Bound::Included(right)) = (&self.range.0, &self.range.1){
                            match (left.as_slice(), right.as_slice()) {
                                $(
                                    // Compute the gap in i128 so that it cannot overflow.
                                    (
                                        [Some(ScalarImpl::$type_name(left))],
                                        [Some(ScalarImpl::$type_name(right))],
                                    ) if (0..i128::from(max_gap))
                                        .contains(&(i128::from(*right) - i128::from(*left))) =>
                                    {
                                        return Some(
                                            (*left..=*right)
                                                .map(|i| ScanRange {
                                                    eq_conds: vec![Some(ScalarImpl::$type_name(i))],
                                                    range: full_range(),
                                                })
                                                .collect(),
                                        );
                                    }
                                )*
                                _ => {}
                            }
                        }
                    }

                    None
                }
            }
        }
    };
}

for_all_scalar_int_variants! { impl_split_small_range }

#[cfg(test)]
mod tests {
    use super::*;
    use crate::row::OwnedRow;
    // dist_key is prefix of pk
    #[test]
    fn test_vnode_prefix() {
        let dist_key = vec![1, 3];
        let pk = vec![1, 3, 2];
        let dist_key_idx_in_pk =
            crate::catalog::get_dist_key_in_pk_indices(&dist_key, &pk).unwrap();
        let dist = TableDistribution::all(dist_key_idx_in_pk, VirtualNode::COUNT_FOR_TEST);

        let mut scan_range = ScanRange::full_table_scan();
        assert!(scan_range.try_compute_vnode(&dist).is_none());

        scan_range.eq_conds.push(Some(ScalarImpl::from(114)));
        assert!(scan_range.try_compute_vnode(&dist).is_none());

        scan_range.eq_conds.push(Some(ScalarImpl::from(514)));
        let row = OwnedRow::new(vec![
            Some(ScalarImpl::from(114)),
            Some(ScalarImpl::from(514)),
        ]);

        let vnode = VirtualNode::compute_row_for_test(&row, &[0, 1]);

        assert_eq!(scan_range.try_compute_vnode(&dist), Some(vnode));
    }

    // dist_key is not prefix of pk
    #[test]
    fn test_vnode_not_prefix() {
        let dist_key = vec![2, 3];
        let pk = vec![1, 3, 2];
        let dist_key_idx_in_pk =
            crate::catalog::get_dist_key_in_pk_indices(&dist_key, &pk).unwrap();
        let dist = TableDistribution::all(dist_key_idx_in_pk, VirtualNode::COUNT_FOR_TEST);

        let mut scan_range = ScanRange::full_table_scan();
        assert!(scan_range.try_compute_vnode(&dist).is_none());

        scan_range.eq_conds.push(Some(ScalarImpl::from(114)));
        assert!(scan_range.try_compute_vnode(&dist).is_none());

        scan_range.eq_conds.push(Some(ScalarImpl::from(514)));
        assert!(scan_range.try_compute_vnode(&dist).is_none());

        scan_range.eq_conds.push(Some(ScalarImpl::from(114514)));
        let row = OwnedRow::new(vec![
            Some(ScalarImpl::from(114)),
            Some(ScalarImpl::from(514)),
            Some(ScalarImpl::from(114514)),
        ]);

        let vnode = VirtualNode::compute_row_for_test(&row, &[2, 1]);

        assert_eq!(scan_range.try_compute_vnode(&dist), Some(vnode));
    }

    #[test]
    fn test_convert_to_range() {
        {
            // test empty eq_conds
            let scan_range = ScanRange {
                eq_conds: vec![],
                range: (
                    Bound::Included(vec![Some(ScalarImpl::from(1))]),
                    Bound::Included(vec![Some(ScalarImpl::from(2))]),
                ),
            };

            let (left, right) = scan_range.convert_to_range();
            assert_eq!(left, Bound::Included(vec![Some(ScalarImpl::from(1))]));
            assert_eq!(right, Bound::Included(vec![Some(ScalarImpl::from(2))]));
        }

        {
            // test exclude bound with empty eq_conds
            let scan_range = ScanRange {
                eq_conds: vec![],
                range: (
                    Bound::Excluded(vec![Some(ScalarImpl::from(1))]),
                    Bound::Excluded(vec![Some(ScalarImpl::from(2))]),
                ),
            };

            let (left, right) = scan_range.convert_to_range();
            assert_eq!(left, Bound::Excluded(vec![Some(ScalarImpl::from(1))]));
            assert_eq!(right, Bound::Excluded(vec![Some(ScalarImpl::from(2))]));
        }

        {
            // test include bound with empty eq_conds
            let scan_range = ScanRange {
                eq_conds: vec![],
                range: (
                    Bound::Included(vec![Some(ScalarImpl::from(1))]),
                    Bound::Unbounded,
                ),
            };

            let (left, right) = scan_range.convert_to_range();
            assert_eq!(left, Bound::Included(vec![Some(ScalarImpl::from(1))]));
            assert_eq!(right, Bound::Unbounded);
        }

        {
            // test exclude bound with non-empty eq_conds
            let scan_range = ScanRange {
                eq_conds: vec![Some(ScalarImpl::from(1))],
                range: (
                    Bound::Excluded(vec![Some(ScalarImpl::from(2))]),
                    Bound::Excluded(vec![Some(ScalarImpl::from(3))]),
                ),
            };

            let (left, right) = scan_range.convert_to_range();
            assert_eq!(
                left,
                Bound::Excluded(vec![Some(ScalarImpl::from(1)), Some(ScalarImpl::from(2))])
            );
            assert_eq!(
                right,
                Bound::Excluded(vec![Some(ScalarImpl::from(1)), Some(ScalarImpl::from(3))])
            );
        }

        {
            // test include bound with non-empty eq_conds
            let scan_range = ScanRange {
                eq_conds: vec![Some(ScalarImpl::from(1))],
                range: (
                    Bound::Included(vec![Some(ScalarImpl::from(2))]),
                    Bound::Included(vec![Some(ScalarImpl::from(3))]),
                ),
            };

            let (left, right) = scan_range.convert_to_range();
            assert_eq!(
                left,
                Bound::Included(vec![Some(ScalarImpl::from(1)), Some(ScalarImpl::from(2))])
            );
            assert_eq!(
                right,
                Bound::Included(vec![Some(ScalarImpl::from(1)), Some(ScalarImpl::from(3))])
            );
        }

        {
            let scan_range = ScanRange {
                eq_conds: vec![Some(ScalarImpl::from(1))],
                range: (
                    Bound::Included(vec![Some(ScalarImpl::from(2))]),
                    Bound::Unbounded,
                ),
            };

            let (left, right) = scan_range.convert_to_range();
            assert_eq!(
                left,
                Bound::Included(vec![Some(ScalarImpl::from(1)), Some(ScalarImpl::from(2))])
            );
            assert_eq!(right, Bound::Included(vec![Some(ScalarImpl::from(1))]));
        }
    }

    fn range_overlap_check(
        left: (Bound<Vec<Datum>>, Bound<Vec<Datum>>),
        right: (Bound<Vec<Datum>>, Bound<Vec<Datum>>),
        order_types: &[OrderType],
    ) -> bool {
        let to_scan_range = |range| ScanRange {
            eq_conds: vec![],
            range,
        };
        ScanRange::is_overlap(&to_scan_range(left), &to_scan_range(right), order_types)
    }

    #[test]
    fn test_range_overlap_check() {
        let order_types = vec![OrderType::ascending()];

        // (Included, Included) vs (Included, Included)
        assert!(range_overlap_check(
            (
                Bound::Included(vec![Some(ScalarImpl::Int32(1))]),
                Bound::Included(vec![Some(ScalarImpl::Int32(5))])
            ),
            (
                Bound::Included(vec![Some(ScalarImpl::Int32(3))]),
                Bound::Included(vec![Some(ScalarImpl::Int32(7))])
            ),
            &order_types
        ));

        // (Included, Included) vs (Included, Excluded)
        assert!(range_overlap_check(
            (
                Bound::Included(vec![Some(ScalarImpl::Int32(1))]),
                Bound::Included(vec![Some(ScalarImpl::Int32(5))])
            ),
            (
                Bound::Included(vec![Some(ScalarImpl::Int32(3))]),
                Bound::Excluded(vec![Some(ScalarImpl::Int32(7))])
            ),
            &order_types
        ));

        // (Included, Included) vs (Excluded, Included)
        assert!(range_overlap_check(
            (
                Bound::Included(vec![Some(ScalarImpl::Int32(1))]),
                Bound::Included(vec![Some(ScalarImpl::Int32(5))])
            ),
            (
                Bound::Excluded(vec![Some(ScalarImpl::Int32(3))]),
                Bound::Included(vec![Some(ScalarImpl::Int32(7))])
            ),
            &order_types
        ));

        // (Included, Included) vs (Excluded, Excluded)
        assert!(range_overlap_check(
            (
                Bound::Included(vec![Some(ScalarImpl::Int32(1))]),
                Bound::Included(vec![Some(ScalarImpl::Int32(5))])
            ),
            (
                Bound::Excluded(vec![Some(ScalarImpl::Int32(3))]),
                Bound::Excluded(vec![Some(ScalarImpl::Int32(7))])
            ),
            &order_types
        ));

        // (Included, Excluded) vs (Included, Included)
        assert!(range_overlap_check(
            (
                Bound::Included(vec![Some(ScalarImpl::Int32(1))]),
                Bound::Excluded(vec![Some(ScalarImpl::Int32(5))])
            ),
            (
                Bound::Included(vec![Some(ScalarImpl::Int32(3))]),
                Bound::Included(vec![Some(ScalarImpl::Int32(7))])
            ),
            &order_types
        ));

        // (Included, Excluded) vs (Included, Excluded)
        assert!(range_overlap_check(
            (
                Bound::Included(vec![Some(ScalarImpl::Int32(1))]),
                Bound::Excluded(vec![Some(ScalarImpl::Int32(5))])
            ),
            (
                Bound::Included(vec![Some(ScalarImpl::Int32(3))]),
                Bound::Excluded(vec![Some(ScalarImpl::Int32(7))])
            ),
            &order_types
        ));

        // (Included, Excluded) vs (Excluded, Included)
        assert!(range_overlap_check(
            (
                Bound::Included(vec![Some(ScalarImpl::Int32(1))]),
                Bound::Excluded(vec![Some(ScalarImpl::Int32(5))])
            ),
            (
                Bound::Excluded(vec![Some(ScalarImpl::Int32(3))]),
                Bound::Included(vec![Some(ScalarImpl::Int32(7))])
            ),
            &order_types
        ));

        // (Included, Excluded) vs (Excluded, Excluded)
        assert!(range_overlap_check(
            (
                Bound::Included(vec![Some(ScalarImpl::Int32(1))]),
                Bound::Excluded(vec![Some(ScalarImpl::Int32(5))])
            ),
            (
                Bound::Excluded(vec![Some(ScalarImpl::Int32(3))]),
                Bound::Excluded(vec![Some(ScalarImpl::Int32(7))])
            ),
            &order_types
        ));

        // (Excluded, Included) vs (Included, Included)
        assert!(range_overlap_check(
            (
                Bound::Excluded(vec![Some(ScalarImpl::Int32(1))]),
                Bound::Included(vec![Some(ScalarImpl::Int32(5))])
            ),
            (
                Bound::Included(vec![Some(ScalarImpl::Int32(3))]),
                Bound::Included(vec![Some(ScalarImpl::Int32(7))])
            ),
            &order_types
        ));

        // (Excluded, Included) vs (Included, Excluded)
        assert!(range_overlap_check(
            (
                Bound::Excluded(vec![Some(ScalarImpl::Int32(1))]),
                Bound::Included(vec![Some(ScalarImpl::Int32(5))])
            ),
            (
                Bound::Included(vec![Some(ScalarImpl::Int32(3))]),
                Bound::Excluded(vec![Some(ScalarImpl::Int32(7))])
            ),
            &order_types
        ));

        // (Excluded, Included) vs (Excluded, Included)
        assert!(range_overlap_check(
            (
                Bound::Excluded(vec![Some(ScalarImpl::Int32(1))]),
                Bound::Included(vec![Some(ScalarImpl::Int32(5))])
            ),
            (
                Bound::Excluded(vec![Some(ScalarImpl::Int32(3))]),
                Bound::Included(vec![Some(ScalarImpl::Int32(7))])
            ),
            &order_types
        ));

        // (Excluded, Included) vs (Excluded, Excluded)
        assert!(range_overlap_check(
            (
                Bound::Excluded(vec![Some(ScalarImpl::Int32(1))]),
                Bound::Included(vec![Some(ScalarImpl::Int32(5))])
            ),
            (
                Bound::Excluded(vec![Some(ScalarImpl::Int32(3))]),
                Bound::Excluded(vec![Some(ScalarImpl::Int32(7))])
            ),
            &order_types
        ));

        // (Excluded, Excluded) vs (Included, Included)
        assert!(range_overlap_check(
            (
                Bound::Excluded(vec![Some(ScalarImpl::Int32(1))]),
                Bound::Excluded(vec![Some(ScalarImpl::Int32(5))])
            ),
            (
                Bound::Included(vec![Some(ScalarImpl::Int32(3))]),
                Bound::Included(vec![Some(ScalarImpl::Int32(7))])
            ),
            &order_types
        ));

        // (Excluded, Excluded) vs (Included, Excluded)
        assert!(range_overlap_check(
            (
                Bound::Excluded(vec![Some(ScalarImpl::Int32(1))]),
                Bound::Excluded(vec![Some(ScalarImpl::Int32(5))])
            ),
            (
                Bound::Included(vec![Some(ScalarImpl::Int32(3))]),
                Bound::Excluded(vec![Some(ScalarImpl::Int32(7))])
            ),
            &order_types
        ));

        // (Excluded, Excluded) vs (Excluded, Included)
        assert!(range_overlap_check(
            (
                Bound::Excluded(vec![Some(ScalarImpl::Int32(1))]),
                Bound::Excluded(vec![Some(ScalarImpl::Int32(5))])
            ),
            (
                Bound::Excluded(vec![Some(ScalarImpl::Int32(3))]),
                Bound::Included(vec![Some(ScalarImpl::Int32(7))])
            ),
            &order_types
        ));

        // (Excluded, Excluded) vs (Excluded, Excluded)
        assert!(range_overlap_check(
            (
                Bound::Excluded(vec![Some(ScalarImpl::Int32(1))]),
                Bound::Excluded(vec![Some(ScalarImpl::Int32(5))])
            ),
            (
                Bound::Excluded(vec![Some(ScalarImpl::Int32(3))]),
                Bound::Excluded(vec![Some(ScalarImpl::Int32(7))])
            ),
            &order_types
        ));

        // (Included, Included) vs (Included, Included)
        assert!(range_overlap_check(
            (
                Bound::Included(vec![Some(ScalarImpl::Int32(3))]),
                Bound::Included(vec![Some(ScalarImpl::Int32(7))])
            ),
            (
                Bound::Included(vec![Some(ScalarImpl::Int32(1))]),
                Bound::Included(vec![Some(ScalarImpl::Int32(5))])
            ),
            &order_types
        ));

        // (Included, Included) vs (Included, Included)
        assert!(range_overlap_check(
            (
                Bound::Included(vec![Some(ScalarImpl::Int32(3)), Some(ScalarImpl::Int32(3))]),
                Bound::Included(vec![Some(ScalarImpl::Int32(7)), Some(ScalarImpl::Int32(7))])
            ),
            (
                Bound::Included(vec![Some(ScalarImpl::Int32(1))]),
                Bound::Included(vec![Some(ScalarImpl::Int32(5))])
            ),
            &order_types
        ));

        // (Included, Included) vs (Included, Included)
        assert!(!range_overlap_check(
            (
                Bound::Included(vec![Some(ScalarImpl::Int32(3))]),
                Bound::Included(vec![Some(ScalarImpl::Int32(7))])
            ),
            (
                Bound::Included(vec![Some(ScalarImpl::Int32(1))]),
                Bound::Included(vec![Some(ScalarImpl::Int32(2))])
            ),
            &order_types
        ));

        // (Included, Included) vs (Included, Included)
        assert!(range_overlap_check(
            (
                Bound::Included(vec![Some(ScalarImpl::Int32(1))]),
                Bound::Included(vec![Some(ScalarImpl::Int32(3))])
            ),
            (
                Bound::Included(vec![Some(ScalarImpl::Int32(3))]),
                Bound::Included(vec![Some(ScalarImpl::Int32(7))])
            ),
            &order_types
        ));

        // (Included, Included) vs (Excluded, Included)
        assert!(!range_overlap_check(
            (
                Bound::Included(vec![Some(ScalarImpl::Int32(1))]),
                Bound::Included(vec![Some(ScalarImpl::Int32(3))])
            ),
            (
                Bound::Excluded(vec![Some(ScalarImpl::Int32(3))]),
                Bound::Excluded(vec![Some(ScalarImpl::Int32(7))])
            ),
            &order_types
        ));

        // (Included, Included) vs (Included, Included)
        assert!(range_overlap_check(
            (
                Bound::Included(vec![Some(ScalarImpl::Int32(1))]),
                Bound::Included(vec![Some(ScalarImpl::Int32(3))])
            ),
            (
                Bound::Included(vec![Some(ScalarImpl::Int32(1))]),
                Bound::Excluded(vec![Some(ScalarImpl::Int32(7))])
            ),
            &order_types
        ));

        assert!(!range_overlap_check(
            (
                Bound::Unbounded,
                Bound::Included(vec![Some(ScalarImpl::Int32(3))])
            ),
            (
                Bound::Included(vec![Some(ScalarImpl::Int32(5))]),
                Bound::Unbounded,
            ),
            &order_types
        ));

        assert!(range_overlap_check(
            (
                Bound::Unbounded,
                Bound::Included(vec![Some(ScalarImpl::Int32(10))])
            ),
            (
                Bound::Included(vec![Some(ScalarImpl::Int32(5))]),
                Bound::Unbounded,
            ),
            &order_types
        ));

        assert!(range_overlap_check(
            (Bound::Unbounded, Bound::Unbounded,),
            (
                Bound::Included(vec![Some(ScalarImpl::Int32(5))]),
                Bound::Unbounded,
            ),
            &order_types
        ));

        assert!(range_overlap_check(
            (Bound::Unbounded, Bound::Unbounded),
            (Bound::Unbounded, Bound::Unbounded),
            &order_types
        ));

        assert!(!range_overlap_check(
            (
                Bound::Included(vec![Some(ScalarImpl::Int32(1))]),
                Bound::Included(vec![Some(ScalarImpl::Int32(3))])
            ),
            (
                Bound::Included(vec![Some(ScalarImpl::Int32(5))]),
                Bound::Unbounded,
            ),
            &order_types
        ));

        assert!(range_overlap_check(
            (
                Bound::Included(vec![Some(ScalarImpl::Int32(1))]),
                Bound::Included(vec![Some(ScalarImpl::Int32(3))])
            ),
            (
                Bound::Unbounded,
                Bound::Included(vec![Some(ScalarImpl::Int32(5))]),
            ),
            &order_types
        ));
    }
    #[test]
    fn test_overlap_and_union_with_prefix_bounds() {
        let order_types = vec![OrderType::ascending(), OrderType::ascending()];
        let int = |v: i32| Some(ScalarImpl::Int32(v));
        let range = |eq_conds: Vec<Datum>, range| ScanRange { eq_conds, range };

        // `a = 1` covers `a = 1 AND b > 5`.
        let a_eq_1 = range(vec![int(1)], full_range());
        let a_eq_1_b_gt_5 = range(
            vec![int(1)],
            (Bound::Excluded(vec![int(5)]), Bound::Unbounded),
        );
        assert!(ScanRange::is_overlap(&a_eq_1, &a_eq_1_b_gt_5, &order_types));
        assert_eq!(
            a_eq_1.clone().union(a_eq_1_b_gt_5.clone(), &order_types),
            a_eq_1
        );

        // `a BETWEEN 0 AND 100` covers `a = 1 AND b > 5`.
        let a_0_100 = range(
            vec![],
            (
                Bound::Included(vec![int(0)]),
                Bound::Included(vec![int(100)]),
            ),
        );
        assert!(ScanRange::is_overlap(
            &a_eq_1_b_gt_5,
            &a_0_100,
            &order_types
        ));
        assert_eq!(
            a_eq_1_b_gt_5.clone().union(a_0_100.clone(), &order_types),
            a_0_100
        );

        // `a >= 1` covers `(a, b) > (1, 5)`.
        let a_b_gt_1_5 = range(
            vec![],
            (Bound::Excluded(vec![int(1), int(5)]), Bound::Unbounded),
        );
        let a_ge_1 = range(vec![], (Bound::Included(vec![int(1)]), Bound::Unbounded));
        assert_eq!(
            a_b_gt_1_5.clone().union(a_ge_1.clone(), &order_types),
            a_ge_1
        );
        assert_eq!(
            ScanRange::cmp_start(&a_ge_1, &a_b_gt_1_5, &order_types),
            Ordering::Less
        );

        // `a > 0` skips NULLs, so it does not overlap with `a IS NULL`.
        let a_is_null = range(vec![None], full_range());
        let a_gt_0 = range(vec![], (Bound::Excluded(vec![int(0)]), Bound::Unbounded));
        assert!(!ScanRange::is_overlap(&a_is_null, &a_gt_0, &order_types));

        // Otherwise, the union is widened to the common prefix of `eq_conds`.
        let a_b_gt_1_6 = range(
            vec![],
            (Bound::Excluded(vec![int(1), int(6)]), Bound::Unbounded),
        );
        assert!(ScanRange::is_overlap(
            &a_eq_1_b_gt_5,
            &a_b_gt_1_6,
            &order_types
        ));
        assert_eq!(
            a_eq_1_b_gt_5.union(a_b_gt_1_6, &order_types),
            ScanRange::full_table_scan()
        );
    }
}

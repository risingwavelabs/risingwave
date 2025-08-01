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

use std::cmp::Ordering;
use std::ops::{Bound, RangeBounds};

use itertools::Itertools;
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

    pub fn is_overlap(left: &ScanRange, right: &ScanRange, order_types: &[OrderType]) -> bool {
        let range_left = left.convert_to_range();
        let range_right = right.convert_to_range();
        Self::range_overlap_check(range_left, range_right, order_types)
    }

    fn range_overlap_check(
        left: (Bound<Vec<Datum>>, Bound<Vec<Datum>>),
        right: (Bound<Vec<Datum>>, Bound<Vec<Datum>>),
        order_types: &[OrderType],
    ) -> bool {
        let (left_start, left_end) = &left;
        let (right_start, right_end) = &right;

        let left_start_vec = match &left_start {
            Bound::Included(vec) | Bound::Excluded(vec) => vec,
            _ => &vec![],
        };
        let right_start_vec = match &right_start {
            Bound::Included(vec) | Bound::Excluded(vec) => vec,
            _ => &vec![],
        };

        if left_start_vec.is_empty() && right_start_vec.is_empty() {
            return true;
        }

        let order_types = if order_types.iter().all(|o| o.is_ascending()) {
            order_types
        } else {
            // reverse order types to ascending
            &order_types
                .iter()
                .cloned()
                .map(|o| if o.is_descending() { o.reverse() } else { o })
                .collect_vec()
        };

        // Unbounded is always less than any other bound
        if left_start_vec.is_empty() {
            // pass
        } else if right_start_vec.is_empty() {
            return Self::range_overlap_check(right, left, order_types);
        } else {
            assert!(!left_start_vec.is_empty());
            assert!(!right_start_vec.is_empty());
            let cmp_column_len = left_start_vec.len().min(right_start_vec.len());
            let cmp_start = cmp_rows(
                &left_start_vec[0..cmp_column_len],
                &right_start_vec[0..cmp_column_len],
                &order_types[0..cmp_column_len],
            );

            let right_start_before_left_start = cmp_start.is_gt();

            if right_start_before_left_start {
                return Self::range_overlap_check(right, left, order_types);
            }

            if cmp_start == Ordering::Equal
                && let (Bound::Included(_), Bound::Included(_)) = (left_start, right_start)
            {
                return true;
            }
        }

        let left_end_vec = match &left_end {
            Bound::Included(vec) | Bound::Excluded(vec) => vec,
            _ => &vec![],
        };
        let right_end_vec = match &right_end {
            Bound::Included(vec) | Bound::Excluded(vec) => vec,
            _ => &vec![],
        };

        if left_end_vec.is_empty() && right_end_vec.is_empty() {
            return true;
        }

        if left_end_vec.is_empty() {
            true
        } else {
            // cmp left_end and right_start
            assert!(!left_end_vec.is_empty());
            assert!(!right_start_vec.is_empty());

            let cmp_column_len = left_end_vec.len().min(right_start_vec.len());
            let cmp_end = cmp_rows(
                &left_end_vec[0..cmp_column_len],
                &right_start_vec[0..cmp_column_len],
                &order_types[0..cmp_column_len],
            );

            match cmp_end {
                Ordering::Equal => {
                    if let (Bound::Included(_), Bound::Included(_)) = (left_end, right_start) {
                        return true;
                    }
                }

                Ordering::Greater => {
                    return true;
                }

                Ordering::Less => {
                    return false;
                }
            }

            false
        }
    }
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
                            match (left.get(0),right.get(0)) {
                                $(
                                    (
                                        Some(Some(ScalarImpl::$type_name(left))),
                                        Some(Some(ScalarImpl::$type_name(right))),
                                    ) => {

                                        if (right - left + 1) as u64 <= max_gap {
                                            return Some(
                                                (*left..=*right)
                                                    .into_iter()
                                                    .map(|i| ScanRange {
                                                        eq_conds: vec![Some(ScalarImpl::$type_name(i))],
                                                        range: full_range(),
                                                    })
                                                    .collect(),
                                            );
                                        }
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

    #[test]
    fn test_range_overlap_check() {
        let order_types = vec![OrderType::ascending()];

        // (Included, Included) vs (Included, Included)
        assert!(ScanRange::range_overlap_check(
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
        assert!(ScanRange::range_overlap_check(
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
        assert!(ScanRange::range_overlap_check(
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
        assert!(ScanRange::range_overlap_check(
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
        assert!(ScanRange::range_overlap_check(
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
        assert!(ScanRange::range_overlap_check(
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
        assert!(ScanRange::range_overlap_check(
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
        assert!(ScanRange::range_overlap_check(
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
        assert!(ScanRange::range_overlap_check(
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
        assert!(ScanRange::range_overlap_check(
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
        assert!(ScanRange::range_overlap_check(
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
        assert!(ScanRange::range_overlap_check(
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
        assert!(ScanRange::range_overlap_check(
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
        assert!(ScanRange::range_overlap_check(
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
        assert!(ScanRange::range_overlap_check(
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
        assert!(ScanRange::range_overlap_check(
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
        assert!(ScanRange::range_overlap_check(
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
        assert!(ScanRange::range_overlap_check(
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
        assert!(!ScanRange::range_overlap_check(
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
        assert!(ScanRange::range_overlap_check(
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

        // (Included, Included) vs (Excluded, Encluded)
        assert!(!ScanRange::range_overlap_check(
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

        // (Included, Included) vs (Included, Encluded)
        assert!(ScanRange::range_overlap_check(
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

        assert!(!ScanRange::range_overlap_check(
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

        assert!(ScanRange::range_overlap_check(
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

        assert!(ScanRange::range_overlap_check(
            (Bound::Unbounded, Bound::Unbounded,),
            (
                Bound::Included(vec![Some(ScalarImpl::Int32(5))]),
                Bound::Unbounded,
            ),
            &order_types
        ));

        assert!(ScanRange::range_overlap_check(
            (Bound::Unbounded, Bound::Unbounded),
            (Bound::Unbounded, Bound::Unbounded),
            &order_types
        ));

        assert!(!ScanRange::range_overlap_check(
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

        assert!(ScanRange::range_overlap_check(
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
}

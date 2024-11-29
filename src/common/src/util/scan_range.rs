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

use std::ops::{Bound, RangeBounds};

use paste::paste;
use risingwave_pb::batch_plan::scan_range::prefix_scan_range::Bound as PbAndBound;
use risingwave_pb::batch_plan::scan_range::row_scan_range::Bound as PbStructBound;
use risingwave_pb::batch_plan::scan_range::{PbPrefixScanRange, PbRowScanRange};
use risingwave_pb::batch_plan::ScanRange as PbScanRange;

use super::value_encoding::serialize_datum;
use crate::hash::table_distribution::TableDistribution;
use crate::hash::VirtualNode;
use crate::types::{Datum, ScalarImpl};
use crate::util::value_encoding::serialize_datum_into;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ScanRange {
    PrefixScanRange(PrefixScanRange),
    RowScanRange(RowScanRange),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RowScanRange {
    pub range: (Bound<Vec<Datum>>, Bound<Vec<Datum>>),
}

impl RowScanRange {
    pub fn to_protobuf(&self) -> PbRowScanRange {
        PbRowScanRange {
            lower_bound: bound_vec_datum_to_proto(&self.range.0),
            upper_bound: bound_vec_datum_to_proto(&self.range.1),
        }
    }

    pub fn is_full_table_scan(&self) -> bool {
        self.range == full_range()
    }

    pub fn has_eq_conds(&self) -> bool {
        false
    }

    pub fn two_side_bound(&self) -> bool {
        let bounds = &self.range;
        !matches!(bounds.start_bound(), Bound::Unbounded)
            && !matches!(bounds.end_bound(), Bound::Unbounded)
    }

    pub fn try_compute_vnode(&self, table_distribution: &TableDistribution) -> Option<VirtualNode> {
        let prefix: Vec<Datum> = Vec::default();
        table_distribution.try_compute_vnode_by_pk_prefix(prefix.as_slice())
    }
}

fn bound_vec_datum_to_proto(bound: &Bound<Vec<Datum>>) -> Option<PbStructBound> {
    match bound {
        Bound::Included(literal) => Some(PbStructBound {
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
        Bound::Excluded(literal) => Some(PbStructBound {
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
/// See also [`PbScanRange`]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PrefixScanRange {
    pub eq_conds: Vec<Datum>,
    pub range: (Bound<ScalarImpl>, Bound<ScalarImpl>),
}

impl PrefixScanRange {
    pub fn to_protobuf(&self) -> PbPrefixScanRange {
        PbPrefixScanRange {
            eq_conds: self
                .eq_conds
                .iter()
                .map(|datum| {
                    let mut encoded = vec![];
                    serialize_datum_into(datum, &mut encoded);
                    encoded
                })
                .collect(),
            lower_bound: bound_scalar_to_proto(&self.range.0),
            upper_bound: bound_scalar_to_proto(&self.range.1),
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
}

fn bound_scalar_to_proto(bound: &Bound<ScalarImpl>) -> Option<PbAndBound> {
    match bound {
        Bound::Included(literal) => Some(PbAndBound {
            value: serialize_datum(Some(literal)),
            inclusive: true,
        }),
        Bound::Excluded(literal) => Some(PbAndBound {
            value: serialize_datum(Some(literal)),
            inclusive: false,
        }),
        Bound::Unbounded => None,
    }
}

impl ScanRange {
    pub fn to_protobuf(&self) -> PbScanRange {
        match self {
            ScanRange::PrefixScanRange(scan_range) => PbScanRange {
                scan_range: Some(
                    risingwave_pb::batch_plan::scan_range::ScanRange::PrefixScanRange(
                        scan_range.to_protobuf(),
                    ),
                ),
            },
            ScanRange::RowScanRange(scan_range) => PbScanRange {
                scan_range: Some(
                    risingwave_pb::batch_plan::scan_range::ScanRange::RowScanRange(
                        scan_range.to_protobuf(),
                    ),
                ),
            },
        }
    }

    pub fn is_full_table_scan(&self) -> bool {
        match self {
            ScanRange::PrefixScanRange(scan_range) => scan_range.is_full_table_scan(),
            ScanRange::RowScanRange(scan_range) => scan_range.is_full_table_scan(),
        }
    }

    pub fn is_and_scan_range(&self) -> bool {
        matches!(self, ScanRange::PrefixScanRange(_))
    }

    pub fn has_eq_conds(&self) -> bool {
        match self {
            ScanRange::PrefixScanRange(scan_range) => scan_range.has_eq_conds(),
            ScanRange::RowScanRange(scan_range) => scan_range.has_eq_conds(),
        }
    }

    pub fn two_side_bound(&self) -> bool {
        match self {
            ScanRange::PrefixScanRange(scan_range) => scan_range.two_side_bound(),
            ScanRange::RowScanRange(scan_range) => scan_range.two_side_bound(),
        }
    }

    pub fn try_compute_vnode(&self, table_distribution: &TableDistribution) -> Option<VirtualNode> {
        match self {
            ScanRange::PrefixScanRange(scan_range) => {
                scan_range.try_compute_vnode(table_distribution)
            }
            ScanRange::RowScanRange(scan_range) => scan_range.try_compute_vnode(table_distribution),
        }
    }

    pub const fn full_and_table_scan() -> Self {
        Self::PrefixScanRange(PrefixScanRange {
            eq_conds: vec![],
            range: full_range(),
        })
    }

    pub const fn full_struct_table_scan() -> Self {
        Self::RowScanRange(RowScanRange {
            range: full_range(),
        })
    }

    pub fn extend_eq_conds(&mut self, eq_conds: impl IntoIterator<Item = Datum>) {
        match self {
            ScanRange::PrefixScanRange(and_scan_range) => and_scan_range.eq_conds.extend(eq_conds),
            ScanRange::RowScanRange(_) => panic!("extend_eq_conds to RowScanRange"),
        }
    }

    pub fn set_and_scan_range(&mut self, range: (Bound<ScalarImpl>, Bound<ScalarImpl>)) {
        match self {
            ScanRange::PrefixScanRange(and_scan_range) => and_scan_range.range = range,
            ScanRange::RowScanRange(_) => {
                panic!("set_and_scan_range to RowScanRange")
            }
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
                    if let ScanRange::PrefixScanRange(and_scan_range) = self {
                        if and_scan_range.eq_conds.is_empty() {
                            match and_scan_range.range {
                                $(
                                    (
                                        Bound::Included(ScalarImpl::$type_name(ref left)),
                                        Bound::Included(ScalarImpl::$type_name(ref right)),
                                    ) => {
                                        if (right - left + 1) as u64 <= max_gap {
                                            return Some(
                                                (*left..=*right)
                                                    .into_iter()
                                                    .map(|i| ScanRange::PrefixScanRange(PrefixScanRange {
                                                        eq_conds: vec![Some(ScalarImpl::$type_name(i))],
                                                        range: full_range(),
                                                    }))
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

        let mut scan_range = ScanRange::full_and_table_scan();
        assert!(scan_range.try_compute_vnode(&dist).is_none());

        scan_range.extend_eq_conds(vec![Some(ScalarImpl::from(114))]);
        assert!(scan_range.try_compute_vnode(&dist).is_none());

        scan_range.extend_eq_conds(vec![Some(ScalarImpl::from(514))]);
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

        let mut scan_range = ScanRange::full_and_table_scan();
        assert!(scan_range.try_compute_vnode(&dist).is_none());

        scan_range.extend_eq_conds(vec![Some(ScalarImpl::from(514))]);
        assert!(scan_range.try_compute_vnode(&dist).is_none());

        scan_range.extend_eq_conds(vec![Some(ScalarImpl::from(114))]);
        assert!(scan_range.try_compute_vnode(&dist).is_none());

        scan_range.extend_eq_conds(vec![Some(ScalarImpl::from(114514))]);
        let row = OwnedRow::new(vec![
            Some(ScalarImpl::from(114)),
            Some(ScalarImpl::from(514)),
            Some(ScalarImpl::from(114514)),
        ]);

        let vnode = VirtualNode::compute_row_for_test(&row, &[2, 1]);

        assert_eq!(scan_range.try_compute_vnode(&dist), Some(vnode));
    }
}

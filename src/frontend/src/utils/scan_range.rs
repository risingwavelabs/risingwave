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

use std::ops::{Bound, RangeBounds};

use itertools::Itertools;
use risingwave_common::array::Row;
use risingwave_common::types::{ScalarImpl, VirtualNode};
use risingwave_common::util::hash_util::CRC32FastBuilder;
use risingwave_pb::batch_plan::scan_range::Bound as BoundProst;
use risingwave_pb::batch_plan::ScanRange as ScanRangeProst;

/// See also [`ScanRangeProst`]
#[derive(Debug, Clone)]
pub struct ScanRange {
    pub eq_conds: Vec<ScalarImpl>,
    pub range: (Bound<ScalarImpl>, Bound<ScalarImpl>),
}

fn bound_to_proto(bound: &Bound<ScalarImpl>) -> Option<BoundProst> {
    match bound {
        Bound::Included(literal) => Some(BoundProst {
            value: literal.to_protobuf(),
            inclusive: true,
        }),
        Bound::Excluded(literal) => Some(BoundProst {
            value: literal.to_protobuf(),
            inclusive: false,
        }),
        Bound::Unbounded => None,
    }
}

impl ScanRange {
    pub fn to_protobuf(&self) -> ScanRangeProst {
        ScanRangeProst {
            eq_conds: self.eq_conds.iter().map(|lit| lit.to_protobuf()).collect(),
            lower_bound: bound_to_proto(&self.range.0),
            upper_bound: bound_to_proto(&self.range.1),
        }
    }

    pub fn is_full_table_scan(&self) -> bool {
        self.eq_conds.is_empty() && self.range == full_range()
    }

    pub const fn full_table_scan() -> Self {
        Self {
            eq_conds: vec![],
            range: full_range(),
        }
    }

    pub fn try_compute_vnode(
        &self,
        dist_key_indices: &[usize],
        pk_indices: &[usize],
    ) -> Option<VirtualNode> {
        if dist_key_indices.is_empty() {
            return None;
        }

        let dist_key_in_pk_indices = dist_key_indices
            .iter()
            .map(|&di| {
                pk_indices
                    .iter()
                    .position(|&pi| di == pi)
                    .unwrap_or_else(|| {
                        panic!(
                            "distribution keys {:?} must be a subset of primary keys {:?}",
                            dist_key_indices, pk_indices
                        )
                    })
            })
            .collect_vec();
        let pk_prefix_len = self.eq_conds.len();
        if dist_key_in_pk_indices.iter().any(|&i| i >= pk_prefix_len) {
            return None;
        }

        let pk_prefix_value = Row(self
            .eq_conds
            .iter()
            .map(|scalar| Some(scalar.clone()))
            .collect());
        let vnode = pk_prefix_value
            .hash_by_indices(&dist_key_in_pk_indices, &CRC32FastBuilder {})
            .to_vnode();
        Some(vnode)
    }
}

pub const fn full_range<T>() -> (Bound<T>, Bound<T>) {
    (Bound::Unbounded, Bound::Unbounded)
}

pub fn is_full_range<T>(bounds: &impl RangeBounds<T>) -> bool {
    matches!(bounds.start_bound(), Bound::Unbounded)
        && matches!(bounds.end_bound(), Bound::Unbounded)
}

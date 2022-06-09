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

use risingwave_pb::batch_plan::scan_range::Bound as BoundProst;
use risingwave_pb::batch_plan::ScanRange as ScanRangeProst;

use crate::expr::{Expr, Literal};

/// See also [`ScanRangeProst`]
#[derive(Debug, Clone)]
pub struct ScanRange {
    pub eq_conds: Vec<Literal>,
    pub range: (Bound<Literal>, Bound<Literal>),
}

fn bound_to_proto(bound: &Bound<Literal>) -> Option<BoundProst> {
    match bound {
        Bound::Included(literal) => Some(BoundProst {
            value: Some(literal.to_expr_proto()),
            inclusive: true,
        }),
        Bound::Excluded(literal) => Some(BoundProst {
            value: Some(literal.to_expr_proto()),
            inclusive: false,
        }),
        Bound::Unbounded => None,
    }
}

impl ScanRange {
    pub fn to_protobuf(&self) -> ScanRangeProst {
        ScanRangeProst {
            eq_conds: self
                .eq_conds
                .iter()
                .map(|lit| lit.to_expr_proto())
                .collect(),
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
}

pub const fn full_range<T>() -> (Bound<T>, Bound<T>) {
    (Bound::Unbounded, Bound::Unbounded)
}

pub fn is_full_range<T>(bounds: &impl RangeBounds<T>) -> bool {
    matches!(bounds.start_bound(), Bound::Unbounded)
        && matches!(bounds.end_bound(), Bound::Unbounded)
}

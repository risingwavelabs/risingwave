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

use std::ops::Deref;

use super::{OwnedRow, Row};
use crate::estimate_size::EstimateSize;
use crate::util::sort_util::{cmp_datum_iter, partial_cmp_datum_iter, OrderType};

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub struct OrdRow<R: Row> {
    inner: R,
}

impl<R: Row + EstimateSize> EstimateSize for OrdRow<R> {
    fn estimated_heap_size(&self) -> usize {
        self.inner.estimated_heap_size()
    }
}

impl<R: Row> OrdRow<R> {
    pub fn new(inner: R) -> Self {
        Self { inner }
    }

    pub fn into_inner(self) -> R {
        self.inner
    }

    pub fn as_inner(&self) -> &R {
        &self.inner
    }
}

impl<R: Row> From<R> for OrdRow<R> {
    fn from(inner: R) -> Self {
        Self::new(inner)
    }
}

impl<R: Row> Deref for OrdRow<R> {
    type Target = R;

    fn deref(&self) -> &Self::Target {
        self.as_inner()
    }
}

impl<R: Row> Row for OrdRow<R> {
    type Iter<'a> = R::Iter<'a> where Self: 'a;

    deref_forward_row! {}

    fn into_owned_row(self) -> OwnedRow {
        self.inner.into_owned_row()
    }
}

impl<R: Row> PartialOrd for OrdRow<R> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        // NOTE(rc): This is slightly different from `partial_cmp_rows`, for this function won't
        // check the length of rows.
        partial_cmp_datum_iter(
            self.iter(),
            other.iter(),
            std::iter::repeat(OrderType::default()),
        )
    }
}

impl<R: Row> Ord for OrdRow<R> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        cmp_datum_iter(
            self.iter(),
            other.iter(),
            std::iter::repeat(OrderType::default()),
        )
    }
}

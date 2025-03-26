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

use std::ops::{Range, RangeBounds};

use super::Row;
use crate::types::DatumRef;

/// Row for the [`slice`](super::RowExt::slice) method.
#[derive(Debug, Clone)]
pub struct Slice<R> {
    row: R,
    range: Range<usize>,
}

impl<R: Row> PartialEq for Slice<R> {
    fn eq(&self, other: &Self) -> bool {
        self.iter().eq(other.iter())
    }
}
impl<R: Row> Eq for Slice<R> {}

impl<R: Row> Row for Slice<R> {
    #[inline]
    fn datum_at(&self, index: usize) -> DatumRef<'_> {
        assert!(index < self.len());
        // SAFETY: we have checked that `self.indices` are all valid in `new`.
        unsafe { self.row.datum_at_unchecked(self.range.start + index) }
    }

    #[inline]
    unsafe fn datum_at_unchecked(&self, index: usize) -> DatumRef<'_> {
        unsafe { self.row.datum_at_unchecked(self.range.start + index) }
    }

    #[inline]
    fn len(&self) -> usize {
        self.range.len()
    }

    #[inline]
    fn iter(&self) -> impl Iterator<Item = DatumRef<'_>> {
        self.row
            .iter()
            .skip(self.range.start)
            .take(self.range.len())
    }
}

impl<R: Row> Slice<R> {
    pub(crate) fn new(row: R, range: impl RangeBounds<usize>) -> Self {
        use std::ops::Bound::*;
        let start = match range.start_bound() {
            Included(&i) => i,
            Excluded(&i) => i + 1,
            Unbounded => 0,
        };
        let end = match range.end_bound() {
            Included(&i) => i + 1,
            Excluded(&i) => i,
            Unbounded => row.len(),
        };
        let range = start..end;
        assert!(range.end <= row.len(), "range out of bounds");
        Self { row, range }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::row::{OwnedRow, RowExt};
    use crate::types::{ScalarImpl, ScalarRefImpl};

    #[test]
    fn test_slice() {
        let r0 = OwnedRow::new((0..=8).map(|i| Some(ScalarImpl::Int64(i))).collect());
        let r_expected = OwnedRow::new((2..4).map(|i| Some(ScalarImpl::Int64(i))).collect());

        let r = r0.slice(2..4);
        assert_eq!(r.len(), 2);
        assert!(r.iter().eq(r_expected.iter()));

        for i in 0..2 {
            assert_eq!(r.datum_at(i), Some(ScalarRefImpl::Int64(i as i64 + 2)));
        }
    }
}

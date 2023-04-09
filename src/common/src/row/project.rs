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

use super::Row;
use crate::types::DatumRef;

/// Row for the [`project`](super::RowExt::project) method.
#[derive(Debug, Clone, Copy)]
pub struct Project<'i, R> {
    row: R,
    indices: &'i [usize],
}

impl<'i, R: Row> PartialEq for Project<'i, R> {
    fn eq(&self, other: &Self) -> bool {
        self.iter().eq(other.iter())
    }
}
impl<'i, R: Row> Eq for Project<'i, R> {}

impl<'i, R: Row> Row for Project<'i, R> {
    type Iter<'a> = std::iter::Map<std::slice::Iter<'i, usize>, impl FnMut(&'i usize) -> DatumRef<'a>>
    where
        R: 'a,
        'i: 'a;

    #[inline]
    fn datum_at(&self, index: usize) -> DatumRef<'_> {
        // SAFETY: we have checked that `self.indices` are all valid in `new`.
        unsafe { self.row.datum_at_unchecked(self.indices[index]) }
    }

    #[inline]
    unsafe fn datum_at_unchecked(&self, index: usize) -> DatumRef<'_> {
        self.row
            .datum_at_unchecked(*self.indices.get_unchecked(index))
    }

    #[inline]
    fn len(&self) -> usize {
        self.indices.len()
    }

    #[inline]
    fn iter(&self) -> Self::Iter<'_> {
        self.indices.iter().map(|&i|
                // SAFETY: we have checked that `self.indices` are all valid in `new`.
                unsafe { self.row.datum_at_unchecked(i) })
    }
}

impl<'i, R: Row> Project<'i, R> {
    pub(crate) fn new(row: R, indices: &'i [usize]) -> Self {
        if let Some(index) = indices.iter().find(|&&i| i >= row.len()) {
            panic!(
                "index {} out of bounds for row of length {}",
                index,
                row.len()
            );
        }
        Self { row, indices }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::row::{OwnedRow, RowExt};
    use crate::types::{ScalarImpl, ScalarRefImpl};

    #[test]
    fn test_project_row() {
        let r0 = OwnedRow::new((0..=8).map(|i| Some(ScalarImpl::Int64(i))).collect());
        let indices = vec![1, 1, 4, 5, 1, 4];

        let r_expected = OwnedRow::new(
            indices
                .iter()
                .map(|&i| Some(ScalarImpl::Int64(i as _)))
                .collect(),
        );

        let r = r0.project(&indices);
        assert_eq!(r.len(), 6);
        assert!(r.iter().eq(r_expected.iter()));

        for (i, &v) in indices.iter().enumerate() {
            assert_eq!(r.datum_at(i), Some(ScalarRefImpl::Int64(v as _)));
        }
    }
}

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

use super::Row2;
use crate::types::DatumRef;

/// Row for the [`project`](super::RowExt::project) method.
#[derive(Debug)]
pub struct Project<'i, R> {
    row: R,
    indices: &'i [usize],
}

impl<'i, R: Row2> PartialEq for Project<'i, R> {
    fn eq(&self, other: &Self) -> bool {
        self.iter().eq(other.iter())
    }
}
impl<'i, R: Row2> Eq for Project<'i, R> {}

impl<'i, R: Row2> Row2 for Project<'i, R> {
    type Iter<'a> = impl Iterator<Item = DatumRef<'a>>
    where
        R: 'a,
        'i: 'a;

    fn datum_at(&self, index: usize) -> DatumRef<'_> {
        // SAFETY: we have checked that `self.indices` are all valid in `RowExt::project`.
        unsafe { self.row.datum_at_unchecked(self.indices[index]) }
    }

    unsafe fn datum_at_unchecked(&self, index: usize) -> DatumRef<'_> {
        self.row
            .datum_at_unchecked(*self.indices.get_unchecked(index))
    }

    fn len(&self) -> usize {
        self.indices.len()
    }

    fn iter(&self) -> Self::Iter<'_> {
        self.indices.iter().map(|&i|
                // SAFETY: we have checked that `self.indices` are all valid in `RowExt::project`.
                unsafe { self.row.datum_at_unchecked(i) })
    }
}

impl<'i, R: Row2> Project<'i, R> {
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

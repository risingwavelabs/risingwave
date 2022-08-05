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

use itertools::Itertools;

use super::Row;
use crate::array::column::Column;
use crate::array::DataChunk;
use crate::types::{DatumRef, ToOwnedDatum};

#[derive(Clone)]
pub struct RowRef<'a> {
    chunk: &'a DataChunk,

    idx: usize,
}

impl<'a> std::fmt::Debug for RowRef<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list().entries(self.values()).finish()
    }
}

impl<'a> RowRef<'a> {
    pub fn new(chunk: &'a DataChunk, idx: usize) -> Self {
        debug_assert!(idx < chunk.capacity());
        Self { chunk, idx }
    }

    pub fn value_at(&self, pos: usize) -> DatumRef<'_> {
        debug_assert!(self.idx < self.chunk.capacity());
        // for `RowRef`, the index is always in bound.
        unsafe {
            self.chunk.columns()[pos]
                .array_ref()
                .value_at_unchecked(self.idx)
        }
    }

    pub fn size(&self) -> usize {
        self.chunk.columns().len()
    }

    pub fn values<'b>(&'b self) -> impl Iterator<Item = DatumRef<'a>>
    where
        'a: 'b,
    {
        debug_assert!(self.idx < self.chunk.capacity());
        RowRefIter::<'a> {
            columns: self.chunk.columns().iter(),
            row_idx: self.idx,
        }
    }

    pub fn to_owned_row(&self) -> Row {
        Row(self.values().map(ToOwnedDatum::to_owned_datum).collect())
    }

    /// Get an owned `Row` by the given `indices` from current row ref.
    ///
    /// Use `datum_refs_by_indices` if possible instead to avoid allocating owned datums.
    pub fn row_by_indices(&self, indices: &[usize]) -> Row {
        Row(indices
            .iter()
            .map(|&idx| self.value_at(idx).to_owned_datum())
            .collect_vec())
    }

    /// Get an iterator of datum refs by the given `indices` from current row ref.
    pub fn datum_refs_by_indices<'b, 'c>(
        &'b self,
        indices: &'c [usize],
    ) -> impl Iterator<Item = DatumRef<'c>>
    where
        'a: 'b,
        'b: 'c,
    {
        indices.iter().map(|&idx| self.value_at(idx))
    }

    /// Get the index of this row in the data chunk.
    #[must_use]
    pub fn index(&self) -> usize {
        self.idx
    }
}

impl<'a> PartialEq for RowRef<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.values()
            .zip_longest(other.values())
            .all(|pair| pair.both().map(|(a, b)| a == b).unwrap_or(false))
    }
}

impl<'a> Eq for RowRef<'a> {}

#[derive(Clone)]
struct RowRefIter<'a> {
    columns: std::slice::Iter<'a, Column>,
    row_idx: usize,
}

impl<'a> Iterator for RowRefIter<'a> {
    type Item = DatumRef<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        // SAFETY: for `RowRef`, the index is always in bound.
        unsafe {
            self.columns
                .next()
                .map(|col| col.array_ref().value_at_unchecked(self.row_idx))
        }
    }
}

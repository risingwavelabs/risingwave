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

use super::column::Column;
use crate::array::DataChunk;
use crate::row::{Row, Row2};
use crate::types::{DatumRef, ToOwnedDatum};

impl DataChunk {
    /// Get an iterator for visible rows.
    pub fn rows(&self) -> impl Iterator<Item = RowRef<'_>> {
        DataChunkRefIter {
            chunk: self,
            idx: Some(0),
        }
    }

    /// Get an iterator for all rows in the chunk, and a `None` represents an invisible row.
    pub fn rows_with_holes(&self) -> impl Iterator<Item = Option<RowRef<'_>>> {
        DataChunkRefIterWithHoles {
            chunk: self,
            idx: 0,
        }
    }
}

struct DataChunkRefIter<'a> {
    chunk: &'a DataChunk,
    /// `None` means finished
    idx: Option<usize>,
}

impl<'a> Iterator for DataChunkRefIter<'a> {
    type Item = RowRef<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.idx {
            None => None,
            Some(idx) => {
                self.idx = self.chunk.next_visible_row_idx(idx);
                match self.idx {
                    None => None,
                    Some(idx) => {
                        self.idx = Some(idx + 1);
                        Some(RowRef {
                            chunk: self.chunk,
                            idx,
                        })
                    }
                }
            }
        }
    }
}

struct DataChunkRefIterWithHoles<'a> {
    chunk: &'a DataChunk,
    idx: usize,
}

impl<'a> Iterator for DataChunkRefIterWithHoles<'a> {
    type Item = Option<RowRef<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        let len = self.chunk.capacity();
        let vis = self.chunk.vis();
        if self.idx == len {
            None
        } else {
            let ret = Some(if !vis.is_set(self.idx) {
                None
            } else {
                Some(RowRef {
                    chunk: self.chunk,
                    idx: self.idx,
                })
            });
            self.idx += 1;
            ret
        }
    }
}

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

    /// Returns the datum ref at the `pos`, without doing bounds checking.
    ///
    /// # Safety
    /// Calling this method with an out-of-bounds index is undefined behavior.
    pub unsafe fn value_at_unchecked(&self, pos: usize) -> DatumRef<'_> {
        debug_assert!(self.idx < self.chunk.capacity());
        // for `RowRef`, the index is always in bound.
        self.chunk
            .columns()
            .get_unchecked(pos)
            .array_ref()
            .value_at_unchecked(self.idx)
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

impl PartialEq for RowRef<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.values().eq(other.values())
    }
}
impl Eq for RowRef<'_> {}

impl PartialOrd for RowRef<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.values().partial_cmp(other.values())
    }
}
impl Ord for RowRef<'_> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.values().cmp(other.values())
    }
}

impl Row2 for RowRef<'_> {
    type Iter<'a> = impl Iterator<Item = DatumRef<'a>>
    where
        Self: 'a;

    fn datum_at(&self, index: usize) -> DatumRef<'_> {
        RowRef::value_at(self, index)
    }

    unsafe fn datum_at_unchecked(&self, index: usize) -> DatumRef<'_> {
        RowRef::value_at_unchecked(self, index)
    }

    fn len(&self) -> usize {
        RowRef::size(self)
    }

    fn iter(&self) -> Self::Iter<'_> {
        RowRef::values(self)
    }
}

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

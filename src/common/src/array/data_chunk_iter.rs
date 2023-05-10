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

use std::iter::TrustedLen;

use super::ArrayRef;
use crate::array::DataChunk;
use crate::row::Row;
use crate::types::DatumRef;

impl DataChunk {
    /// Get an iterator for visible rows.
    pub fn rows(&self) -> DataChunkRefIter<'_> {
        DataChunkRefIter {
            chunk: self,
            idx: Some(0),
        }
    }

    /// Get an iterator for all rows in the chunk, and a `None` represents an invisible row.
    pub fn rows_with_holes(&self) -> DataChunkRefIterWithHoles<'_> {
        DataChunkRefIterWithHoles {
            chunk: self,
            idx: 0,
        }
    }
}

pub struct DataChunkRefIter<'a> {
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

    fn size_hint(&self) -> (usize, Option<usize>) {
        if let Some(idx) = self.idx {
            (
                // if all following rows are invisible
                0,
                // if all following rows are visible
                Some(std::cmp::min(
                    self.chunk.capacity() - idx,
                    self.chunk.cardinality(),
                )),
            )
        } else {
            (0, Some(0))
        }
    }
}

pub struct DataChunkRefIterWithHoles<'a> {
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

    fn size_hint(&self) -> (usize, Option<usize>) {
        let size = self.chunk.capacity() - self.idx;
        (size, Some(size))
    }
}

impl ExactSizeIterator for DataChunkRefIterWithHoles<'_> {}
unsafe impl TrustedLen for DataChunkRefIterWithHoles<'_> {}

#[derive(Clone, Copy)]
pub struct RowRef<'a> {
    chunk: &'a DataChunk,

    idx: usize,
}

impl<'a> std::fmt::Debug for RowRef<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list().entries(self.iter()).finish()
    }
}

impl<'a> RowRef<'a> {
    pub fn new(chunk: &'a DataChunk, idx: usize) -> Self {
        debug_assert!(idx < chunk.capacity());
        Self { chunk, idx }
    }

    /// Get the index of this row in the data chunk.
    #[must_use]
    pub fn index(&self) -> usize {
        self.idx
    }
}

impl PartialEq for RowRef<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.iter().eq(other.iter())
    }
}
impl Eq for RowRef<'_> {}

impl PartialOrd for RowRef<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.iter().partial_cmp(other.iter())
    }
}
impl Ord for RowRef<'_> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.iter().cmp(other.iter())
    }
}

impl Row for RowRef<'_> {
    type Iter<'a> = RowRefIter<'a>
    where
        Self: 'a;

    fn datum_at(&self, index: usize) -> DatumRef<'_> {
        debug_assert!(self.idx < self.chunk.capacity());
        // for `RowRef`, the index is always in bound.
        unsafe { self.chunk.columns()[index].value_at_unchecked(self.idx) }
    }

    unsafe fn datum_at_unchecked(&self, index: usize) -> DatumRef<'_> {
        debug_assert!(self.idx < self.chunk.capacity());
        // for `RowRef`, the index is always in bound.
        self.chunk
            .columns()
            .get_unchecked(index)
            .value_at_unchecked(self.idx)
    }

    fn len(&self) -> usize {
        self.chunk.columns().len()
    }

    fn iter(&self) -> Self::Iter<'_> {
        debug_assert!(self.idx < self.chunk.capacity());
        RowRefIter {
            columns: self.chunk.columns().iter(),
            row_idx: self.idx,
        }
    }
}

#[derive(Clone)]
pub struct RowRefIter<'a> {
    columns: std::slice::Iter<'a, ArrayRef>,
    row_idx: usize,
}

impl<'a> Iterator for RowRefIter<'a> {
    type Item = DatumRef<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        // SAFETY: for `RowRef`, the index is always in bound.
        unsafe {
            self.columns
                .next()
                .map(|col| col.value_at_unchecked(self.row_idx))
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.columns.size_hint()
    }
}

impl ExactSizeIterator for RowRefIter<'_> {}
unsafe impl TrustedLen for RowRefIter<'_> {}

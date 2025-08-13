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

use std::hash::Hash;
use std::iter::{FusedIterator, TrustedLen};
use std::ops::Range;

use risingwave_common::array::ColumnChunk;

use super::{ArrayRef, RowChunk};
use crate::array::DataChunk;
use crate::row::Row;
use crate::types::DatumRef;

impl ColumnChunk {
    /// Get an iterator for visible rows.
    pub fn rows(&self) -> ColumnChunkRefIter<'_> {
        self.rows_in(0..self.capacity())
    }

    /// Get an iterator for visible rows in range.
    pub fn rows_in(&self, range: Range<usize>) -> ColumnChunkRefIter<'_> {
        ColumnChunkRefIter {
            chunk: self,
            idx: range,
        }
    }

    /// Get an iterator for all rows in the chunk, and a `None` represents an invisible row.
    pub fn rows_with_holes(&self) -> ColumnChunkRefIterWithHoles<'_> {
        ColumnChunkRefIterWithHoles {
            chunk: self,
            idx: 0,
        }
    }
}

impl RowChunk {
    pub fn rows(&self) -> RowChunkRefIter<'_> {
        self.rows_in(0..self.capacity())
    }

    /// Get an iterator for visible rows in range.
    pub fn rows_in(&self, range: Range<usize>) -> RowChunkRefIter<'_> {
        RowChunkRefIter {
            chunk: self,
            idx: range,
        }
    }

    /// Get an iterator for all rows in the chunk, and a `None` represents an invisible row.
    pub fn rows_with_holes(&self) -> RowChunkRefIterWithHoles<'_> {
        RowChunkRefIterWithHoles {
            chunk: self,
            idx: 0,
        }
    }
}

impl DataChunk {
    /// Get an iterator for visible rows.
    pub fn rows(&self) -> DataChunkRefIter<'_> {
        match self {
            DataChunk::Columns(c) => DataChunkRefIter::Columns(c.rows()),
            DataChunk::Rows(r) => DataChunkRefIter::Rows(r.rows()),
        }
    }

    /// Get an iterator for visible rows in range.
    pub fn rows_in(&self, range: Range<usize>) -> DataChunkRefIter<'_> {
        match self {
            DataChunk::Columns(c) => DataChunkRefIter::Columns(c.rows_in(range)),
            DataChunk::Rows(r) => DataChunkRefIter::Rows(r.rows_in(range)),
        }
    }

    /// Get an iterator for all rows in the chunk, and a `None` represents an invisible row.
    pub fn rows_with_holes(&self) -> DataChunkRefIterWithHoles<'_> {
        match self {
            DataChunk::Columns(c) => DataChunkRefIterWithHoles::Columns(c.rows_with_holes()),
            DataChunk::Rows(r) => DataChunkRefIterWithHoles::Rows(r.rows_with_holes()),
        }
    }
}

pub enum DataChunkRefIter<'a> {
    Columns(ColumnChunkRefIter<'a>),
    Rows(RowChunkRefIter<'a>),
}

impl<'a> Iterator for DataChunkRefIter<'a> {
    type Item = RowRef<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            DataChunkRefIter::Columns(c) => c.next(),
            DataChunkRefIter::Rows(r) => r.next(),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            DataChunkRefIter::Columns(c) => c.size_hint(),
            DataChunkRefIter::Rows(r) => r.size_hint(),
        }
    }
}

pub struct RowChunkRefIter<'a> {
    chunk: &'a RowChunk,
    idx: Range<usize>,
}

impl<'a> Iterator for RowChunkRefIter<'a> {
    type Item = RowRef<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.idx.start == self.idx.end {
            return None;
        }
        match self.chunk.next_visible_row_idx(self.idx.start) {
            Some(idx) if idx < self.idx.end => {
                self.idx.start = idx + 1;
                Some(RowRef::from_row_chunk(self.chunk, idx))
            }
            _ => {
                self.idx.start = self.idx.end;
                None
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        if self.idx.start != self.idx.end {
            (
                // if all following rows are invisible
                0,
                // if all following rows are visible
                Some(std::cmp::min(
                    self.idx.end - self.idx.start,
                    self.chunk.cardinality(),
                )),
            )
        } else {
            (0, Some(0))
        }
    }
}

pub struct ColumnChunkRefIter<'a> {
    chunk: &'a ColumnChunk,
    idx: Range<usize>,
}

impl<'a> Iterator for ColumnChunkRefIter<'a> {
    type Item = RowRef<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.idx.start == self.idx.end {
            return None;
        }
        match self.chunk.next_visible_row_idx(self.idx.start) {
            Some(idx) if idx < self.idx.end => {
                self.idx.start = idx + 1;
                Some(RowRef::from_column_chunk(self.chunk, idx))
            }
            _ => {
                self.idx.start = self.idx.end;
                None
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        if self.idx.start != self.idx.end {
            (
                // if all following rows are invisible
                0,
                // if all following rows are visible
                Some(std::cmp::min(
                    self.idx.end - self.idx.start,
                    self.chunk.cardinality(),
                )),
            )
        } else {
            (0, Some(0))
        }
    }
}

impl FusedIterator for DataChunkRefIter<'_> {}

impl FusedIterator for ColumnChunkRefIter<'_> {}

impl FusedIterator for RowChunkRefIter<'_> {}

pub enum DataChunkRefIterWithHoles<'a> {
    Columns(ColumnChunkRefIterWithHoles<'a>),
    Rows(RowChunkRefIterWithHoles<'a>),
}

impl<'a> Iterator for DataChunkRefIterWithHoles<'a> {
    type Item = Option<RowRef<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            DataChunkRefIterWithHoles::Columns(c) => c.next(),
            DataChunkRefIterWithHoles::Rows(r) => r.next(),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            DataChunkRefIterWithHoles::Columns(c) => c.size_hint(),
            DataChunkRefIterWithHoles::Rows(r) => r.size_hint(),
        }
    }
}

pub struct RowChunkRefIterWithHoles<'a> {
    chunk: &'a RowChunk,
    idx: usize,
}

impl<'a> Iterator for RowChunkRefIterWithHoles<'a> {
    type Item = Option<RowRef<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        let len = self.chunk.capacity();
        let vis = self.chunk.visibility();
        if self.idx == len {
            None
        } else {
            let ret = Some(if !vis.is_set(self.idx) {
                None
            } else {
                Some(RowRef::from_row_chunk(self.chunk, self.idx))
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

pub struct ColumnChunkRefIterWithHoles<'a> {
    chunk: &'a ColumnChunk,
    idx: usize,
}

impl<'a> Iterator for ColumnChunkRefIterWithHoles<'a> {
    type Item = Option<RowRef<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        let len = self.chunk.capacity();
        let vis = self.chunk.visibility();
        if self.idx == len {
            None
        } else {
            let ret = Some(if !vis.is_set(self.idx) {
                None
            } else {
                Some(RowRef::from_column_chunk(self.chunk, self.idx))
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

impl ExactSizeIterator for ColumnChunkRefIterWithHoles<'_> {}
unsafe impl TrustedLen for ColumnChunkRefIterWithHoles<'_> {}

impl ExactSizeIterator for RowChunkRefIterWithHoles<'_> {}
unsafe impl TrustedLen for RowChunkRefIterWithHoles<'_> {}

// Deliberately making `RowRef` and `RowRefIter` defined in a private module to ensure
// the checks in the constructors are always performed.
mod row_ref {
    use risingwave_common::row::OwnedRow;

    use super::*;

    #[derive(Clone, Copy)]
    pub enum RowRef<'a> {
        Column { columns: &'a [ArrayRef], idx: usize },
        Row { row: &'a OwnedRow, idx: usize },
    }

    impl std::fmt::Debug for RowRef<'_> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_list().entries(self.iter()).finish()
        }
    }

    impl<'a> RowRef<'a> {
        pub fn from_row_chunk(row_chunk: &'a RowChunk, idx: usize) -> Self {
            Self::with_rows(&row_chunk.raw_rows()[idx], idx)
        }

        pub fn with_rows(row: &'a OwnedRow, idx: usize) -> Self {
            RowRef::Row { row, idx }
        }

        pub fn from_column_chunk(column_chunk: &'a ColumnChunk, idx: usize) -> Self {
            Self::with_columns(column_chunk.columns(), idx)
        }

        pub fn new(chunk: &'a DataChunk, idx: usize) -> Self {
            assert!(
                idx < chunk.capacity(),
                "index {idx} out of bound {}",
                chunk.capacity()
            );

            RowRef::Column {
                columns: chunk.columns(),
                idx,
            }
        }

        pub fn with_columns(columns: &'a [ArrayRef], idx: usize) -> Self {
            for column in columns {
                assert!(
                    idx < column.len(),
                    "index {idx} out of bound {}",
                    column.len()
                );
            }

            RowRef::Column { columns, idx }
        }

        /// Get the index of this row in the data chunk.
        #[must_use]
        pub fn index(&self) -> usize {
            match self {
                RowRef::Column { columns: _, idx } => *idx,
                RowRef::Row { row: _, idx } => *idx,
            }
        }
    }

    impl PartialEq for RowRef<'_> {
        fn eq(&self, other: &Self) -> bool {
            self.iter().eq(other.iter())
        }
    }
    impl Eq for RowRef<'_> {}

    impl Hash for RowRef<'_> {
        fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
            self.hash_datums_into(state)
        }
    }

    impl Row for RowRef<'_> {
        fn datum_at(&self, index: usize) -> DatumRef<'_> {
            match self {
                RowRef::Column { columns, idx } => {
                    // SAFETY: `self.idx` is already checked in `new` or `with_columns`.
                    unsafe { columns[index].value_at_unchecked(*idx) }
                }
                RowRef::Row { row, idx: _ } => row.datum_at(index),
            }
        }

        unsafe fn datum_at_unchecked(&self, index: usize) -> DatumRef<'_> {
            match self {
                RowRef::Column { columns, idx } => unsafe {
                    columns.get_unchecked(index).value_at_unchecked(*idx)
                },
                RowRef::Row { row, idx: _ } => unsafe { row.datum_at_unchecked(index) },
            }
        }

        fn len(&self) -> usize {
            match self {
                RowRef::Column { columns, idx: _ } => columns.len(),
                RowRef::Row { row, idx: _ } => row.len(),
            }
        }

        fn iter(&self) -> impl ExactSizeIterator<Item = DatumRef<'_>> {
            match self {
                RowRef::Column { columns, idx } => RowRefIter::Column {
                    columns: columns.iter(),
                    row_idx: *idx,
                },
                RowRef::Row { row, idx: _ } => RowRefIter::Row { row: row, idx: 0 },
            }
        }
    }

    #[derive(Clone)]
    pub enum RowRefIter<'a> {
        Column {
            columns: std::slice::Iter<'a, ArrayRef>,
            row_idx: usize,
        },
        Row {
            row: &'a OwnedRow,
            idx: usize,
        },
    }

    impl<'a> Iterator for RowRefIter<'a> {
        type Item = DatumRef<'a>;

        fn next(&mut self) -> Option<Self::Item> {
            match self {
                RowRefIter::Column { columns, row_idx } => unsafe {
                    columns.next().map(|col| col.value_at_unchecked(*row_idx))
                },
                RowRefIter::Row { row, idx: row_idx } => {
                    if *row_idx < row.len() {
                        let ret = Some(row.datum_at(*row_idx));
                        *row_idx += 1;
                        ret
                    } else {
                        None
                    }
                }
            }
            // SAFETY: `self.row_idx` is already checked in `new` or `with_columns` of `RowRef`.
        }

        fn size_hint(&self) -> (usize, Option<usize>) {
            match self {
                RowRefIter::Column {
                    columns,
                    row_idx: _,
                } => columns.size_hint(),
                RowRefIter::Row { row, idx: _ } => row.iter().size_hint(),
            }
        }
    }

    impl ExactSizeIterator for RowRefIter<'_> {}
    unsafe impl TrustedLen for RowRefIter<'_> {}
}

pub use row_ref::{RowRef, RowRefIter};

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use crate::array::StreamChunk;
    use crate::test_prelude::StreamChunkTestExt;

    #[test]
    fn test_row_ref_hash() {
        let mut set = HashSet::new();
        let chunk1 = StreamChunk::from_pretty(
            " I I I
            + 2 5 1
            + 4 9 2
            - 2 5 1",
        );
        for (_, row) in chunk1.rows() {
            set.insert(row);
        }
        assert_eq!(set.len(), 2);

        let chunk2 = StreamChunk::from_pretty(
            " I I I
            - 4 9 2",
        );
        for (_, row) in chunk2.rows() {
            set.insert(row);
        }
        assert_eq!(set.len(), 2);

        let chunk3 = StreamChunk::from_pretty(
            " I I I
            + 1 2 3",
        );
        for (_, row) in chunk3.rows() {
            set.insert(row);
        }
        assert_eq!(set.len(), 3);
    }
}

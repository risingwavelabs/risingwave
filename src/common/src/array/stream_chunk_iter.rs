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

use std::iter::once;

use auto_enums::auto_enum;

use super::data_chunk_iter::DataChunkRefIter;
use super::RowRef;
use crate::array::{Op, StreamChunk};

impl StreamChunk {
    /// Return an iterator on stream records of this stream chunk.
    pub fn records(&self) -> StreamChunkRefIter<'_> {
        StreamChunkRefIter {
            chunk: self,
            inner: self.data.rows(),
        }
    }

    /// Return an iterator on rows of this stream chunk.
    ///
    /// Should consider using [`StreamChunk::records`] if possible.
    pub fn rows(&self) -> impl Iterator<Item = (Op, RowRef<'_>)> {
        self.data.rows().map(|row| {
            (
                // SAFETY: index is checked since we are in the iterator.
                unsafe { *self.ops().get_unchecked(row.index()) },
                row,
            )
        })
    }
}

pub struct StreamChunkRefIter<'a> {
    chunk: &'a StreamChunk,

    inner: DataChunkRefIter<'a>,
}

impl<'a> Iterator for StreamChunkRefIter<'a> {
    type Item = RecordRef<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let row = self.inner.next()?;
        // SAFETY: index is checked since `row` is `Some`.
        let op = unsafe { self.chunk.ops().get_unchecked(row.index()) };

        match op {
            Op::Insert => Some(RecordRef::Insert(row)),
            Op::Delete => Some(RecordRef::Delete(row)),
            Op::UpdateDelete => {
                let insert_row = self.inner.next().expect("expect a row after U-");
                // SAFETY: index is checked since `insert_row` is `Some`.
                let op = unsafe { *self.chunk.ops().get_unchecked(insert_row.index()) };
                debug_assert_eq!(op, Op::UpdateInsert, "expect a U+ after U-");

                Some(RecordRef::Update {
                    delete: row,
                    insert: insert_row,
                })
            }
            Op::UpdateInsert => panic!("expect a U- before U+"),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let (lower, upper) = self.inner.size_hint();
        (lower / 2, upper)
    }
}

#[derive(Debug, Clone)]
pub enum RecordRef<'a> {
    Insert(RowRef<'a>),
    Delete(RowRef<'a>),
    Update {
        delete: RowRef<'a>,
        insert: RowRef<'a>,
    },
}

impl<'a> RecordRef<'a> {
    /// Convert this stream record to one or two rows with corresponding ops.
    #[auto_enum(Iterator)]
    pub fn into_row_refs(self) -> impl Iterator<Item = (Op, RowRef<'a>)> {
        match self {
            RecordRef::Insert(row_ref) => once((Op::Insert, row_ref)),
            RecordRef::Delete(row_ref) => once((Op::Delete, row_ref)),
            RecordRef::Update { delete, insert } => {
                [(Op::UpdateDelete, delete), (Op::UpdateInsert, insert)].into_iter()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    extern crate test;
    use itertools::Itertools;
    use test::Bencher;

    use super::RecordRef;
    use crate::row::Row;
    use crate::test_utils::test_stream_chunk::{
        BigStreamChunk, TestStreamChunk, WhatEverStreamChunk,
    };

    #[test]
    fn test_chunk_rows() {
        let test = WhatEverStreamChunk;
        let chunk = test.stream_chunk();
        let mut rows = chunk.rows().map(|(op, row)| (op, row.into_owned_row()));
        assert_eq!(Some(test.row_with_op_at(0)), rows.next());
        assert_eq!(Some(test.row_with_op_at(1)), rows.next());
        assert_eq!(Some(test.row_with_op_at(2)), rows.next());
        assert_eq!(Some(test.row_with_op_at(3)), rows.next());
    }

    #[test]
    fn test_chunk_records() {
        let test = WhatEverStreamChunk;
        let chunk = test.stream_chunk();
        let mut rows = chunk
            .records()
            .flat_map(RecordRef::into_row_refs)
            .map(|(op, row)| (op, row.into_owned_row()));
        assert_eq!(Some(test.row_with_op_at(0)), rows.next());
        assert_eq!(Some(test.row_with_op_at(1)), rows.next());
        assert_eq!(Some(test.row_with_op_at(2)), rows.next());
        assert_eq!(Some(test.row_with_op_at(3)), rows.next());
    }

    #[bench]
    fn bench_rows_iterator_from_records(b: &mut Bencher) {
        let chunk = BigStreamChunk::new(10000).stream_chunk();
        b.iter(|| {
            for (_op, row) in chunk.records().flat_map(RecordRef::into_row_refs) {
                test::black_box(row.iter().count());
            }
        })
    }

    #[bench]
    fn bench_rows_iterator(b: &mut Bencher) {
        let chunk = BigStreamChunk::new(10000).stream_chunk();
        b.iter(|| {
            for (_op, row) in chunk.rows() {
                test::black_box(row.iter().count());
            }
        })
    }

    #[bench]
    fn bench_rows_iterator_vec_of_datum_refs(b: &mut Bencher) {
        let chunk = BigStreamChunk::new(10000).stream_chunk();
        b.iter(|| {
            for (_op, row) in chunk.rows() {
                // Mimic the old `RowRef(Vec<DatumRef>)`
                let row = row.iter().collect_vec();
                test::black_box(row);
            }
        })
    }

    #[bench]
    fn bench_record_iterator(b: &mut Bencher) {
        let chunk = BigStreamChunk::new(10000).stream_chunk();
        b.iter(|| {
            for record in chunk.records() {
                match record {
                    RecordRef::Insert(row) => test::black_box(row.iter().count()),
                    RecordRef::Delete(row) => test::black_box(row.iter().count()),
                    RecordRef::Update { delete, insert } => {
                        test::black_box(delete.iter().count());
                        test::black_box(insert.iter().count())
                    }
                };
            }
        })
    }
}

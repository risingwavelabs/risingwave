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

use std::iter::once;

use auto_enums::auto_enum;

use super::column::Column;
use super::data_chunk_iter::RowRef as DataChunkRowRef;
use super::Row;
use crate::array::{Op, StreamChunk};
use crate::types::{DatumRef, ToOwnedDatum};

impl StreamChunk {
    pub fn records(&self) -> impl Iterator<Item = RecordRef<'_>> {
        StreamChunkRefIter {
            chunk: self,
            inner: self.data_chunk.rows(),
        }
    }

    pub fn rows(&self) -> impl Iterator<Item = (Op, DataChunkRowRef<'_>)> {
        self.records().flat_map(|r| r.into_row_refs())
    }
}

type DataChunkRefIterr<'a> = impl Iterator<Item = DataChunkRowRef<'a>>;

struct StreamChunkRefIter<'a> {
    chunk: &'a StreamChunk,

    inner: DataChunkRefIterr<'a>,
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
                let op = unsafe { self.chunk.ops().get_unchecked(insert_row.index()) };
                assert_eq!(*op, Op::UpdateInsert, "expect a U+ after U-");

                Some(RecordRef::Update {
                    delete: row,
                    insert: insert_row,
                })
            }
            Op::UpdateInsert => panic!("expect a U- before U+"),
        }
    }
}

pub enum RecordRef<'a> {
    Insert(DataChunkRowRef<'a>),
    Delete(DataChunkRowRef<'a>),
    Update {
        delete: DataChunkRowRef<'a>,
        insert: DataChunkRowRef<'a>,
    },
}

impl<'a> RecordRef<'a> {
    #[auto_enum(Iterator)]
    pub fn into_row_refs(self) -> impl Iterator<Item = (Op, DataChunkRowRef<'a>)> {
        match self {
            RecordRef::Insert(row_ref) => once((Op::Insert, row_ref)),
            RecordRef::Delete(row_ref) => once((Op::Delete, row_ref)),
            RecordRef::Update { delete, insert } => {
                [(Op::UpdateDelete, delete), (Op::UpdateInsert, insert)].into_iter()
            }
        }
    }
}

pub struct RowRef<'a> {
    pub(super) chunk: &'a StreamChunk,

    pub(super) idx: usize,
}

impl<'a> RowRef<'a> {
    pub fn value_at(&self, pos: usize) -> DatumRef<'_> {
        debug_assert!(self.idx < self.chunk.capacity());
        // TODO: It's safe to use value_at_unchecked here.
        self.chunk.columns()[pos].array_ref().value_at(self.idx)
    }

    pub fn op(&self) -> Op {
        debug_assert!(self.idx < self.chunk.capacity());
        // SAFETY: idx is checked while creating `RowRefV2`.
        unsafe { *self.chunk.ops().get_unchecked(self.idx) }
    }

    pub fn size(&self) -> usize {
        self.chunk.columns().len()
    }

    pub fn values<'b>(&'b self) -> RowRefIter<'a>
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
}

#[derive(Clone)]
pub struct RowRefIter<'a> {
    columns: std::slice::Iter<'a, Column>,
    row_idx: usize,
}

impl<'a> Iterator for RowRefIter<'a> {
    type Item = DatumRef<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        // TODO: It's safe to use value_at_unchecked here.
        self.columns
            .next()
            .map(|col| col.array_ref().value_at(self.row_idx))
    }
}

#[cfg(test)]
mod tests {
    use crate::test_utils::test_stream_chunk::{TestStreamChunk, WhatEverStreamChunk};

    #[test]
    fn test_chunk_rows() {
        let chunk = WhatEverStreamChunk::stream_chunk();
        let mut rows = chunk.rows().map(|(op, row)| (op, row.to_owned_row()));
        assert_eq!(Some(WhatEverStreamChunk::row_with_op_at(0)), rows.next());
        assert_eq!(Some(WhatEverStreamChunk::row_with_op_at(1)), rows.next());
        assert_eq!(Some(WhatEverStreamChunk::row_with_op_at(2)), rows.next());
        assert_eq!(Some(WhatEverStreamChunk::row_with_op_at(3)), rows.next());
    }
}

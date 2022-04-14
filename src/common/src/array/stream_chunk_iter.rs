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

use super::column::Column;
use crate::array::{Op, StreamChunk};
use crate::types::DatumRef;

impl StreamChunk {
    pub fn rows(&self) -> impl Iterator<Item = RowRef<'_>> {
        StreamChunkRefIter {
            chunk: self,
            idx: 0,
        }
    }
}

struct StreamChunkRefIter<'a> {
    chunk: &'a StreamChunk,
    idx: usize,
}

impl<'a> Iterator for StreamChunkRefIter<'a> {
    type Item = RowRef<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.chunk.visibility() {
            Some(bitmap) => {
                loop {
                    let idx = self.idx;
                    if idx >= self.chunk.capacity() {
                        return None;
                    }
                    // SAFETY: idx is checked.
                    let vis = unsafe { bitmap.is_set_unchecked(idx) };
                    self.idx += 1;
                    if vis {
                        return Some(RowRef {
                            chunk: self.chunk,
                            idx,
                        });
                    }
                }
            }
            None => {
                let idx = self.idx;
                if idx >= self.chunk.capacity() {
                    return None;
                }
                self.idx += 1;
                Some(RowRef {
                    chunk: self.chunk,
                    idx,
                })
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
    use super::RowRef;
    use crate::array::Row;
    use crate::test_utils::test_stream_chunk::{TestStreamChunk, WhatEverStreamChunk};
    use crate::types::ToOwnedDatum;

    #[test]
    fn test_chunk_rows() {
        let chunk = WhatEverStreamChunk::stream_chunk();
        let row_to_owned = |row: RowRef| {
            (
                row.op(),
                Row(row
                    .values()
                    .map(ToOwnedDatum::to_owned_datum)
                    .collect::<Vec<_>>()),
            )
        };
        let mut rows = chunk.rows().map(row_to_owned);
        assert_eq!(Some(WhatEverStreamChunk::row_with_op_at(0)), rows.next());
        assert_eq!(Some(WhatEverStreamChunk::row_with_op_at(1)), rows.next());
        assert_eq!(Some(WhatEverStreamChunk::row_with_op_at(2)), rows.next());
        assert_eq!(Some(WhatEverStreamChunk::row_with_op_at(3)), rows.next());
    }
}

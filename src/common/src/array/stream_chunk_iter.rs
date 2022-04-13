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

pub struct StreamChunkRefIter<'a> {
    chunk: &'a StreamChunk,
    idx: usize,
}

/// Data Chunk iter only iterate visible tuples.
impl<'a> Iterator for StreamChunkRefIter<'a> {
    type Item = RowRef<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.idx >= self.chunk.capacity() {
                return None;
            }
            let (cur_val, vis) = self.chunk.row_at(self.idx).ok()?;
            self.idx += 1;
            if vis {
                return Some(cur_val);
            }
        }
    }
}

impl<'a> StreamChunkRefIter<'a> {
    pub fn new(chunk: &'a StreamChunk) -> Self {
        Self { chunk, idx: 0 }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct RowRef<'a> {
    pub op: Op,
    pub values: Vec<DatumRef<'a>>,
}

impl<'a> RowRef<'a> {
    pub fn new(op: Op, values: Vec<DatumRef<'a>>) -> Self {
        Self { op, values }
    }

    pub fn value_at(&self, pos: usize) -> DatumRef<'a> {
        self.values[pos]
    }

    pub fn op(&self) -> Op {
        self.op
    }

    pub fn size(&self) -> usize {
        self.values.len()
    }
}

impl StreamChunk {
    pub fn rows_v2(&self) -> impl Iterator<Item = RowRefV2<'_>> {
        StreamChunkRefIterV2 {
            chunk: self,
            idx: 0,
        }
    }
}

struct StreamChunkRefIterV2<'a> {
    chunk: &'a StreamChunk,
    idx: usize,
}

impl<'a> Iterator for StreamChunkRefIterV2<'a> {
    type Item = RowRefV2<'a>;

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
                        return Some(RowRefV2 {
                            chunk: self.chunk,
                            idx,
                        });
                    }
                }
            }
            None => {
                if self.idx >= self.chunk.capacity() {
                    return None;
                }
                self.idx += 1;
                Some(RowRefV2 {
                    chunk: self.chunk,
                    idx: self.idx,
                })
            }
        }
    }
}

pub struct RowRefV2<'a> {
    chunk: &'a StreamChunk,
    idx: usize,
}

impl RowRefV2<'_> {
    pub fn value_at(&self, pos: usize) -> DatumRef<'_> {
        // TODO: It's safe to use value_at_unchecked here.
        self.chunk.columns()[pos].array_ref().value_at(self.idx)
    }

    pub fn op(&self) -> Op {
        // SAFETY: idx is checked while creating `RowRefV2`.
        unsafe { *self.chunk.ops().get_unchecked(self.idx) }
    }

    pub fn size(&self) -> usize {
        self.chunk.columns().len()
    }

    pub fn values(&self) -> impl Iterator<Item = DatumRef<'_>> {
        RowRefV2Iter {
            columns: self.chunk.columns().iter(),
            row_idx: self.idx,
        }
    }
}

struct RowRefV2Iter<'a> {
    columns: std::slice::Iter<'a, Column>,
    row_idx: usize,
}

impl<'a> Iterator for RowRefV2Iter<'a> {
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
    use super::RowRefV2;
    use crate::array::Row;
    use crate::test_utils::test_stream_chunk::{TestStreamChunk, WhatEverStreamChunk};
    use crate::types::ToOwnedDatum;

    #[test]
    fn test_chunk_rows() {
        let chunk = WhatEverStreamChunk::stream_chunk();
        let row_to_owned = |row: RowRefV2| {
            (
                row.op(),
                Row(row
                    .values()
                    .map(ToOwnedDatum::to_owned_datum)
                    .collect::<Vec<_>>()),
            )
        };
        let mut rows = chunk.rows_v2().map(row_to_owned);
        assert_eq!(Some(WhatEverStreamChunk::row_with_op_at(0)), rows.next());
        assert_eq!(Some(WhatEverStreamChunk::row_with_op_at(1)), rows.next());
        assert_eq!(Some(WhatEverStreamChunk::row_with_op_at(2)), rows.next());
        assert_eq!(Some(WhatEverStreamChunk::row_with_op_at(3)), rows.next());
    }
}

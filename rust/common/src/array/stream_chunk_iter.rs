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
//
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

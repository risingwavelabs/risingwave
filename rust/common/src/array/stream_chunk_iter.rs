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

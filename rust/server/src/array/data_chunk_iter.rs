use crate::array::DataChunk;
use crate::types::DatumRef;

pub struct DataChunkIter<'a> {
    chunk: &'a DataChunk,
    idx: usize,
}

/// Data Chunk iter only iterate visible tuples.
impl<'a> Iterator for DataChunkIter<'a> {
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

impl<'a> DataChunkIter<'a> {
    pub fn new(chunk: &'a DataChunk) -> Self {
        Self { chunk, idx: 0 }
    }
}

/// TODO: Consider merge with Row in storage. It is end with Ref because it do not own data
/// and avoid conflict with [`Row`].
#[derive(Debug, PartialEq)]
pub struct RowRef<'a>(Vec<DatumRef<'a>>);

impl<'a> RowRef<'a> {
    pub fn new(values: Vec<DatumRef<'a>>) -> Self {
        Self(values)
    }

    pub fn value_at(&self, pos: usize) -> DatumRef<'a> {
        self.0[pos]
    }

    pub fn size(&self) -> usize {
        self.0.len()
    }
}

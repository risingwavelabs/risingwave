use crate::array2::DataChunk;
use crate::types::ScalarRefImpl;

pub struct DataChunkIter<'a> {
    chunk: &'a DataChunk,
    pos: usize,
}

pub struct DataTuple<'a>(Vec<Option<ScalarRefImpl<'a>>>);

impl<'a> DataTuple<'a> {
    pub fn new(values: Vec<Option<ScalarRefImpl<'a>>>) -> Self {
        Self(values)
    }

    pub fn value_at(&self, pos: usize) -> Option<ScalarRefImpl<'a>> {
        self.0[pos]
    }
}
impl<'a> Iterator for DataChunkIter<'a> {
    type Item = DataTuple<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos >= self.chunk.cardinality() {
            None
        } else {
            let item = self.chunk.row_at(self.pos);
            self.pos += 1;
            Some(item)
        }
    }
}

impl<'a> DataChunkIter<'a> {
    pub fn new(chunk: &'a DataChunk) -> Self {
        DataChunkIter { chunk, pos: 0 }
    }
}

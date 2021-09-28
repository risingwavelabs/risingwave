use crate::array2::iterator::ArrayImplIterator;
use crate::types::ScalarRefImpl;

pub struct DataChunkIter<'a> {
    array_iters: Vec<ArrayImplIterator<'a>>,
}

// TODO: Consider merge with Row in storage. It is end with Ref because it do not own data
// and avoid conflict with Row.
pub struct RowRef<'a>(Vec<Option<ScalarRefImpl<'a>>>);

impl<'a> RowRef<'a> {
    pub fn new(values: Vec<Option<ScalarRefImpl<'a>>>) -> Self {
        Self(values)
    }

    pub fn value_at(&self, pos: usize) -> Option<ScalarRefImpl<'a>> {
        self.0[pos]
    }
}

impl<'a> Iterator for DataChunkIter<'a> {
    type Item = RowRef<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut ret = Vec::with_capacity(self.array_iters.len());
        for iter in self.array_iters.iter_mut() {
            // Once one column iter return None => the end reached
            match iter.next() {
                Some(val) => ret.push(val),
                None => return None,
            }
        }
        return Some(RowRef(ret));
    }
}

impl<'a> DataChunkIter<'a> {
    pub fn new(array_iters: Vec<ArrayImplIterator<'a>>) -> Self {
        DataChunkIter { array_iters }
    }
}

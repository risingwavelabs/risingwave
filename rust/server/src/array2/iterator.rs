use super::Array;
use std::iter::Iterator;
use std::marker::PhantomData;

pub struct ArrayIterator<'a, A: Array> {
    data: &'a A,
    pos: usize,
    _phantom: PhantomData<&'a usize>,
}

impl<'a, A: Array> ArrayIterator<'a, A> {
    pub fn new(data: &'a A) -> Self {
        Self {
            data,
            pos: 0,
            _phantom: PhantomData,
        }
    }
}

impl<'a, A: Array> Iterator for ArrayIterator<'a, A> {
    type Item = Option<A::RefItem<'a>>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.pos >= self.data.len() {
            None
        } else {
            let item = self.data.value_at(self.pos);
            self.pos += 1;
            Some(item)
        }
    }
}

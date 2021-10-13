use super::Array;
use crate::array::ArrayImpl;
use crate::types::ScalarRefImpl;

pub struct ArrayIterator<'a, A: Array> {
    data: &'a A,
    pos: usize,
}

impl<'a, A: Array> ArrayIterator<'a, A> {
    pub fn new(data: &'a A) -> Self {
        Self { data, pos: 0 }
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

pub struct ArrayImplIterator<'a> {
    data: &'a ArrayImpl,
    pos: usize,
}

impl<'a> ArrayImplIterator<'a> {
    pub fn new(data: &'a ArrayImpl) -> Self {
        Self { data, pos: 0 }
    }
}

impl<'a> Iterator for ArrayImplIterator<'a> {
    type Item = Option<ScalarRefImpl<'a>>;
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

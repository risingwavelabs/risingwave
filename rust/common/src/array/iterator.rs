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
use super::Array;
use crate::array::ArrayImpl;
use crate::types::DatumRef;

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
    type Item = DatumRef<'a>;
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

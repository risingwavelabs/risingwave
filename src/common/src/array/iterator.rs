// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::iter::TrustedLen;

use super::Array;

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
            // SAFETY: bounds check is done by `self.pos < self.data.len()`.
            let item = unsafe { self.data.value_at_unchecked(self.pos) };
            self.pos += 1;
            Some(item)
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let size = self.data.len() - self.pos;
        (size, Some(size))
    }
}

impl<'a, A: Array> ExactSizeIterator for ArrayIterator<'a, A> {}
unsafe impl<'a, A: Array> TrustedLen for ArrayIterator<'a, A> {}

#[cfg(test)]
mod tests {
    use paste::paste;

    use super::*;
    use crate::array::{ArrayBuilder, ArrayImpl};
    use crate::for_all_variants;

    macro_rules! test_trusted_len {
        ($( { $variant_name:ident, $suffix_name:ident, $array:ty, $builder:ty } ),*) => {
            $(
                paste! {
                    #[test]
                    fn [<test_trusted_len_for_ $suffix_name _array>]() {
                        use crate::array::$builder;
                        let mut builder = $builder::new(5);
                        for _ in 0..5 {
                            builder.append_null();
                        }
                        let array = builder.finish();
                        let mut iter = array.iter();

                        assert_eq!(iter.size_hint(), (5, Some(5))); iter.next();
                        assert_eq!(iter.size_hint(), (4, Some(4))); iter.next();
                        assert_eq!(iter.size_hint(), (3, Some(3))); iter.nth(0);
                        assert_eq!(iter.size_hint(), (2, Some(2))); iter.nth(1);
                        assert_eq!(iter.size_hint(), (0, Some(0)));

                        let array_impl = ArrayImpl::from(array);
                        let mut iter = array_impl.iter();

                        assert_eq!(iter.size_hint(), (5, Some(5))); iter.next();
                        assert_eq!(iter.size_hint(), (4, Some(4))); iter.next();
                        assert_eq!(iter.size_hint(), (3, Some(3))); iter.nth(0);
                        assert_eq!(iter.size_hint(), (2, Some(2))); iter.nth(1);
                        assert_eq!(iter.size_hint(), (0, Some(0)));
                    }
                }
            )*
        };
    }

    for_all_variants! { test_trusted_len }
}

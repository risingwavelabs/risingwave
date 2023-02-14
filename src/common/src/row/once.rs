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

use super::{assert_row, Row};
use crate::types::{DatumRef, ToDatumRef};

/// Row for the [`once`] function.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct Once<D>(D);

impl<D: ToDatumRef> Row for Once<D> {
    type Iter<'a> = std::iter::Once<DatumRef<'a>>
    where
        Self: 'a;

    #[inline]
    fn datum_at(&self, index: usize) -> DatumRef<'_> {
        if index == 0 {
            self.0.to_datum_ref()
        } else {
            panic!("index out of bounds: the len of `Once` is 1 but the index is {index}")
        }
    }

    #[inline]
    unsafe fn datum_at_unchecked(&self, _index: usize) -> DatumRef<'_> {
        // Always ignore the index and return the datum, which is okay for undefined behavior.
        self.0.to_datum_ref()
    }

    #[inline]
    fn len(&self) -> usize {
        1
    }

    #[inline]
    fn iter(&self) -> Self::Iter<'_> {
        std::iter::once(self.0.to_datum_ref())
    }
}

/// Creates a row which contains a single [`Datum`](crate::types::Datum) or [`DatumRef`].
pub fn once<D: ToDatumRef>(datum: D) -> Once<D> {
    assert_row(Once(datum))
}

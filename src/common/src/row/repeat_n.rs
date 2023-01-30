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

use super::Row;
use crate::types::{DatumRef, ToDatumRef};

/// Row for the [`repeat_n`] function.
#[derive(Debug, Clone, Copy)]
pub struct RepeatN<D> {
    datum: D,
    n: usize,
}

impl<D: PartialEq> PartialEq for RepeatN<D> {
    fn eq(&self, other: &Self) -> bool {
        if self.n == 0 && other.n == 0 {
            true
        } else {
            self.datum == other.datum && self.n == other.n
        }
    }
}
impl<D: Eq> Eq for RepeatN<D> {}

impl<D: ToDatumRef> Row for RepeatN<D> {
    type Iter<'a> = std::iter::Take<std::iter::Repeat<DatumRef<'a>>>
    where
        Self: 'a;

    #[inline]
    fn datum_at(&self, index: usize) -> crate::types::DatumRef<'_> {
        if index < self.n {
            self.datum.to_datum_ref()
        } else {
            panic!(
                "index out of bounds: the len is {} but the index is {}",
                self.n, index
            )
        }
    }

    #[inline]
    unsafe fn datum_at_unchecked(&self, _index: usize) -> crate::types::DatumRef<'_> {
        // Always ignore the index and return the datum, which is okay for undefined behavior.
        self.datum.to_datum_ref()
    }

    #[inline]
    fn len(&self) -> usize {
        self.n
    }

    #[inline]
    fn iter(&self) -> Self::Iter<'_> {
        std::iter::repeat(self.datum.to_datum_ref()).take(self.n)
    }
}

/// Create a row which contains `n` repetitions of `datum`.
pub fn repeat_n<D: ToDatumRef>(datum: D, n: usize) -> RepeatN<D> {
    RepeatN { datum, n }
}

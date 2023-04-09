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
use crate::types::DatumRef;

/// Row for the [`empty`] function.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct Empty {
    _private: (),
}

impl Row for Empty {
    type Iter<'a> = std::iter::Empty<DatumRef<'a>>
    where
        Self: 'a;

    #[inline]
    fn datum_at(&self, index: usize) -> DatumRef<'_> {
        panic!("index out of bounds: the len of `Empty` is 0 but the index is {index}")
    }

    #[inline]
    unsafe fn datum_at_unchecked(&self, _index: usize) -> DatumRef<'_> {
        // Always ignore the index and return `NULL`, which is okay for undefined behavior.
        None
    }

    #[inline]
    fn len(&self) -> usize {
        0
    }

    #[inline]
    fn iter(&self) -> Self::Iter<'_> {
        std::iter::empty()
    }
}

/// Creates a row which contains no datums.
pub const fn empty() -> Empty {
    assert_row(Empty { _private: () })
}

pub(super) static EMPTY: Empty = empty();

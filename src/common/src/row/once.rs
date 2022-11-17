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

use super::{assert_row, Row2};
use crate::types::{DatumRef, ToDatumRef};

#[derive(Debug, PartialEq, Eq)]
pub struct Once<D>(D);

impl<D: ToDatumRef> Row2 for Once<D> {
    type Iter<'a> = impl Iterator<Item = DatumRef<'a>>
    where
        Self: 'a;

    fn datum_at(&self, index: usize) -> DatumRef<'_> {
        [self.0.to_datum_ref()][index]
    }

    unsafe fn datum_at_unchecked(&self, index: usize) -> DatumRef<'_> {
        *[self.0.to_datum_ref()].get_unchecked(index)
    }

    fn len(&self) -> usize {
        1
    }

    fn iter(&self) -> Self::Iter<'_> {
        std::iter::once(self.0.to_datum_ref())
    }
}

pub fn once<D: ToDatumRef>(datum: D) -> Once<D> {
    assert_row(Once(datum))
}

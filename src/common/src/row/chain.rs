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

use bytes::BufMut;

use super::Row2;
use crate::types::DatumRef;

/// Row for the [`chain`](super::RowExt::chain) method.
#[derive(Debug)]
pub struct Chain<R1, R2> {
    r1: R1,
    r2: R2,
}

impl<R1: Row2, R2: Row2> PartialEq for Chain<R1, R2> {
    fn eq(&self, other: &Self) -> bool {
        self.iter().eq(other.iter())
    }
}
impl<R1: Row2, R2: Row2> Eq for Chain<R1, R2> {}

impl<R1: Row2, R2: Row2> Row2 for Chain<R1, R2> {
    type Iter<'a> = impl Iterator<Item = DatumRef<'a>>
    where
        R1: 'a,
        R2: 'a;

    fn datum_at(&self, index: usize) -> DatumRef<'_> {
        if index < self.r1.len() {
            // SAFETY: `index < self.r1.len()` implies the index is valid.
            unsafe { self.r1.datum_at_unchecked(index) }
        } else {
            self.r2.datum_at(index - self.r1.len())
        }
    }

    unsafe fn datum_at_unchecked(&self, index: usize) -> DatumRef<'_> {
        if index < self.r1.len() {
            self.r1.datum_at_unchecked(index)
        } else {
            self.r2.datum_at_unchecked(index - self.r1.len())
        }
    }

    fn len(&self) -> usize {
        self.r1.len() + self.r2.len()
    }

    fn is_empty(&self) -> bool {
        self.r1.is_empty() && self.r2.is_empty()
    }

    fn iter(&self) -> Self::Iter<'_> {
        self.r1.iter().chain(self.r2.iter())
    }

    // Manually implemented in case `R1` or `R2` has a more efficient implementation.
    fn value_serialize_into(&self, mut buf: impl BufMut) {
        buf.put_slice(&self.r1.value_serialize());
        buf.put_slice(&self.r2.value_serialize());
    }
}

impl<R1, R2> Chain<R1, R2> {
    pub(super) fn new(r1: R1, r2: R2) -> Self {
        Self { r1, r2 }
    }
}

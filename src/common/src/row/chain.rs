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

use bytes::BufMut;

use super::Row;
use crate::types::DatumRef;

/// Row for the [`chain`](super::RowExt::chain) method.
#[derive(Debug, Clone, Copy)]
pub struct Chain<R1, R2> {
    r1: R1,
    r2: R2,
}

impl<R1: Row, R2: Row> PartialEq for Chain<R1, R2> {
    fn eq(&self, other: &Self) -> bool {
        self.iter().eq(other.iter())
    }
}
impl<R1: Row, R2: Row> Eq for Chain<R1, R2> {}

impl<R1: Row, R2: Row> Row for Chain<R1, R2> {
    type Iter<'a> = std::iter::Chain<R1::Iter<'a>, R2::Iter<'a>>
    where
        R1: 'a,
        R2: 'a;

    #[inline]
    fn datum_at(&self, index: usize) -> DatumRef<'_> {
        if index < self.r1.len() {
            // SAFETY: `index < self.r1.len()` implies the index is valid.
            unsafe { self.r1.datum_at_unchecked(index) }
        } else {
            self.r2.datum_at(index - self.r1.len())
        }
    }

    #[inline]
    unsafe fn datum_at_unchecked(&self, index: usize) -> DatumRef<'_> {
        if index < self.r1.len() {
            self.r1.datum_at_unchecked(index)
        } else {
            self.r2.datum_at_unchecked(index - self.r1.len())
        }
    }

    #[inline]
    fn len(&self) -> usize {
        self.r1.len() + self.r2.len()
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.r1.is_empty() && self.r2.is_empty()
    }

    #[inline]
    fn iter(&self) -> Self::Iter<'_> {
        self.r1.iter().chain(self.r2.iter())
    }

    // Manually implemented in case `R1` or `R2` has a more efficient implementation.
    #[inline]
    fn value_serialize_into(&self, mut buf: impl BufMut) {
        self.r1.value_serialize_into(&mut buf);
        self.r2.value_serialize_into(buf);
    }
}

impl<R1, R2> Chain<R1, R2> {
    pub(super) fn new(r1: R1, r2: R2) -> Self {
        Self { r1, r2 }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::row::OwnedRow;
    use crate::types::{ScalarImpl, ScalarRefImpl};

    #[test]
    fn test_chain_row() {
        let r1 = || OwnedRow::new((1..=3).map(|i| Some(ScalarImpl::Int64(i))).collect());
        let r2 = || OwnedRow::new((4..=6).map(|i| Some(ScalarImpl::Int64(i))).collect());
        let r3 = || OwnedRow::new((7..=9).map(|i| Some(ScalarImpl::Int64(i))).collect());

        let r_expected = OwnedRow::new((1..=9).map(|i| Some(ScalarImpl::Int64(i))).collect());

        macro_rules! test {
            ($r:expr) => {
                let r = $r;
                assert_eq!(r.len(), 9);
                assert!(r.iter().eq(r_expected.iter()));

                for i in 0..9 {
                    assert_eq!(r.datum_at(i), Some(ScalarRefImpl::Int64(i as i64 + 1)));
                }
            };
        }

        test!(Chain::new(r1(), Chain::new(r2(), r3())));
        test!(Chain::new(Chain::new(r1(), r2()), r3()));
    }
}

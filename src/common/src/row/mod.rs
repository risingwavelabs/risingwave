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

mod compacted_row;
mod owned_row;

use std::hash::{BuildHasher, Hasher};

pub use compacted_row::CompactedRow;
pub use owned_row::{Row, RowDeserializer};

use crate::array::RowRef;
use crate::hash::HashCode;
use crate::types::{hash_datum_ref, to_datum_ref, Datum, DatumRef, ToDatumRef, ToOwnedDatum};
use crate::util::value_encoding;

pub trait Row2: Sized + std::fmt::Debug + PartialEq + Eq {
    type Iter<'a>: Iterator<Item = DatumRef<'a>>
    where
        Self: 'a;

    fn datum_at(&self, index: usize) -> DatumRef<'_>;

    unsafe fn datum_at_unchecked(&self, index: usize) -> DatumRef<'_>;

    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn iter(&self) -> Self::Iter<'_>;

    fn to_owned_row(&self) -> Row {
        Row(self.iter().map(|d| d.to_owned_datum()).collect())
    }

    fn into_owned_row(self) -> Row {
        self.to_owned_row()
    }

    fn value_serialize(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        for datum in self.iter() {
            value_encoding::serialize_datum_ref(&datum, &mut buf);
        }
        buf
    }

    fn hash<H: BuildHasher>(&self, hash_builder: H) -> HashCode {
        let mut hasher = hash_builder.build_hasher();
        for datum in self.iter() {
            hash_datum_ref(datum, &mut hasher);
        }
        HashCode(hasher.finish())
    }
}

const fn assert_row<R: Row2>(r: R) -> R {
    r
}

pub trait RowExt: Row2 {
    fn chain<R: Row2>(self, other: R) -> Chain<Self, R>
    where
        Self: Sized,
    {
        assert_row(Chain {
            r1: self,
            r2: other,
        })
    }

    fn project(self, indices: &[usize]) -> Project<'_, Self>
    where
        Self: Sized,
    {
        if let Some(index) = indices.iter().find(|&&i| i >= self.len()) {
            panic!(
                "index {} out of bounds for row of length {}",
                index,
                self.len()
            );
        }
        assert_row(Project { row: self, indices })
    }
}

impl<R: Row2> RowExt for R {}

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

    fn value_serialize(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend(self.r1.value_serialize());
        buf.extend(self.r2.value_serialize());
        buf
    }
}

#[derive(Debug)]
pub struct Project<'i, R> {
    row: R,
    indices: &'i [usize],
}

impl<'i, R: Row2> PartialEq for Project<'i, R> {
    fn eq(&self, other: &Self) -> bool {
        self.iter().eq(other.iter())
    }
}
impl<'i, R: Row2> Eq for Project<'i, R> {}

impl<'i, R: Row2> Row2 for Project<'i, R> {
    type Iter<'a> = impl Iterator<Item = DatumRef<'a>>
    where
        R: 'a,
        'i: 'a;

    fn datum_at(&self, index: usize) -> DatumRef<'_> {
        // SAFETY: we have checked that `self.indices` are all valid in `RowExt::project`.
        unsafe { self.row.datum_at_unchecked(self.indices[index]) }
    }

    unsafe fn datum_at_unchecked(&self, index: usize) -> DatumRef<'_> {
        self.row
            .datum_at_unchecked(*self.indices.get_unchecked(index))
    }

    fn len(&self) -> usize {
        self.indices.len()
    }

    fn iter(&self) -> Self::Iter<'_> {
        self.indices.iter().map(|&i|
                // SAFETY: we have checked that `self.indices` are all valid in `RowExt::project`.
                unsafe { self.row.datum_at_unchecked(i) })
    }
}

macro_rules! deref_forward_row {
    () => {
        fn datum_at(&self, index: usize) -> DatumRef<'_> {
            (**self).datum_at(index)
        }

        unsafe fn datum_at_unchecked(&self, index: usize) -> DatumRef<'_> {
            (**self).datum_at_unchecked(index)
        }

        fn len(&self) -> usize {
            (**self).len()
        }

        fn is_empty(&self) -> bool {
            (**self).is_empty()
        }

        fn iter(&self) -> Self::Iter<'_> {
            (**self).iter()
        }

        fn to_owned_row(&self) -> Row {
            (**self).to_owned_row()
        }

        fn value_serialize(&self) -> Vec<u8> {
            (**self).value_serialize()
        }

        fn hash<H: BuildHasher>(&self, hash_builder: H) -> HashCode {
            (**self).hash(hash_builder)
        }
    };
}

impl<R: Row2> Row2 for &R {
    type Iter<'a> = R::Iter<'a>
    where
        Self: 'a;

    deref_forward_row!();
}

impl<R: Row2> Row2 for Box<R> {
    type Iter<'a> = R::Iter<'a>
    where
        Self: 'a;

    deref_forward_row!();

    fn into_owned_row(self) -> Row {
        (*self).into_owned_row()
    }
}

impl Row2 for &[Datum] {
    type Iter<'a> = impl Iterator<Item = DatumRef<'a>>
    where
        Self: 'a;

    fn datum_at(&self, index: usize) -> DatumRef<'_> {
        to_datum_ref(&self[index])
    }

    unsafe fn datum_at_unchecked(&self, index: usize) -> DatumRef<'_> {
        to_datum_ref(self.get_unchecked(index))
    }

    fn len(&self) -> usize {
        self.as_ref().len()
    }

    fn iter(&self) -> Self::Iter<'_> {
        Iterator::map(self.as_ref().iter(), to_datum_ref)
    }
}

impl Row2 for &[DatumRef<'_>] {
    type Iter<'a> = impl Iterator<Item = DatumRef<'a>>
    where
        Self: 'a;

    fn datum_at(&self, index: usize) -> DatumRef<'_> {
        self[index]
    }

    unsafe fn datum_at_unchecked(&self, index: usize) -> DatumRef<'_> {
        *self.get_unchecked(index)
    }

    fn len(&self) -> usize {
        <[DatumRef<'_>]>::len(self)
    }

    fn iter(&self) -> Self::Iter<'_> {
        <[DatumRef<'_>]>::iter(self).copied()
    }
}

impl Row2 for Row {
    type Iter<'a> = impl Iterator<Item = DatumRef<'a>>
    where
        Self: 'a;

    fn datum_at(&self, index: usize) -> DatumRef<'_> {
        to_datum_ref(&self[index])
    }

    unsafe fn datum_at_unchecked(&self, index: usize) -> DatumRef<'_> {
        to_datum_ref(self.0.get_unchecked(index))
    }

    fn len(&self) -> usize {
        self.0.len()
    }

    fn iter(&self) -> Self::Iter<'_> {
        Iterator::map(self.0.iter(), to_datum_ref)
    }

    fn to_owned_row(&self) -> Row {
        self.clone()
    }

    fn into_owned_row(self) -> Row {
        self
    }
}

impl Row2 for RowRef<'_> {
    type Iter<'a> = impl Iterator<Item = DatumRef<'a>>
    where
        Self: 'a;

    fn datum_at(&self, index: usize) -> DatumRef<'_> {
        RowRef::value_at(self, index)
    }

    unsafe fn datum_at_unchecked(&self, index: usize) -> DatumRef<'_> {
        RowRef::value_at_unchecked(self, index)
    }

    fn len(&self) -> usize {
        RowRef::size(self)
    }

    fn iter(&self) -> Self::Iter<'_> {
        RowRef::values(self)
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct Empty(());

impl Row2 for Empty {
    type Iter<'a> = impl Iterator<Item = DatumRef<'a>>
    where
        Self: 'a;

    fn datum_at(&self, index: usize) -> DatumRef<'_> {
        [][index]
    }

    unsafe fn datum_at_unchecked(&self, index: usize) -> DatumRef<'_> {
        *[].get_unchecked(index)
    }

    fn len(&self) -> usize {
        0
    }

    fn iter(&self) -> Self::Iter<'_> {
        std::iter::empty()
    }
}

pub fn empty() -> Empty {
    assert_row(Empty(()))
}

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

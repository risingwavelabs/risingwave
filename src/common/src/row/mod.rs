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

mod chain;
mod compacted_row;
mod empty;
mod once;
mod owned_row;
mod project;

use std::hash::{BuildHasher, Hasher};

pub use chain::Chain;
pub use compacted_row::CompactedRow;
pub use empty::{empty, Empty};
pub use once::{once, Once};
pub use owned_row::{Row, RowDeserializer};
pub use project::Project;

use crate::hash::HashCode;
use crate::types::{hash_datum_ref, to_datum_ref, Datum, DatumRef, ToOwnedDatum};
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
        assert_row(Chain::new(self, other))
    }

    fn project(self, indices: &[usize]) -> Project<'_, Self>
    where
        Self: Sized,
    {
        assert_row(Project::new(self, indices))
    }
}

impl<R: Row2> RowExt for R {}

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

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

use std::borrow::Cow;
use std::fmt::Display;
use std::hash::{BuildHasher, Hasher};

use bytes::{BufMut, Bytes, BytesMut};
use itertools::Itertools;

use self::empty::EMPTY;
use crate::hash::HashCode;
use crate::types::to_text::ToText;
use crate::types::{hash_datum, DatumRef, ToDatumRef, ToOwnedDatum};
use crate::util::ordered::OrderedRowSerde;
use crate::util::value_encoding;

/// The trait for abstracting over a Row-like type.
pub trait Row: Sized + std::fmt::Debug + PartialEq + Eq {
    type Iter<'a>: Iterator<Item = DatumRef<'a>>
    where
        Self: 'a;

    /// Returns the [`DatumRef`] at the given `index`.
    fn datum_at(&self, index: usize) -> DatumRef<'_>;

    /// Returns the [`DatumRef`] at the given `index` without bounds checking.
    ///
    /// # Safety
    /// Calling this method with an out-of-bounds index is undefined behavior.
    unsafe fn datum_at_unchecked(&self, index: usize) -> DatumRef<'_>;

    /// Returns the number of datum in the row.
    fn len(&self) -> usize;

    /// Returns `true` if the row contains no datum.
    #[inline]
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns an iterator over the datums in the row, in [`DatumRef`] form.
    fn iter(&self) -> Self::Iter<'_>;

    /// Converts the row into an [`OwnedRow`].
    ///
    /// Prefer `into_owned_row` if the row is already owned.
    #[inline]
    fn to_owned_row(&self) -> OwnedRow {
        OwnedRow::new(self.iter().map(|d| d.to_owned_datum()).collect())
    }

    /// Consumes `self` and converts it into an [`OwnedRow`].
    #[inline]
    fn into_owned_row(self) -> OwnedRow {
        self.to_owned_row()
    }

    /// Serializes the row with value encoding, into the given `buf`.
    #[inline]
    fn value_serialize_into(&self, mut buf: impl BufMut) {
        for datum in self.iter() {
            value_encoding::serialize_datum_into(datum, &mut buf);
        }
    }

    /// Serializes the row with value encoding and returns the bytes.
    #[inline]
    fn value_serialize(&self) -> Vec<u8> {
        let estimate_size = self
            .iter()
            .map(value_encoding::estimate_serialize_datum_size)
            .sum();
        let mut buf = Vec::with_capacity(estimate_size);
        self.value_serialize_into(&mut buf);
        buf
    }

    /// Serializes the row with value encoding and returns the bytes.
    #[inline]
    fn value_serialize_bytes(&self) -> Bytes {
        let estimate_size = self
            .iter()
            .map(value_encoding::estimate_serialize_datum_size)
            .sum();
        let mut buf = BytesMut::with_capacity(estimate_size);
        self.value_serialize_into(&mut buf);
        buf.freeze()
    }

    /// Serializes the row with memcomparable encoding, into the given `buf`. As each datum may have
    /// different order type, a `serde` should be provided.
    #[inline]
    fn memcmp_serialize_into(&self, serde: &OrderedRowSerde, buf: impl BufMut) {
        serde.serialize(self, buf);
    }

    /// Serializes the row with memcomparable encoding and return the bytes. As each datum may have
    /// different order type, a `serde` should be provided.
    #[inline]
    fn memcmp_serialize(&self, serde: &OrderedRowSerde) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.len()); // each datum is at least 1 byte
        self.memcmp_serialize_into(serde, &mut buf);
        buf
    }

    /// Returns the hash code of the row.
    #[inline]
    fn hash<H: BuildHasher>(&self, hash_builder: H) -> HashCode {
        let mut hasher = hash_builder.build_hasher();
        for datum in self.iter() {
            hash_datum(datum, &mut hasher);
        }
        HashCode(hasher.finish())
    }

    /// Determines whether the datums of this row are equal to those of another.
    #[inline]
    fn eq(this: &Self, other: impl Row) -> bool {
        this.iter().eq(other.iter())
    }
}

const fn assert_row<R: Row>(r: R) -> R {
    r
}

/// An extension trait for [`Row`]s that provides a variety of convenient adapters.
pub trait RowExt: Row {
    /// Adapter for chaining two rows together.
    fn chain<R: Row>(self, other: R) -> Chain<Self, R>
    where
        Self: Sized,
    {
        assert_row(Chain::new(self, other))
    }

    /// Adapter for projecting a row onto a subset of its columns with the given `indices`.
    ///
    /// # Panics
    /// Panics if `indices` contains an out-of-bounds index.
    fn project(self, indices: &[usize]) -> Project<'_, Self>
    where
        Self: Sized,
    {
        assert_row(Project::new(self, indices))
    }

    fn display(&self) -> impl Display + '_ {
        struct D<'a, T: Row>(&'a T);
        impl<'a, T: Row> Display for D<'a, T> {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(
                    f,
                    "{}",
                    self.0.iter().format_with(" | ", |datum, f| {
                        match datum {
                            None => f(&"NULL"),
                            Some(scalar) => f(&format_args!("{}", scalar.to_text())),
                        }
                    })
                )
            }
        }
        D(self)
    }
}

impl<R: Row> RowExt for R {}

/// Forward the implementation of [`Row`] to the deref target.
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

        fn to_owned_row(&self) -> OwnedRow {
            (**self).to_owned_row()
        }

        fn value_serialize_into(&self, buf: impl bytes::BufMut) {
            (**self).value_serialize_into(buf)
        }

        fn value_serialize(&self) -> Vec<u8> {
            (**self).value_serialize()
        }

        fn memcmp_serialize_into(
            &self,
            serde: &$crate::util::ordered::OrderedRowSerde,
            buf: impl bytes::BufMut,
        ) {
            (**self).memcmp_serialize_into(serde, buf)
        }

        fn memcmp_serialize(&self, serde: &$crate::util::ordered::OrderedRowSerde) -> Vec<u8> {
            (**self).memcmp_serialize(serde)
        }

        fn hash<H: std::hash::BuildHasher>(&self, hash_builder: H) -> $crate::hash::HashCode {
            (**self).hash(hash_builder)
        }

        fn eq(this: &Self, other: impl Row) -> bool {
            Row::eq(&(**this), other)
        }
    };
}

impl<R: Row> Row for &R {
    type Iter<'a> = R::Iter<'a>
    where
        Self: 'a;

    deref_forward_row!();
}

impl<R: Row + Clone> Row for Cow<'_, R> {
    type Iter<'a> = R::Iter<'a>
    where
        Self: 'a;

    deref_forward_row!();

    // Manually implemented in case `R` has a more efficient implementation.
    fn into_owned_row(self) -> OwnedRow {
        self.into_owned().into_owned_row()
    }
}

impl<R: Row> Row for Box<R> {
    type Iter<'a> = R::Iter<'a>
    where
        Self: 'a;

    deref_forward_row!();

    // Manually implemented in case the `Cow` is `Owned` and `R` has a more efficient
    // implementation.
    fn into_owned_row(self) -> OwnedRow {
        (*self).into_owned_row()
    }
}

/// Implements [`Row`] for a slice of datums.
macro_rules! impl_slice_row {
    () => {
        #[inline]
        fn datum_at(&self, index: usize) -> DatumRef<'_> {
            self[index].to_datum_ref()
        }

        #[inline]
        unsafe fn datum_at_unchecked(&self, index: usize) -> DatumRef<'_> {
            self.get_unchecked(index).to_datum_ref()
        }

        #[inline]
        fn len(&self) -> usize {
            self.as_ref().len()
        }

        #[inline]
        fn iter(&self) -> Self::Iter<'_> {
            self.as_ref().iter().map(ToDatumRef::to_datum_ref)
        }
    };
}

type SliceIter<'a, D> = std::iter::Map<std::slice::Iter<'a, D>, fn(&'a D) -> DatumRef<'a>>;

impl<D: ToDatumRef> Row for &[D] {
    type Iter<'a> = SliceIter<'a, D>
    where
        Self: 'a;

    impl_slice_row!();
}

impl<D: ToDatumRef, const N: usize> Row for [D; N] {
    type Iter<'a> = SliceIter<'a, D>
    where
        Self: 'a;

    impl_slice_row!();
}

/// Implements [`Row`] for an optional row.
impl<R: Row> Row for Option<R> {
    type Iter<'a> = itertools::Either<R::Iter<'a>, <Empty as Row>::Iter<'a>>
    where
        Self: 'a;

    fn datum_at(&self, index: usize) -> DatumRef<'_> {
        match self {
            Some(row) => row.datum_at(index),
            None => EMPTY.datum_at(index),
        }
    }

    unsafe fn datum_at_unchecked(&self, index: usize) -> DatumRef<'_> {
        match self {
            Some(row) => row.datum_at_unchecked(index),
            None => EMPTY.datum_at_unchecked(index),
        }
    }

    fn len(&self) -> usize {
        match self {
            Some(row) => row.len(),
            None => 0,
        }
    }

    fn iter(&self) -> Self::Iter<'_> {
        match self {
            Some(row) => itertools::Either::Left(row.iter()),
            None => itertools::Either::Right(EMPTY.iter()),
        }
    }

    fn to_owned_row(&self) -> OwnedRow {
        match self {
            Some(row) => row.to_owned_row(),
            None => OwnedRow::new(Vec::new()),
        }
    }

    fn into_owned_row(self) -> OwnedRow {
        match self {
            Some(row) => row.into_owned_row(),
            None => OwnedRow::new(Vec::new()),
        }
    }

    fn value_serialize_into(&self, buf: impl BufMut) {
        if let Some(row) = self {
            row.value_serialize_into(buf);
        }
    }

    fn memcmp_serialize_into(&self, serde: &OrderedRowSerde, buf: impl BufMut) {
        if let Some(row) = self {
            row.memcmp_serialize_into(serde, buf);
        }
    }
}

mod chain;
mod compacted_row;
mod empty;
mod once;
mod owned_row;
mod project;
mod repeat_n;
pub use chain::Chain;
pub use compacted_row::CompactedRow;
pub use empty::{empty, Empty};
pub use once::{once, Once};
pub use owned_row::{AscentOwnedRow, OwnedRow, RowDeserializer};
pub use project::Project;
pub use repeat_n::{repeat_n, RepeatN};

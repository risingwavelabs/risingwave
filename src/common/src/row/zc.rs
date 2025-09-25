// Copyright 2025 RisingWave Labs
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

//! A row type that is more compact and has better locality, while still being able to borrow
//! `DatumRef` from it with no allocation (zero-copy).

use musli_zerocopy::buf::Load;
use musli_zerocopy::{Buf, OwnedBuf, Ref, ZeroCopy};
use static_assertions::const_assert_eq;

use crate::row::{OwnedRow, Row};
use crate::types::{Datum, DatumRef, F32, F64, ScalarRefImpl, Serial, Timestamptz, ToOwnedDatum};

/// The zero-copy representation of `Datum`.
// TODO(zc): variants that are commented out are not supported yet.
#[derive(Debug, Copy, Clone, ZeroCopy)]
#[repr(u8)]
enum ZcDatum {
    Null,
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Int256(Ref<[i128; 2]>),
    Float32(F32),
    Float64(F64),
    Utf8(Ref<str>),
    Bool(bool),
    // Decimal(crate::types::Decimal),
    // Interval(crate::types::Interval),
    // Date(crate::types::Date),
    // Time(crate::types::Time),
    // Timestamp(crate::types::Timestamp),
    Timestamptz(Timestamptz),
    // Jsonb(crate::types::JsonbRef<'scalar>),
    Serial(Serial),
    // Struct(crate::types::StructRef<'scalar>),
    // List(crate::types::ListRef<'scalar>),
    // Map(crate::types::MapRef<'scalar>),
    Vector(Ref<[F32]>),
    Bytea(Ref<[u8]>),

    /// For unsupported variants, we place the original [`Datum`] separately in an [`OwnedRow`]
    /// aside, and store its index here.
    Todo(usize),
}

// Demonstrate that each datum is 16 bytes.
const_assert_eq!(std::mem::size_of::<ZcDatum>(), 16);

impl ScalarRefImpl<'_> {
    /// Convert this `ScalarRefImpl` into `ZcDatum` by storing necessary data.
    ///
    /// - If it cannot be inlined, some data will be stored to `buf`.
    /// - If it's not supported yet, the owned datum will be stored to `todo`.
    fn store_to(self, buf: &mut OwnedBuf, todo: &mut Vec<Datum>) -> ZcDatum {
        match self {
            ScalarRefImpl::Int16(v) => ZcDatum::Int16(v),
            ScalarRefImpl::Int32(v) => ZcDatum::Int32(v),
            ScalarRefImpl::Int64(v) => ZcDatum::Int64(v),
            ScalarRefImpl::Int256(v) => ZcDatum::Int256(buf.store(&v.0.0)),
            ScalarRefImpl::Float32(v) => ZcDatum::Float32(v),
            ScalarRefImpl::Float64(v) => ZcDatum::Float64(v),
            ScalarRefImpl::Utf8(v) => ZcDatum::Utf8(buf.store_unsized(v)),
            ScalarRefImpl::Bool(v) => ZcDatum::Bool(v),
            ScalarRefImpl::Timestamptz(v) => ZcDatum::Timestamptz(v),
            ScalarRefImpl::Serial(v) => ZcDatum::Serial(v),
            ScalarRefImpl::Vector(v) => ZcDatum::Vector(buf.store_unsized(v.as_slice())),
            ScalarRefImpl::Bytea(v) => ZcDatum::Bytea(buf.store_unsized(v)),

            _ => {
                todo.push(self.to_owned_datum());
                ZcDatum::Todo(todo.len() - 1)
            }
        }
    }
}

impl ZcDatum {
    /// Convert this `ZcDatum` into `DatumRef` by loading necessary data.
    ///
    /// - If it's inlined, we load the data from `buf`.
    /// - If it's not supported yet, we directly load the datum from `todo`.
    fn load<'a>(self, buf: &'a Buf, todo: &'a impl Row) -> DatumRef<'a> {
        use crate::types::*;

        let scalar = match self {
            ZcDatum::Null => return None,
            ZcDatum::Todo(index) => return todo.datum_at(index),

            ZcDatum::Int16(v) => ScalarRefImpl::Int16(v),
            ZcDatum::Int32(v) => ScalarRefImpl::Int32(v),
            ZcDatum::Int64(v) => ScalarRefImpl::Int64(v),
            ZcDatum::Int256(v) => {
                ScalarRefImpl::Int256(Int256Ref::from_words(v.load(buf).unwrap()))
            }
            ZcDatum::Float32(v) => ScalarRefImpl::Float32(v),
            ZcDatum::Float64(v) => ScalarRefImpl::Float64(v),
            ZcDatum::Utf8(v) => ScalarRefImpl::Utf8(v.load(buf).unwrap()),
            ZcDatum::Bool(v) => ScalarRefImpl::Bool(v),
            ZcDatum::Timestamptz(v) => ScalarRefImpl::Timestamptz(v),
            ZcDatum::Serial(v) => ScalarRefImpl::Serial(v),
            ZcDatum::Vector(v) => {
                ScalarRefImpl::Vector(VectorRef::from_slice_unchecked(v.load(buf).unwrap()))
            }
            ZcDatum::Bytea(v) => ScalarRefImpl::Bytea(v.load(buf).unwrap()),
        };
        Some(scalar)
    }
}

/// The stored data for [`ZcRow`].
#[derive(Clone)]
struct ZcRowData {
    /// The data for supported datums.
    buf: OwnedBuf,
    /// The datums that are not supported to be zero-copy.
    todo: OwnedRow,
}

impl PartialEq for ZcRowData {
    fn eq(&self, other: &Self) -> bool {
        self.buf.as_slice() == other.buf.as_slice() && self.todo == other.todo
    }
}
impl Eq for ZcRowData {}

impl std::fmt::Debug for ZcRowData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ZcRowData")
            .field("buf", &self.buf.as_slice())
            .field("todo", &self.todo)
            .finish()
    }
}

/// A row type that is more compact and has better locality, while still being able to borrow
/// `DatumRef` from it with no allocation (zero-copy).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ZcRow {
    data: Box<ZcRowData>,
    /// The root metadata.
    zc: Ref<[ZcDatum]>,
}

/// Reference to a [`ZcRow`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ZcRowRef<'a> {
    data: &'a ZcRowData,
    /// The root metadata.
    zc: Ref<[ZcDatum]>,
}

impl ZcRow {
    /// Convert `self` into a reference.
    pub fn as_ref(&self) -> ZcRowRef<'_> {
        ZcRowRef {
            data: &self.data,
            zc: self.zc,
        }
    }
}

impl<'a> ZcRowRef<'a> {
    /// Load all `ZcDatum`s.
    fn zc_datums(self) -> &'a [ZcDatum] {
        self.zc.load(&self.data.buf).unwrap()
    }

    /// Convert the given `ZcDatum` into `DatumRef` by loading necessary data from `data`.
    fn load_datum(self, datum: ZcDatum) -> DatumRef<'a> {
        datum.load(&self.data.buf, &self.data.todo)
    }
}

/// A set of methods similar to `Row` trait but consuming `self`.
impl<'a> ZcRowRef<'a> {
    fn datum_at(self, i: usize) -> DatumRef<'a> {
        let zc = self.zc_datums()[i];
        self.load_datum(zc)
    }

    unsafe fn datum_at_unchecked(self, i: usize) -> DatumRef<'a> {
        let zc = *unsafe { self.zc_datums().get_unchecked(i) };
        self.load_datum(zc)
    }

    fn iter(self) -> impl Iterator<Item = DatumRef<'a>> {
        self.zc_datums().iter().map(move |zc| self.load_datum(*zc))
    }
}

/// Implement `Row` trait by dereferencing `self` and calling consuming methods.
impl<'a> Row for ZcRowRef<'a> {
    fn datum_at(&self, i: usize) -> DatumRef<'a> {
        ZcRowRef::datum_at(*self, i)
    }

    unsafe fn datum_at_unchecked(&self, i: usize) -> DatumRef<'a> {
        unsafe { ZcRowRef::datum_at_unchecked(*self, i) }
    }

    fn len(&self) -> usize {
        self.zc.len()
    }

    fn iter(&self) -> impl Iterator<Item = DatumRef<'_>> {
        ZcRowRef::iter(*self)
    }
}

/// Forward the implementation to `ZcRowRef`.
impl Row for ZcRow {
    fn datum_at(&self, i: usize) -> DatumRef<'_> {
        self.as_ref().datum_at(i)
    }

    unsafe fn datum_at_unchecked(&self, i: usize) -> DatumRef<'_> {
        unsafe { self.as_ref().datum_at_unchecked(i) }
    }

    fn len(&self) -> usize {
        self.as_ref().len()
    }

    fn iter(&self) -> impl Iterator<Item = DatumRef<'_>> {
        self.as_ref().iter()
    }
}

/// Store the given row into `buf` and `todo` by storing each datum, and return the root metadata
/// for all datums.
fn row_store_to<R: Row>(row: R, buf: &mut OwnedBuf, todo: &mut Vec<Datum>) -> Ref<[ZcDatum]> {
    let len = row.len();
    let mut zcs = Vec::with_capacity(len);

    for datum in row.iter() {
        let zc = match datum {
            Some(scalar) => scalar.store_to(buf, todo),
            None => ZcDatum::Null,
        };
        zcs.push(zc);
    }

    buf.store_slice(&zcs)
}

#[easy_ext::ext(RowZcEncodeExt)]
impl<R: Row> R {
    /// Convert the given row into a [`ZcRow`].
    pub fn zc_encode(&self) -> ZcRow {
        let mut buf = OwnedBuf::new();
        let mut todo = Vec::new(); // TODO: reserve first
        let zc = row_store_to(self, &mut buf, &mut todo);

        ZcRow {
            data: Box::new(ZcRowData {
                buf,
                todo: OwnedRow::new(todo),
            }),
            zc,
        }
    }
}

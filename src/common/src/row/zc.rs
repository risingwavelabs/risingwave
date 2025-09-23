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

use educe::Educe;
use musli_zerocopy::buf::Load;
use musli_zerocopy::{Buf, OwnedBuf, Ref, ZeroCopy};

use crate::row::{OwnedRow, Row};
use crate::types::{Datum, DatumRef, ScalarRefImpl, ToOwnedDatum as _};

#[derive(Debug, Copy, Clone, ZeroCopy)]
#[repr(u8)]
enum ZcDatum {
    Null,
    Int16(i16),
    Int32(i32),
    Int64(i64),
    // Int256(Int256Ref<'scalar>),
    Float32(f32),
    Float64(f64),
    Utf8(Ref<str>),
    Bool(bool),
    // Decimal(crate::types::Decimal),
    // Interval(crate::types::Interval),
    // Date(crate::types::Date),
    // Time(crate::types::Time),
    // Timestamp(crate::types::Timestamp),
    // Timestamptz(crate::types::Timestamptz),
    // Jsonb(crate::types::JsonbRef<'scalar>),
    // Serial(crate::types::Serial),
    // Struct(crate::types::StructRef<'scalar>),
    // List(crate::types::ListRef<'scalar>),
    // Map(crate::types::MapRef<'scalar>),
    // Vector(crate::types::VectorRef<'scalar>),
    Bytea(Ref<[u8]>),
    Todo(usize),
}

impl ScalarRefImpl<'_> {
    fn encode_to(self, buf: &mut OwnedBuf, todo: &mut Vec<Datum>) -> ZcDatum {
        match self {
            ScalarRefImpl::Int16(v) => ZcDatum::Int16(v),
            ScalarRefImpl::Int32(v) => ZcDatum::Int32(v),
            ScalarRefImpl::Int64(v) => ZcDatum::Int64(v),
            ScalarRefImpl::Float32(v) => ZcDatum::Float32(v.into_inner()),
            ScalarRefImpl::Float64(v) => ZcDatum::Float64(v.into_inner()),
            ScalarRefImpl::Utf8(v) => ZcDatum::Utf8(buf.store_unsized(v)),
            ScalarRefImpl::Bool(v) => ZcDatum::Bool(v),
            ScalarRefImpl::Bytea(v) => ZcDatum::Bytea(buf.store_unsized(v)),

            _ => {
                todo.push(self.to_owned_datum());
                ZcDatum::Todo(todo.len() - 1)
            }
        }
    }
}

impl ZcDatum {
    fn load<'a>(self, buf: &'a Buf, todo: &'a impl Row) -> DatumRef<'a> {
        let scalar = match self {
            ZcDatum::Null => return None,
            ZcDatum::Todo(index) => return todo.datum_at(index),

            ZcDatum::Int16(v) => ScalarRefImpl::Int16(v),
            ZcDatum::Int32(v) => ScalarRefImpl::Int32(v),
            ZcDatum::Int64(v) => ScalarRefImpl::Int64(v),
            ZcDatum::Float32(v) => ScalarRefImpl::Float32(v.into()),
            ZcDatum::Float64(v) => ScalarRefImpl::Float64(v.into()),
            ZcDatum::Utf8(v) => ScalarRefImpl::Utf8(v.load(buf).unwrap()),
            ZcDatum::Bool(v) => ScalarRefImpl::Bool(v),
            ZcDatum::Bytea(v) => ScalarRefImpl::Bytea(v.load(buf).unwrap()),
        };
        Some(scalar)
    }
}

#[derive(Educe, Clone)]
#[educe(Debug)]
struct ZcRowData {
    #[educe(Debug(ignore))]
    buf: OwnedBuf,
    remaining: OwnedRow,
}

impl PartialEq for ZcRowData {
    fn eq(&self, other: &Self) -> bool {
        self.buf.as_slice() == other.buf.as_slice() && self.remaining == other.remaining
    }
}
impl Eq for ZcRowData {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ZcRow {
    data: Box<ZcRowData>,
    zc: Ref<[ZcDatum]>,
}

impl ZcRow {
    fn zc_datums(&self) -> &[ZcDatum] {
        self.zc.load(&self.data.buf).unwrap()
    }

    /// Load `ZcDatum` into `DatumRef`.
    fn load_datum(&self, datum: ZcDatum) -> DatumRef<'_> {
        datum.load(&self.data.buf, &self.data.remaining)
    }
}

impl Row for ZcRow {
    fn datum_at(&self, i: usize) -> DatumRef<'_> {
        let zc = self.zc_datums()[i];
        self.load_datum(zc)
    }

    unsafe fn datum_at_unchecked(&self, i: usize) -> crate::types::DatumRef<'_> {
        let zc = *unsafe { self.zc_datums().get_unchecked(i) };
        self.load_datum(zc)
    }

    fn len(&self) -> usize {
        self.zc.len()
    }

    fn iter(&self) -> impl Iterator<Item = crate::types::DatumRef<'_>> {
        self.zc_datums().iter().map(|zc| self.load_datum(*zc))
    }
}

fn row_encode_to<R: Row>(row: R, buf: &mut OwnedBuf, todo: &mut Vec<Datum>) -> Ref<[ZcDatum]> {
    let len = row.len();
    let mut zcs = Vec::with_capacity(len);

    for datum in row.iter() {
        let zc = match datum {
            Some(scalar) => scalar.encode_to(buf, todo),
            None => ZcDatum::Null,
        };
        zcs.push(zc);
    }

    buf.store_slice(&zcs)
}

#[easy_ext::ext(RowZcEncodeExt)]
impl<R: Row> R {
    pub fn encode(&self) -> ZcRow {
        let mut buf = OwnedBuf::new();
        let mut todo = Vec::new(); // TODO: reserve first
        let zc = row_encode_to(self, &mut buf, &mut todo);

        ZcRow {
            data: Box::new(ZcRowData {
                buf,
                remaining: OwnedRow::new(todo),
            }),
            zc,
        }
    }
}

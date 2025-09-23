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
use crate::types::{DatumRef, ScalarRefImpl};

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

#[derive(Debug, Clone)]
struct Todo;

impl ScalarRefImpl<'_> {
    fn encode_to(self, buf: &mut OwnedBuf) -> Result<ZcDatum, Todo> {
        todo!()
    }
}

impl ZcDatum {
    fn load(self, buf: &Buf) -> DatumRef<'_> {
        todo!()
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
    /// Load `ZcDatum` into `DatumRef`.
    fn load_datum(&self, datum: ZcDatum) -> DatumRef<'_> {
        match datum {
            ZcDatum::Todo(j) => self.data.remaining.datum_at(j),
            _ => datum.load(&self.data.buf),
        }
    }
}

impl Row for ZcRow {
    fn datum_at(&self, i: usize) -> DatumRef<'_> {
        let zc = self.zc.load(&self.data.buf).unwrap()[i];
        self.load_datum(zc)
    }

    unsafe fn datum_at_unchecked(&self, i: usize) -> crate::types::DatumRef<'_> {
        let zc = *unsafe { self.zc.load(&self.data.buf).unwrap().get_unchecked(i) };
        self.load_datum(zc)
    }

    fn len(&self) -> usize {
        self.zc.len()
    }

    fn iter(&self) -> impl Iterator<Item = crate::types::DatumRef<'_>> {
        let zcs = self.zc.load(&self.data.buf).unwrap();
        zcs.iter().map(|zc| self.load_datum(*zc))
    }
}

#[easy_ext::ext(RowZcEncodeExt)]
impl<R: Row> R {
    pub fn encode(&self) -> ZcRow {
        todo!()
    }
}

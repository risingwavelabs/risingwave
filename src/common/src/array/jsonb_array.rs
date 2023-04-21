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

use std::fmt;
use std::hash::Hash;
use std::mem::size_of;

use postgres_types::{FromSql as _, ToSql as _, Type};
use serde_json::Value;

use super::{Array, ArrayBuilder};
use crate::buffer::{Bitmap, BitmapBuilder};
use crate::estimate_size::EstimateSize;
use crate::types::{Scalar, ScalarRef};
use crate::util::iter_util::ZipEqFast;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JsonbVal(Box<Value>); // The `Box` is just to keep `size_of::<ScalarImpl>` smaller.

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct JsonbRef<'a>(&'a Value);

/// The display of `JsonbVal` is pg-compatible format which has slightly different from
/// `serde_json::Value`.
impl fmt::Display for JsonbVal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        crate::types::to_text::ToText::write(&self.as_scalar_ref(), f)
    }
}

/// The display of `JsonbRef` is pg-compatible format which has slightly different from
/// `serde_json::Value`.
impl fmt::Display for JsonbRef<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        crate::types::to_text::ToText::write(self, f)
    }
}

impl Scalar for JsonbVal {
    type ScalarRefType<'a> = JsonbRef<'a>;

    fn as_scalar_ref(&self) -> Self::ScalarRefType<'_> {
        JsonbRef(self.0.as_ref())
    }
}

impl<'a> ScalarRef<'a> for JsonbRef<'a> {
    type ScalarType = JsonbVal;

    fn to_owned_scalar(&self) -> Self::ScalarType {
        JsonbVal(self.0.clone().into())
    }

    fn hash_scalar<H: std::hash::Hasher>(&self, state: &mut H) {
        self.hash(state)
    }
}

impl Hash for JsonbRef<'_> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        // We do not intend to support hashing `jsonb` type.
        // Before #7981 is done, we do not panic but just hash its string representation.
        // Note that `serde_json` without feature `preserve_order` uses `BTreeMap` for json object.
        // So its string form always have keys sorted.
        self.0.to_string().hash(state)
    }
}

impl Hash for JsonbVal {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.to_string().hash(state)
    }
}

impl PartialOrd for JsonbVal {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for JsonbVal {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.as_scalar_ref().cmp(&other.as_scalar_ref())
    }
}

impl PartialOrd for JsonbRef<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for JsonbRef<'_> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // We do not intend to support ordering `jsonb` type.
        // Before #7981 is done, we do not panic but just compare its string representation.
        // Note that `serde_json` without feature `preserve_order` uses `BTreeMap` for json object.
        // So its string form always have keys sorted.
        //
        // In PostgreSQL, Object > Array > Boolean > Number > String > Null.
        // But here we have Object > true > Null > false > Array > Number > String.
        // Because in ascii: `{` > `t` > `n` > `f` > `[` > `9` `-` > `"`.
        //
        // This is just to keep consistent with the memcomparable encoding, which uses string form.
        // If we implemented the same typed comparison as PostgreSQL, we would need a corresponding
        // memcomparable encoding for it.
        self.0.to_string().cmp(&other.0.to_string())
    }
}

impl crate::types::to_text::ToText for JsonbRef<'_> {
    fn write<W: std::fmt::Write>(&self, f: &mut W) -> std::fmt::Result {
        struct FmtToIoUnchecked<F>(F);
        impl<F: std::fmt::Write> std::io::Write for FmtToIoUnchecked<F> {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                let s = unsafe { std::str::from_utf8_unchecked(buf) };
                self.0.write_str(s).map_err(|_| std::io::ErrorKind::Other)?;
                Ok(buf.len())
            }

            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        // Use custom [`ToTextFormatter`] to serialize. If we are okay with the default, this can be
        // just `write!(f, "{}", self.0)`
        use serde::Serialize as _;
        let mut ser =
            serde_json::ser::Serializer::with_formatter(FmtToIoUnchecked(f), ToTextFormatter);
        self.0.serialize(&mut ser).map_err(|_| std::fmt::Error)
    }

    fn write_with_type<W: std::fmt::Write>(
        &self,
        _ty: &crate::types::DataType,
        f: &mut W,
    ) -> std::fmt::Result {
        self.write(f)
    }
}

impl crate::types::to_binary::ToBinary for JsonbRef<'_> {
    fn to_binary_with_type(
        &self,
        _ty: &crate::types::DataType,
    ) -> crate::error::Result<Option<bytes::Bytes>> {
        let mut output = bytes::BytesMut::new();
        self.0.to_sql(&Type::JSONB, &mut output).unwrap();
        Ok(Some(output.freeze()))
    }
}

impl std::str::FromStr for JsonbVal {
    type Err = <Value as std::str::FromStr>::Err;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let v: Value = s.parse()?;
        Ok(Self(v.into()))
    }
}

impl JsonbVal {
    /// Avoid this function (or `impl From<Value>`) which is leak of abstraction.
    /// In most cases you would be using `JsonbRef`.
    pub fn from_serde(v: Value) -> Self {
        Self(v.into())
    }

    /// Constructs a value without specific meaning. Usually used as a lightweight placeholder.
    pub fn dummy() -> Self {
        Self(Value::Null.into())
    }

    pub fn memcmp_deserialize(
        deserializer: &mut memcomparable::Deserializer<impl bytes::Buf>,
    ) -> memcomparable::Result<Self> {
        let v: Value = <String as serde::Deserialize>::deserialize(deserializer)?
            .parse()
            .map_err(|_| memcomparable::Error::Message("invalid json".into()))?;
        Ok(Self(v.into()))
    }

    pub fn value_deserialize(buf: &[u8]) -> Option<Self> {
        let v = Value::from_sql(&Type::JSONB, buf).ok()?;
        Some(Self(v.into()))
    }

    pub fn take(mut self) -> Value {
        self.0.take()
    }
}

impl From<Value> for JsonbVal {
    fn from(v: Value) -> Self {
        Self(v.into())
    }
}

impl JsonbRef<'_> {
    pub fn memcmp_serialize(
        &self,
        serializer: &mut memcomparable::Serializer<impl bytes::BufMut>,
    ) -> memcomparable::Result<()> {
        // As mentioned with `cmp`, this implementation is not intended to be used.
        // But before #7981 is done, we do not want to `panic` here.
        let s = self.0.to_string();
        serde::Serialize::serialize(&s, serializer)
    }

    pub fn value_serialize(&self) -> Vec<u8> {
        // Reuse the pgwire "BINARY" encoding for jsonb type.
        // It is not truly binary, but one byte of version `1u8` followed by string form.
        // This version number helps us maintain compatibility when we switch to more efficient
        // encoding later.
        let mut output = bytes::BytesMut::new();
        self.0.to_sql(&Type::JSONB, &mut output).unwrap();
        output.freeze().into()
    }

    pub fn is_jsonb_null(&self) -> bool {
        matches!(self.0, Value::Null)
    }

    pub fn type_name(&self) -> &'static str {
        match self.0 {
            Value::Null => "null",
            Value::Bool(_) => "boolean",
            Value::Number(_) => "number",
            Value::String(_) => "string",
            Value::Array(_) => "array",
            Value::Object(_) => "object",
        }
    }

    pub fn array_len(&self) -> Result<usize, String> {
        match self.0 {
            Value::Array(v) => Ok(v.len()),
            _ => Err(format!(
                "cannot get array length of a jsonb {}",
                self.type_name()
            )),
        }
    }

    pub fn as_bool(&self) -> Result<bool, String> {
        match self.0 {
            Value::Bool(v) => Ok(*v),
            _ => Err(format!(
                "cannot cast jsonb {} to type boolean",
                self.type_name()
            )),
        }
    }

    /// Attempt to read jsonb as a JSON number.
    ///
    /// According to RFC 8259, only number within IEEE 754 binary64 (double precision) has good
    /// interoperability. We do not support arbitrary precision like PostgreSQL `numeric` right now.
    pub fn as_number(&self) -> Result<f64, String> {
        match self.0 {
            Value::Number(v) => v.as_f64().ok_or_else(|| "jsonb number out of range".into()),
            _ => Err(format!(
                "cannot cast jsonb {} to type number",
                self.type_name()
            )),
        }
    }

    /// This is part of the `->>` or `#>>` syntax to access a child as string.
    ///
    /// * It is not `as_str`, because there is no runtime error when the jsonb type is not string.
    /// * It is not same as [`Display`] or [`ToText`] (cast to string) in the following 2 cases:
    ///   * Jsonb null is displayed as 4-letter `null` but treated as sql null here.
    ///       * This function writes nothing and the caller is responsible for checking
    ///         [`is_jsonb_null`] to differentiate it from an empty string.
    ///   * Jsonb string is displayed with quotes but treated as its inner value here.
    pub fn force_str<W: std::fmt::Write>(&self, writer: &mut W) -> std::fmt::Result {
        match self.0 {
            Value::String(v) => writer.write_str(v),
            Value::Null => Ok(()),
            Value::Bool(_) | Value::Number(_) | Value::Array(_) | Value::Object(_) => {
                use crate::types::to_text::ToText as _;
                self.write_with_type(&crate::types::DataType::Jsonb, writer)
            }
        }
    }

    pub fn access_object_field(&self, field: &str) -> Option<Self> {
        self.0.get(field).map(Self)
    }

    pub fn access_array_element(&self, idx: usize) -> Option<Self> {
        self.0.get(idx).map(Self)
    }
}

impl FromIterator<Option<JsonbVal>> for JsonbArray {
    fn from_iter<I: IntoIterator<Item = Option<JsonbVal>>>(iter: I) -> Self {
        let iter = iter.into_iter();
        let mut builder = <Self as Array>::Builder::new(iter.size_hint().0);
        for i in iter {
            match i {
                Some(x) => builder.append_move(x),
                None => builder.append(None),
            }
        }
        builder.finish()
    }
}

impl FromIterator<JsonbVal> for JsonbArray {
    fn from_iter<I: IntoIterator<Item = JsonbVal>>(iter: I) -> Self {
        iter.into_iter().map(Some).collect()
    }
}

#[derive(Debug)]
pub struct JsonbArrayBuilder {
    bitmap: BitmapBuilder,
    data: Vec<Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JsonbArray {
    bitmap: Bitmap,
    data: Vec<Value>,
}

impl ArrayBuilder for JsonbArrayBuilder {
    type ArrayType = JsonbArray;

    fn with_meta(capacity: usize, _meta: super::ArrayMeta) -> Self {
        Self {
            bitmap: BitmapBuilder::with_capacity(capacity),
            data: Vec::with_capacity(capacity),
        }
    }

    fn append_n(&mut self, n: usize, value: Option<<Self::ArrayType as Array>::RefItem<'_>>) {
        match value {
            Some(x) => {
                self.bitmap.append_n(n, true);
                self.data
                    .extend(std::iter::repeat(x).take(n).map(|x| x.0.clone()));
            }
            None => {
                self.bitmap.append_n(n, false);
                self.data
                    .extend(std::iter::repeat(*JsonbVal::dummy().0).take(n));
            }
        }
    }

    fn append_array(&mut self, other: &Self::ArrayType) {
        for bit in other.bitmap.iter() {
            self.bitmap.append(bit);
        }
        self.data.extend_from_slice(&other.data);
    }

    fn pop(&mut self) -> Option<()> {
        self.data.pop().map(|_| self.bitmap.pop().unwrap())
    }

    fn finish(self) -> Self::ArrayType {
        Self::ArrayType {
            bitmap: self.bitmap.finish(),
            data: self.data,
        }
    }
}

impl JsonbArrayBuilder {
    pub fn append_move(&mut self, value: JsonbVal) {
        self.bitmap.append(true);
        self.data.push(*value.0);
    }
}

impl Array for JsonbArray {
    type Builder = JsonbArrayBuilder;
    type OwnedItem = JsonbVal;
    type RefItem<'a> = JsonbRef<'a>;

    unsafe fn raw_value_at_unchecked(&self, idx: usize) -> Self::RefItem<'_> {
        JsonbRef(self.data.get_unchecked(idx))
    }

    fn len(&self) -> usize {
        self.data.len()
    }

    fn to_protobuf(&self) -> super::PbArray {
        // The memory layout contains `serde_json::Value` trees, but in protobuf we transmit this as
        // variable length bytes in value encoding. That is, one buffer of length n+1 containing
        // start and end offsets into the 2nd buffer containing all value bytes concatenated.

        use risingwave_pb::common::buffer::CompressionType;
        use risingwave_pb::common::Buffer;

        let mut offset_buffer =
            Vec::<u8>::with_capacity((1 + self.data.len()) * std::mem::size_of::<u64>());
        let mut data_buffer = Vec::<u8>::with_capacity(self.data.len());

        let mut offset = 0;
        for (v, not_null) in self.data.iter().zip_eq_fast(self.null_bitmap().iter()) {
            if !not_null {
                continue;
            }
            let d = JsonbRef(v).value_serialize();
            offset_buffer.extend_from_slice(&(offset as u64).to_be_bytes());
            data_buffer.extend_from_slice(&d);
            offset += d.len();
        }
        offset_buffer.extend_from_slice(&(offset as u64).to_be_bytes());

        let values = vec![
            Buffer {
                compression: CompressionType::None as i32,
                body: offset_buffer,
            },
            Buffer {
                compression: CompressionType::None as i32,
                body: data_buffer,
            },
        ];

        let null_bitmap = self.null_bitmap().to_protobuf();
        super::PbArray {
            null_bitmap: Some(null_bitmap),
            values,
            array_type: super::PbArrayType::Jsonb as i32,
            struct_array_data: None,
            list_array_data: None,
        }
    }

    fn null_bitmap(&self) -> &Bitmap {
        &self.bitmap
    }

    fn into_null_bitmap(self) -> Bitmap {
        self.bitmap
    }

    fn set_bitmap(&mut self, bitmap: Bitmap) {
        self.bitmap = bitmap;
    }

    fn create_builder(&self, capacity: usize) -> super::ArrayBuilderImpl {
        let array_builder = Self::Builder::new(capacity);
        super::ArrayBuilderImpl::Jsonb(array_builder)
    }
}

/// A custom implementation for [`serde_json::ser::Formatter`] to match PostgreSQL, which adds extra
/// space after `,` and `:` in array and object.
struct ToTextFormatter;

impl serde_json::ser::Formatter for ToTextFormatter {
    fn begin_array_value<W>(&mut self, writer: &mut W, first: bool) -> std::io::Result<()>
    where
        W: ?Sized + std::io::Write,
    {
        if first {
            Ok(())
        } else {
            writer.write_all(b", ")
        }
    }

    fn begin_object_key<W>(&mut self, writer: &mut W, first: bool) -> std::io::Result<()>
    where
        W: ?Sized + std::io::Write,
    {
        if first {
            Ok(())
        } else {
            writer.write_all(b", ")
        }
    }

    fn begin_object_value<W>(&mut self, writer: &mut W) -> std::io::Result<()>
    where
        W: ?Sized + std::io::Write,
    {
        writer.write_all(b": ")
    }
}

// TODO: We need to fix this later.
impl EstimateSize for JsonbArray {
    fn estimated_heap_size(&self) -> usize {
        self.bitmap.estimated_heap_size() + self.data.capacity() * size_of::<Value>()
    }
}

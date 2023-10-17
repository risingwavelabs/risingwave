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

use bytes::Buf;
use jsonbb::{Value, ValueRef};

use crate::estimate_size::EstimateSize;
use crate::types::{Scalar, ScalarRef};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct JsonbVal(pub(crate) Value);

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct JsonbRef<'a>(pub(crate) ValueRef<'a>);

impl EstimateSize for JsonbVal {
    fn estimated_heap_size(&self) -> usize {
        self.0.capacity()
    }
}

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
        Ok(Some(self.value_serialize().into()))
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
    /// Constructs a value without specific meaning. Usually used as a lightweight placeholder.
    pub fn dummy() -> Self {
        Self(Value::null())
    }

    /// Deserialize from a memcomparable encoding.
    pub fn memcmp_deserialize(
        deserializer: &mut memcomparable::Deserializer<impl bytes::Buf>,
    ) -> memcomparable::Result<Self> {
        let v: Value = <String as serde::Deserialize>::deserialize(deserializer)?
            .parse()
            .map_err(|_| memcomparable::Error::Message("invalid json".into()))?;
        Ok(Self(v.into()))
    }

    /// Deserialize from a pgwire "BINARY" encoding.
    pub fn value_deserialize(mut buf: &[u8]) -> Option<Self> {
        if buf.is_empty() || buf.get_u8() != 1 {
            return None;
        }
        Value::from_text(buf).ok().map(Self)
    }

    /// Convert the value to a [`serde_json::Value`].
    pub fn take(self) -> serde_json::Value {
        self.0.into()
    }
}

impl From<serde_json::Value> for JsonbVal {
    fn from(v: serde_json::Value) -> Self {
        Self(v.into())
    }
}

impl<'a> JsonbRef<'a> {
    pub fn memcmp_serialize(
        &self,
        serializer: &mut memcomparable::Serializer<impl bytes::BufMut>,
    ) -> memcomparable::Result<()> {
        // As mentioned with `cmp`, this implementation is not intended to be used.
        // But before #7981 is done, we do not want to `panic` here.
        let s = self.0.to_string();
        serde::Serialize::serialize(&s, serializer)
    }

    /// Serialize to a pgwire "BINARY" encoding.
    pub fn value_serialize(&self) -> Vec<u8> {
        use std::io::Write;
        // Reuse the pgwire "BINARY" encoding for jsonb type.
        // It is not truly binary, but one byte of version `1u8` followed by string form.
        // This version number helps us maintain compatibility when we switch to more efficient
        // encoding later.
        let mut buf = Vec::with_capacity(self.0.capacity());
        buf.push(1);
        write!(&mut buf, "{}", self.0).unwrap();
        buf
    }

    /// Returns true if this is a jsonb `null`.
    pub fn is_null(&self) -> bool {
        self.0.as_null().is_some()
    }

    /// Returns the type name of this jsonb.
    ///
    /// Possible values are: `null`, `boolean`, `number`, `string`, `array`, `object`.
    pub fn type_name(&self) -> &'static str {
        match self.0 {
            ValueRef::Null => "null",
            ValueRef::Bool(_) => "boolean",
            ValueRef::Number(_) => "number",
            ValueRef::String(_) => "string",
            ValueRef::Array(_) => "array",
            ValueRef::Object(_) => "object",
        }
    }

    /// Returns the length of this json array.
    pub fn array_len(&self) -> Result<usize, String> {
        let array = self
            .0
            .as_array()
            .ok_or_else(|| format!("cannot get array length of a jsonb {}", self.type_name()))?;
        Ok(array.len())
    }

    /// If the JSON is a boolean, returns the associated bool.
    pub fn as_bool(&self) -> Result<bool, String> {
        self.0
            .as_bool()
            .ok_or_else(|| format!("cannot cast jsonb {} to type boolean", self.type_name()))
    }

    /// Attempt to read jsonb as a JSON number.
    ///
    /// According to RFC 8259, only number within IEEE 754 binary64 (double precision) has good
    /// interoperability. We do not support arbitrary precision like PostgreSQL `numeric` right now.
    pub fn as_number(&self) -> Result<f64, String> {
        self.0
            .as_number()
            .ok_or_else(|| format!("cannot cast jsonb {} to type number", self.type_name()))?
            .as_f64()
            .ok_or_else(|| "jsonb number out of range".into())
    }

    /// This is part of the `->>` or `#>>` syntax to access a child as string.
    ///
    /// * It is not `as_str`, because there is no runtime error when the jsonb type is not string.
    /// * It is not same as [`std::fmt::Display`] or [`super::ToText`] (cast to string) in the
    ///   following 2 cases:
    ///   * Jsonb null is displayed as 4-letter `null` but treated as sql null here.
    ///       * This function writes nothing and the caller is responsible for checking
    ///         [`Self::is_jsonb_null`] to differentiate it from an empty string.
    ///   * Jsonb string is displayed with quotes but treated as its inner value here.
    pub fn force_str<W: std::fmt::Write>(&self, writer: &mut W) -> std::fmt::Result {
        match self.0 {
            ValueRef::String(v) => writer.write_str(v),
            ValueRef::Null => Ok(()),
            ValueRef::Bool(_) | ValueRef::Number(_) | ValueRef::Array(_) | ValueRef::Object(_) => {
                use crate::types::to_text::ToText as _;
                self.write_with_type(&crate::types::DataType::Jsonb, writer)
            }
        }
    }

    pub fn force_string(&self) -> String {
        let mut s = String::new();
        self.force_str(&mut s).unwrap();
        s
    }

    pub fn access_object_field(&self, field: &str) -> Option<Self> {
        self.0.get(field).map(Self)
    }

    pub fn access_array_element(&self, idx: usize) -> Option<Self> {
        self.0.get(idx).map(Self)
    }

    /// Returns an iterator over the elements if this is an array.
    pub fn array_elements(self) -> Result<impl Iterator<Item = JsonbRef<'a>>, String> {
        let array = self
            .0
            .as_array()
            .ok_or_else(|| format!("cannot extract elements from a jsonb {}", self.type_name()))?;
        Ok(array.iter().map(Self))
    }

    /// Returns an iterator over the keys if this is an object.
    pub fn object_keys(self) -> Result<impl Iterator<Item = &'a str>, String> {
        let object = self.0.as_object().ok_or_else(|| {
            format!(
                "cannot call jsonb_object_keys on a jsonb {}",
                self.type_name()
            )
        })?;
        Ok(object.keys())
    }

    /// Returns an iterator over the key-value pairs if this is an object.
    pub fn object_key_values(
        self,
    ) -> Result<impl Iterator<Item = (&'a str, JsonbRef<'a>)>, String> {
        let object = self
            .0
            .as_object()
            .ok_or_else(|| format!("cannot deconstruct a jsonb {}", self.type_name()))?;
        Ok(object.iter().map(|(k, v)| (k, Self(v))))
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

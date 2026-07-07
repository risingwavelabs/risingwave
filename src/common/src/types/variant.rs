// Copyright 2026 RisingWave Labs
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

use std::collections::BTreeSet;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::str::FromStr;

use anyhow::{Context, bail};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use itertools::Itertools;
use memcomparable::{Deserializer, Serializer};
use parquet_variant::{
    ObjectFieldBuilder, Variant as ParquetVariant, VariantBuilder, VariantBuilderExt,
    VariantDecimal16,
};
use parquet_variant_json::VariantToJson;
use postgres_types::{FromSql, IsNull, ToSql, Type, accepts, to_sql_checked};
use risingwave_common_estimate_size::EstimateSize;
use serde::{Deserialize, Serialize};

use super::jsonb::{JsonbRef, JsonbVal};
use super::to_binary::ToBinary;
use super::to_text::ToText;
use super::{
    DataType, Decimal, Scalar, ScalarRef, ScalarRefImpl, StructType, scalar_ref_type_match,
};
use crate::util::iter_util::ZipEqFast;

const METADATA_LEN_SIZE: usize = size_of::<u32>();
const ENCODING_VERSION_LEN: usize = size_of::<u8>();
const VARIANT_ENCODING_VERSION: u8 = 1;

/// Owned value of the `variant` type.
///
/// The inner bytes are a RisingWave format tag followed by the Apache Parquet / Iceberg Variant
/// `metadata` and `value` sections. This is the first RisingWave `variant` encoding, so all
/// internal bytes must use the current tagged format.
#[derive(Debug, Clone)]
pub struct VariantVal {
    data: Box<[u8]>,
}

/// Borrowed value of the `variant` type.
#[derive(Debug, Copy, Clone)]
pub struct VariantRef<'a> {
    data: &'a [u8],
}

impl EstimateSize for VariantVal {
    fn estimated_heap_size(&self) -> usize {
        self.data.len()
    }
}

impl fmt::Display for VariantVal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.as_scalar_ref().write(f)
    }
}

impl fmt::Display for VariantRef<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.write(f)
    }
}

impl Scalar for VariantVal {
    type ScalarRefType<'a> = VariantRef<'a>;

    fn as_scalar_ref(&self) -> Self::ScalarRefType<'_> {
        VariantRef { data: &self.data }
    }
}

impl<'a> ScalarRef<'a> for VariantRef<'a> {
    type ScalarType = VariantVal;

    fn to_owned_scalar(&self) -> Self::ScalarType {
        VariantVal {
            data: self.data.into(),
        }
    }

    fn hash_scalar<H: std::hash::Hasher>(&self, state: &mut H) {
        self.hash(state);
    }
}

impl PartialEq for VariantVal {
    fn eq(&self, other: &Self) -> bool {
        self.as_scalar_ref() == other.as_scalar_ref()
    }
}

impl Eq for VariantVal {}

impl Hash for VariantVal {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_scalar_ref().hash(state);
    }
}

impl PartialEq for VariantRef<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.data == other.data
    }
}

impl Eq for VariantRef<'_> {}

impl Hash for VariantRef<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.data.hash(state);
    }
}

impl PartialOrd for VariantVal {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for VariantVal {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.as_scalar_ref().cmp(&other.as_scalar_ref())
    }
}

impl PartialOrd for VariantRef<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for VariantRef<'_> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Must agree with `memcmp_serialize`, which encodes the whole tagged buffer.
        self.data.cmp(other.data)
    }
}

impl ToText for VariantRef<'_> {
    fn write<W: fmt::Write>(&self, f: &mut W) -> fmt::Result {
        let json = self
            .parquet_variant()
            .to_json_string()
            .map_err(|_| fmt::Error)?;
        f.write_str(&json)
    }

    fn write_with_type<W: fmt::Write>(&self, _ty: &DataType, f: &mut W) -> fmt::Result {
        self.write(f)
    }
}

impl ToBinary for VariantRef<'_> {
    fn to_binary_with_type(&self, _ty: &DataType) -> super::to_binary::Result<Bytes> {
        Ok(Bytes::from(self.value_serialize()))
    }
}

impl FromStr for VariantVal {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let json = serde_json::Value::from_str(s)?;
        Self::from_json_value(&json)
    }
}

impl VariantVal {
    pub fn null() -> Self {
        Self::from_parquet_variant(ParquetVariant::Null).expect("null variant should encode")
    }

    pub fn from_parts(metadata: &[u8], value: &[u8]) -> anyhow::Result<Self> {
        let variant =
            ParquetVariant::try_new(metadata, value).context("invalid variant encoding")?;
        Self::from_parquet_variant(variant)
    }

    fn from_canonical_parts(metadata: &[u8], value: &[u8]) -> Self {
        // Builder output is trusted; untrusted bytes are validated in `from_parts` and friends.
        debug_assert!(
            ParquetVariant::try_new(metadata, value).is_ok(),
            "canonical variant parts should be valid"
        );
        let metadata_len =
            u32::try_from(metadata.len()).expect("variant metadata exceeds u32::MAX bytes");
        let mut data = Vec::with_capacity(
            ENCODING_VERSION_LEN + METADATA_LEN_SIZE + metadata.len() + value.len(),
        );
        data.put_u8(VARIANT_ENCODING_VERSION);
        data.put_u32_le(metadata_len);
        data.extend_from_slice(metadata);
        data.extend_from_slice(value);
        Self {
            data: data.into_boxed_slice(),
        }
    }

    pub fn from_parquet_variant(variant: ParquetVariant<'_, '_>) -> anyhow::Result<Self> {
        let field_names = collect_variant_field_names(variant.clone())?;
        let mut builder = canonical_builder(&field_names);
        builder
            .try_append_value(variant)
            .context("failed to encode variant")?;
        let (metadata, value) = builder.finish();
        Ok(Self::from_canonical_parts(&metadata, &value))
    }

    pub fn from_json_value(json: &serde_json::Value) -> anyhow::Result<Self> {
        let mut field_names = BTreeSet::new();
        collect_json_field_names(json, &mut field_names);
        let mut builder = canonical_builder(&field_names);
        append_json_value(json, &mut builder)?;
        let (metadata, value) = builder.finish();
        Ok(Self::from_canonical_parts(&metadata, &value))
    }

    pub fn try_from_scalar_ref(
        value: Option<ScalarRefImpl<'_>>,
        data_type: &DataType,
    ) -> anyhow::Result<Self> {
        let mut field_names = BTreeSet::new();
        collect_datum_field_names(value, data_type, &mut field_names)?;
        let mut builder = canonical_builder(&field_names);
        append_datum_value(value, data_type, &mut builder)?;
        let (metadata, value) = builder.finish();
        Ok(Self::from_canonical_parts(&metadata, &value))
    }

    /// Decodes a value produced by [`VariantRef::value_serialize`], checking structure only.
    /// Use only on trusted bytes; external bytes must go through
    /// [`Self::from_serialized_untrusted`] to re-establish the canonical invariants.
    pub fn value_deserialize(buf: &[u8]) -> Option<Self> {
        let (metadata, value) = split_serialized_value(buf)?;
        ParquetVariant::try_new(metadata, value).ok()?;
        Some(Self { data: buf.into() })
    }

    /// Decodes a tagged buffer from an untrusted origin (e.g. pgwire binary parameters),
    /// re-canonicalizing it so non-canonical or non-finite inputs cannot break `Eq`/`Hash`/`Ord`.
    pub fn from_serialized_untrusted(buf: &[u8]) -> anyhow::Result<Self> {
        let (metadata, value) = split_serialized_value(buf).context("invalid variant encoding")?;
        Self::from_parts(metadata, value)
    }

    pub fn memcmp_deserialize(
        deserializer: &mut Deserializer<impl Buf>,
    ) -> memcomparable::Result<Self> {
        let bytes = <serde_bytes::ByteBuf as Deserialize>::deserialize(deserializer)?;
        Self::value_deserialize(&bytes)
            .ok_or_else(|| memcomparable::Error::Message("invalid variant".into()))
    }

    pub fn from_jsonb(jsonb: JsonbRef<'_>) -> anyhow::Result<Self> {
        Self::from_json_value(&serde_json::Value::from_str(&jsonb.to_string())?)
    }

    pub fn metadata(&self) -> &[u8] {
        expect_serialized_value(&self.data).0
    }

    pub fn value(&self) -> &[u8] {
        expect_serialized_value(&self.data).1
    }

    pub fn parquet_variant(&self) -> ParquetVariant<'_, '_> {
        ParquetVariant::new(self.metadata(), self.value())
    }

    pub fn serialized_len(&self) -> usize {
        self.data.len()
    }
}

impl<'a> VariantRef<'a> {
    pub fn value_serialize(&self) -> Vec<u8> {
        expect_serialized_value(self.data);
        self.data.to_vec()
    }

    pub fn memcmp_serialize(
        &self,
        serializer: &mut Serializer<impl BufMut>,
    ) -> memcomparable::Result<()> {
        let bytes = self.value_serialize();
        Serialize::serialize(&serde_bytes::Bytes::new(&bytes), serializer)
    }

    pub fn from_serialized(buf: &'a [u8]) -> Option<Self> {
        let (metadata, value) = split_serialized_value(buf)?;
        ParquetVariant::try_new(metadata, value).ok()?;
        Some(Self { data: buf })
    }

    pub fn metadata(&self) -> &'a [u8] {
        expect_serialized_value(self.data).0
    }

    pub fn value(&self) -> &'a [u8] {
        expect_serialized_value(self.data).1
    }

    pub fn parquet_variant(&self) -> ParquetVariant<'a, 'a> {
        ParquetVariant::new(self.metadata(), self.value())
    }

    pub fn is_variant_null(&self) -> bool {
        matches!(self.parquet_variant(), ParquetVariant::Null)
    }

    pub fn is_array(&self) -> bool {
        matches!(self.parquet_variant(), ParquetVariant::List(_))
    }

    pub fn is_object(&self) -> bool {
        matches!(self.parquet_variant(), ParquetVariant::Object(_))
    }

    pub fn type_name(&self) -> &'static str {
        match self.parquet_variant() {
            ParquetVariant::Null => "null",
            ParquetVariant::BooleanTrue | ParquetVariant::BooleanFalse => "boolean",
            ParquetVariant::Int8(_) => "int8",
            ParquetVariant::Int16(_) => "int16",
            ParquetVariant::Int32(_) => "int32",
            ParquetVariant::Int64(_) => "int64",
            ParquetVariant::Float(_) => "float",
            ParquetVariant::Double(_) => "double",
            ParquetVariant::Decimal4(_) => "decimal4",
            ParquetVariant::Decimal8(_) => "decimal8",
            ParquetVariant::Decimal16(_) => "decimal16",
            ParquetVariant::Date(_) => "date",
            ParquetVariant::TimestampMicros(_) => "timestamp_micros",
            ParquetVariant::TimestampNtzMicros(_) => "timestamp_ntz_micros",
            ParquetVariant::TimestampNanos(_) => "timestamp_nanos",
            ParquetVariant::TimestampNtzNanos(_) => "timestamp_ntz_nanos",
            ParquetVariant::Binary(_) => "binary",
            ParquetVariant::String(_) | ParquetVariant::ShortString(_) => "string",
            ParquetVariant::Time(_) => "time",
            ParquetVariant::Uuid(_) => "uuid",
            ParquetVariant::Object(_) => "object",
            ParquetVariant::List(_) => "array",
        }
    }

    pub fn access_path(self, path: &str) -> Option<VariantVal> {
        self.access_path_strict(path).ok().flatten()
    }

    pub fn access_path_strict(self, path: &str) -> anyhow::Result<Option<VariantVal>> {
        // Walk the whole path on borrowed variants sharing the same metadata, and canonicalize
        // (re-encode) only the final leaf.
        let mut variant = self.parquet_variant();
        for token in parse_path(path)? {
            let next = match token {
                PathToken::Field(field) => variant.get_object_field(&field),
                // Pattern-match instead of `as_list`, whose `&'m self` receiver would keep
                // `variant` borrowed and forbid the reassignment below.
                PathToken::Index(index) => match &variant {
                    ParquetVariant::List(list) => {
                        let index = if index >= 0 {
                            Some(index as usize)
                        } else {
                            list.len().checked_sub(index.unsigned_abs() as usize)
                        };
                        index.and_then(|index| list.get(index))
                    }
                    _ => None,
                },
            };
            match next {
                Some(next) => variant = next,
                None => return Ok(None),
            }
        }
        VariantVal::from_parquet_variant(variant).map(Some)
    }

    pub fn to_jsonb(self) -> anyhow::Result<JsonbVal> {
        let json = self
            .parquet_variant()
            .to_json_value()
            .context("failed to convert variant to jsonb")?;
        Ok(json.into())
    }
}

impl From<VariantRef<'_>> for VariantVal {
    fn from(value: VariantRef<'_>) -> Self {
        value.to_owned_scalar()
    }
}

impl<'a> FromSql<'a> for VariantVal {
    accepts!(JSON, JSONB);

    fn from_sql(
        ty: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        Ok(match *ty {
            Type::JSON => Self::from_str(std::str::from_utf8(raw)?)?,
            Type::JSONB => {
                let mut raw = raw;
                if raw.is_empty() || raw.get_u8() != 1 {
                    return Err("invalid postgres jsonb encoding".into());
                }
                Self::from_str(std::str::from_utf8(raw)?)?
            }
            _ => {
                bail_not_implemented!("the VariantVal's postgres decoding for {ty} is unsupported")
            }
        })
    }
}

impl ToSql for VariantRef<'_> {
    accepts!(JSON, JSONB);

    to_sql_checked!();

    fn to_sql(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>>
    where
        Self: Sized,
    {
        if matches!(*ty, Type::JSONB) {
            out.put_u8(1);
        }
        out.extend_from_slice(self.to_text().as_bytes());
        Ok(IsNull::No)
    }
}

fn split_serialized_value(buf: &[u8]) -> Option<(&[u8], &[u8])> {
    let buf = strip_current_encoding_tag(buf)?;
    if buf.len() < METADATA_LEN_SIZE {
        return None;
    }
    let metadata_len = metadata_len_from_serialized(buf)? as usize;
    let metadata_end = METADATA_LEN_SIZE.checked_add(metadata_len)?;
    if metadata_end > buf.len() {
        return None;
    }
    Some((&buf[METADATA_LEN_SIZE..metadata_end], &buf[metadata_end..]))
}

fn metadata_len_from_serialized(buf: &[u8]) -> Option<u32> {
    if buf.len() < METADATA_LEN_SIZE {
        return None;
    }
    Some(u32::from_le_bytes(
        buf[..METADATA_LEN_SIZE].try_into().unwrap(),
    ))
}

fn expect_serialized_value(buf: &[u8]) -> (&[u8], &[u8]) {
    split_serialized_value(buf).expect("variant should use current serialized format")
}

fn strip_current_encoding_tag(buf: &[u8]) -> Option<&[u8]> {
    let (&version, rest) = buf.split_first()?;
    if version == VARIANT_ENCODING_VERSION {
        Some(rest)
    } else {
        None
    }
}

fn canonical_builder(field_names: &BTreeSet<String>) -> VariantBuilder {
    VariantBuilder::new().with_field_names(field_names.iter().map(String::as_str))
}

fn collect_json_field_names(json: &serde_json::Value, field_names: &mut BTreeSet<String>) {
    match json {
        serde_json::Value::Array(values) => {
            for value in values {
                collect_json_field_names(value, field_names);
            }
        }
        serde_json::Value::Object(fields) => {
            for (field, value) in fields {
                field_names.insert(field.clone());
                collect_json_field_names(value, field_names);
            }
        }
        _ => {}
    }
}

fn collect_variant_field_names(
    variant: ParquetVariant<'_, '_>,
) -> anyhow::Result<BTreeSet<String>> {
    let mut field_names = BTreeSet::new();
    collect_variant_field_names_inner(variant, &mut field_names)?;
    Ok(field_names)
}

/// Collects object field names and, as the single walk over untrusted input, also rejects
/// non-finite floats.
fn collect_variant_field_names_inner(
    variant: ParquetVariant<'_, '_>,
    field_names: &mut BTreeSet<String>,
) -> anyhow::Result<()> {
    match variant {
        ParquetVariant::Object(object) => {
            for field in object.iter_try() {
                let (field_name, value) = field.context("failed to read variant object")?;
                field_names.insert(field_name.to_owned());
                collect_variant_field_names_inner(value, field_names)?;
            }
        }
        ParquetVariant::List(list) => {
            for value in list.iter_try() {
                collect_variant_field_names_inner(
                    value.context("failed to read variant list")?,
                    field_names,
                )?;
            }
        }
        ParquetVariant::Float(v) if !v.is_finite() => {
            bail!("non-finite float cannot be converted to variant")
        }
        ParquetVariant::Double(v) if !v.is_finite() => {
            bail!("non-finite float cannot be converted to variant")
        }
        _ => {}
    }
    Ok(())
}

fn collect_datum_field_names(
    value: Option<ScalarRefImpl<'_>>,
    data_type: &DataType,
    field_names: &mut BTreeSet<String>,
) -> anyhow::Result<()> {
    let Some(value) = value else {
        return Ok(());
    };

    match (value, data_type) {
        (ScalarRefImpl::Jsonb(v), _) => {
            collect_json_field_names(&serde_json::Value::from_str(&v.to_string())?, field_names);
        }
        (ScalarRefImpl::Variant(v), _) => {
            collect_variant_field_names_inner(v.parquet_variant(), field_names)?;
        }
        (ScalarRefImpl::List(v), DataType::List(list_type)) => {
            for value in v.iter() {
                collect_datum_field_names(value, list_type.elem(), field_names)?;
            }
        }
        (ScalarRefImpl::Struct(v), DataType::Struct(struct_type)) => {
            for (value, (field_name, field_type)) in
                v.iter_fields_ref().zip_eq_fast(struct_type.iter())
            {
                field_names.insert(field_name.to_owned());
                collect_datum_field_names(value, field_type, field_names)?;
            }
        }
        (ScalarRefImpl::Map(v), DataType::Map(map_type)) => {
            for (key, value) in v.iter() {
                let field = key.to_text_with_type(map_type.key());
                field_names.insert(field);
                collect_datum_field_names(value, map_type.value(), field_names)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn append_json_value(
    json: &serde_json::Value,
    builder: &mut impl VariantBuilderExt,
) -> anyhow::Result<()> {
    match json {
        serde_json::Value::Null => builder.append_value(ParquetVariant::Null),
        serde_json::Value::Bool(v) => builder.append_value(*v),
        serde_json::Value::Number(v) => {
            if let Some(v) = v.as_i64() {
                builder.append_value(v);
            } else if let Some(v) = v.as_u64() {
                builder.append_value(v);
            } else if let Some(v) = v.as_f64() {
                append_float64(v, builder)?;
            } else {
                bail!("unsupported JSON number: {v}");
            }
        }
        serde_json::Value::String(v) => builder.append_value(v.as_str()),
        serde_json::Value::Array(values) => {
            let mut list = builder
                .try_new_list()
                .context("failed to create variant list")?;
            for value in values {
                append_json_value(value, &mut list)?;
            }
            list.finish();
        }
        serde_json::Value::Object(fields) => {
            let mut object = builder
                .try_new_object()
                .context("failed to create variant object")?;
            for (field, value) in fields.iter().sorted_by(|a, b| a.0.cmp(b.0)) {
                let mut field_builder = ObjectFieldBuilder::new(field.as_str(), &mut object);
                append_json_value(value, &mut field_builder)?;
            }
            object.finish();
        }
    }
    Ok(())
}

fn append_datum_value(
    value: Option<ScalarRefImpl<'_>>,
    data_type: &DataType,
    builder: &mut impl VariantBuilderExt,
) -> anyhow::Result<()> {
    let Some(value) = value else {
        builder.append_value(ParquetVariant::Null);
        return Ok(());
    };
    assert!(
        scalar_ref_type_match(data_type, value),
        "variant conversion input {} does not match {data_type}",
        value.get_ident()
    );

    // Preserve the SQL type identity when Parquet Variant V1 can represent it.
    // Untyped JSON construction still follows its own default Variant primitive types.
    match (value, data_type) {
        (ScalarRefImpl::Bool(v), _) => builder.append_value(v),
        (ScalarRefImpl::Int16(v), _) => builder.append_value(v),
        (ScalarRefImpl::Int32(v), _) => builder.append_value(v),
        (ScalarRefImpl::Int64(v), _) => builder.append_value(v),
        (ScalarRefImpl::Serial(v), _) => builder.append_value(v.into_inner()),
        (ScalarRefImpl::Float32(v), _) => append_float64(f64::from(v.into_inner()), builder)?,
        (ScalarRefImpl::Float64(v), _) => append_float64(v.into_inner(), builder)?,
        (ScalarRefImpl::Decimal(v), _) => append_decimal(v, builder)?,
        (ScalarRefImpl::Utf8(v), _) => builder.append_value(v),
        (ScalarRefImpl::Bytea(v), _) => builder.append_value(v),
        (ScalarRefImpl::Date(v), _) => builder.append_value(v.0),
        (ScalarRefImpl::Time(v), _) => builder.append_value(v.0),
        (ScalarRefImpl::Timestamp(v), _) => builder.append_value(v.0),
        (ScalarRefImpl::Timestamptz(v), _) => builder.append_value(v.to_datetime_utc()),
        (ScalarRefImpl::Jsonb(v), _) => {
            append_json_value(&serde_json::Value::from_str(&v.to_string())?, builder)?
        }
        (ScalarRefImpl::Variant(v), _) => builder.append_value(v.parquet_variant()),
        (ScalarRefImpl::Int256(_), _)
        | (ScalarRefImpl::Interval(_), _)
        | (ScalarRefImpl::Vector(_), _) => {
            bail!("{data_type} cannot be converted to variant without a standard Variant type")
        }
        (ScalarRefImpl::List(v), DataType::List(list_type)) => {
            let mut list = builder
                .try_new_list()
                .context("failed to create variant list")?;
            for value in v.iter() {
                append_datum_value(value, list_type.elem(), &mut list)?;
            }
            list.finish();
        }
        (ScalarRefImpl::Struct(v), DataType::Struct(struct_type)) => {
            append_struct(v, struct_type, builder)?;
        }
        (ScalarRefImpl::Map(v), DataType::Map(map_type)) => {
            let mut object = builder
                .try_new_object()
                .context("failed to create variant map object")?;
            let entries = v
                .iter()
                .map(|(key, value)| {
                    let field = key.to_text_with_type(map_type.key());
                    (field, value)
                })
                .sorted_by(|a, b| a.0.cmp(&b.0))
                .collect_vec();
            for (field, value) in entries {
                let mut field_builder = ObjectFieldBuilder::new(field.as_str(), &mut object);
                append_datum_value(value, map_type.value(), &mut field_builder)?;
            }
            object.finish();
        }
        (value, ty) => bail!("cannot convert {} as {ty} to variant", value.get_ident()),
    }
    Ok(())
}

fn append_struct(
    value: super::StructRef<'_>,
    struct_type: &StructType,
    builder: &mut impl VariantBuilderExt,
) -> anyhow::Result<()> {
    let mut object = builder
        .try_new_object()
        .context("failed to create variant struct object")?;
    let fields = value
        .iter_fields_ref()
        .zip_eq_fast(struct_type.iter())
        .sorted_by(|(_, (field_a, _)), (_, (field_b, _))| field_a.cmp(field_b));
    for (value, (field_name, field_type)) in fields {
        let mut field_builder = ObjectFieldBuilder::new(field_name, &mut object);
        append_datum_value(value, field_type, &mut field_builder)?;
    }
    object.finish();
    Ok(())
}

fn append_float64(value: f64, builder: &mut impl VariantBuilderExt) -> anyhow::Result<()> {
    if !value.is_finite() {
        bail!("non-finite float cannot be converted to variant");
    }
    let value = if value == 0.0 { 0.0 } else { value };
    builder.append_value(value);
    Ok(())
}

fn append_decimal(value: Decimal, builder: &mut impl VariantBuilderExt) -> anyhow::Result<()> {
    match value {
        Decimal::Normalized(value) => {
            let value = value.normalize();
            let decimal = VariantDecimal16::try_new(value.mantissa(), value.scale() as u8)
                .context("failed to encode decimal as variant")?;
            builder.append_value(decimal);
        }
        Decimal::NaN | Decimal::PositiveInf | Decimal::NegativeInf => {
            builder.append_value(value.to_text().as_str());
        }
    }
    Ok(())
}

enum PathToken {
    Field(String),
    Index(i32),
}

fn parse_path(path: &str) -> anyhow::Result<Vec<PathToken>> {
    let original_path = path;
    let path = path.strip_prefix('$').unwrap_or(path);
    let mut chars = path.chars().peekable();
    let mut tokens = vec![];
    while let Some(ch) = chars.next() {
        match ch {
            '.' => {
                let mut field = String::new();
                while let Some(&c) = chars.peek() {
                    if c == '.' || c == '[' {
                        break;
                    }
                    field.push(c);
                    chars.next();
                }
                if field.is_empty() {
                    bail!("invalid variant path `{original_path}`");
                }
                tokens.push(PathToken::Field(field));
            }
            '[' => {
                if matches!(chars.peek(), Some('\'') | Some('"')) {
                    let quote = chars.next().unwrap();
                    let mut field = String::new();
                    let mut closed = false;
                    for c in chars.by_ref() {
                        if c == quote {
                            closed = true;
                            break;
                        }
                        field.push(c);
                    }
                    if !closed || chars.next() != Some(']') {
                        bail!("invalid variant path `{original_path}`");
                    }
                    tokens.push(PathToken::Field(field));
                } else {
                    let mut index = String::new();
                    while let Some(&c) = chars.peek() {
                        if c == ']' {
                            break;
                        }
                        index.push(c);
                        chars.next();
                    }
                    if chars.next() != Some(']') {
                        bail!("invalid variant path `{original_path}`");
                    }
                    tokens.push(PathToken::Index(index.parse().with_context(|| {
                        format!("invalid variant path `{original_path}`")
                    })?));
                }
            }
            _ if tokens.is_empty() => {
                let mut field = String::from(ch);
                while let Some(&c) = chars.peek() {
                    if c == '.' || c == '[' {
                        break;
                    }
                    field.push(c);
                    chars.next();
                }
                tokens.push(PathToken::Field(field));
            }
            _ => bail!("invalid variant path `{original_path}`"),
        }
    }
    Ok(tokens)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::StructValue;
    use crate::types::{Date, F32, F64, Int256, Interval, Serial, Time, Timestamptz};

    fn scalar_variant(value: ScalarRefImpl<'_>, data_type: &DataType) -> VariantVal {
        VariantVal::try_from_scalar_ref(Some(value), data_type).unwrap()
    }

    fn assert_same_variant(lhs: &VariantVal, rhs: &VariantVal) {
        assert_eq!(lhs, rhs);
        assert_eq!(
            lhs.as_scalar_ref().value_serialize(),
            rhs.as_scalar_ref().value_serialize()
        );
    }

    fn assert_variant_parts(
        name: &str,
        variant: &VariantVal,
        expected_metadata_hex: &str,
        expected_value_hex: &str,
    ) {
        assert_eq!(
            hex::encode(variant.metadata()),
            expected_metadata_hex,
            "{name} metadata bytes changed"
        );
        assert_eq!(
            hex::encode(variant.value()),
            expected_value_hex,
            "{name} value bytes changed"
        );
    }

    #[test]
    fn path_access_supports_dot_and_bracket() {
        let v: VariantVal = r#"{"a":[{"b":7}]}"#.parse().unwrap();
        assert_eq!(
            v.as_scalar_ref()
                .access_path("$.a[0].b")
                .unwrap()
                .to_string(),
            "7"
        );
        assert_eq!(
            v.as_scalar_ref()
                .access_path("a[0]['b']")
                .unwrap()
                .to_string(),
            "7"
        );
        assert_eq!(
            v.as_scalar_ref()
                .access_path("$.a[-1].b")
                .unwrap()
                .to_string(),
            "7"
        );
        assert!(v.as_scalar_ref().access_path("$.a[-2]").is_none());
        assert!(v.as_scalar_ref().access_path("$.a[0].b[0]").is_none());
        assert!(
            v.as_scalar_ref()
                .access_path_strict("$.missing")
                .unwrap()
                .is_none()
        );
        assert!(v.as_scalar_ref().access_path_strict("$.").is_err());
        assert!(v.as_scalar_ref().access_path("$.").is_none());
    }

    #[test]
    fn ord_matches_memcmp_encoding_order() {
        fn memcmp_encode(v: &VariantVal) -> Vec<u8> {
            let mut serializer = Serializer::new(vec![]);
            v.as_scalar_ref().memcmp_serialize(&mut serializer).unwrap();
            serializer.into_inner()
        }

        // Include values with different metadata lengths.
        let values: Vec<VariantVal> = [
            "1",
            r#""short""#,
            r#"{"b":1,"c":1}"#,
            r#"{"aaaaaaaaaa":1}"#,
            r#"{"a":1}"#,
            "[1,2,3]",
        ]
        .iter()
        .map(|s| s.parse().unwrap())
        .collect();
        for a in &values {
            for b in &values {
                assert_eq!(
                    a.cmp(b),
                    memcmp_encode(a).cmp(&memcmp_encode(b)),
                    "Ord and memcmp encoding disagree for {a} vs {b}"
                );
            }
        }
    }

    #[test]
    fn deep_jsonb_to_variant_returns_error() {
        let shallow = JsonbVal::from(serde_json::Value::from(1));
        assert!(VariantVal::from_jsonb(shallow.as_scalar_ref()).is_ok());

        // Deeper than serde_json's 128-level parser limit; must error, not panic.
        let mut json = serde_json::Value::from(1);
        for _ in 0..200 {
            json = serde_json::Value::Array(vec![json]);
        }
        let deep = JsonbVal::from(json);
        assert!(VariantVal::from_jsonb(deep.as_scalar_ref()).is_err());
    }

    #[test]
    fn rejects_non_finite_double_from_parquet_variant() {
        let mut builder = VariantBuilder::new();
        builder.append_value(f64::NAN);
        let (metadata, value) = builder.finish();

        assert!(VariantVal::from_parts(&metadata, &value).is_err());

        // Even if such a value slipped through, converting it to jsonb must error, not panic.
        let v = VariantVal::from_canonical_parts(&metadata, &value);
        assert!(v.as_scalar_ref().to_jsonb().is_err());
    }

    #[test]
    fn untrusted_serialized_rejects_non_finite_double() {
        let mut builder = VariantBuilder::new();
        builder.append_value(f64::NAN);
        let (metadata, value) = builder.finish();
        let bytes = VariantVal::from_canonical_parts(&metadata, &value)
            .as_scalar_ref()
            .value_serialize();

        assert!(VariantVal::from_serialized_untrusted(&bytes).is_err());
        // The trusted decode path only checks structure.
        assert!(VariantVal::value_deserialize(&bytes).is_some());
    }

    #[test]
    fn untrusted_serialized_canonicalizes_unsorted_dictionary() {
        // Structurally valid but non-canonical: unsorted metadata dictionary `["b", "a"]`.
        let mut builder = VariantBuilder::new();
        builder.add_field_name("b");
        builder.add_field_name("a");
        let mut object = builder.new_object();
        object.insert("a", 1i64);
        object.insert("b", 2i64);
        object.finish();
        let (metadata, value) = builder.finish();
        let non_canonical = VariantVal::from_canonical_parts(&metadata, &value)
            .as_scalar_ref()
            .value_serialize();

        let canonical: VariantVal = r#"{"a":1,"b":2}"#.parse().unwrap();
        let canonical_bytes = canonical.as_scalar_ref().value_serialize();

        assert_ne!(non_canonical, canonical_bytes);
        let recanonicalized = VariantVal::from_serialized_untrusted(&non_canonical).unwrap();
        assert_same_variant(&recanonicalized, &canonical);
    }

    #[test]
    fn maps_numeric_scalars_to_variant_types() {
        let json_int: VariantVal = "1".parse().unwrap();
        let int16 =
            VariantVal::try_from_scalar_ref(Some(ScalarRefImpl::Int16(1)), &DataType::Int16)
                .unwrap();
        let int32 =
            VariantVal::try_from_scalar_ref(Some(ScalarRefImpl::Int32(1)), &DataType::Int32)
                .unwrap();
        let int64 =
            VariantVal::try_from_scalar_ref(Some(ScalarRefImpl::Int64(1)), &DataType::Int64)
                .unwrap();
        let serial = VariantVal::try_from_scalar_ref(
            Some(ScalarRefImpl::Serial(Serial::from(1))),
            &DataType::Serial,
        )
        .unwrap();

        assert_eq!(json_int.as_scalar_ref().type_name(), "int64");
        assert_eq!(int16.as_scalar_ref().type_name(), "int16");
        assert_eq!(int32.as_scalar_ref().type_name(), "int32");
        assert_eq!(int64.as_scalar_ref().type_name(), "int64");
        assert_ne!(json_int, int16);
        assert_ne!(json_int, int32);
        assert_same_variant(&json_int, &int64);
        assert_same_variant(&json_int, &serial);

        let json_float: VariantVal = "1.5".parse().unwrap();
        let float32 = VariantVal::try_from_scalar_ref(
            Some(ScalarRefImpl::Float32(F32::from(1.5))),
            &DataType::Float32,
        )
        .unwrap();
        let float64 = VariantVal::try_from_scalar_ref(
            Some(ScalarRefImpl::Float64(F64::from(1.5))),
            &DataType::Float64,
        )
        .unwrap();

        assert_eq!(json_float.as_scalar_ref().type_name(), "double");
        assert_same_variant(&json_float, &float32);
        assert_same_variant(&json_float, &float64);

        let positive_zero: VariantVal = "0.0".parse().unwrap();
        let negative_zero = VariantVal::try_from_scalar_ref(
            Some(ScalarRefImpl::Float64(F64::from(-0.0))),
            &DataType::Float64,
        )
        .unwrap();
        assert_same_variant(&positive_zero, &negative_zero);
    }

    #[test]
    fn rejects_non_canonical_sql_only_scalars() {
        assert!(
            VariantVal::try_from_scalar_ref(
                Some(ScalarRefImpl::Float64(F64::from(f64::NAN))),
                &DataType::Float64,
            )
            .is_err()
        );
        assert!(
            VariantVal::try_from_scalar_ref(
                Some(ScalarRefImpl::Interval(Interval::from_month_day_usec(
                    1, 2, 3
                ))),
                &DataType::Interval,
            )
            .is_err()
        );

        let int256 = Int256::from(1);
        assert!(
            VariantVal::try_from_scalar_ref(
                Some(int256.as_scalar_ref().into()),
                &DataType::Int256,
            )
            .is_err()
        );
    }

    #[test]
    fn maps_temporal_scalars_to_variant_temporal_types() {
        let date = VariantVal::try_from_scalar_ref(
            Some(ScalarRefImpl::Date(Date::from_ymd_uncheck(2024, 1, 2))),
            &DataType::Date,
        )
        .unwrap();
        let time = VariantVal::try_from_scalar_ref(
            Some(ScalarRefImpl::Time(Time::from_hms_micro_uncheck(
                3, 4, 5, 6000,
            ))),
            &DataType::Time,
        )
        .unwrap();
        let timestamp = VariantVal::try_from_scalar_ref(
            Some(ScalarRefImpl::Timestamp(
                Date::from_ymd_uncheck(2024, 1, 2).and_hms_micro_uncheck(3, 4, 5, 6000),
            )),
            &DataType::Timestamp,
        )
        .unwrap();
        let timestamptz = VariantVal::try_from_scalar_ref(
            Some(ScalarRefImpl::Timestamptz(Timestamptz::from_micros(1))),
            &DataType::Timestamptz,
        )
        .unwrap();

        assert_eq!(date.as_scalar_ref().type_name(), "date");
        assert_eq!(time.as_scalar_ref().type_name(), "time");
        assert_eq!(
            timestamp.as_scalar_ref().type_name(),
            "timestamp_ntz_micros"
        );
        assert_eq!(timestamptz.as_scalar_ref().type_name(), "timestamp_micros");
    }

    #[test]
    fn canonicalizes_object_fields_across_construction_paths() {
        let json: VariantVal = r#"{"a":1,"c":2}"#.parse().unwrap();
        let struct_type = DataType::Struct(StructType::new(vec![
            ("c", DataType::Int64),
            ("a", DataType::Int64),
        ]));
        let struct_value = StructValue::new(vec![
            Some(ScalarRefImpl::Int64(2).into()),
            Some(ScalarRefImpl::Int64(1).into()),
        ]);
        let variant = VariantVal::try_from_scalar_ref(
            Some(ScalarRefImpl::Struct(struct_value.as_scalar_ref())),
            &struct_type,
        )
        .unwrap();

        assert_eq!(json, variant);
        assert_eq!(
            json.as_scalar_ref().value_serialize(),
            variant.as_scalar_ref().value_serialize()
        );
    }

    #[test]
    fn canonicalizes_extracted_subvalues() {
        let root: VariantVal = r#"{"a":[{"b":1}]}"#.parse().unwrap();
        let extracted = root.as_scalar_ref().access_path("$.a[0]").unwrap();
        let parsed: VariantVal = r#"{"b":1}"#.parse().unwrap();

        assert_same_variant(&extracted, &parsed);
    }

    #[test]
    fn serializes_current_canonical_snapshot() {
        let v: VariantVal = r#"{"a":1,"c":[true,null]}"#.parse().unwrap();
        let bytes = v.as_scalar_ref().value_serialize();
        assert_eq!(
            hex::encode(&bytes),
            "0107000000110200010261630202000100091018010000000000000003020001020400"
        );
        assert_eq!(bytes[0], VARIANT_ENCODING_VERSION);
        assert_eq!(VariantVal::value_deserialize(&bytes).unwrap(), v);
        assert!(VariantVal::value_deserialize(&bytes[1..]).is_none());
        assert!(VariantRef::from_serialized(&bytes[1..]).is_none());
    }

    #[test]
    fn golden_bytes_for_object_field_order_and_nested_values() {
        let ordered_object: VariantVal = r#"{"a":1,"c":2}"#.parse().unwrap();
        let reordered_object: VariantVal = r#"{"c":2,"a":1}"#.parse().unwrap();
        assert_same_variant(&ordered_object, &reordered_object);
        assert_variant_parts(
            "object",
            &reordered_object,
            "11020001026163",
            "02020001000912180100000000000000180200000000000000",
        );

        let nested: VariantVal = r#"{"z":[{"b":true,"a":1},["short",null]],"a":{"c":"longish"}}"#
            .parse()
            .unwrap();
        assert_variant_parts(
            "nested",
            &nested,
            "110400010203046162637a",
            "02020003000d2f02010200081d6c6f6e67697368030200111d0202000100090a1801000000000000000403020006071573686f727400",
        );
    }

    #[test]
    fn golden_bytes_for_scalar_variant_values() {
        assert_variant_parts(
            "short_string",
            &scalar_variant(ScalarRefImpl::Utf8("short"), &DataType::Varchar),
            "010000",
            "1573686f7274",
        );
        assert_variant_parts(
            "long_string",
            &scalar_variant(
                ScalarRefImpl::Utf8(
                    "0123456789012345678901234567890123456789012345678901234567890123",
                ),
                &DataType::Varchar,
            ),
            "010000",
            "404000000030313233343536373839303132333435363738393031323334353637383930313233343536373839303132333435363738393031323334353637383930313233",
        );
        assert_variant_parts(
            "int16",
            &scalar_variant(ScalarRefImpl::Int16(1), &DataType::Int16),
            "010000",
            "100100",
        );
        assert_variant_parts(
            "int32",
            &scalar_variant(ScalarRefImpl::Int32(1), &DataType::Int32),
            "010000",
            "1401000000",
        );
        assert_variant_parts(
            "int64",
            &scalar_variant(ScalarRefImpl::Int64(1), &DataType::Int64),
            "010000",
            "180100000000000000",
        );
        assert_variant_parts(
            "decimal",
            &scalar_variant(
                ScalarRefImpl::Decimal("123.45".parse().unwrap()),
                &DataType::Decimal,
            ),
            "010000",
            "280239300000000000000000000000000000",
        );
        assert_variant_parts(
            "date",
            &scalar_variant(
                ScalarRefImpl::Date(Date::from_ymd_uncheck(2024, 1, 2)),
                &DataType::Date,
            ),
            "010000",
            "2c0c4d0000",
        );
        assert_variant_parts(
            "time",
            &scalar_variant(
                ScalarRefImpl::Time(Time::from_hms_micro_uncheck(3, 4, 5, 6000)),
                &DataType::Time,
            ),
            "010000",
            "44b06a559202000000",
        );
        assert_variant_parts(
            "timestamp",
            &scalar_variant(
                ScalarRefImpl::Timestamp(
                    Date::from_ymd_uncheck(2024, 1, 2).and_hms_micro_uncheck(3, 4, 5, 6000),
                ),
                &DataType::Timestamp,
            ),
            "010000",
            "34b0ea4dc0ed0d0600",
        );
        assert_variant_parts(
            "timestamptz",
            &scalar_variant(
                ScalarRefImpl::Timestamptz(Timestamptz::from_micros(1)),
                &DataType::Timestamptz,
            ),
            "010000",
            "300100000000000000",
        );
        assert_variant_parts(
            "binary",
            &scalar_variant(ScalarRefImpl::Bytea(&[0x12, 0x34, 0xff]), &DataType::Bytea),
            "010000",
            "3c030000001234ff",
        );
    }
}

// Copyright 2024 RisingWave Labs
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

mod schema;
use std::sync::LazyLock;

use apache_avro::schema::{DecimalSchema, RecordSchema};
use apache_avro::types::{Value, ValueKind};
use apache_avro::{Decimal as AvroDecimal, Schema};
use chrono::Datelike;
use itertools::Itertools;
use num_bigint::{BigInt, Sign};
use risingwave_common::array::{ListValue, StructValue};
use risingwave_common::bail;
use risingwave_common::log::LogSuppresser;
use risingwave_common::types::{
    DataType, Date, Interval, JsonbVal, ScalarImpl, Time, Timestamp, Timestamptz,
};
use risingwave_common::util::iter_util::ZipEqFast;

pub use self::schema::{avro_schema_to_column_descs, MapHandling, ResolvedAvroSchema};
use super::utils::extract_decimal;
use super::{bail_uncategorized, uncategorized, Access, AccessError, AccessResult};

#[derive(Clone)]
/// Options for parsing an `AvroValue` into Datum, with an optional avro schema.
pub struct AvroParseOptions<'a> {
    /// Currently, this schema is only used for decimal.
    ///
    /// FIXME: In theory we should use resolved schema.
    /// e.g., it's possible that a field is a reference to a decimal or a record containing a decimal field.
    pub schema: Option<&'a Schema>,
    /// Strict Mode
    /// If strict mode is disabled, an int64 can be parsed from an `AvroInt` (int32) value.
    pub relax_numeric: bool,
}

impl<'a> AvroParseOptions<'a> {
    pub fn create(schema: &'a Schema) -> Self {
        Self {
            schema: Some(schema),
            relax_numeric: true,
        }
    }
}

impl<'a> AvroParseOptions<'a> {
    fn extract_inner_schema(&self, key: Option<&'a str>) -> Option<&'a Schema> {
        self.schema
            .map(|schema| avro_extract_field_schema(schema, key))
            .transpose()
            .map_err(|_err| {
                static LOG_SUPPERSSER: LazyLock<LogSuppresser> =
                    LazyLock::new(LogSuppresser::default);
                if let Ok(suppressed_count) = LOG_SUPPERSSER.check() {
                    tracing::error!(suppressed_count, "extract sub-schema");
                }
            })
            .ok()
            .flatten()
    }

    /// Parse an avro value into expected type.
    ///
    /// 3 kinds of type info are used to parsing:
    /// - `type_expected`. The type that we expect the value is.
    /// - value type. The type info together with the value argument.
    /// - schema. The `AvroSchema` provided in option.
    ///
    /// Cases: (FIXME: Is this precise?)
    /// - If both `type_expected` and schema are provided, it will check both strictly.
    /// - If only `type_expected` is provided, it will try to match the value type and the
    ///    `type_expected`, converting the value if possible.
    /// - If only value is provided (without schema and `type_expected`),
    ///     the `DataType` will be inferred.
    pub fn convert_to_datum<'b>(
        &self,
        value: &'b Value,
        type_expected: &'b DataType,
    ) -> AccessResult
    where
        'b: 'a,
    {
        let create_error = || AccessError::TypeError {
            expected: format!("{:?}", type_expected),
            got: format!("{:?}", value),
            value: String::new(),
        };

        let v: ScalarImpl = match (type_expected, value) {
            (_, Value::Null) => return Ok(None),
            (_, Value::Union(_, v)) => {
                let schema = self.extract_inner_schema(None);
                return Self {
                    schema,
                    relax_numeric: self.relax_numeric,
                }
                .convert_to_datum(v, type_expected);
            }
            // ---- Boolean -----
            (DataType::Boolean, Value::Boolean(b)) => (*b).into(),
            // ---- Int16 -----
            (DataType::Int16, Value::Int(i)) if self.relax_numeric => (*i as i16).into(),
            (DataType::Int16, Value::Long(i)) if self.relax_numeric => (*i as i16).into(),

            // ---- Int32 -----
            (DataType::Int32, Value::Int(i)) => (*i).into(),
            (DataType::Int32, Value::Long(i)) if self.relax_numeric => (*i as i32).into(),
            // ---- Int64 -----
            (DataType::Int64, Value::Long(i)) => (*i).into(),
            (DataType::Int64, Value::Int(i)) if self.relax_numeric => (*i as i64).into(),
            // ---- Float32 -----
            (DataType::Float32, Value::Float(i)) => (*i).into(),
            (DataType::Float32, Value::Double(i)) => (*i as f32).into(),
            // ---- Float64 -----
            (DataType::Float64, Value::Double(i)) => (*i).into(),
            (DataType::Float64, Value::Float(i)) => (*i as f64).into(),
            // ---- Decimal -----
            (DataType::Decimal, Value::Decimal(avro_decimal)) => {
                let (precision, scale) = match self.schema {
                    Some(Schema::Decimal(DecimalSchema {
                        precision, scale, ..
                    })) => (*precision, *scale),
                    _ => Err(create_error())?,
                };
                let decimal = avro_decimal_to_rust_decimal(avro_decimal.clone(), precision, scale)
                    .map_err(|_| create_error())?;
                ScalarImpl::Decimal(risingwave_common::types::Decimal::Normalized(decimal))
            }
            (DataType::Decimal, Value::Record(fields)) => {
                // VariableScaleDecimal has fixed fields, scale(int) and value(bytes)
                let find_in_records = |field_name: &str| {
                    fields
                        .iter()
                        .find(|field| field.0 == field_name)
                        .map(|field| &field.1)
                        .ok_or_else(|| {
                            uncategorized!("`{field_name}` field not found in VariableScaleDecimal")
                        })
                };
                let scale = match find_in_records("scale")? {
                    Value::Int(scale) => *scale,
                    avro_value => bail_uncategorized!(
                        "scale field in VariableScaleDecimal is not int, got {:?}",
                        avro_value
                    ),
                };

                let value: BigInt = match find_in_records("value")? {
                    Value::Bytes(bytes) => BigInt::from_signed_bytes_be(bytes),
                    avro_value => bail_uncategorized!(
                        "value field in VariableScaleDecimal is not bytes, got {:?}",
                        avro_value
                    ),
                };

                let negative = value.sign() == Sign::Minus;
                let (lo, mid, hi) = extract_decimal(value.to_bytes_be().1)?;
                let decimal =
                    rust_decimal::Decimal::from_parts(lo, mid, hi, negative, scale as u32);
                ScalarImpl::Decimal(risingwave_common::types::Decimal::Normalized(decimal))
            }
            // ---- Time -----
            (DataType::Time, Value::TimeMillis(ms)) => Time::with_milli(*ms as u32)
                .map_err(|_| create_error())?
                .into(),
            (DataType::Time, Value::TimeMicros(us)) => Time::with_micro(*us as u64)
                .map_err(|_| create_error())?
                .into(),
            // ---- Date -----
            (DataType::Date, Value::Date(days)) => Date::with_days(days + unix_epoch_days())
                .map_err(|_| create_error())?
                .into(),
            // ---- Varchar -----
            (DataType::Varchar, Value::Enum(_, symbol)) => symbol.clone().into_boxed_str().into(),
            (DataType::Varchar, Value::String(s)) => s.clone().into_boxed_str().into(),
            // ---- Timestamp -----
            (DataType::Timestamp, Value::LocalTimestampMillis(ms)) => Timestamp::with_millis(*ms)
                .map_err(|_| create_error())?
                .into(),
            (DataType::Timestamp, Value::LocalTimestampMicros(us)) => Timestamp::with_micros(*us)
                .map_err(|_| create_error())?
                .into(),

            // ---- TimestampTz -----
            (DataType::Timestamptz, Value::TimestampMillis(ms)) => Timestamptz::from_millis(*ms)
                .ok_or_else(|| {
                    uncategorized!("timestamptz with milliseconds {ms} * 1000 is out of range")
                })?
                .into(),
            (DataType::Timestamptz, Value::TimestampMicros(us)) => {
                Timestamptz::from_micros(*us).into()
            }

            // ---- Interval -----
            (DataType::Interval, Value::Duration(duration)) => {
                let months = u32::from(duration.months()) as i32;
                let days = u32::from(duration.days()) as i32;
                let usecs = (u32::from(duration.millis()) as i64) * 1000; // never overflows
                ScalarImpl::Interval(Interval::from_month_day_usec(months, days, usecs))
            }
            // ---- Struct -----
            (DataType::Struct(struct_type_info), Value::Record(descs)) => StructValue::new(
                struct_type_info
                    .names()
                    .zip_eq_fast(struct_type_info.types())
                    .map(|(field_name, field_type)| {
                        let maybe_value = descs.iter().find(|(k, _v)| k == field_name);
                        if let Some((_, value)) = maybe_value {
                            let schema = self.extract_inner_schema(Some(field_name));
                            Ok(Self {
                                schema,
                                relax_numeric: self.relax_numeric,
                            }
                            .convert_to_datum(value, field_type)?)
                        } else {
                            Ok(None)
                        }
                    })
                    .collect::<Result<_, AccessError>>()?,
            )
            .into(),
            // ---- List -----
            (DataType::List(item_type), Value::Array(array)) => ListValue::new({
                let schema = self.extract_inner_schema(None);
                let mut builder = item_type.create_array_builder(array.len());
                for v in array {
                    let value = Self {
                        schema,
                        relax_numeric: self.relax_numeric,
                    }
                    .convert_to_datum(v, item_type)?;
                    builder.append(value);
                }
                builder.finish()
            })
            .into(),
            // ---- Bytea -----
            (DataType::Bytea, Value::Bytes(value)) => value.clone().into_boxed_slice().into(),
            // ---- Jsonb -----
            (DataType::Jsonb, v @ Value::Map(_)) => {
                let mut builder = jsonbb::Builder::default();
                avro_to_jsonb(v, &mut builder)?;
                let jsonb = builder.finish();
                debug_assert!(jsonb.as_ref().is_object());
                JsonbVal::from(jsonb).into()
            }
            (DataType::Varchar, Value::Uuid(uuid)) => {
                uuid.as_hyphenated().to_string().into_boxed_str().into()
            }

            (_expected, _got) => Err(create_error())?,
        };
        Ok(Some(v))
    }
}

pub struct AvroAccess<'a, 'b> {
    value: &'a Value,
    options: AvroParseOptions<'b>,
}

impl<'a, 'b> AvroAccess<'a, 'b> {
    pub fn new(value: &'a Value, options: AvroParseOptions<'b>) -> Self {
        Self { value, options }
    }
}

impl<'a, 'b> Access for AvroAccess<'a, 'b>
where
    'a: 'b,
{
    fn access(&self, path: &[&str], type_expected: &DataType) -> AccessResult {
        let mut value = self.value;
        let mut options: AvroParseOptions<'_> = self.options.clone();

        let mut i = 0;
        while i < path.len() {
            let key = path[i];
            let create_error = || AccessError::Undefined {
                name: key.to_string(),
                path: path.iter().take(i).join("."),
            };
            match value {
                Value::Union(_, v) => {
                    value = v;
                    options.schema = options.extract_inner_schema(None);
                    continue;
                }
                Value::Record(fields) => {
                    if let Some((_, v)) = fields.iter().find(|(k, _)| k == key) {
                        value = v;
                        options.schema = options.extract_inner_schema(Some(key));
                        i += 1;
                        continue;
                    }
                }
                _ => (),
            }
            Err(create_error())?;
        }

        options.convert_to_datum(value, type_expected)
    }
}

pub(crate) fn avro_decimal_to_rust_decimal(
    avro_decimal: AvroDecimal,
    _precision: usize,
    scale: usize,
) -> AccessResult<rust_decimal::Decimal> {
    let negative = !avro_decimal.is_positive();
    let bytes = avro_decimal.to_vec_unsigned();

    let (lo, mid, hi) = extract_decimal(bytes)?;
    Ok(rust_decimal::Decimal::from_parts(
        lo,
        mid,
        hi,
        negative,
        scale as u32,
    ))
}

pub fn avro_schema_skip_union(schema: &Schema) -> anyhow::Result<&Schema> {
    match schema {
        Schema::Union(union_schema) => {
            let inner_schema = union_schema
                .variants()
                .iter()
                .find(|s| !matches!(s, &&Schema::Null))
                .ok_or_else(|| {
                    anyhow::format_err!("illegal avro record schema {:?}", union_schema)
                })?;
            Ok(inner_schema)
        }
        other => Ok(other),
    }
}

// extract inner filed/item schema of record/array/union
pub fn avro_extract_field_schema<'a>(
    schema: &'a Schema,
    name: Option<&'a str>,
) -> anyhow::Result<&'a Schema> {
    match schema {
        Schema::Record(RecordSchema { fields, lookup, .. }) => {
            let name =
                name.ok_or_else(|| anyhow::format_err!("no name provided for a field in record"))?;
            let index = lookup.get(name).ok_or_else(|| {
                anyhow::format_err!("no field named '{}' in record: {:?}", name, schema)
            })?;
            let field = fields
                .get(*index)
                .ok_or_else(|| anyhow::format_err!("illegal avro record schema {:?}", schema))?;
            Ok(&field.schema)
        }
        Schema::Array(schema) => Ok(schema),
        Schema::Union(_) => avro_schema_skip_union(schema),
        Schema::Map(schema) => Ok(schema),
        _ => bail!("avro schema does not have inner item, schema: {:?}", schema),
    }
}

pub(crate) fn unix_epoch_days() -> i32 {
    Date::from_ymd_uncheck(1970, 1, 1).0.num_days_from_ce()
}

pub(crate) fn avro_to_jsonb(avro: &Value, builder: &mut jsonbb::Builder) -> AccessResult<()> {
    match avro {
        Value::Null => builder.add_null(),
        Value::Boolean(b) => builder.add_bool(*b),
        Value::Int(i) => builder.add_i64(*i as i64),
        Value::String(s) => builder.add_string(s),
        Value::Map(m) => {
            builder.begin_object();
            for (k, v) in m {
                builder.add_string(k);
                avro_to_jsonb(v, builder)?;
            }
            builder.end_object()
        }
        // same representation as map
        Value::Record(r) => {
            builder.begin_object();
            for (k, v) in r {
                builder.add_string(k);
                avro_to_jsonb(v, builder)?;
            }
            builder.end_object()
        }
        Value::Array(a) => {
            builder.begin_array();
            for v in a {
                avro_to_jsonb(v, builder)?;
            }
            builder.end_array()
        }

        // TODO: figure out where the following encoding is reasonable before enabling them.
        // See discussions: https://github.com/risingwavelabs/risingwave/pull/16948

        // jsonbb supports int64, but JSON spec does not allow it. How should we handle it?
        // BTW, protobuf canonical JSON converts int64 to string.
        // Value::Long(l) => builder.add_i64(*l),
        // Value::Float(f) => {
        //     if f.is_nan() || f.is_infinite() {
        //         // XXX: pad null or return err here?
        //         builder.add_null()
        //     } else {
        //         builder.add_f64(*f as f64)
        //     }
        // }
        // Value::Double(f) => {
        //     if f.is_nan() || f.is_infinite() {
        //         // XXX: pad null or return err here?
        //         builder.add_null()
        //     } else {
        //         builder.add_f64(*f)
        //     }
        // }
        // // XXX: What encoding to use?
        // // ToText is \x plus hex string.
        // Value::Bytes(b) => builder.add_string(&ToText::to_text(&b.as_slice())),
        // Value::Enum(_, symbol) => {
        //     builder.add_string(&symbol);
        // }
        // Value::Uuid(id) => builder.add_string(&id.as_hyphenated().to_string()),
        // // For Union, one concern is that the avro union is tagged (like rust enum) but json union is untagged (like c union).
        // // When the union consists of multiple records, it is possible to distinguish which variant is active in avro, but in json they will all become jsonb objects and indistinguishable.
        // Value::Union(_, v) => avro_to_jsonb(v, builder)?
        // XXX: pad null or return err here?
        v @ (Value::Long(_)
        | Value::Float(_)
        | Value::Double(_)
        | Value::Bytes(_)
        | Value::Enum(_, _)
        | Value::Fixed(_, _)
        | Value::Date(_)
        | Value::Decimal(_)
        | Value::TimeMillis(_)
        | Value::TimeMicros(_)
        | Value::TimestampMillis(_)
        | Value::TimestampMicros(_)
        | Value::LocalTimestampMillis(_)
        | Value::LocalTimestampMicros(_)
        | Value::Duration(_)
        | Value::Uuid(_)
        | Value::Union(_, _)) => {
            bail_uncategorized!(
                "unimplemented conversion from avro to jsonb: {:?}",
                ValueKind::from(v)
            )
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use apache_avro::Decimal as AvroDecimal;
    use risingwave_common::types::{Datum, Decimal};

    use super::*;

    #[test]
    fn test_convert_decimal() {
        // 280
        let v = vec![1, 24];
        let avro_decimal = AvroDecimal::from(v);
        let rust_decimal = avro_decimal_to_rust_decimal(avro_decimal, 28, 0).unwrap();
        assert_eq!(rust_decimal, rust_decimal::Decimal::from(280));

        // 28.1
        let v = vec![1, 25];
        let avro_decimal = AvroDecimal::from(v);
        let rust_decimal = avro_decimal_to_rust_decimal(avro_decimal, 28, 1).unwrap();
        assert_eq!(rust_decimal, rust_decimal::Decimal::try_from(28.1).unwrap());

        // 1.1234567891
        let value = BigInt::from(11234567891_i64);
        let negative = value.sign() == Sign::Minus;
        let (lo, mid, hi) = extract_decimal(value.to_bytes_be().1).unwrap();
        let decimal = rust_decimal::Decimal::from_parts(lo, mid, hi, negative, 10);
        assert_eq!(
            decimal,
            rust_decimal::Decimal::try_from(1.1234567891).unwrap()
        );

        // 1.123456789123456789123456789
        let v = vec![3, 161, 77, 58, 146, 180, 49, 220, 100, 4, 95, 21];
        let avro_decimal = AvroDecimal::from(v);
        let rust_decimal = avro_decimal_to_rust_decimal(avro_decimal, 28, 27).unwrap();
        assert_eq!(
            rust_decimal,
            rust_decimal::Decimal::from_str("1.123456789123456789123456789").unwrap()
        );
    }

    /// Convert Avro value to datum.For now, support the following [Avro type](https://avro.apache.org/docs/current/spec.html).
    ///  - boolean
    ///  - int : i32
    ///  - long: i64
    ///  - float: f32
    ///  - double: f64
    ///  - string: String
    ///  - Date (the number of days from the unix epoch, 1970-1-1 UTC)
    ///  - Timestamp (the number of milliseconds from the unix epoch,  1970-1-1 00:00:00.000 UTC)
    fn from_avro_value(
        value: Value,
        value_schema: &Schema,
        shape: &DataType,
    ) -> anyhow::Result<Datum> {
        AvroParseOptions::create(value_schema)
            .convert_to_datum(&value, shape)
            .map_err(Into::into)
    }

    #[test]
    fn test_avro_timestamptz_micros() {
        let v1 = Value::TimestampMicros(1620000000000000);
        let v2 = Value::TimestampMillis(1620000000000);
        let value_schema1 = Schema::TimestampMicros;
        let value_schema2 = Schema::TimestampMillis;
        let datum1 = from_avro_value(v1, &value_schema1, &DataType::Timestamptz).unwrap();
        let datum2 = from_avro_value(v2, &value_schema2, &DataType::Timestamptz).unwrap();
        assert_eq!(
            datum1,
            Some(ScalarImpl::Timestamptz(
                Timestamptz::from_str("2021-05-03T00:00:00Z").unwrap()
            ))
        );
        assert_eq!(
            datum2,
            Some(ScalarImpl::Timestamptz(
                Timestamptz::from_str("2021-05-03T00:00:00Z").unwrap()
            ))
        );
    }

    #[test]
    fn test_decimal_truncate() {
        let schema = Schema::parse_str(
            r#"
            {
                "type": "bytes",
                "logicalType": "decimal",
                "precision": 38,
                "scale": 18
            }
            "#,
        )
        .unwrap();
        let bytes = vec![0x3f, 0x3f, 0x3f, 0x3f, 0x3f, 0x3f, 0x3f];
        let value = Value::Decimal(AvroDecimal::from(bytes));
        let options = AvroParseOptions::create(&schema);
        let resp = options
            .convert_to_datum(&value, &DataType::Decimal)
            .unwrap();
        assert_eq!(
            resp,
            Some(ScalarImpl::Decimal(Decimal::Normalized(
                rust_decimal::Decimal::from_str("0.017802464409370431").unwrap()
            )))
        );
    }

    #[test]
    fn test_variable_scale_decimal() {
        let schema = Schema::parse_str(
            r#"
            {
                "type": "record",
                "name": "VariableScaleDecimal",
                "namespace": "io.debezium.data",
                "fields": [
                    {
                        "name": "scale",
                        "type": "int"
                    },
                    {
                        "name": "value",
                        "type": "bytes"
                    }
                ]
            }
            "#,
        )
        .unwrap();
        let value = Value::Record(vec![
            ("scale".to_string(), Value::Int(0)),
            ("value".to_string(), Value::Bytes(vec![0x01, 0x02, 0x03])),
        ]);

        let options = AvroParseOptions::create(&schema);
        let resp = options
            .convert_to_datum(&value, &DataType::Decimal)
            .unwrap();
        assert_eq!(resp, Some(ScalarImpl::Decimal(Decimal::from(66051))));
    }
}

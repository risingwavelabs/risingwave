use std::str::FromStr;

use apache_avro::types::Value;
use apache_avro::Schema;
use itertools::Itertools;
use risingwave_common::array::{ListValue, StructValue};
use risingwave_common::cast::{i64_to_timestamp, i64_to_timestamptz};
use risingwave_common::types::{DataType, Date, Datum, Interval, JsonbVal, ScalarImpl};
use risingwave_common::util::iter_util::ZipEqFast;

use super::{Access, AccessError, AccessResult};
use crate::parser::avro::util::{
    avro_decimal_to_rust_decimal, extract_inner_field_schema, unix_epoch_days,
};
#[derive(Clone)]
/// Options for parsing an AvroValue into Datum, with an optional avro schema.
pub struct AvroParseOptions<'a> {
    pub schema: Option<&'a Schema>,
    /// Strict Mode
    /// If strict mode is disabled, an int64 can be parsed from an AvroInt (int32) value.
    pub relax_numeric: bool,
}

impl<'a> Default for AvroParseOptions<'a> {
    fn default() -> Self {
        Self {
            schema: None,
            relax_numeric: true,
        }
    }
}

impl<'a> AvroParseOptions<'a> {
    pub fn with_schema(mut self, schema: &'a Schema) -> Self {
        self.schema = Some(schema);
        self
    }

    fn extract_inner_schema(&self, key: Option<&'a str>) -> Option<&'a Schema> {
        self.schema
            .map(|schema| extract_inner_field_schema(schema, key))
            .transpose()
            .map_err(|err| tracing::error!(?err, "extract sub-schema"))
            .ok()
            .flatten()
    }

    /// Parse an avro value into expected type.
    /// 3 kinds of type info are used to parsing things.
    ///     - type_expected. The type that we expect the value is.
    ///     - value type. The type info together with the value argument.
    ///     - schema. The AvroSchema provided in option.
    /// If both type_expected and schema are provided, it will check both strictly.
    /// If only type_expected is provided, it will try to match the value type and the
    /// type_expected, coverting the value if possible. If only value is provided (without
    /// schema and type_expected), the DateType will be inferred.
    pub fn parse<'b>(&self, value: &'b Value, type_expected: Option<&'b DataType>) -> AccessResult
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
                .parse(v, type_expected);
            }
            // ---- Boolean -----
            (Some(DataType::Boolean) | None, Value::Boolean(b)) => (*b).into(),
            // ---- Int16 -----
            (Some(DataType::Int16), Value::Int(i)) if self.relax_numeric => (*i as i16).into(),
            (Some(DataType::Int16), Value::Long(i)) if self.relax_numeric => (*i as i16).into(),

            // ---- Int32 -----
            (Some(DataType::Int32) | None, Value::Int(i)) => (*i).into(),
            (Some(DataType::Int32), Value::Long(i)) if self.relax_numeric => (*i as i32).into(),
            // ---- Int64 -----
            (Some(DataType::Int64) | None, Value::Long(i)) => (*i).into(),
            (Some(DataType::Int64), Value::Int(i)) if self.relax_numeric => (*i as i64).into(),
            // ---- Float32 -----
            (Some(DataType::Float32) | None, Value::Float(i)) => (*i).into(),
            (Some(DataType::Float32), Value::Double(i)) => (*i as f32).into(),
            // ---- Float64 -----
            (Some(DataType::Float64) | None, Value::Double(i)) => (*i).into(),
            (Some(DataType::Float64), Value::Float(i)) => (*i as f64).into(),
            // ---- Decimal -----
            (Some(DataType::Decimal) | None, Value::Decimal(avro_decimal)) => {
                let (precision, scale) = match self.schema {
                    Some(Schema::Decimal {
                        precision, scale, ..
                    }) => (*precision, *scale),
                    _ => Err(create_error())?,
                };
                let decimal = avro_decimal_to_rust_decimal(avro_decimal.clone(), precision, scale)
                    .map_err(|_| create_error())?;
                ScalarImpl::Decimal(risingwave_common::types::Decimal::Normalized(decimal))
            }

            // ---- Date -----
            (Some(DataType::Date) | None, Value::Date(days)) => {
                Date::with_days(days + unix_epoch_days())
                    .map_err(|_| create_error())?
                    .into()
            }
            // ---- Varchar -----
            (Some(DataType::Varchar) | None, Value::Enum(_, symbol)) => {
                symbol.clone().into_boxed_str().into()
            }
            (Some(DataType::Varchar) | None, Value::String(s)) => s.clone().into_boxed_str().into(),
            // ---- Timestamp -----
            (Some(DataType::Timestamp) | None, Value::TimestampMillis(ms)) => {
                i64_to_timestamp(*ms).map_err(|_| create_error())?.into()
            }
            (Some(DataType::Timestamp) | None, Value::TimestampMicros(us)) => {
                i64_to_timestamp(*us).map_err(|_| create_error())?.into()
            }

            // ---- TimestampTz -----
            (Some(DataType::Timestamptz), Value::TimestampMillis(ms)) => {
                i64_to_timestamptz(*ms).map_err(|_| create_error())?.into()
            }
            (Some(DataType::Timestamptz), Value::TimestampMicros(us)) => {
                i64_to_timestamptz(*us).map_err(|_| create_error())?.into()
            }

            // ---- Interval -----
            (Some(DataType::Interval) | None, Value::Duration(duration)) => {
                let months = u32::from(duration.months()) as i32;
                let days = u32::from(duration.days()) as i32;
                let usecs = (u32::from(duration.millis()) as i64) * 1000; // never overflows
                ScalarImpl::Interval(Interval::from_month_day_usec(months, days, usecs))
            }
            // ---- Struct -----
            (Some(DataType::Struct(struct_type_info)), Value::Record(descs)) => StructValue::new(
                struct_type_info
                    .field_names
                    .iter()
                    .zip_eq_fast(struct_type_info.fields.iter())
                    .map(|(field_name, field_type)| {
                        let maybe_value = descs.iter().find(|(k, _v)| k == field_name);
                        if let Some((_, value)) = maybe_value {
                            let schema = self.extract_inner_schema(Some(field_name));
                            Ok(Self {
                                schema,
                                relax_numeric: self.relax_numeric,
                            }
                            .parse(value, Some(field_type))?)
                        } else {
                            Ok(None)
                        }
                    })
                    .collect::<Result<_, AccessError>>()?,
            )
            .into(),
            (None, Value::Record(descs)) => {
                let rw_values = descs
                    .into_iter()
                    .map(|(field_name, field_value)| {
                        let schema = self.extract_inner_schema(Some(field_name));
                        Ok(Self {
                            schema,
                            relax_numeric: self.relax_numeric,
                        }
                        .parse(field_value, None)?)
                    })
                    .collect::<Result<Vec<Datum>, AccessError>>()?;
                ScalarImpl::Struct(StructValue::new(rw_values))
            }
            // ---- List -----
            (Some(DataType::List(item_type)), Value::Array(arr)) => ListValue::new(
                arr.iter()
                    .map(|v| {
                        let schema = self.extract_inner_schema(None);
                        Self {
                            schema,
                            relax_numeric: self.relax_numeric,
                        }
                        .parse(v, Some(item_type))
                    })
                    .collect::<Result<Vec<_>, AccessError>>()?,
            )
            .into(),
            (None, Value::Array(arr)) => ListValue::new(
                arr.iter()
                    .map(|v| {
                        let schema = self.extract_inner_schema(None);
                        Self {
                            schema,
                            relax_numeric: self.relax_numeric,
                        }
                        .parse(v, None)
                    })
                    .collect::<Result<Vec<_>, AccessError>>()?,
            )
            .into(),
            // ---- Bytea -----
            (Some(DataType::Bytea) | None, Value::Bytes(value)) => {
                value.clone().into_boxed_slice().into()
            }
            // ---- Jsonb -----
            (Some(DataType::Jsonb), Value::String(s)) => {
                JsonbVal::from_str(s).map_err(|_| create_error())?.into()
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
    fn access(&self, path: &[&str], type_expected: Option<&DataType>) -> AccessResult {
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
                Value::Map(fields) if fields.contains_key(key) => {
                    value = fields.get(key).unwrap();
                    options.schema = None;
                    i += 1;
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

        options.parse(value, type_expected)
    }
}


use std::str::FromStr;

use apache_avro::types::{Value};
use apache_avro::Schema;
use risingwave_common::array::{ListValue, StructValue};
use risingwave_common::cast::i64_to_timestamp;
use risingwave_common::types::{DataType, Date, Interval, JsonbVal, ScalarImpl};
use risingwave_common::util::iter_util::ZipEqFast;

use super::{AccessError, AccessResult};
use crate::parser::avro::util::{
    avro_decimal_to_rust_decimal, extract_inner_field_schema, unix_epoch_days,
};
#[derive(Clone)]
pub struct AvroParseOptions<'a> {
    pub schema: Option<&'a Schema>,
    /// Strict Mode
    /// If strict mode is disabled, an int64 can be parsed from an AvroInt (int32) value.
    pub relax_numeric: bool,
}

impl<'a> AvroParseOptions<'a> {
    fn extract_inner_schema(&self, key: Option<&'a str>) -> Option<&'a Schema> {
        self.schema
            .map(|schema| extract_inner_field_schema(schema, key))
            .transpose()
            .map_err(|err| tracing::error!(?err, "extract sub-schema"))
            .ok()
            .flatten()
    }

    pub fn parse<'b>(&self, value: &'b Value, shape: &'b DataType) -> AccessResult
    where
        'b: 'a,
    {
        let create_error = || AccessError::TypeError {
            expected: shape.to_string(),
            got: format!("{:?}", value),
            value: String::new(),
        };
        let v: ScalarImpl = match (shape, value) {
            (_, Value::Null) => return Ok(None),

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
            (DataType::Date, Value::Date(days)) => Date::with_days(days + unix_epoch_days())
                .map_err(|_| create_error())?
                .into(),
            // ---- Varchar -----
            (DataType::Varchar, Value::Enum(_, symbol)) => symbol.clone().into_boxed_str().into(),
            (DataType::Varchar, Value::String(s)) => s.clone().into_boxed_str().into(),
            // ---- Timestamp -----
            (DataType::Timestamp, Value::TimestampMillis(ms)) => {
                i64_to_timestamp(*ms).map_err(|_| create_error())?.into()
            }
            (DataType::Timestamp, Value::TimestampMicros(us)) => {
                i64_to_timestamp(*us).map_err(|_| create_error())?.into()
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
                            .parse(value, field_type)?)
                        } else {
                            Ok(None)
                        }
                    })
                    .collect::<Result<_, AccessError>>()?,
            )
            .into(),
            // ---- List -----
            (DataType::List(item_type), Value::Array(arr)) => ListValue::new(
                arr.iter()
                    .map(|_v| {
                        let schema = self.extract_inner_schema(None);
                        Ok(Self {
                            schema,
                            relax_numeric: self.relax_numeric,
                        }
                        .parse(value, item_type)?)
                    })
                    .collect::<Result<Vec<_>, AccessError>>()?,
            )
            .into(),
            // ---- Bytea -----
            (DataType::Bytea, Value::Bytes(value)) => value.clone().into_boxed_slice().into(),
            // ---- Jsonb -----
            (DataType::Jsonb, Value::String(s)) => {
                JsonbVal::from_str(s).map_err(|_| create_error())?.into()
            }

            (_expected, _got) => Err(create_error())?,
        };
        Ok(Some(v))
    }
}

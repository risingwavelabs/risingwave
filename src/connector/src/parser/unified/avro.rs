use apache_avro::types::{Record, Value};
use apache_avro::Schema;
use risingwave_common::types::{DataType, ScalarImpl};

use super::AccessError;
pub struct AvroParseOptions {
    pub schema: Option<Schema>,
    /// Strict Mode
    /// If strict mode is disabled, an int64 can be parsed from an AvroInt (int32) value.
    pub relax_numeric: bool,
}

impl AvroParseOptions {
    pub fn parse(&self, value: &Value, shape: &DataType) -> AccessResult {
        let create_error = || AccessError::TypeError {
            expected: shape.to_string(),
            got: value.to_string(),
            value: value.to_string(),
        };
        let v: ScalarImpl = match (shape, value) {
            (_, Value::Null) => return Ok(None),

            // ---- Boolean -----
            (DataType::Boolean, Value::Boolean(b)) => b.into(),
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
            (
                DataType::Decimal,
                Value::Decimal(avro_decimal)
            ) => {
                let (precision, scale) = match self.schema {
                    Schema::Decimal {
                        precision, scale, ..
                    } => (*precision, *scale),
                    _ => {
                        return Err(RwError::from(InternalError(
                            "avro value is and decimal but schema not".to_owned(),
                        )));
                    }
                };
                let decimal = avro_decimal_to_rust_decimal(avro_decimal, precision, scale)?;
                ScalarImpl::Decimal(risingwave_common::types::Decimal::Normalized(decimal))
            }

          
            // ---- Date -----
            (
                DataType::Date,
                ValueType::I64 | ValueType::I128 | ValueType::U64 | ValueType::U128,
            ) => Date::with_days_since_unix_epoch(value.try_as_i32()?)
                .map_err(|_| create_error())?
                .into(),
            (DataType::Date, ValueType::String) => str_to_date(value.as_str().unwrap())
                .map_err(|_| create_error())?
                .into(),
            // ---- Varchar -----
            (DataType::Varchar, ValueType::String) => value.as_str().unwrap().into(),
            // ---- Time -----
            (DataType::Time, ValueType::String) => str_to_time(value.as_str().unwrap())
                .map_err(|_| create_error())?
                .into(),
            (
                DataType::Time,
                ValueType::I64 | ValueType::I128 | ValueType::U64 | ValueType::U128,
            ) => value
                .as_i64()
                .map(|i| match self.time_handling {
                    TimeHandling::Milli => Time::with_milli(i as u32),
                    TimeHandling::Micro => Time::with_micro(i as u64),
                })
                .unwrap()
                .map_err(|_| create_error())?
                .into(),
            // ---- Timestamp -----
            (DataType::Timestamp, ValueType::String) => str_to_timestamp(value.as_str().unwrap())
                .map_err(|_| create_error())?
                .into(),
            (
                DataType::Timestamp,
                ValueType::I64 | ValueType::I128 | ValueType::U64 | ValueType::U128,
            ) => i64_to_timestamp(value.as_i64().unwrap())
                .map_err(|_| create_error())?
                .into(),
            // ---- Timestamptz -----
            (DataType::Timestamptz, ValueType::String) => {
                str_with_time_zone_to_timestamptz(value.as_str().unwrap())
                    .map_err(|_| create_error())?
                    .into()
            }
            (
                DataType::Timestamptz,
                ValueType::I64 | ValueType::I128 | ValueType::U64 | ValueType::U128,
            ) => i64_to_timestamptz(value.as_i64().unwrap())
                .map_err(|_| create_error())?
                .into(),
            // ---- Interval -----
            (DataType::Interval, ValueType::String) => {
                Interval::from_iso_8601(value.as_str().unwrap())
                    .map_err(|_| create_error())?
                    .into()
            }
            // ---- Struct -----
            (DataType::Struct(struct_type_info), ValueType::Object) => StructValue::new(
                struct_type_info
                    .field_names
                    .iter()
                    .zip_eq_fast(struct_type_info.fields.iter())
                    .map(|(field_name, field_type)| {
                        self.parse(
                            json_object_smart_get_value(value, field_name.into())
                                .unwrap_or(&BorrowedValue::Static(simd_json::StaticNode::Null)),
                            field_type,
                        )
                    })
                    .collect::<Result<_, _>>()?,
            )
            .into(),
            // ---- List -----
            (DataType::List(item_type), ValueType::Array) => ListValue::new(
                value
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|v| self.parse(v, item_type))
                    .collect::<Result<Vec<_>, _>>()?,
            )
            .into(),
            // ---- Bytea -----
            (DataType::Bytea, ValueType::String) => match self.bytea_handling {
                ByteaHandling::Standard => str_to_bytea(value.as_str().unwrap())
                    .map_err(|_| create_error())?
                    .into(),
                ByteaHandling::Base64 => base64::engine::general_purpose::STANDARD
                    .decode(value.as_str().unwrap())
                    .map_err(|_| create_error())?
                    .into_boxed_slice()
                    .into(),
            },
            // ---- Jsonb -----
            (DataType::Jsonb, ValueType::String)
                if matches!(self.json_value_handling, JsonValueHandling::AsString) =>
            {
                JsonbVal::from_str(value.as_str().unwrap())
                    .map_err(|_| create_error())?
                    .into()
            }
            (DataType::Jsonb, _)
                if matches!(self.json_value_handling, JsonValueHandling::AsValue) =>
            {
                JsonbVal::from_serde(value.clone().try_into().map_err(|_| create_error())?).into()
            }
            // ---- Int256 -----
            (
                DataType::Int256,
                ValueType::I64 | ValueType::I128 | ValueType::U64 | ValueType::U128,
            ) => Int256::from(value.try_as_i64()?).into(),

            (DataType::Int256, ValueType::String) => Int256::from_str(value.as_str().unwrap())
                .map_err(|_| create_error())?
                .into(),

            (expected, got) => Err(create_error())?,
        };
    }
}

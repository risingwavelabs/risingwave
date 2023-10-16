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

use std::collections::HashMap;

use base64::engine::general_purpose;
use base64::Engine as _;
use chrono::{Datelike, Timelike};
use indexmap::IndexMap;
use risingwave_common::array::{ArrayError, ArrayResult};
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::row::Row;
use risingwave_common::types::{DataType, DatumRef, Decimal, ScalarRefImpl, ToText};
use risingwave_common::util::iter_util::ZipEqDebug;
use serde_json::{json, Map, Value};

use super::{CustomJsonType, Result, RowEncoder, SerTo, TimestampHandlingMode};
use crate::sink::SinkError;

pub struct JsonEncoder {
    schema: Schema,
    col_indices: Option<Vec<usize>>,
    timestamp_handling_mode: TimestampHandlingMode,
    custom_json_type: CustomJsonType,
}

impl JsonEncoder {
    pub fn new(
        schema: Schema,
        col_indices: Option<Vec<usize>>,
        timestamp_handling_mode: TimestampHandlingMode,
    ) -> Self {
        Self {
            schema,
            col_indices,
            timestamp_handling_mode,
            custom_json_type: CustomJsonType::None,
        }
    }

    pub fn new_with_doris(
        schema: Schema,
        col_indices: Option<Vec<usize>>,
        timestamp_handling_mode: TimestampHandlingMode,
        map: HashMap<String, (u8, u8)>,
    ) -> Self {
        Self {
            schema,
            col_indices,
            timestamp_handling_mode,
            custom_json_type: CustomJsonType::Doris(map),
        }
    }
}

impl RowEncoder for JsonEncoder {
    type Output = Map<String, Value>;

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn col_indices(&self) -> Option<&[usize]> {
        self.col_indices.as_ref().map(Vec::as_ref)
    }

    fn encode_cols(
        &self,
        row: impl Row,
        col_indices: impl Iterator<Item = usize>,
    ) -> Result<Self::Output> {
        let mut mappings = Map::with_capacity(self.schema.len());
        for idx in col_indices {
            let field = &self.schema[idx];
            let key = field.name.clone();
            let value = datum_to_json_object(
                field,
                row.datum_at(idx),
                self.timestamp_handling_mode,
                &self.custom_json_type,
            )
            .map_err(|e| SinkError::Encode(e.to_string()))?;
            mappings.insert(key, value);
        }
        Ok(mappings)
    }
}

impl SerTo<String> for Map<String, Value> {
    fn ser_to(self) -> Result<String> {
        Value::Object(self).ser_to()
    }
}

impl SerTo<String> for Value {
    fn ser_to(self) -> Result<String> {
        Ok(self.to_string())
    }
}

fn datum_to_json_object(
    field: &Field,
    datum: DatumRef<'_>,
    timestamp_handling_mode: TimestampHandlingMode,
    custom_json_type: &CustomJsonType,
) -> ArrayResult<Value> {
    let scalar_ref = match datum {
        None => return Ok(Value::Null),
        Some(datum) => datum,
    };

    let data_type = field.data_type();

    tracing::debug!("datum_to_json_object: {:?}, {:?}", data_type, scalar_ref);

    let value = match (data_type, scalar_ref) {
        (DataType::Boolean, ScalarRefImpl::Bool(v)) => {
            json!(v)
        }
        (DataType::Int16, ScalarRefImpl::Int16(v)) => {
            json!(v)
        }
        (DataType::Int32, ScalarRefImpl::Int32(v)) => {
            json!(v)
        }
        (DataType::Int64, ScalarRefImpl::Int64(v)) => {
            json!(v)
        }
        (DataType::Float32, ScalarRefImpl::Float32(v)) => {
            json!(f32::from(v))
        }
        (DataType::Float64, ScalarRefImpl::Float64(v)) => {
            json!(f64::from(v))
        }
        (DataType::Varchar, ScalarRefImpl::Utf8(v)) => {
            json!(v)
        }
        (DataType::Decimal, ScalarRefImpl::Decimal(mut v)) => match custom_json_type {
            CustomJsonType::Doris(map) => {
                if !matches!(v, Decimal::Normalized(_)) {
                    return Err(ArrayError::internal(
                        "doris can't support decimal Inf, -Inf, Nan".to_string(),
                    ));
                }
                let (p, s) = map.get(&field.name).unwrap();
                v.rescale(*s as u32);
                let v_string = v.to_text();
                if v_string.len() > *p as usize {
                    return Err(ArrayError::internal(
                        format!("rw Decimal's precision is large than doris max decimal len is {:?}, doris max is {:?}",v_string.len(),p)));
                }
                json!(v_string)
            }
            CustomJsonType::None => {
                json!(v.to_text())
            }
        },
        (DataType::Timestamptz, ScalarRefImpl::Timestamptz(v)) => {
            // risingwave's timestamp with timezone is stored in UTC and does not maintain the
            // timezone info and the time is in microsecond.
            let parsed = v.to_datetime_utc().naive_utc();
            let v = parsed.format("%Y-%m-%d %H:%M:%S%.6f").to_string();
            json!(v)
        }
        (DataType::Time, ScalarRefImpl::Time(v)) => {
            // todo: just ignore the nanos part to avoid leap second complex
            json!(v.0.num_seconds_from_midnight() as i64 * 1000)
        }
        (DataType::Date, ScalarRefImpl::Date(v)) => match custom_json_type {
            CustomJsonType::None => json!(v.0.num_days_from_ce()),
            CustomJsonType::Doris(_) => {
                let a = v.0.format("%Y-%m-%d").to_string();
                json!(a)
            }
        },
        (DataType::Timestamp, ScalarRefImpl::Timestamp(v)) => match timestamp_handling_mode {
            TimestampHandlingMode::Milli => json!(v.0.timestamp_millis()),
            TimestampHandlingMode::String => json!(v.0.format("%Y-%m-%d %H:%M:%S%.6f").to_string()),
        },
        (DataType::Bytea, ScalarRefImpl::Bytea(v)) => {
            json!(general_purpose::STANDARD_NO_PAD.encode(v))
        }
        // P<years>Y<months>M<days>DT<hours>H<minutes>M<seconds>S
        (DataType::Interval, ScalarRefImpl::Interval(v)) => {
            json!(v.as_iso_8601())
        }
        (DataType::Jsonb, ScalarRefImpl::Jsonb(jsonb_ref)) => {
            json!(jsonb_ref.to_string())
        }
        (DataType::List(datatype), ScalarRefImpl::List(list_ref)) => {
            let elems = list_ref.iter();
            let mut vec = Vec::with_capacity(elems.len());
            let inner_field = Field::unnamed(Box::<DataType>::into_inner(datatype));
            for sub_datum_ref in elems {
                let value = datum_to_json_object(
                    &inner_field,
                    sub_datum_ref,
                    timestamp_handling_mode,
                    custom_json_type,
                )?;
                vec.push(value);
            }
            json!(vec)
        }
        (DataType::Struct(st), ScalarRefImpl::Struct(struct_ref)) => {
            match custom_json_type {
                CustomJsonType::Doris(_) => {
                    // We need to ensure that the order of elements in the json matches the insertion order.
                    let mut map = IndexMap::with_capacity(st.len());
                    for (sub_datum_ref, sub_field) in struct_ref.iter_fields_ref().zip_eq_debug(
                        st.iter()
                            .map(|(name, dt)| Field::with_name(dt.clone(), name)),
                    ) {
                        let value = datum_to_json_object(
                            &sub_field,
                            sub_datum_ref,
                            timestamp_handling_mode,
                            custom_json_type,
                        )?;
                        map.insert(sub_field.name.clone(), value);
                    }
                    Value::String(serde_json::to_string(&map).map_err(|err| {
                        ArrayError::internal(format!("Json to string err{:?}", err))
                    })?)
                }
                CustomJsonType::None => {
                    let mut map = Map::with_capacity(st.len());
                    for (sub_datum_ref, sub_field) in struct_ref.iter_fields_ref().zip_eq_debug(
                        st.iter()
                            .map(|(name, dt)| Field::with_name(dt.clone(), name)),
                    ) {
                        let value = datum_to_json_object(
                            &sub_field,
                            sub_datum_ref,
                            timestamp_handling_mode,
                            custom_json_type,
                        )?;
                        map.insert(sub_field.name.clone(), value);
                    }
                    json!(map)
                }
            }
        }
        (data_type, scalar_ref) => {
            return Err(ArrayError::internal(
                format!("datum_to_json_object: unsupported data type: field name: {:?}, logical type: {:?}, physical type: {:?}", field.name, data_type, scalar_ref),
            ));
        }
    };

    Ok(value)
}

#[cfg(test)]
mod tests {

    use risingwave_common::types::{
        DataType, Date, Interval, Scalar, ScalarImpl, StructRef, StructType, StructValue, Time,
        Timestamp,
    };

    use super::*;
    #[test]
    fn test_to_json_basic_type() {
        let mock_field = Field {
            data_type: DataType::Boolean,
            name: Default::default(),
            sub_fields: Default::default(),
            type_name: Default::default(),
        };

        let boolean_value = datum_to_json_object(
            &Field {
                data_type: DataType::Boolean,
                ..mock_field.clone()
            },
            Some(ScalarImpl::Bool(false).as_scalar_ref_impl()),
            TimestampHandlingMode::String,
            &CustomJsonType::None,
        )
        .unwrap();
        assert_eq!(boolean_value, json!(false));

        let int16_value = datum_to_json_object(
            &Field {
                data_type: DataType::Int16,
                ..mock_field.clone()
            },
            Some(ScalarImpl::Int16(16).as_scalar_ref_impl()),
            TimestampHandlingMode::String,
            &CustomJsonType::None,
        )
        .unwrap();
        assert_eq!(int16_value, json!(16));

        let int64_value = datum_to_json_object(
            &Field {
                data_type: DataType::Int64,
                ..mock_field.clone()
            },
            Some(ScalarImpl::Int64(std::i64::MAX).as_scalar_ref_impl()),
            TimestampHandlingMode::String,
            &CustomJsonType::None,
        )
        .unwrap();
        assert_eq!(
            serde_json::to_string(&int64_value).unwrap(),
            std::i64::MAX.to_string()
        );

        // https://github.com/debezium/debezium/blob/main/debezium-core/src/main/java/io/debezium/time/ZonedTimestamp.java
        let tstz_inner = "2018-01-26T18:30:09.453Z".parse().unwrap();
        let tstz_value = datum_to_json_object(
            &Field {
                data_type: DataType::Timestamptz,
                ..mock_field.clone()
            },
            Some(ScalarImpl::Timestamptz(tstz_inner).as_scalar_ref_impl()),
            TimestampHandlingMode::String,
            &CustomJsonType::None,
        )
        .unwrap();
        assert_eq!(tstz_value, "2018-01-26 18:30:09.453000");

        let ts_value = datum_to_json_object(
            &Field {
                data_type: DataType::Timestamp,
                ..mock_field.clone()
            },
            Some(
                ScalarImpl::Timestamp(Timestamp::from_timestamp_uncheck(1000, 0))
                    .as_scalar_ref_impl(),
            ),
            TimestampHandlingMode::Milli,
            &CustomJsonType::None,
        )
        .unwrap();
        assert_eq!(ts_value, json!(1000 * 1000));

        let ts_value = datum_to_json_object(
            &Field {
                data_type: DataType::Timestamp,
                ..mock_field.clone()
            },
            Some(
                ScalarImpl::Timestamp(Timestamp::from_timestamp_uncheck(1000, 0))
                    .as_scalar_ref_impl(),
            ),
            TimestampHandlingMode::String,
            &CustomJsonType::None,
        )
        .unwrap();
        assert_eq!(ts_value, json!("1970-01-01 00:16:40.000000".to_string()));

        // Represents the number of microseconds past midnigh, io.debezium.time.Time
        let time_value = datum_to_json_object(
            &Field {
                data_type: DataType::Time,
                ..mock_field.clone()
            },
            Some(
                ScalarImpl::Time(Time::from_num_seconds_from_midnight_uncheck(1000, 0))
                    .as_scalar_ref_impl(),
            ),
            TimestampHandlingMode::String,
            &CustomJsonType::None,
        )
        .unwrap();
        assert_eq!(time_value, json!(1000 * 1000));

        let interval_value = datum_to_json_object(
            &Field {
                data_type: DataType::Interval,
                ..mock_field.clone()
            },
            Some(
                ScalarImpl::Interval(Interval::from_month_day_usec(13, 2, 1000000))
                    .as_scalar_ref_impl(),
            ),
            TimestampHandlingMode::String,
            &CustomJsonType::None,
        )
        .unwrap();
        assert_eq!(interval_value, json!("P1Y1M2DT0H0M1S"));

        let mut map = HashMap::default();
        map.insert("aaa".to_string(), (10_u8, 5_u8));
        let decimal = datum_to_json_object(
            &Field {
                data_type: DataType::Decimal,
                name: "aaa".to_string(),
                ..mock_field.clone()
            },
            Some(ScalarImpl::Decimal(Decimal::try_from(1.1111111).unwrap()).as_scalar_ref_impl()),
            TimestampHandlingMode::String,
            &CustomJsonType::Doris(map),
        )
        .unwrap();
        assert_eq!(decimal, json!("1.11111"));

        let date_value = datum_to_json_object(
            &Field {
                data_type: DataType::Date,
                ..mock_field.clone()
            },
            Some(ScalarImpl::Date(Date::from_ymd_uncheck(2010, 10, 10)).as_scalar_ref_impl()),
            TimestampHandlingMode::String,
            &CustomJsonType::Doris(HashMap::default()),
        )
        .unwrap();
        assert_eq!(date_value, json!("2010-10-10"));

        let value = StructValue::new(vec![
            Some(3_i32.to_scalar_value()),
            Some(2_i32.to_scalar_value()),
            Some(1_i32.to_scalar_value()),
        ]);

        let interval_value = datum_to_json_object(
            &Field {
                data_type: DataType::Struct(StructType::new(vec![
                    ("v3", DataType::Int32),
                    ("v2", DataType::Int32),
                    ("v1", DataType::Int32),
                ])),
                ..mock_field.clone()
            },
            Some(ScalarRefImpl::Struct(StructRef::ValueRef { val: &value })),
            TimestampHandlingMode::String,
            &CustomJsonType::Doris(HashMap::default()),
        )
        .unwrap();
        assert_eq!(interval_value, json!("{\"v3\":3,\"v2\":2,\"v1\":1}"));
    }
}

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

use chrono::{NaiveDate, Utc};
use risingwave_common::catalog::Schema;
use risingwave_common::types::{
    DataType, Date, Datum, Decimal, Interval, JsonbVal, ListValue, ScalarImpl, Time, Timestamp,
    Timestamptz,
};
use rust_decimal::Decimal as RustDecimal;

use crate::source::cdc::external::ConnectorResult;

macro_rules! handle_data_type {
    ($row:expr, $i:expr, $type:ty, $builder:expr) => {
        let v = $row.try_get::<_, Option<Vec<$type>>>($i)?;
        v.map(|vec| {
            vec.into_iter()
                .for_each(|val| $builder.append(Some(ScalarImpl::from(val))))
        });
    };
    ($row:expr, $i:expr, $type:ty, $builder:expr, $rw_type:ty) => {
        let v = $row.try_get::<_, Option<Vec<$type>>>($i)?;
        v.map(|vec| {
            vec.into_iter()
                .for_each(|val| $builder.append(Some(ScalarImpl::from(<$rw_type>::from(val)))))
        });
    };
}

pub fn postgres_row_to_datums(
    row: tokio_postgres::Row,
    schema: &Schema,
) -> ConnectorResult<Vec<Datum>> {
    let mut datums = vec![];
    for i in 0..schema.fields.len() {
        let rw_field = &schema.fields[i];
        let datum = {
            match &rw_field.data_type {
                DataType::Boolean => {
                    let v = row.try_get::<_, Option<bool>>(i)?;
                    v.map(ScalarImpl::from)
                }
                DataType::Int16 => {
                    let v = row.try_get::<_, Option<i16>>(i)?;
                    v.map(ScalarImpl::from)
                }
                DataType::Int32 => {
                    let v = row.try_get::<_, Option<i32>>(i)?;
                    v.map(ScalarImpl::from)
                }
                DataType::Int64 => {
                    let v = row.try_get::<_, Option<i64>>(i)?;
                    v.map(ScalarImpl::from)
                }
                DataType::Float32 => {
                    let v = row.try_get::<_, Option<f32>>(i)?;
                    v.map(ScalarImpl::from)
                }
                DataType::Float64 => {
                    let v = row.try_get::<_, Option<f64>>(i)?;
                    v.map(ScalarImpl::from)
                }
                DataType::Decimal => {
                    let v = row.try_get::<_, Option<RustDecimal>>(i)?;
                    v.map(|v| ScalarImpl::from(Decimal::from(v)))
                }
                DataType::Varchar => {
                    let v = row.try_get::<_, Option<String>>(i)?;
                    v.map(ScalarImpl::from)
                }
                DataType::Date => {
                    let v = row.try_get::<_, Option<NaiveDate>>(i)?;
                    v.map(|v| ScalarImpl::from(Date::from(v)))
                }
                DataType::Time => {
                    let v = row.try_get::<_, Option<chrono::NaiveTime>>(i)?;
                    v.map(|v| ScalarImpl::from(Time::from(v)))
                }
                DataType::Timestamp => {
                    let v = row.try_get::<_, Option<chrono::NaiveDateTime>>(i)?;
                    v.map(|v| ScalarImpl::from(Timestamp::from(v)))
                }
                DataType::Timestamptz => {
                    let v = row.try_get::<_, Option<chrono::DateTime<Utc>>>(i)?;
                    v.map(|v| ScalarImpl::from(Timestamptz::from(v)))
                }
                DataType::Bytea => {
                    let v = row.try_get::<_, Option<Vec<u8>>>(i)?;
                    v.map(|v| ScalarImpl::from(v.into_boxed_slice()))
                }
                DataType::Jsonb => {
                    let v = row.try_get::<_, Option<serde_json::Value>>(i)?;
                    v.map(|v| ScalarImpl::from(JsonbVal::from(v)))
                }
                DataType::Interval => {
                    let v = row.try_get::<_, Option<Interval>>(i)?;
                    v.map(ScalarImpl::from)
                }
                DataType::List(dtype) => {
                    let mut builder = dtype.create_array_builder(0);
                    match **dtype {
                        DataType::Boolean => {
                            handle_data_type!(row, i, bool, builder);
                        }
                        DataType::Int16 => {
                            handle_data_type!(row, i, i16, builder);
                        }
                        DataType::Int32 => {
                            handle_data_type!(row, i, i32, builder);
                        }
                        DataType::Int64 => {
                            handle_data_type!(row, i, i64, builder);
                        }
                        DataType::Float32 => {
                            handle_data_type!(row, i, f32, builder);
                        }
                        DataType::Float64 => {
                            handle_data_type!(row, i, f64, builder);
                        }
                        DataType::Decimal => {
                            handle_data_type!(row, i, RustDecimal, builder, Decimal);
                        }
                        DataType::Date => {
                            handle_data_type!(row, i, NaiveDate, builder, Date);
                        }
                        DataType::Varchar => {
                            handle_data_type!(row, i, String, builder);
                        }
                        DataType::Time => {
                            handle_data_type!(row, i, chrono::NaiveTime, builder, Time);
                        }
                        DataType::Timestamp => {
                            handle_data_type!(row, i, chrono::NaiveDateTime, builder, Timestamp);
                        }
                        DataType::Timestamptz => {
                            handle_data_type!(row, i, chrono::DateTime<Utc>, builder, Timestamptz);
                        }
                        DataType::Interval => {
                            handle_data_type!(row, i, Interval, builder);
                        }
                        DataType::Jsonb => {
                            handle_data_type!(row, i, serde_json::Value, builder, JsonbVal);
                        }
                        DataType::Bytea => {
                            let v = row.try_get::<_, Option<Vec<Vec<u8>>>>(i)?;
                            if let Some(vec) = v {
                                vec.into_iter().for_each(|val| {
                                    builder.append(Some(ScalarImpl::from(val.into_boxed_slice())))
                                })
                            }
                        }
                        DataType::Struct(_)
                        | DataType::List(_)
                        | DataType::Serial
                        | DataType::Int256 => {
                            tracing::warn!(
                                "unsupported List data type {:?}, set the List to empty",
                                **dtype
                            );
                        }
                    };

                    Some(ScalarImpl::from(ListValue::new(builder.finish())))
                }
                DataType::Struct(_) | DataType::Int256 | DataType::Serial => {
                    // Interval, Struct, List, Int256 are not supported
                    tracing::warn!(rw_field.name, ?rw_field.data_type, "unsupported data type, set to null");
                    None
                }
            }
        };
        datums.push(datum);
    }

    Ok(datums)
}

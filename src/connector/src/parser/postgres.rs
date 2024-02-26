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

use std::sync::LazyLock;

use chrono::{NaiveDate, Utc};
use risingwave_common::catalog::Schema;
use risingwave_common::log::LogSuppresser;
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{
    DataType, Date, Decimal, Interval, JsonbVal, ListValue, ScalarImpl, Time, Timestamp,
    Timestamptz,
};
use rust_decimal::Decimal as RustDecimal;
use thiserror_ext::AsReport;
use tokio_postgres::types::Type;

static LOG_SUPPERSSER: LazyLock<LogSuppresser> = LazyLock::new(LogSuppresser::default);

macro_rules! handle_list_data_type {
    ($row:expr, $i:expr, $name:expr, $type:ty, $builder:expr) => {
        let res = $row.try_get::<_, Option<Vec<$type>>>($i);
        match res {
            Ok(val) => {
                if let Some(v) = val {
                    v.into_iter()
                        .for_each(|val| $builder.append(Some(ScalarImpl::from(val))))
                }
            }
            Err(err) => {
                if let Ok(sc) = LOG_SUPPERSSER.check() {
                    tracing::error!(
                        "parse column \"{}\" fail: {} ({} suppressed)",
                        $name,
                        err,
                        sc
                    );
                }
            }
        }
    };
    ($row:expr, $i:expr, $name:expr, $type:ty, $builder:expr, $rw_type:ty) => {
        let res = $row.try_get::<_, Option<Vec<$type>>>($i);
        match res {
            Ok(val) => {
                if let Some(v) = val {
                    v.into_iter().for_each(|val| {
                        $builder.append(Some(ScalarImpl::from(<$rw_type>::from(val))))
                    })
                }
            }
            Err(err) => {
                if let Ok(sc) = LOG_SUPPERSSER.check() {
                    tracing::error!(
                        "parse column \"{}\" fail: {} ({} suppressed)",
                        $name,
                        err,
                        sc
                    );
                }
            }
        }
    };
}

macro_rules! handle_data_type {
    ($row:expr, $i:expr, $name:expr, $type:ty) => {{
        let res = $row.try_get::<_, Option<$type>>($i);
        match res {
            Ok(val) => val.map(|v| ScalarImpl::from(v)),
            Err(err) => {
                if let Ok(sc) = LOG_SUPPERSSER.check() {
                    tracing::error!(
                        "parse column \"{}\" fail: {} ({} suppressed)",
                        $name,
                        err,
                        sc
                    );
                }
                None
            }
        }
    }};
    ($row:expr, $i:expr, $name:expr, $type:ty, $rw_type:ty) => {{
        let res = $row.try_get::<_, Option<$type>>($i);
        match res {
            Ok(val) => val.map(|v| ScalarImpl::from(<$rw_type>::from(v))),
            Err(err) => {
                if let Ok(sc) = LOG_SUPPERSSER.check() {
                    tracing::error!(
                        "parse column \"{}\" fail: {} ({} suppressed)",
                        $name,
                        err,
                        sc
                    );
                }
                None
            }
        }
    }};
}

pub fn postgres_row_to_owned_row(row: tokio_postgres::Row, schema: &Schema) -> OwnedRow {
    let mut datums = vec![];
    for i in 0..schema.fields.len() {
        let rw_field = &schema.fields[i];
        let name = rw_field.name.as_str();
        let datum = {
            match &rw_field.data_type {
                DataType::Boolean => {
                    handle_data_type!(row, i, name, bool)
                }
                DataType::Int16 => {
                    handle_data_type!(row, i, name, i16)
                }
                DataType::Int32 => {
                    handle_data_type!(row, i, name, i32)
                }
                DataType::Int64 => {
                    handle_data_type!(row, i, name, i64)
                }
                DataType::Float32 => {
                    handle_data_type!(row, i, name, f32)
                }
                DataType::Float64 => {
                    handle_data_type!(row, i, name, f64)
                }
                DataType::Decimal => {
                    handle_data_type!(row, i, name, RustDecimal, Decimal)
                }
                DataType::Varchar => {
                    match row.columns()[i].type_() {
                        // Since we don't support UUID natively, adapt it to a VARCHAR column
                        &Type::UUID => {
                            let res = row.try_get::<_, Option<uuid::Uuid>>(i);
                            match res {
                                Ok(val) => val.map(|v| ScalarImpl::from(v.to_string())),
                                Err(err) => {
                                    if let Ok(sc) = LOG_SUPPERSSER.check() {
                                        tracing::error!(
                                            suppressed_count = sc,
                                            column_name = name,
                                            error = %err.as_report(),
                                            "parse uuid column failed",
                                        );
                                    }
                                    None
                                }
                            }
                        }
                        _ => {
                            handle_data_type!(row, i, name, String)
                        }
                    }
                }
                DataType::Date => {
                    handle_data_type!(row, i, name, NaiveDate, Date)
                }
                DataType::Time => {
                    handle_data_type!(row, i, name, chrono::NaiveTime, Time)
                }
                DataType::Timestamp => {
                    handle_data_type!(row, i, name, chrono::NaiveDateTime, Timestamp)
                }
                DataType::Timestamptz => {
                    handle_data_type!(row, i, name, chrono::DateTime<Utc>, Timestamptz)
                }
                DataType::Bytea => {
                    let res = row.try_get::<_, Option<Vec<u8>>>(i);
                    match res {
                        Ok(val) => val.map(|v| ScalarImpl::from(v.into_boxed_slice())),
                        Err(err) => {
                            if let Ok(sc) = LOG_SUPPERSSER.check() {
                                tracing::error!(
                                    suppressed_count = sc,
                                    column_name = name,
                                    error = %err.as_report(),
                                    "parse column failed",
                                );
                            }
                            None
                        }
                    }
                }
                DataType::Jsonb => {
                    handle_data_type!(row, i, name, serde_json::Value, JsonbVal)
                }
                DataType::Interval => {
                    handle_data_type!(row, i, name, Interval)
                }
                DataType::List(dtype) => {
                    let mut builder = dtype.create_array_builder(0);
                    match **dtype {
                        DataType::Boolean => {
                            handle_list_data_type!(row, i, name, bool, builder);
                        }
                        DataType::Int16 => {
                            handle_list_data_type!(row, i, name, i16, builder);
                        }
                        DataType::Int32 => {
                            handle_list_data_type!(row, i, name, i32, builder);
                        }
                        DataType::Int64 => {
                            handle_list_data_type!(row, i, name, i64, builder);
                        }
                        DataType::Float32 => {
                            handle_list_data_type!(row, i, name, f32, builder);
                        }
                        DataType::Float64 => {
                            handle_list_data_type!(row, i, name, f64, builder);
                        }
                        DataType::Decimal => {
                            handle_list_data_type!(row, i, name, RustDecimal, builder, Decimal);
                        }
                        DataType::Date => {
                            handle_list_data_type!(row, i, name, NaiveDate, builder, Date);
                        }
                        DataType::Varchar => {
                            handle_list_data_type!(row, i, name, String, builder);
                        }
                        DataType::Time => {
                            handle_list_data_type!(row, i, name, chrono::NaiveTime, builder, Time);
                        }
                        DataType::Timestamp => {
                            handle_list_data_type!(
                                row,
                                i,
                                name,
                                chrono::NaiveDateTime,
                                builder,
                                Timestamp
                            );
                        }
                        DataType::Timestamptz => {
                            handle_list_data_type!(
                                row,
                                i,
                                name,
                                chrono::DateTime<Utc>,
                                builder,
                                Timestamptz
                            );
                        }
                        DataType::Interval => {
                            handle_list_data_type!(row, i, name, Interval, builder);
                        }
                        DataType::Jsonb => {
                            handle_list_data_type!(
                                row,
                                i,
                                name,
                                serde_json::Value,
                                builder,
                                JsonbVal
                            );
                        }
                        DataType::Bytea => {
                            let res = row.try_get::<_, Option<Vec<Vec<u8>>>>(i);
                            match res {
                                Ok(val) => {
                                    if let Some(v) = val {
                                        v.into_iter().for_each(|val| {
                                            builder.append(Some(ScalarImpl::from(
                                                val.into_boxed_slice(),
                                            )))
                                        })
                                    }
                                }
                                Err(err) => {
                                    if let Ok(sc) = LOG_SUPPERSSER.check() {
                                        tracing::error!(
                                            suppressed_count = sc,
                                            column_name = name,
                                            error = %err.as_report(),
                                            "parse column failed",
                                        );
                                    }
                                }
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
    OwnedRow::new(datums)
}

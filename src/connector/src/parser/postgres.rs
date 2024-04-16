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

use std::str::FromStr;
use std::sync::LazyLock;

use chrono::{NaiveDate, Utc};
use pg_bigdecimal::PgNumeric;
use risingwave_common::array::ArrayBuilderImpl;
use risingwave_common::catalog::Schema;
use risingwave_common::log::LogSuppresser;
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{
    DataType, Date, Decimal, Int256, Interval, JsonbVal, ListValue, ScalarImpl, Time, Timestamp,
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
                if let Ok(suppressed_count) = LOG_SUPPERSSER.check() {
                    tracing::error!(
                        column = $name,
                        error = %err.as_report(),
                        suppressed_count,
                        "parse column failed",
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
                if let Ok(suppressed_count) = LOG_SUPPERSSER.check() {
                    tracing::error!(
                        column = $name,
                        error = %err.as_report(),
                        suppressed_count,
                        "parse column failed",
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
                if let Ok(suppressed_count) = LOG_SUPPERSSER.check() {
                    tracing::error!(
                        column = $name,
                        error = %err.as_report(),
                        suppressed_count,
                        "parse column failed",
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
                if let Ok(suppressed_count) = LOG_SUPPERSSER.check() {
                    tracing::error!(
                        column = $name,
                        error = %err.as_report(),
                        suppressed_count,
                        "parse column failed",
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
                DataType::Int256 => postgres_decimal_to_rw_int256(&row, i, name, None),
                DataType::Varchar => {
                    match *row.columns()[i].type_() {
                        // Since we don't support UUID natively, adapt it to a VARCHAR column
                        Type::UUID => {
                            let res = row.try_get::<_, Option<uuid::Uuid>>(i);
                            match res {
                                Ok(val) => val.map(|v| ScalarImpl::from(v.to_string())),
                                Err(err) => {
                                    if let Ok(suppressed_count) = LOG_SUPPERSSER.check() {
                                        tracing::error!(
                                            suppressed_count,
                                            column = name,
                                            error = %err.as_report(),
                                            "parse uuid column failed",
                                        );
                                    }
                                    None
                                }
                            }
                        }
                        // support converting NUMERIC to VARCHAR implicitly
                        Type::NUMERIC => postgres_decimal_to_varchar(&row, i, name, None),
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
                            if let Ok(suppressed_count) = LOG_SUPPERSSER.check() {
                                tracing::error!(
                                    suppressed_count,
                                    column = name,
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
                            match *row.columns()[i].type_() {
                                // Since we don't support UUID natively, adapt it to a VARCHAR column
                                Type::UUID => {
                                    let res = row.try_get::<_, Option<uuid::Uuid>>(i);
                                    match res {
                                        Ok(val) => {
                                            if let Some(v) = val {
                                                builder
                                                    .append(Some(ScalarImpl::from(v.to_string())));
                                            }
                                        }
                                        Err(err) => {
                                            if let Ok(suppressed_count) = LOG_SUPPERSSER.check() {
                                                tracing::error!(
                                                    suppressed_count,
                                                    column = name,
                                                    error = %err.as_report(),
                                                    "parse uuid column failed",
                                                );
                                            }
                                        }
                                    };
                                }
                                // support converting NUMERIC to VARCHAR implicitly
                                Type::NUMERIC => {
                                    let _ = postgres_decimal_to_varchar(&row, i, name, None);
                                }
                                _ => {
                                    handle_list_data_type!(row, i, name, String, builder);
                                }
                            }
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
                                    if let Ok(suppressed_count) = LOG_SUPPERSSER.check() {
                                        tracing::error!(
                                            suppressed_count,
                                            column = name,
                                            error = %err.as_report(),
                                            "parse column failed",
                                        );
                                    }
                                }
                            }
                        }
                        DataType::Int256 => {
                            let _ =
                                postgres_decimal_to_rw_int256(&row, i, name, Some(&mut builder));
                        }
                        DataType::Struct(_) | DataType::List(_) | DataType::Serial => {
                            tracing::warn!(
                                "unsupported List data type {:?}, set the List to empty",
                                **dtype
                            );
                        }
                    };

                    Some(ScalarImpl::from(ListValue::new(builder.finish())))
                }
                DataType::Struct(_) | DataType::Serial => {
                    // Interval, Struct, List are not supported
                    tracing::warn!(rw_field.name, ?rw_field.data_type, "unsupported data type, set to null");
                    None
                }
            }
        };
        datums.push(datum);
    }
    OwnedRow::new(datums)
}

fn postgres_decimal_to_string(row: &tokio_postgres::Row, idx: usize, name: &str) -> Option<String> {
    let res = row.try_get::<_, Option<PgNumeric>>(idx);
    match res {
        Ok(val) => {
            if let Some(PgNumeric {
                n: Some(big_decimal),
            }) = val
            {
                Some(big_decimal.to_string())
            } else {
                None
            }
        }
        Err(err) => {
            if let Ok(suppressed_count) = LOG_SUPPERSSER.check() {
                tracing::error!(
                    column = name,
                    error = %err.as_report(),
                    suppressed_count,
                    "parse column failed",
                );
            }
            None
        }
    }
}

fn postgres_decimal_to_rw_int256(
    row: &tokio_postgres::Row,
    idx: usize,
    name: &str,
    builder: Option<&mut ArrayBuilderImpl>,
) -> Option<ScalarImpl> {
    // Currently in order to handle the decimal beyond RustDecimal,
    // we use the PgNumeric type to convert the decimal to a string.
    // Then we convert the string to Int256.
    let string = postgres_decimal_to_string(row, idx, name)?;
    match Int256::from_str(string.as_str()) {
        Ok(num) => {
            if let Some(builder) = builder {
                builder.append(Some(ScalarImpl::from(num)));
                None
            } else {
                Some(ScalarImpl::from(num))
            }
        }
        Err(err) => {
            if let Ok(suppressed_count) = LOG_SUPPERSSER.check() {
                tracing::error!(
                    column = name,
                    error = %err.as_report(),
                    suppressed_count,
                    "parse column failed",
                );
            }
            None
        }
    }
}

fn postgres_decimal_to_varchar(
    row: &tokio_postgres::Row,
    idx: usize,
    name: &str,
    builder: Option<&mut ArrayBuilderImpl>,
) -> Option<ScalarImpl> {
    // Currently in order to handle the decimal beyond RustDecimal,
    // we use the PgNumeric type to convert the decimal to a string.
    let string = postgres_decimal_to_string(row, idx, name)?;

    if let Some(builder) = builder {
        builder.append(Some(ScalarImpl::from(string)));
        None
    } else {
        Some(ScalarImpl::from(string))
    }
}

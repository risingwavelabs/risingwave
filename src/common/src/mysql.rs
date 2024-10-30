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

use anyhow::anyhow;
use chrono::NaiveDate;
use mysql_async::Row as MysqlRow;
use risingwave_common::types::{
    DataType, Date, Decimal, JsonbVal, ScalarImpl, Time, Timestamp, Timestamptz,
};
use rust_decimal::Decimal as RustDecimal;

use crate::types::Datum;

macro_rules! handle_data_type {
    ($row:expr, $i:expr, $name:expr, $type:ty) => {{
        match $row.take_opt::<Option<$type>, _>($i) {
            None => bail!("no value found at column: {}, index: {}", $name, $i),
            Some(Ok(val)) => Ok(val.map(|v| ScalarImpl::from(v))),
            Some(Err(e)) => Err(anyhow::Error::new(e.clone())
                .context("failed to deserialize MySQL value into rust value")
                .context(format!(
                    "column: {}, index: {}, rust_type: {}",
                    $name,
                    $i,
                    stringify!($type),
                ))),
        }
    }};
    ($row:expr, $i:expr, $name:expr, $type:ty, $rw_type:ty) => {{
        match $row.take_opt::<Option<$type>, _>($i) {
            None => bail!("no value found at column: {}, index: {}", $name, $i),
            Some(Ok(val)) => Ok(val.map(|v| ScalarImpl::from(<$rw_type>::from(v)))),
            Some(Err(e)) => Err(anyhow::Error::new(e.clone())
                .context("failed to deserialize MySQL value into rust value")
                .context(format!(
                    "column: {}, index: {}, rust_type: {}",
                    $name,
                    $i,
                    stringify!($ty),
                ))),
        }
    }};
}

/// The decoding result can be interpreted as follows:
/// Ok(value) => The value was found and successfully decoded.
/// Err(error) => The value was found but could not be decoded,
///               either because it was not supported,
///               or there was an error during conversion.
pub fn mysql_datum_to_rw_datum(
    mysql_row: &mut MysqlRow,
    mysql_datum_index: usize,
    column_name: &str,
    rw_data_type: &DataType,
) -> Result<Datum, anyhow::Error> {
    match rw_data_type {
        DataType::Boolean => {
            // Bit(1)
            match mysql_row.take_opt::<Option<Vec<u8>>, _>(mysql_datum_index) {
                None => bail!(
                    "no value found at column: {}, index: {}",
                    column_name,
                    mysql_datum_index
                ),
                Some(Ok(val)) => match val {
                    None => Ok(None),
                    Some(val) => match val.as_slice() {
                        [0] => Ok(Some(ScalarImpl::from(false))),
                        [1] => Ok(Some(ScalarImpl::from(true))),
                        _ => Err(anyhow!("invalid value for boolean: {:?}", val)),
                    },
                },
                Some(Err(e)) => Err(anyhow::Error::new(e.clone())
                    .context("failed to deserialize MySQL value into rust value")
                    .context(format!(
                        "column: {}, index: {}, rust_type: Vec<u8>",
                        column_name, mysql_datum_index,
                    ))),
            }
        }
        DataType::Int16 => {
            handle_data_type!(mysql_row, mysql_datum_index, column_name, i16)
        }
        DataType::Int32 => {
            handle_data_type!(mysql_row, mysql_datum_index, column_name, i32)
        }
        DataType::Int64 => {
            handle_data_type!(mysql_row, mysql_datum_index, column_name, i64)
        }
        DataType::Float32 => {
            handle_data_type!(mysql_row, mysql_datum_index, column_name, f32)
        }
        DataType::Float64 => {
            handle_data_type!(mysql_row, mysql_datum_index, column_name, f64)
        }
        DataType::Decimal => {
            handle_data_type!(
                mysql_row,
                mysql_datum_index,
                column_name,
                RustDecimal,
                Decimal
            )
        }
        DataType::Varchar => {
            handle_data_type!(mysql_row, mysql_datum_index, column_name, String)
        }
        DataType::Date => {
            handle_data_type!(mysql_row, mysql_datum_index, column_name, NaiveDate, Date)
        }
        DataType::Time => {
            handle_data_type!(
                mysql_row,
                mysql_datum_index,
                column_name,
                chrono::NaiveTime,
                Time
            )
        }
        DataType::Timestamp => {
            handle_data_type!(
                mysql_row,
                mysql_datum_index,
                column_name,
                chrono::NaiveDateTime,
                Timestamp
            )
        }
        DataType::Timestamptz => {
            match mysql_row.take_opt::<Option<chrono::NaiveDateTime>, _>(mysql_datum_index) {
                None => bail!(
                    "no value found at column: {}, index: {}",
                    column_name,
                    mysql_datum_index
                ),
                Some(Ok(val)) => Ok(val.map(|v| {
                    ScalarImpl::from(Timestamptz::from_micros(v.and_utc().timestamp_micros()))
                })),
                Some(Err(err)) => Err(anyhow::Error::new(err.clone())
                    .context("failed to deserialize MySQL value into rust value")
                    .context(format!(
                        "column: {}, index: {}, rust_type: chrono::NaiveDateTime",
                        column_name, mysql_datum_index,
                    ))),
            }
        }
        DataType::Bytea => match mysql_row.take_opt::<Option<Vec<u8>>, _>(mysql_datum_index) {
            None => bail!(
                "no value found at column: {}, index: {}",
                column_name,
                mysql_datum_index
            ),
            Some(Ok(val)) => Ok(val.map(ScalarImpl::from)),
            Some(Err(err)) => Err(anyhow::Error::new(err.clone())
                .context("failed to deserialize MySQL value into rust value")
                .context(format!(
                    "column: {}, index: {}, rust_type: Vec<u8>",
                    column_name, mysql_datum_index,
                ))),
        },
        DataType::Jsonb => {
            handle_data_type!(
                mysql_row,
                mysql_datum_index,
                column_name,
                serde_json::Value,
                JsonbVal
            )
        }
        DataType::Interval
        | DataType::Struct(_)
        | DataType::List(_)
        | DataType::Int256
        | DataType::Serial
        | DataType::Map(_) => Err(anyhow!(
            "unsupported data type: {}, set to null",
            rw_data_type
        )),
    }
}

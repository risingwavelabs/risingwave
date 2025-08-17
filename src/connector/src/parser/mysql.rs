// Copyright 2025 RisingWave Labs
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

use mysql_async::Row as MysqlRow;
use risingwave_common::catalog::Schema;
use risingwave_common::log::LogSuppresser;
use risingwave_common::row::OwnedRow;
use thiserror_ext::AsReport;

use crate::parser::utils::log_error;

static LOG_SUPPERSSER: LazyLock<LogSuppresser> = LazyLock::new(LogSuppresser::default);
use anyhow::anyhow;
use chrono::NaiveDate;
use risingwave_common::bail;
use risingwave_common::types::{
    DataType, Date, Datum, Decimal, JsonbVal, ScalarImpl, Time, Timestamp, Timestamptz,
};
use rust_decimal::Decimal as RustDecimal;

macro_rules! handle_data_type {
    ($row:expr, $i:expr, $name:expr, $typ:ty) => {{
        match $row.take_opt::<Option<$typ>, _>($i) {
            None => bail!("no value found at column: {}, index: {}", $name, $i),
            Some(Ok(val)) => Ok(val.map(|v| ScalarImpl::from(v))),
            Some(Err(e)) => Err(anyhow::Error::new(e.clone())
                .context("failed to deserialize MySQL value into rust value")
                .context(format!(
                    "column: {}, index: {}, rust_type: {}",
                    $name,
                    $i,
                    stringify!($typ),
                ))),
        }
    }};
    ($row:expr, $i:expr, $name:expr, $typ:ty, $rw_type:ty) => {{
        match $row.take_opt::<Option<$typ>, _>($i) {
            None => bail!("no value found at column: {}, index: {}", $name, $i),
            Some(Ok(val)) => Ok(val.map(|v| ScalarImpl::from(<$rw_type>::from(v)))),
            Some(Err(e)) => Err(anyhow::Error::new(e.clone())
                .context("failed to deserialize MySQL value into rw value")
                .context(format!(
                    "column: {}, index: {}, rw_type: {}",
                    $name,
                    $i,
                    stringify!($rw_type),
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
            // TinyInt(1) is used to represent boolean in MySQL
            // This handles backwards compatibility,
            // before https://github.com/risingwavelabs/risingwave/pull/19071
            // we permit boolean and tinyint(1) to be equivalent to boolean in RW.
            if let Some(Ok(val)) = mysql_row.get_opt::<Option<bool>, _>(mysql_datum_index) {
                let _ = mysql_row.take::<bool, _>(mysql_datum_index);
                return Ok(val.map(ScalarImpl::from));
            }
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
        DataType::Vector(_) => todo!("VECTOR_PLACEHOLDER"),
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

pub fn mysql_row_to_owned_row(mysql_row: &mut MysqlRow, schema: &Schema) -> OwnedRow {
    let mut datums = vec![];
    for i in 0..schema.fields.len() {
        let rw_field = &schema.fields[i];
        let name = rw_field.name.as_str();
        let datum = match mysql_datum_to_rw_datum(mysql_row, i, name, &rw_field.data_type) {
            Ok(val) => val,
            Err(e) => {
                log_error!(name, e, "parse column failed");
                None
            }
        };
        datums.push(datum);
    }
    OwnedRow::new(datums)
}

#[cfg(test)]
mod tests {

    use futures::pin_mut;
    use mysql_async::Row as MySqlRow;
    use mysql_async::prelude::*;
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::row::Row;
    use risingwave_common::types::DataType;
    use tokio_stream::StreamExt;

    use crate::parser::mysql_row_to_owned_row;

    // manual test case
    #[ignore]
    #[tokio::test]
    async fn test_convert_mysql_row_to_owned_row() {
        let pool = mysql_async::Pool::new("mysql://root:123456@localhost:8306/mydb");

        let t1schema = Schema::new(vec![
            Field::with_name(DataType::Int32, "v1"),
            Field::with_name(DataType::Int32, "v2"),
            Field::with_name(DataType::Timestamptz, "v3"),
        ]);

        let mut conn = pool.get_conn().await.unwrap();
        conn.exec_drop("SET time_zone = \"+08:00\"", ())
            .await
            .unwrap();

        let mut result_set = conn.query_iter("SELECT * FROM `t1m`").await.unwrap();
        let s = result_set.stream::<MySqlRow>().await.unwrap().unwrap();
        let row_stream = s.map(|row| {
            // convert mysql row into OwnedRow
            let mut mysql_row = row.unwrap();
            Ok::<_, anyhow::Error>(Some(mysql_row_to_owned_row(&mut mysql_row, &t1schema)))
        });
        pin_mut!(row_stream);
        while let Some(row) = row_stream.next().await {
            if let Ok(ro) = row
                && ro.is_some()
            {
                let owned_row = ro.unwrap();
                let d = owned_row.datum_at(2);
                if let Some(scalar) = d {
                    let v = scalar.into_timestamptz();
                    println!("timestamp: {:?}", v);
                }
            }
        }
    }
}

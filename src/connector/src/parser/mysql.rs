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

use std::sync::LazyLock;

use chrono::NaiveDate;
use mysql_async::Row as MysqlRow;
use risingwave_common::catalog::Schema;
use risingwave_common::log::LogSuppresser;
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{
    DataType, Date, Decimal, JsonbVal, ScalarImpl, Time, Timestamp, Timestamptz,
};
use rust_decimal::Decimal as RustDecimal;

macro_rules! handle_data_type {
    ($row:expr, $i:expr, $name:expr, $type:ty) => {{
        let res = $row.take_opt::<Option<$type>, _>($i).unwrap_or(Ok(None));
        match res {
            Ok(val) => val.map(|v| ScalarImpl::from(v)),
            Err(err) => {
                tracing::error!("parse column `{}` fail: {}", $name, err);
                None
            }
        }
    }};
    ($row:expr, $i:expr, $name:expr, $type:ty, $rw_type:ty) => {{
        let res = $row.take_opt::<Option<$type>, _>($i).unwrap_or(Ok(None));
        match res {
            Ok(val) => val.map(|v| ScalarImpl::from(<$rw_type>::from(v))),
            Err(err) => {
                tracing::error!("parse column `{}` fail: {}", $name, err);
                None
            }
        }
    }};
}

pub fn mysql_row_to_datums(mysql_row: &mut MysqlRow, schema: &Schema) -> OwnedRow {
    let mut datums = vec![];
    for i in 0..schema.fields.len() {
        let rw_field = &schema.fields[i];
        let name = rw_field.name.as_str();
        let datum = {
            match rw_field.data_type {
                DataType::Boolean => {
                    handle_data_type!(mysql_row, i, name, bool)
                }
                DataType::Int16 => {
                    handle_data_type!(mysql_row, i, name, i16)
                }
                DataType::Int32 => {
                    handle_data_type!(mysql_row, i, name, i32)
                }
                DataType::Int64 => {
                    handle_data_type!(mysql_row, i, name, i64)
                }
                DataType::Float32 => {
                    handle_data_type!(mysql_row, i, name, f32)
                }
                DataType::Float64 => {
                    handle_data_type!(mysql_row, i, name, f64)
                }
                DataType::Decimal => {
                    handle_data_type!(mysql_row, i, name, RustDecimal, Decimal)
                }
                DataType::Varchar => {
                    handle_data_type!(mysql_row, i, name, String)
                }
                DataType::Date => {
                    handle_data_type!(mysql_row, i, name, NaiveDate, Date)
                }
                DataType::Time => {
                    handle_data_type!(mysql_row, i, name, chrono::NaiveTime, Time)
                }
                DataType::Timestamp => {
                    handle_data_type!(mysql_row, i, name, chrono::NaiveDateTime, Timestamp)
                }
                DataType::Timestamptz => {
                    let res = mysql_row
                        .take_opt::<Option<chrono::NaiveDateTime>, _>(i)
                        .unwrap_or(Ok(None));
                    match res {
                        Ok(val) => val.map(|v| {
                            ScalarImpl::from(Timestamptz::from_micros(v.timestamp_micros()))
                        }),
                        Err(err) => {
                            tracing::error!("parse column `{}` fail: {}", name, err);
                            None
                        }
                    }
                }
                DataType::Bytea => {
                    let res = mysql_row
                        .take_opt::<Option<Vec<u8>>, _>(i)
                        .unwrap_or(Ok(None));
                    match res {
                        Ok(val) => val.map(|v| ScalarImpl::from(v.into_boxed_slice())),
                        Err(err) => {
                            tracing::error!("parse column `{}` fail: {}", name, err);
                            None
                        }
                    }
                }
                DataType::Jsonb => {
                    handle_data_type!(mysql_row, i, name, serde_json::Value, JsonbVal)
                }
                DataType::Interval
                | DataType::Struct(_)
                | DataType::List(_)
                | DataType::Int256
                | DataType::Serial => {
                    // Interval, Struct, List, Int256 are not supported
                    static LOG_SUPPERSSER: LazyLock<LogSuppresser> =
                        LazyLock::new(LogSuppresser::default);
                    if let Ok(suppressed_count) = LOG_SUPPERSSER.check() {
                        tracing::warn!(column = rw_field.name, ?rw_field.data_type, suppressed_count, "unsupported data type, set to null");
                    }
                    None
                }
            }
        };
        datums.push(datum);
    }
    OwnedRow::new(datums)
}

#[cfg(test)]
mod tests {

    use futures::pin_mut;
    use mysql_async::prelude::*;
    use mysql_async::Row as MySqlRow;
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::row::Row;
    use risingwave_common::types::{DataType, ToText};
    use tokio_stream::StreamExt;

    use crate::parser::mysql_row_to_datums;

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
            Ok::<_, anyhow::Error>(Some(mysql_row_to_datums(&mut mysql_row, &t1schema)))
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
                    println!("timestamp: {}", v.to_text());
                }
            }
        }
    }
}

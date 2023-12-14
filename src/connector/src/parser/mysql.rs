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

use chrono::NaiveDate;
use mysql_async::Row as MysqlRow;
use risingwave_common::catalog::Schema;
use risingwave_common::types::{
    DataType, Date, Datum, Decimal, JsonbVal, ScalarImpl, Time, Timestamp, Timestamptz,
};
use rust_decimal::Decimal as RustDecimal;

pub fn mysql_row_to_datums(mysql_row: &mut MysqlRow, schema: &Schema) -> Vec<Datum> {
    let mut datums = vec![];
    for i in 0..schema.fields.len() {
        let rw_field = &schema.fields[i];
        let datum = {
            match rw_field.data_type {
                DataType::Boolean => {
                    let v = mysql_row.take::<bool, _>(i);
                    v.map(ScalarImpl::from)
                }
                DataType::Int16 => {
                    let v = mysql_row.take::<i16, _>(i);
                    v.map(ScalarImpl::from)
                }
                DataType::Int32 => {
                    let v = mysql_row.take::<i32, _>(i);
                    v.map(ScalarImpl::from)
                }
                DataType::Int64 => {
                    let v = mysql_row.take::<i64, _>(i);
                    v.map(ScalarImpl::from)
                }
                DataType::Float32 => {
                    let v = mysql_row.take::<f32, _>(i);
                    v.map(ScalarImpl::from)
                }
                DataType::Float64 => {
                    let v = mysql_row.take::<f64, _>(i);
                    v.map(ScalarImpl::from)
                }
                DataType::Decimal => {
                    let v = mysql_row.take::<RustDecimal, _>(i);
                    v.map(|v| ScalarImpl::from(Decimal::from(v)))
                }
                DataType::Varchar => {
                    let v = mysql_row.take::<String, _>(i);
                    v.map(ScalarImpl::from)
                }
                DataType::Date => {
                    let v = mysql_row.take::<NaiveDate, _>(i);
                    v.map(|v| ScalarImpl::from(Date::from(v)))
                }
                DataType::Time => {
                    let v = mysql_row.take::<chrono::NaiveTime, _>(i);
                    v.map(|v| ScalarImpl::from(Time::from(v)))
                }
                DataType::Timestamp => {
                    let v = mysql_row.take::<chrono::NaiveDateTime, _>(i);
                    v.map(|v| ScalarImpl::from(Timestamp::from(v)))
                }
                DataType::Timestamptz => {
                    let v = mysql_row.take::<chrono::NaiveDateTime, _>(i);
                    v.map(|v| ScalarImpl::from(Timestamptz::from_micros(v.timestamp_micros())))
                }
                DataType::Bytea => {
                    let v = mysql_row.take::<Vec<u8>, _>(i);
                    v.map(|v| ScalarImpl::from(v.into_boxed_slice()))
                }
                DataType::Jsonb => {
                    let v = mysql_row.take::<serde_json::Value, _>(i);
                    v.map(|v| ScalarImpl::from(JsonbVal::from(v)))
                }
                DataType::Interval
                | DataType::Struct(_)
                | DataType::List(_)
                | DataType::Int256
                | DataType::Serial => {
                    // Interval, Struct, List, Int256 are not supported
                    tracing::warn!(rw_field.name, ?rw_field.data_type, "unsupported data type, set to null");
                    None
                }
            }
        };
        datums.push(datum);
    }
    datums
}

#[cfg(test)]
mod tests {

    use futures::pin_mut;
    use mysql_async::prelude::*;
    use mysql_async::Row as MySqlRow;
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::row::{OwnedRow, Row};
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
            let datums = mysql_row_to_datums(&mut mysql_row, &t1schema);
            Ok::<_, anyhow::Error>(Some(OwnedRow::new(datums)))
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

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

use std::borrow::Cow;

use chrono::NaiveDate;
use mysql_async::Row as MysqlRow;
use risingwave_common::catalog::{Schema, OFFSET_COLUMN_NAME};
use risingwave_common::types::{DataType, Date, Datum, Decimal, ScalarImpl, Time, Timestamp};
use rust_decimal::Decimal as RustDecimal;
use simd_json::{BorrowedValue, ValueAccess};

pub(crate) fn json_object_smart_get_value<'a, 'b>(
    v: &'b simd_json::BorrowedValue<'a>,
    key: Cow<'b, str>,
) -> Option<&'b BorrowedValue<'a>> {
    let obj = v.as_object()?;
    let value = obj.get(key.as_ref());
    if value.is_some() {
        return value;
    }
    for (k, v) in obj {
        if k.eq_ignore_ascii_case(key.as_ref()) {
            return Some(v);
        }
    }
    None
}

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
                    // snapshot data doesn't contain offset, just fill None
                    if rw_field.name.as_str() == OFFSET_COLUMN_NAME {
                        None
                    } else {
                        let v = mysql_row.take::<String, _>(i);
                        v.map(ScalarImpl::from)
                    }
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
                _ => unimplemented!("unsupported data type: {:?}", rw_field.data_type),
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
    use mysql_async::Row;
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::row::OwnedRow;
    use risingwave_common::types::{DataType, ScalarImpl};
    use tokio_stream::StreamExt;

    // manual test case
    #[ignore]
    #[tokio::test]
    async fn test_convert_mysql_row_to_owned_row() {
        let pool = mysql_async::Pool::new("mysql://root:123456@localhost:8306/mydb");

        let t1schema = Schema::new(vec![
            Field::with_name(DataType::Int32, "v1"),
            Field::with_name(DataType::Int32, "v2"),
        ]);

        let mut conn = pool.get_conn().await.unwrap();
        let mut result_set = conn.query_iter("SELECT * FROM `t1`").await.unwrap();
        let s = result_set.stream::<Row>().await.unwrap().unwrap();
        let row_stream = s.map(|row| {
            // convert mysql row into OwnedRow
            let mut mysql_row = row.unwrap();
            let mut datums = vec![];

            let _mysql_columns = mysql_row.columns_ref();
            for i in 0..mysql_row.len() {
                let rw_field = &t1schema.fields[i];
                let datum = match rw_field.data_type {
                    DataType::Int32 => {
                        let value = mysql_row.take::<i32, _>(i);
                        value.map(ScalarImpl::from)
                    }
                    _ => None,
                };
                datums.push(datum);
            }
            Ok::<_, anyhow::Error>(Some(OwnedRow::new(datums)))
        });
        pin_mut!(row_stream);
        while let Some(row) = row_stream.next().await {
            if let Ok(ro) = row && ro.is_some() {
                println!("OwnedRow {:?}", ro);
            }
        }
    }
}

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

use mysql_async::Row as MysqlRow;
use risingwave_common::catalog::Schema;
use risingwave_common::log::LogSuppresser;
use risingwave_common::row::OwnedRow;
use thiserror_ext::AsReport;

use crate::parser::util::log_error;

static LOG_SUPPERSSER: LazyLock<LogSuppresser> = LazyLock::new(LogSuppresser::default);
use risingwave_common::mysql::mysql_datum_to_rw_datum;

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
    use mysql_async::prelude::*;
    use mysql_async::Row as MySqlRow;
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

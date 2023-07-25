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

use anyhow::anyhow;
use futures::{pin_mut, Stream, StreamExt};
use futures_async_stream::try_stream;
use itertools::Itertools;
use mysql_async::prelude::*;
use risingwave_common::catalog::Schema;
use risingwave_common::error::RwError;
use risingwave_common::row::{OwnedRow, Row};

use crate::error::ConnectorError;
use crate::parser::mysql_row_to_datums;
use crate::source::cdc::CdcProperties;

pub type ConnectorResult<T> = std::result::Result<T, RwError>;

#[derive(Debug, Clone)]
pub struct SchemaTableName {
    pub schema_name: String,
    pub table_name: String,
}

pub trait ExternalTableReader {
    type SnapshotStream<'a>: Stream<Item = ConnectorResult<Option<OwnedRow>>> + Send + 'a
    where
        Self: 'a;

    fn get_normalized_table_name(table_name: &SchemaTableName) -> String;

    fn current_binlog_offset(&self) -> Option<String>;

    fn snapshot_read(
        &self,
        table_name: SchemaTableName,
        primary_keys: Vec<String>,
    ) -> Self::SnapshotStream<'_>;
}

#[derive(Debug)]
pub enum ExternalTableReaderImpl {
    MYSQL(MySqlExternalTableReader),
}

// todo(siyuan): embeded db client in the reader
#[derive(Debug)]
pub struct MySqlExternalTableReader {
    pool: mysql_async::Pool,
    rw_schema: Schema,
}

impl MySqlExternalTableReader {
    pub async fn new(cdc_props: CdcProperties) -> Self {
        let database_url = "mysql://root:123456@localhost:3306/mydb";
        let pool = mysql_async::Pool::new(database_url);
        Self {
            pool,
            rw_schema: cdc_props.schema(),
        }
    }
}

impl ExternalTableReader for MySqlExternalTableReader {
    type SnapshotStream<'a> = impl Stream<Item = ConnectorResult<Option<OwnedRow>>> + 'a;

    fn get_normalized_table_name(table_name: &SchemaTableName) -> String {
        format!("`{}`", table_name.table_name)
    }

    fn current_binlog_offset(&self) -> Option<String> {
        todo!()
    }

    fn snapshot_read(
        &self,
        table_name: SchemaTableName,
        primary_keys: Vec<String>,
    ) -> Self::SnapshotStream<'_> {
        #[try_stream]
        async move {
            let order_key = primary_keys.into_iter().join(",");
            let sql = format!(
                "SELECT * FROM {} ORDER BY {}",
                Self::get_normalized_table_name(&table_name),
                order_key
            );
            let mut conn = self
                .pool
                .get_conn()
                .await
                .map_err(|e| anyhow!("failed to get conn {}", e))?;
            let mut result_set = conn
                .query_iter(sql)
                .await
                .map_err(|e| anyhow!("fail to query {}", e))?;
            let mut rs_stream = result_set
                .stream::<mysql_async::Row>()
                .await
                .map_err(|e| anyhow!(e))?;
            if let Some(mut rs_stream) = rs_stream {
                let row_stream = rs_stream.map(|row| {
                    // convert mysql row into OwnedRow
                    let mut row = row.map_err(|e| anyhow!(e))?;
                    let datums = mysql_row_to_datums(&mut row, &self.rw_schema);
                    Ok::<_, RwError>(Some(OwnedRow::new(datums)))
                });

                pin_mut!(row_stream);
                #[for_await]
                for row in row_stream {
                    let row = row?;
                    yield row;
                }
            }
        }
    }
}

impl ExternalTableReader for ExternalTableReaderImpl {
    type SnapshotStream<'a> = impl Stream<Item = ConnectorResult<Option<OwnedRow>>> + 'a;

    fn get_normalized_table_name(table_name: &SchemaTableName) -> String {
        unimplemented!("get normalized table name")
    }

    fn current_binlog_offset(&self) -> Option<String> {
        unimplemented!("current binlog offset")
    }

    fn snapshot_read(
        &self,
        table_name: SchemaTableName,
        primary_keys: Vec<String>,
    ) -> Self::SnapshotStream<'_> {
        #[try_stream]
        async move {
            let stream = match self {
                ExternalTableReaderImpl::MYSQL(mysql) => {
                    mysql.snapshot_read(table_name, primary_keys)
                }
                _ => {
                    unreachable!("unsupported external table reader")
                }
            };

            pin_mut!(stream);
            #[for_await]
            for row in stream {
                let row = row?;
                yield row;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::{pin_mut, Stream, StreamExt};
    use futures_async_stream::for_await;
    use mysql_async::prelude::*;
    use risingwave_common::catalog::{ColumnDesc, ColumnId};
    use risingwave_common::types::DataType;

    use crate::sink::catalog::SinkType;
    use crate::sink::SinkParam;
    use crate::source::cdc::CdcProperties;
    use crate::source::external::{ExternalTableReader, MySqlExternalTableReader, SchemaTableName};

    // unit test for external table reader
    #[tokio::test]
    async fn test_mysql_table_reader() {
        let param = SinkParam {
            sink_id: Default::default(),
            properties: Default::default(),
            columns: vec![
                ColumnDesc::unnamed(ColumnId::new(1), DataType::Int32),
                ColumnDesc::unnamed(ColumnId::new(2), DataType::Int32),
            ],
            pk_indices: vec![0],
            sink_type: SinkType::AppendOnly,
        };
        let pb_sink = param.to_proto();
        let cdc_props = CdcProperties {
            source_type: "mysql".to_string(),
            props: Default::default(),
            table_schema: pb_sink.table_schema.unwrap(),
        };

        let reader = MySqlExternalTableReader::new(cdc_props).await;
        let table_name = SchemaTableName {
            schema_name: "mydb".to_string(),
            table_name: "t1".to_string(),
        };

        let stream = reader.snapshot_read(table_name, vec!["v1".to_string()]);
        pin_mut!(stream);
        // while let row = stream.next().await {
        //     println!("{:?}", row);
        // }
        #[for_await]
        for row in stream {
            println!("{:?}", row);
        }
    }
}

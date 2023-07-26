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

use std::future::Future;

use anyhow::anyhow;
use futures::{pin_mut, Stream, StreamExt};
use futures_async_stream::try_stream;
use itertools::Itertools;
use mysql_async::prelude::*;
use risingwave_common::catalog::Schema;
use risingwave_common::row::{OwnedRow, Row};
use serde_derive::Deserialize;

use crate::error::ConnectorError;
use crate::parser::mysql_row_to_datums;
use crate::source::cdc::CdcProperties;

pub type ConnectorResult<T> = std::result::Result<T, ConnectorError>;

#[derive(Debug, Clone)]
pub struct SchemaTableName {
    pub schema_name: String,
    pub table_name: String,
}

#[derive(Debug, Clone)]
pub struct BinlogOffset {
    pub filename: String,
    pub position: u64,
}

pub trait ExternalTableReader {
    type BinlogOffsetFuture<'a>: Future<Output = ConnectorResult<Option<BinlogOffset>>> + Send + 'a
    where
        Self: 'a;
    type RowStream<'a>: Stream<Item = ConnectorResult<Option<OwnedRow>>> + Send + 'a
    where
        Self: 'a;

    fn get_normalized_table_name(table_name: &SchemaTableName) -> String;

    fn current_binlog_offset(&self) -> Self::BinlogOffsetFuture<'_>;

    fn snapshot_read(
        &self,
        table_name: SchemaTableName,
        primary_keys: Vec<String>,
    ) -> Self::RowStream<'_>;
}

#[derive(Debug)]
pub enum ExternalTableReaderImpl {
    MYSQL(MySqlExternalTableReader),
}

// todo(siyuan): embeded db client in the reader
#[derive(Debug)]
pub struct MySqlExternalTableReader {
    pool: mysql_async::Pool,
    config: ExternalTableConfig,
    rw_schema: Schema,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExternalTableConfig {
    #[serde(rename = "hostname")]
    pub host: String,
    pub port: String,
    pub username: String,
    pub password: String,
    #[serde(rename = "database.name")]
    pub database: String,
    #[serde(rename = "schema.name", default = "Default::default")]
    pub schema: String,
    #[serde(rename = "table.name")]
    pub table: String,
}

impl MySqlExternalTableReader {
    pub async fn new(cdc_props: CdcProperties) -> ConnectorResult<Self> {
        let rw_schema = cdc_props.schema();
        let config = serde_json::from_value::<ExternalTableConfig>(
            serde_json::to_value(cdc_props.props).unwrap(),
        )
        .map_err(|e| ConnectorError::Config(anyhow!(e)))?;

        let database_url = format!(
            "mysql://{}:{}@{}:{}/{}",
            config.username, config.password, config.host, config.port, config.database
        );
        let pool = mysql_async::Pool::from_url(database_url)?;
        Ok(Self {
            pool,
            config,
            rw_schema,
        })
    }

    pub async fn disconnect(&self) -> ConnectorResult<()> {
        self.pool.clone().disconnect().await?;
        Ok(())
    }
}

impl ExternalTableReader for MySqlExternalTableReader {
    type BinlogOffsetFuture<'a> = impl Future<Output = ConnectorResult<Option<BinlogOffset>>> + 'a;
    type RowStream<'a> = impl Stream<Item = ConnectorResult<Option<OwnedRow>>> + 'a;

    fn get_normalized_table_name(table_name: &SchemaTableName) -> String {
        format!("`{}`", table_name.table_name)
    }

    fn current_binlog_offset(&self) -> Self::BinlogOffsetFuture<'_> {
        async move {
            let mut conn = self
                .pool
                .get_conn()
                .await
                .map_err(|e| ConnectorError::Connection(anyhow!(e)))?;

            let sql = format!("SHOW MASTER STATUS");
            let mut rs = conn.query::<mysql_async::Row, _>(sql).await?;
            let row = rs
                .iter_mut()
                .exactly_one()
                .map_err(|e| ConnectorError::Internal(anyhow!("read binlog error: {}", e)))?;

            Ok(Some(BinlogOffset {
                filename: row.take("File").unwrap(),
                position: row.take("Position").unwrap(),
            }))
        }
    }

    fn snapshot_read(
        &self,
        table_name: SchemaTableName,
        primary_keys: Vec<String>,
    ) -> Self::RowStream<'_> {
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
                .map_err(|e| ConnectorError::Connection(anyhow!(e)))?;
            let mut result_set = conn.query_iter(sql).await?;
            let mut rs_stream = result_set.stream::<mysql_async::Row>().await?;
            if let Some(mut rs_stream) = rs_stream {
                let row_stream = rs_stream.map(|row| {
                    // convert mysql row into OwnedRow
                    let mut row = row?;
                    let datums = mysql_row_to_datums(&mut row, &self.rw_schema);
                    Ok::<_, ConnectorError>(Some(OwnedRow::new(datums)))
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
    type BinlogOffsetFuture<'a> = impl Future<Output = ConnectorResult<Option<BinlogOffset>>> + 'a;
    type RowStream<'a> = impl Stream<Item = ConnectorResult<Option<OwnedRow>>> + 'a;

    fn get_normalized_table_name(table_name: &SchemaTableName) -> String {
        unimplemented!("get normalized table name")
    }

    fn current_binlog_offset(&self) -> Self::BinlogOffsetFuture<'_> {
        async move { Ok(None) }
    }

    fn snapshot_read(
        &self,
        table_name: SchemaTableName,
        primary_keys: Vec<String>,
    ) -> Self::RowStream<'_> {
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
    use std::collections::HashMap;

    use futures::{pin_mut, Stream, StreamExt};
    use futures_async_stream::for_await;
    use maplit::{convert_args, hashmap};
    use mysql_async::prelude::*;
    use risingwave_common::catalog::{ColumnDesc, ColumnId};
    use risingwave_common::types::DataType;

    use crate::sink::catalog::SinkType;
    use crate::sink::SinkParam;
    use crate::source::cdc::CdcProperties;
    use crate::source::external::{ExternalTableReader, MySqlExternalTableReader, SchemaTableName};

    #[ignore]
    #[tokio::test]
    async fn test_mysql_table_reader() {
        let param = SinkParam {
            sink_id: Default::default(),
            properties: Default::default(),
            columns: vec![
                ColumnDesc::unnamed(ColumnId::new(1), DataType::Int32),
                ColumnDesc::unnamed(ColumnId::new(2), DataType::Decimal),
                ColumnDesc::unnamed(ColumnId::new(3), DataType::Varchar),
                ColumnDesc::unnamed(ColumnId::new(4), DataType::Date),
            ],
            pk_indices: vec![0],
            sink_type: SinkType::AppendOnly,
        };
        let pb_sink = param.to_proto();
        let cdc_props = CdcProperties {
            source_type: "mysql".to_string(),
            props: convert_args!(hashmap!(
                "hostname" => "localhost",
                "port" => "8306",
                "username" => "root",
                "password" => "123456",
                "database.name" => "mydb",
                "table.name" => "t1")),
            table_schema: pb_sink.table_schema.unwrap(),
        };

        let reader = MySqlExternalTableReader::new(cdc_props).await.unwrap();
        let offset = reader.current_binlog_offset().await.unwrap();
        println!("BinlogOffset: {:?}", offset);

        let table_name = SchemaTableName {
            schema_name: "mydb".to_string(),
            table_name: "t1".to_string(),
        };

        let stream = reader.snapshot_read(table_name, vec!["v1".to_string()]);
        pin_mut!(stream);
        #[for_await]
        for row in stream {
            println!("OwnedRow: {:?}", row);
        }
    }
}

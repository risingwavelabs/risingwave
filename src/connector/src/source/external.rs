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

use std::collections::HashMap;
use std::future::Future;

use anyhow::anyhow;
use futures::stream::BoxStream;
use futures::{pin_mut, Stream, StreamExt};
use futures_async_stream::try_stream;
use itertools::Itertools;
use mysql_async::prelude::*;
use risingwave_common::bail;
use risingwave_common::catalog::Schema;
use risingwave_common::row::{OwnedRow, Row};
use risingwave_common::types::{Datum, ScalarImpl, F64};
use serde_derive::{Deserialize, Serialize};

use crate::error::ConnectorError;
use crate::parser::mysql_row_to_datums;
use crate::source::mock_external_table::MockExternalTableReader;

pub type ConnectorResult<T> = std::result::Result<T, ConnectorError>;

pub enum ExternalTableType {
    Undefined,
    MySQL,
    Postgres,
}

impl ExternalTableType {
    pub fn from_properties(properties: &HashMap<String, String>) -> Self {
        let connector = properties
            .get("connector")
            .map(|c| c.to_ascii_lowercase())
            .unwrap_or_default();
        match connector.as_str() {
            "mysql-cdc" => Self::MySQL,
            "postgres-cdc" => Self::Postgres,
            _ => Self::Undefined,
        }
    }

    pub fn can_backfill(&self) -> bool {
        match self {
            Self::MySQL => true,
            _ => false,
        }
    }

    pub fn create_table_reader(
        &self,
        properties: HashMap<String, String>,
        schema: Schema,
    ) -> ConnectorResult<ExternalTableReaderImpl> {
        match self {
            Self::MySQL => Ok(ExternalTableReaderImpl::MYSQL(
                MySqlExternalTableReader::new(properties, schema)?,
            )),
            _ => bail!(ConnectorError::Parse("Invalid connector")),
        }
    }
}

#[derive(Debug, Clone)]
pub struct SchemaTableName {
    pub schema_name: String,
    pub table_name: String,
}

const TABLE_NAME_KEY: &str = "table.name";
const SCHEMA_NAME_KEY: &str = "schema.name";

impl SchemaTableName {
    pub fn new(schema_name: String, table_name: String) -> Self {
        Self {
            schema_name,
            table_name,
        }
    }

    pub fn from_properties(properties: &HashMap<String, String>) -> Self {
        let table_name = properties
            .get(TABLE_NAME_KEY)
            .map(|c| c.to_ascii_lowercase())
            .unwrap_or_default();

        let schema_name = properties
            .get(SCHEMA_NAME_KEY)
            .map(|c| c.to_ascii_lowercase())
            .unwrap_or_default();

        Self {
            schema_name,
            table_name,
        }
    }
}

// TODO(siyuan): replace string offset with BinlogOffset
#[derive(Debug, Clone, Default, PartialEq, PartialOrd)]
pub struct MySqlOffset {
    pub filename: String,
    pub position: u64,
}

impl MySqlOffset {
    pub fn new(filename: String, position: u64) -> Self {
        Self { filename, position }
    }

    pub fn min() -> Self {
        Self {
            filename: "".to_string(),
            position: u64::MIN,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, PartialOrd)]
pub struct PostgresOffset {
    pub txid: u64,
    pub lsn: u64,
    pub tx_usec: u64,
}

#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub enum BinlogOffset {
    MySQL(MySqlOffset),
    Postgres(PostgresOffset),
}

// Example debezium offset for Postgres:
// {
//     "sourcePartition":
//     {
//         "server": "RW_CDC_public.te"
//     },
//     "sourceOffset":
//     {
//         "last_snapshot_record": false,
//         "lsn": 29973552,
//         "txId": 1046,
//         "ts_usec": 1670826189008456,
//         "snapshot": true
//     }
// }
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DebeziumOffset {
    #[serde(rename = "sourcePartition")]
    pub source_partition: HashMap<String, String>,
    #[serde(rename = "sourceOffset")]
    pub source_offset: DebeziumSourceOffset,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DebeziumSourceOffset {
    // postgres snapshot progress
    pub last_snapshot_record: Option<bool>,
    // mysql snapshot progress
    pub snapshot: Option<bool>,

    // mysql binlog offset
    pub file: Option<String>,
    pub pos: Option<u64>,

    // postgres binlog offset
    pub lsn: Option<u64>,
    #[serde(rename = "txId")]
    pub txid: Option<u64>,
    pub tx_usec: Option<u64>,
}

impl MySqlOffset {
    pub fn from_str(offset: &str) -> ConnectorResult<Self> {
        let dbz_offset: DebeziumOffset = serde_json::from_str(&offset).map_err(|e| {
            ConnectorError::Internal(anyhow!("invalid upstream offset: {}, error: {}", offset, e))
        })?;

        Ok(Self {
            filename: dbz_offset
                .source_offset
                .file
                .ok_or_else(|| anyhow!("binlog file not found in offset"))?,
            position: dbz_offset
                .source_offset
                .pos
                .ok_or_else(|| anyhow!("binlog position not found in offset"))?,
        })
    }
}

pub trait ExternalTableReader {
    type BinlogOffsetFuture<'a>: Future<Output = ConnectorResult<BinlogOffset>> + Send + 'a
    where
        Self: 'a;

    fn get_normalized_table_name(table_name: &SchemaTableName) -> String;

    fn current_binlog_offset(&self) -> Self::BinlogOffsetFuture<'_>;

    fn parse_binlog_offset(&self, offset: &str) -> ConnectorResult<BinlogOffset>;

    fn snapshot_read(
        &self,
        table_name: SchemaTableName,
        primary_keys: Vec<String>,
    ) -> BoxStream<'_, ConnectorResult<OwnedRow>>;
}

#[derive(Debug)]
pub enum ExternalTableReaderImpl {
    MYSQL(MySqlExternalTableReader),
    MOCK(MockExternalTableReader),
}

// impl ExternalTableReaderImpl {
//     pub fn deserialize_binlog_offset(&self, offset: &str) -> ConnectorResult<BinlogOffset> {
//         match self {
//             ExternalTableReaderImpl::MYSQL(_) => {
//                 Ok(BinlogOffset::MySQL(MySqlOffset::from_str(offset)?))
//             }
//             #[cfg(test)]
//             ExternalTableReaderImpl::MOCK(_) => {
//                 Ok(BinlogOffset::MySQL(MySqlOffset::from_str(offset)?))
//             }
//             _ => {
//                 unreachable!("unexpected external table reader")
//             }
//         }
//     }
// }

#[derive(Debug)]
pub struct MySqlExternalTableReader {
    pool: mysql_async::Pool,
    config: ExternalTableConfig,
    rw_schema: Schema,
}

#[derive(Debug, Clone, Deserialize)]
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

impl ExternalTableReader for MySqlExternalTableReader {
    type BinlogOffsetFuture<'a> = impl Future<Output = ConnectorResult<BinlogOffset>> + 'a;

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

            Ok(BinlogOffset::MySQL(MySqlOffset {
                filename: row.take("File").unwrap(),
                position: row.take("Position").unwrap(),
            }))
        }
    }

    fn parse_binlog_offset(&self, offset: &str) -> ConnectorResult<BinlogOffset> {
        Ok(BinlogOffset::MySQL(MySqlOffset::from_str(offset)?))
    }

    fn snapshot_read(
        &self,
        table_name: SchemaTableName,
        primary_keys: Vec<String>,
    ) -> BoxStream<'_, ConnectorResult<OwnedRow>> {
        self.snapshot_read_inner(table_name, primary_keys)
    }
}

impl MySqlExternalTableReader {
    pub fn new(properties: HashMap<String, String>, rw_schema: Schema) -> ConnectorResult<Self> {
        let config = serde_json::from_value::<ExternalTableConfig>(
            serde_json::to_value(properties).unwrap(),
        )
        .map_err(|e| {
            ConnectorError::Config(anyhow!("fail to extract mysql connector properties: {}", e))
        })?;

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

    #[try_stream(boxed, ok = OwnedRow, error = ConnectorError)]
    async fn snapshot_read_inner(&self, table_name: SchemaTableName, primary_keys: Vec<String>) {
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
        let rs_stream = result_set.stream::<mysql_async::Row>().await?;
        if let Some(rs_stream) = rs_stream {
            let row_stream = rs_stream.map(|row| {
                // convert mysql row into OwnedRow
                let mut row = row?;
                let datums = mysql_row_to_datums(&mut row, &self.rw_schema);
                Ok::<_, ConnectorError>(OwnedRow::new(datums))
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

impl ExternalTableReader for ExternalTableReaderImpl {
    type BinlogOffsetFuture<'a> = impl Future<Output = ConnectorResult<BinlogOffset>> + 'a;

    fn get_normalized_table_name(table_name: &SchemaTableName) -> String {
        unimplemented!("get normalized table name")
    }

    fn current_binlog_offset(&self) -> Self::BinlogOffsetFuture<'_> {
        async move {
            match self {
                ExternalTableReaderImpl::MYSQL(mysql) => mysql.current_binlog_offset().await,
                ExternalTableReaderImpl::MOCK(mock) => mock.current_binlog_offset().await,
            }
        }
    }

    fn parse_binlog_offset(&self, offset: &str) -> ConnectorResult<BinlogOffset> {
        match self {
            ExternalTableReaderImpl::MYSQL(mysql) => mysql.parse_binlog_offset(offset),
            ExternalTableReaderImpl::MOCK(mock) => mock.parse_binlog_offset(offset),
        }
    }

    fn snapshot_read(
        &self,
        table_name: SchemaTableName,
        primary_keys: Vec<String>,
    ) -> BoxStream<'_, ConnectorResult<OwnedRow>> {
        self.snapshot_read_inner(table_name, primary_keys)
    }
}

impl ExternalTableReaderImpl {
    #[try_stream(boxed, ok = OwnedRow, error = ConnectorError)]
    async fn snapshot_read_inner(&self, table_name: SchemaTableName, primary_keys: Vec<String>) {
        let stream = match self {
            ExternalTableReaderImpl::MYSQL(mysql) => mysql.snapshot_read(table_name, primary_keys),
            ExternalTableReaderImpl::MOCK(mock) => mock.snapshot_read(table_name, primary_keys),
        };

        pin_mut!(stream);
        #[for_await]
        for row in stream {
            let row = row?;
            yield row;
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
    use crate::source::external::{ExternalTableReader, MySqlExternalTableReader, SchemaTableName};

    // manual test case
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

        let rw_schema = param.schema();
        let props = convert_args!(hashmap!(
                "hostname" => "localhost",
                "port" => "8306",
                "username" => "root",
                "password" => "123456",
                "database.name" => "mydb",
                "table.name" => "t1"));

        let reader = MySqlExternalTableReader::new(props, rw_schema).unwrap();
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

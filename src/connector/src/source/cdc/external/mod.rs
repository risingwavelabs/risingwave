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

pub mod mock_external_table;
pub mod postgres;

#[cfg(not(madsim))]
mod maybe_tls_connector;
pub mod mysql;

use std::collections::HashMap;
use std::fmt;

use anyhow::{anyhow, Context};
use futures::stream::BoxStream;
use futures::{pin_mut, StreamExt};
use futures_async_stream::try_stream;
use itertools::Itertools;
use mysql_async::prelude::AsQuery;
use risingwave_common::bail;
use risingwave_common::catalog::{ColumnDesc, Schema, OFFSET_COLUMN_NAME};
use risingwave_common::row::OwnedRow;
use risingwave_common::types::DataType;
use risingwave_common::util::iter_util::ZipEqFast;
use serde_derive::{Deserialize, Serialize};

use crate::error::{ConnectorError, ConnectorResult};
use crate::parser::mysql_row_to_owned_row;
use crate::source::cdc::external::mock_external_table::MockExternalTableReader;
use crate::source::cdc::external::mysql::{
    MySqlExternalTable, MySqlExternalTableReader, MySqlOffset,
};
use crate::source::cdc::external::postgres::{
    PostgresExternalTable, PostgresExternalTableReader, PostgresOffset,
};
use crate::source::cdc::CdcSourceType;
use crate::source::{UnknownFields, UPSTREAM_SOURCE_KEY};
use crate::WithPropertiesExt;

#[derive(Debug)]
pub enum CdcTableType {
    Undefined,
    MySql,
    Postgres,
    Citus,
}

impl CdcTableType {
    pub fn from_properties(with_properties: &impl WithPropertiesExt) -> Self {
        let connector = with_properties.get_connector().unwrap_or_default();
        match connector.as_str() {
            "mysql-cdc" => Self::MySql,
            "postgres-cdc" => Self::Postgres,
            "citus-cdc" => Self::Citus,
            _ => Self::Undefined,
        }
    }

    pub fn can_backfill(&self) -> bool {
        matches!(self, Self::MySql | Self::Postgres)
    }

    pub async fn create_table_reader(
        &self,
        with_properties: HashMap<String, String>,
        schema: Schema,
        pk_indices: Vec<usize>,
        scan_limit: u32,
    ) -> ConnectorResult<ExternalTableReaderImpl> {
        match self {
            Self::MySql => Ok(ExternalTableReaderImpl::MySql(
                MySqlExternalTableReader::new(with_properties, schema).await?,
            )),
            Self::Postgres => Ok(ExternalTableReaderImpl::Postgres(
                PostgresExternalTableReader::new(with_properties, schema, pk_indices, scan_limit)
                    .await?,
            )),
            _ => bail!("invalid external table type: {:?}", *self),
        }
    }
}

#[derive(Debug, Clone)]
pub struct SchemaTableName {
    // namespace of the table, e.g. database in mysql, schema in postgres
    pub schema_name: String,
    pub table_name: String,
}

pub const TABLE_NAME_KEY: &str = "table.name";
pub const SCHEMA_NAME_KEY: &str = "schema.name";
pub const DATABASE_NAME_KEY: &str = "database.name";

impl SchemaTableName {
    pub fn new(schema_name: String, table_name: String) -> Self {
        Self {
            schema_name,
            table_name,
        }
    }

    pub fn from_properties(properties: &HashMap<String, String>) -> Self {
        let table_type = CdcTableType::from_properties(properties);
        let table_name = properties.get(TABLE_NAME_KEY).cloned().unwrap_or_default();

        let schema_name = match table_type {
            CdcTableType::MySql => properties
                .get(DATABASE_NAME_KEY)
                .cloned()
                .unwrap_or_default(),
            CdcTableType::Postgres | CdcTableType::Citus => {
                properties.get(SCHEMA_NAME_KEY).cloned().unwrap_or_default()
            }
            _ => {
                unreachable!("invalid external table type: {:?}", table_type);
            }
        };

        Self {
            schema_name,
            table_name,
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum CdcOffset {
    MySql(MySqlOffset),
    Postgres(PostgresOffset),
}

// Example debezium offset for Postgres:
// {
//     "sourcePartition":
//     {
//         "server": "RW_CDC_1004"
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
    #[serde(rename = "isHeartbeat")]
    pub is_heartbeat: bool,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct DebeziumSourceOffset {
    // postgres snapshot progress
    pub last_snapshot_record: Option<bool>,
    // mysql snapshot progress
    pub snapshot: Option<bool>,

    // mysql binlog offset
    pub file: Option<String>,
    pub pos: Option<u64>,

    // postgres offset
    pub lsn: Option<u64>,
    #[serde(rename = "txId")]
    pub txid: Option<i64>,
    pub tx_usec: Option<u64>,
}

pub type CdcOffsetParseFunc = Box<dyn Fn(&str) -> ConnectorResult<CdcOffset> + Send>;

pub trait ExternalTableReader {
    async fn current_cdc_offset(&self) -> ConnectorResult<CdcOffset>;

    fn snapshot_read(
        &self,
        table_name: SchemaTableName,
        start_pk: Option<OwnedRow>,
        primary_keys: Vec<String>,
        limit: u32,
    ) -> BoxStream<'_, ConnectorResult<OwnedRow>>;
}

pub enum ExternalTableReaderImpl {
    MySql(MySqlExternalTableReader),
    Postgres(PostgresExternalTableReader),
    Mock(MockExternalTableReader),
}

#[derive(Debug, Clone, Deserialize)]
pub struct ExternalTableConfig {
    pub connector: String,

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
    /// `ssl.mode` specifies the SSL/TLS encryption level for secure communication with Postgres.
    /// Choices include `disabled`, `preferred`, and `required`.
    /// This field is optional.
    #[serde(rename = "ssl.mode", default = "Default::default")]
    pub sslmode: SslMode,
}

impl ExternalTableConfig {
    pub fn try_from_hashmap(connect_properties: HashMap<String, String>) -> ConnectorResult<Self> {
        let json_value = serde_json::to_value(connect_properties)?;
        let config = serde_json::from_value::<ExternalTableConfig>(json_value)?;
        Ok(config)
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SslMode {
    Disabled,
    Preferred,
    Required,
}

impl Default for SslMode {
    fn default() -> Self {
        // default to `disabled` for backward compatibility
        Self::Disabled
    }
}

impl fmt::Display for SslMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            SslMode::Disabled => "disabled",
            SslMode::Preferred => "preferred",
            SslMode::Required => "required",
        })
    }
}

impl ExternalTableReader for ExternalTableReaderImpl {
    async fn current_cdc_offset(&self) -> ConnectorResult<CdcOffset> {
        match self {
            ExternalTableReaderImpl::MySql(mysql) => mysql.current_cdc_offset().await,
            ExternalTableReaderImpl::Postgres(postgres) => postgres.current_cdc_offset().await,
            ExternalTableReaderImpl::Mock(mock) => mock.current_cdc_offset().await,
        }
    }

    fn snapshot_read(
        &self,
        table_name: SchemaTableName,
        start_pk: Option<OwnedRow>,
        primary_keys: Vec<String>,
        limit: u32,
    ) -> BoxStream<'_, ConnectorResult<OwnedRow>> {
        self.snapshot_read_inner(table_name, start_pk, primary_keys, limit)
    }
}

impl ExternalTableReaderImpl {
    pub fn get_cdc_offset_parser(&self) -> CdcOffsetParseFunc {
        match self {
            ExternalTableReaderImpl::MySql(_) => MySqlExternalTableReader::get_cdc_offset_parser(),
            ExternalTableReaderImpl::Postgres(_) => {
                PostgresExternalTableReader::get_cdc_offset_parser()
            }
            ExternalTableReaderImpl::Mock(_) => MockExternalTableReader::get_cdc_offset_parser(),
        }
    }

    #[try_stream(boxed, ok = OwnedRow, error = ConnectorError)]
    async fn snapshot_read_inner(
        &self,
        table_name: SchemaTableName,
        start_pk: Option<OwnedRow>,
        primary_keys: Vec<String>,
        limit: u32,
    ) {
        let stream = match self {
            ExternalTableReaderImpl::MySql(mysql) => {
                mysql.snapshot_read(table_name, start_pk, primary_keys, limit)
            }
            ExternalTableReaderImpl::Postgres(postgres) => {
                postgres.snapshot_read(table_name, start_pk, primary_keys, limit)
            }
            ExternalTableReaderImpl::Mock(mock) => {
                mock.snapshot_read(table_name, start_pk, primary_keys, limit)
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

pub enum ExternalTableImpl {
    MySql(MySqlExternalTable),
    Postgres(PostgresExternalTable),
}

impl ExternalTableImpl {
    pub async fn connect(config: ExternalTableConfig) -> ConnectorResult<Self> {
        let cdc_source_type = CdcSourceType::from(config.connector.as_str());
        match cdc_source_type {
            CdcSourceType::Mysql => Ok(ExternalTableImpl::MySql(
                MySqlExternalTable::connect(config).await?,
            )),
            CdcSourceType::Postgres => Ok(ExternalTableImpl::Postgres(
                PostgresExternalTable::connect(config).await?,
            )),
            _ => Err(anyhow!("Unsupported cdc connector type: {}", config.connector).into()),
        }
    }

    pub fn column_descs(&self) -> &Vec<ColumnDesc> {
        match self {
            ExternalTableImpl::MySql(mysql) => mysql.column_descs(),
            ExternalTableImpl::Postgres(postgres) => postgres.column_descs(),
        }
    }

    pub fn pk_names(&self) -> &Vec<String> {
        match self {
            ExternalTableImpl::MySql(mysql) => mysql.pk_names(),
            ExternalTableImpl::Postgres(postgres) => postgres.pk_names(),
        }
    }
}

#[cfg(test)]
mod tests {

    use futures::pin_mut;
    use futures_async_stream::for_await;
    use maplit::{convert_args, hashmap};
    use risingwave_common::catalog::{ColumnDesc, ColumnId, Field, Schema};
    use risingwave_common::types::DataType;

    use crate::source::cdc::external::{
        CdcOffset, ExternalTableReader, MySqlExternalTableReader, MySqlOffset, SchemaTableName,
    };

    #[test]
    fn test_mysql_filter_expr() {
        let cols = vec!["id".to_string()];
        let expr = MySqlExternalTableReader::filter_expression(&cols);
        assert_eq!(expr, "(`id` > :id)");

        let cols = vec!["aa".to_string(), "bb".to_string(), "cc".to_string()];
        let expr = MySqlExternalTableReader::filter_expression(&cols);
        assert_eq!(
            expr,
            "(`aa` > :aa) OR ((`aa` = :aa) AND (`bb` > :bb)) OR ((`aa` = :aa) AND (`bb` = :bb) AND (`cc` > :cc))"
        );
    }

    #[test]
    fn test_mysql_binlog_offset() {
        let off0_str = r#"{ "sourcePartition": { "server": "test" }, "sourceOffset": { "ts_sec": 1670876905, "file": "binlog.000001", "pos": 105622, "snapshot": true }, "isHeartbeat": false }"#;
        let off1_str = r#"{ "sourcePartition": { "server": "test" }, "sourceOffset": { "ts_sec": 1670876905, "file": "binlog.000007", "pos": 1062363217, "snapshot": true }, "isHeartbeat": false }"#;
        let off2_str = r#"{ "sourcePartition": { "server": "test" }, "sourceOffset": { "ts_sec": 1670876905, "file": "binlog.000007", "pos": 659687560, "snapshot": true }, "isHeartbeat": false }"#;
        let off3_str = r#"{ "sourcePartition": { "server": "test" }, "sourceOffset": { "ts_sec": 1670876905, "file": "binlog.000008", "pos": 7665875, "snapshot": true }, "isHeartbeat": false }"#;
        let off4_str = r#"{ "sourcePartition": { "server": "test" }, "sourceOffset": { "ts_sec": 1670876905, "file": "binlog.000008", "pos": 7665875, "snapshot": true }, "isHeartbeat": false }"#;

        let off0 = CdcOffset::MySql(MySqlOffset::parse_debezium_offset(off0_str).unwrap());
        let off1 = CdcOffset::MySql(MySqlOffset::parse_debezium_offset(off1_str).unwrap());
        let off2 = CdcOffset::MySql(MySqlOffset::parse_debezium_offset(off2_str).unwrap());
        let off3 = CdcOffset::MySql(MySqlOffset::parse_debezium_offset(off3_str).unwrap());
        let off4 = CdcOffset::MySql(MySqlOffset::parse_debezium_offset(off4_str).unwrap());

        assert!(off0 <= off1);
        assert!(off1 > off2);
        assert!(off2 < off3);
        assert_eq!(off3, off4);
    }

    // manual test case
    #[ignore]
    #[tokio::test]
    async fn test_mysql_table_reader() {
        let columns = vec![
            ColumnDesc::named("v1", ColumnId::new(1), DataType::Int32),
            ColumnDesc::named("v2", ColumnId::new(2), DataType::Decimal),
            ColumnDesc::named("v3", ColumnId::new(3), DataType::Varchar),
            ColumnDesc::named("v4", ColumnId::new(4), DataType::Date),
        ];
        let rw_schema = Schema {
            fields: columns.iter().map(Field::from).collect(),
        };
        let props = convert_args!(hashmap!(
                "hostname" => "localhost",
                "port" => "8306",
                "username" => "root",
                "password" => "123456",
                "database.name" => "mytest",
                "table.name" => "t1"));

        let reader = MySqlExternalTableReader::new(props, rw_schema)
            .await
            .unwrap();
        let offset = reader.current_cdc_offset().await.unwrap();
        println!("BinlogOffset: {:?}", offset);

        let off0_str = r#"{ "sourcePartition": { "server": "test" }, "sourceOffset": { "ts_sec": 1670876905, "file": "binlog.000001", "pos": 105622, "snapshot": true }, "isHeartbeat": false }"#;
        let parser = MySqlExternalTableReader::get_cdc_offset_parser();
        println!("parsed offset: {:?}", parser(off0_str).unwrap());
        let table_name = SchemaTableName {
            schema_name: "mytest".to_string(),
            table_name: "t1".to_string(),
        };

        let stream = reader.snapshot_read(table_name, None, vec!["v1".to_string()], 1000);
        pin_mut!(stream);
        #[for_await]
        for row in stream {
            println!("OwnedRow: {:?}", row);
        }
    }
}

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
use futures::{pin_mut, StreamExt};
use futures_async_stream::try_stream;
use itertools::Itertools;
use mysql_async::prelude::*;
use mysql_common::params::Params;
use mysql_common::value::Value;
use risingwave_common::bail;
use risingwave_common::catalog::{Schema, OFFSET_COLUMN_NAME};
use risingwave_common::row::OwnedRow;
use risingwave_common::types::DataType;
use risingwave_common::util::iter_util::ZipEqFast;
use serde_derive::{Deserialize, Serialize};

use crate::error::ConnectorError;
use crate::parser::mysql_row_to_datums;
use crate::source::MockExternalTableReader;

pub type ConnectorResult<T> = std::result::Result<T, ConnectorError>;

#[derive(Debug)]
pub enum CdcTableType {
    Undefined,
    MySql,
    Postgres,
    Citus,
}

impl CdcTableType {
    pub fn from_properties(properties: &HashMap<String, String>) -> Self {
        let connector = properties
            .get("connector")
            .map(|c| c.to_ascii_lowercase())
            .unwrap_or_default();
        match connector.as_str() {
            "mysql-cdc" => Self::MySql,
            "postgres-cdc" => Self::Postgres,
            "citus-cdc" => Self::Citus,
            _ => Self::Undefined,
        }
    }

    pub fn can_backfill(&self) -> bool {
        matches!(self, Self::MySql)
    }

    pub fn create_table_reader(
        &self,
        properties: HashMap<String, String>,
        schema: Schema,
    ) -> ConnectorResult<ExternalTableReaderImpl> {
        match self {
            Self::MySql => Ok(ExternalTableReaderImpl::MySql(
                MySqlExternalTableReader::new(properties, schema)?,
            )),
            _ => bail!(ConnectorError::Config(anyhow!(
                "invalid external table type: {:?}",
                *self
            ))),
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

#[derive(Debug, Clone, Default, PartialEq, PartialOrd)]
pub struct MySqlOffset {
    pub filename: String,
    pub position: u64,
}

impl MySqlOffset {
    pub fn new(filename: String, position: u64) -> Self {
        Self { filename, position }
    }
}

#[derive(Debug, Clone, Default, PartialEq, PartialOrd)]
pub struct PostgresOffset {
    pub txid: u64,
    pub lsn: u64,
    pub tx_usec: u64,
}

#[derive(Debug, Clone, PartialEq, PartialOrd)]
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

    // postgres binlog offset
    pub lsn: Option<u64>,
    #[serde(rename = "txId")]
    pub txid: Option<u64>,
    pub tx_usec: Option<u64>,
}

impl MySqlOffset {
    pub fn parse_str(offset: &str) -> ConnectorResult<Self> {
        let dbz_offset: DebeziumOffset = serde_json::from_str(offset).map_err(|e| {
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
    fn get_normalized_table_name(&self, table_name: &SchemaTableName) -> String;

    fn current_cdc_offset(&self) -> impl Future<Output = ConnectorResult<CdcOffset>> + Send + '_;

    fn parse_binlog_offset(&self, offset: &str) -> ConnectorResult<CdcOffset>;

    fn snapshot_read(
        &self,
        table_name: SchemaTableName,
        start_pk: Option<OwnedRow>,
        primary_keys: Vec<String>,
    ) -> BoxStream<'_, ConnectorResult<OwnedRow>>;
}

#[derive(Debug)]
pub enum ExternalTableReaderImpl {
    MySql(MySqlExternalTableReader),
    Mock(MockExternalTableReader),
}

#[derive(Debug)]
pub struct MySqlExternalTableReader {
    pool: mysql_async::Pool,
    config: ExternalTableConfig,
    rw_schema: Schema,
    field_names: String,
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
    fn get_normalized_table_name(&self, table_name: &SchemaTableName) -> String {
        // schema name is the database name in mysql
        format!("`{}`.`{}`", table_name.schema_name, table_name.table_name)
    }

    async fn current_cdc_offset(&self) -> ConnectorResult<CdcOffset> {
        let mut conn = self
            .pool
            .get_conn()
            .await
            .map_err(|e| ConnectorError::Connection(anyhow!(e)))?;

        let sql = "SHOW MASTER STATUS".to_string();
        let mut rs = conn.query::<mysql_async::Row, _>(sql).await?;
        let row = rs
            .iter_mut()
            .exactly_one()
            .map_err(|e| ConnectorError::Internal(anyhow!("read binlog error: {}", e)))?;

        Ok(CdcOffset::MySql(MySqlOffset {
            filename: row.take("File").unwrap(),
            position: row.take("Position").unwrap(),
        }))
    }

    fn parse_binlog_offset(&self, offset: &str) -> ConnectorResult<CdcOffset> {
        Ok(CdcOffset::MySql(MySqlOffset::parse_str(offset)?))
    }

    fn snapshot_read(
        &self,
        table_name: SchemaTableName,
        start_pk: Option<OwnedRow>,
        primary_keys: Vec<String>,
    ) -> BoxStream<'_, ConnectorResult<OwnedRow>> {
        self.snapshot_read_inner(table_name, start_pk, primary_keys)
    }
}

impl MySqlExternalTableReader {
    pub fn new(properties: HashMap<String, String>, rw_schema: Schema) -> ConnectorResult<Self> {
        tracing::debug!(?rw_schema, "create mysql external table reader");

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

        let field_names = rw_schema
            .fields
            .iter()
            .filter(|f| f.name != OFFSET_COLUMN_NAME)
            .map(|f| format!("`{}`", f.name.as_str()))
            .join(",");

        Ok(Self {
            pool,
            config,
            rw_schema,
            field_names,
        })
    }

    pub async fn disconnect(&self) -> ConnectorResult<()> {
        self.pool.clone().disconnect().await?;
        Ok(())
    }

    #[try_stream(boxed, ok = OwnedRow, error = ConnectorError)]
    async fn snapshot_read_inner(
        &self,
        table_name: SchemaTableName,
        start_pk_row: Option<OwnedRow>,
        primary_keys: Vec<String>,
    ) {
        let order_key = primary_keys.iter().join(",");
        let sql = if start_pk_row.is_none() {
            format!(
                "SELECT {} FROM {} ORDER BY {}",
                self.field_names,
                self.get_normalized_table_name(&table_name),
                order_key
            )
        } else {
            let filter_expr = Self::filter_expression(&primary_keys);
            format!(
                "SELECT {} FROM {} WHERE {} ORDER BY {}",
                self.field_names,
                self.get_normalized_table_name(&table_name),
                filter_expr,
                order_key
            )
        };

        let mut conn = self
            .pool
            .get_conn()
            .await
            .map_err(|e| ConnectorError::Connection(anyhow!(e)))?;

        // Set session timezone to UTC
        conn.exec_drop("SET time_zone = \"+00:00\"", ()).await?;

        if start_pk_row.is_none() {
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
        } else {
            let field_map = self
                .rw_schema
                .fields
                .iter()
                .map(|f| (f.name.as_str(), f.data_type.clone()))
                .collect::<HashMap<_, _>>();

            // fill in start primary key params
            let params: Vec<_> = primary_keys
                .iter()
                .zip_eq_fast(start_pk_row.unwrap().into_iter())
                .map(|(pk, datum)| {
                    if let Some(value) = datum {
                        let ty = field_map.get(pk.as_str()).unwrap();
                        let val = match ty {
                            DataType::Boolean => Value::from(value.into_bool()),
                            DataType::Int16 => Value::from(value.into_int16()),
                            DataType::Int32 => Value::from(value.into_int32()),
                            DataType::Int64 => Value::from(value.into_int64()),
                            DataType::Float32 => Value::from(value.into_float32().into_inner()),
                            DataType::Float64 => Value::from(value.into_float64().into_inner()),
                            DataType::Varchar => Value::from(String::from(value.into_utf8())),
                            DataType::Date => Value::from(value.into_date().0),
                            DataType::Time => Value::from(value.into_time().0),
                            DataType::Timestamp => Value::from(value.into_timestamp().0),
                            _ => {
                                return Err(ConnectorError::Internal(anyhow!(
                                    "unsupported primary key data type: {}",
                                    ty
                                )))
                            }
                        };
                        Ok((pk.clone(), val))
                    } else {
                        Err(ConnectorError::Internal(anyhow!(
                            "primary key {} cannot be null",
                            pk
                        )))
                    }
                })
                .try_collect()?;

            let rs_stream = sql
                .with(Params::from(params))
                .stream::<mysql_async::Row, _>(&mut conn)
                .await?;

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
        };
    }

    // mysql cannot leverage the given key to narrow down the range of scan,
    // we need to rewrite the comparison conditions by our own.
    // (a, b) > (x, y) => ('a' > x) OR (('a' = x) AND ('b' > y))
    fn filter_expression(columns: &[String]) -> String {
        let mut conditions = vec![];
        // push the first condition
        conditions.push(format!("({} > :{})", columns[0], columns[0]));
        for i in 2..=columns.len() {
            // '=' condition
            let mut condition = String::new();
            for (j, item) in columns.iter().enumerate().take(i - 1) {
                if j == 0 {
                    condition.push_str(&format!("({} = :{})", item, item));
                } else {
                    condition.push_str(&format!(" AND ({} = :{})", item, item));
                }
            }
            // '>' condition
            condition.push_str(&format!(" AND ({} > :{})", columns[i - 1], columns[i - 1]));
            conditions.push(format!("({})", condition));
        }
        if columns.len() > 1 {
            conditions.join(" OR ")
        } else {
            conditions.join("")
        }
    }
}

impl ExternalTableReader for ExternalTableReaderImpl {
    fn get_normalized_table_name(&self, table_name: &SchemaTableName) -> String {
        match self {
            ExternalTableReaderImpl::MySql(mysql) => mysql.get_normalized_table_name(table_name),
            ExternalTableReaderImpl::Mock(mock) => mock.get_normalized_table_name(table_name),
        }
    }

    async fn current_cdc_offset(&self) -> ConnectorResult<CdcOffset> {
        match self {
            ExternalTableReaderImpl::MySql(mysql) => mysql.current_cdc_offset().await,
            ExternalTableReaderImpl::Mock(mock) => mock.current_cdc_offset().await,
        }
    }

    fn parse_binlog_offset(&self, offset: &str) -> ConnectorResult<CdcOffset> {
        match self {
            ExternalTableReaderImpl::MySql(mysql) => mysql.parse_binlog_offset(offset),
            ExternalTableReaderImpl::Mock(mock) => mock.parse_binlog_offset(offset),
        }
    }

    fn snapshot_read(
        &self,
        table_name: SchemaTableName,
        start_pk: Option<OwnedRow>,
        primary_keys: Vec<String>,
    ) -> BoxStream<'_, ConnectorResult<OwnedRow>> {
        self.snapshot_read_inner(table_name, start_pk, primary_keys)
    }
}

impl ExternalTableReaderImpl {
    #[try_stream(boxed, ok = OwnedRow, error = ConnectorError)]
    async fn snapshot_read_inner(
        &self,
        table_name: SchemaTableName,
        start_pk: Option<OwnedRow>,
        primary_keys: Vec<String>,
    ) {
        let stream = match self {
            ExternalTableReaderImpl::MySql(mysql) => {
                mysql.snapshot_read(table_name, start_pk, primary_keys)
            }
            ExternalTableReaderImpl::Mock(mock) => {
                mock.snapshot_read(table_name, start_pk, primary_keys)
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

#[cfg(test)]
mod tests {

    use futures::pin_mut;
    use futures_async_stream::for_await;
    use maplit::{convert_args, hashmap};
    use risingwave_common::catalog::{ColumnDesc, ColumnId};
    use risingwave_common::types::DataType;

    use crate::sink::catalog::SinkType;
    use crate::sink::SinkParam;
    use crate::source::external::{
        CdcOffset, ExternalTableReader, MySqlExternalTableReader, MySqlOffset, SchemaTableName,
    };

    #[test]
    fn test_mysql_filter_expr() {
        let cols = vec!["aa".to_string(), "bb".to_string(), "cc".to_string()];
        let expr = MySqlExternalTableReader::filter_expression(&cols);
        assert_eq!(
            expr,
            "(aa > :aa) OR ((aa = :aa) AND (bb > :bb)) OR ((aa = :aa) AND (bb = :bb) AND (cc > :cc))"
        );
    }

    #[test]
    fn test_mysql_binlog_offset() {
        let off0_str = r#"{ "sourcePartition": { "server": "test" }, "sourceOffset": { "ts_sec": 1670876905, "file": "binlog.000001", "pos": 105622, "snapshot": true }, "isHeartbeat": false }"#;
        let off1_str = r#"{ "sourcePartition": { "server": "test" }, "sourceOffset": { "ts_sec": 1670876905, "file": "binlog.000007", "pos": 1062363217, "snapshot": true }, "isHeartbeat": false }"#;
        let off2_str = r#"{ "sourcePartition": { "server": "test" }, "sourceOffset": { "ts_sec": 1670876905, "file": "binlog.000007", "pos": 659687560, "snapshot": true }, "isHeartbeat": false }"#;
        let off3_str = r#"{ "sourcePartition": { "server": "test" }, "sourceOffset": { "ts_sec": 1670876905, "file": "binlog.000008", "pos": 7665875, "snapshot": true }, "isHeartbeat": false }"#;
        let off4_str = r#"{ "sourcePartition": { "server": "test" }, "sourceOffset": { "ts_sec": 1670876905, "file": "binlog.000008", "pos": 7665875, "snapshot": true }, "isHeartbeat": false }"#;

        let off0 = CdcOffset::MySql(MySqlOffset::parse_str(off0_str).unwrap());
        let off1 = CdcOffset::MySql(MySqlOffset::parse_str(off1_str).unwrap());
        let off2 = CdcOffset::MySql(MySqlOffset::parse_str(off2_str).unwrap());
        let off3 = CdcOffset::MySql(MySqlOffset::parse_str(off3_str).unwrap());
        let off4 = CdcOffset::MySql(MySqlOffset::parse_str(off4_str).unwrap());

        assert!(off0 <= off1);
        assert!(off1 > off2);
        assert!(off2 < off3);
        assert_eq!(off3, off4);
    }

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
            downstream_pk: vec![0],
            sink_type: SinkType::AppendOnly,
            format_desc: None,
            db_name: "db".into(),
            sink_from_name: "table".into(),
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
        let offset = reader.current_cdc_offset().await.unwrap();
        println!("BinlogOffset: {:?}", offset);

        let table_name = SchemaTableName {
            schema_name: "mydb".to_string(),
            table_name: "t1".to_string(),
        };

        let stream = reader.snapshot_read(table_name, None, vec!["v1".to_string()]);
        pin_mut!(stream);
        #[for_await]
        for row in stream {
            println!("OwnedRow: {:?}", row);
        }
    }
}

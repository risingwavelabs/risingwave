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

pub mod mock_external_table;
pub mod postgres;
pub mod sql_server;

pub mod mysql;

use std::collections::{BTreeMap, HashMap};

use anyhow::{Context, anyhow};
use futures::pin_mut;
use futures::stream::BoxStream;
use futures_async_stream::try_stream;
use risingwave_common::bail;
use risingwave_common::catalog::{CdcKeyComparison, ColumnDesc, Field, Schema};
use risingwave_common::row::OwnedRow;
use risingwave_common::secret::LocalSecretManager;
use risingwave_pb::catalog::table::CdcTableType as PbCdcTableType;
use risingwave_pb::secret::PbSecretRef;
use serde::{Deserialize, Serialize};

use crate::WithPropertiesExt;
use crate::connector_common::{PgConnectionConfig, PostgresExternalTable, SslMode};
use crate::error::{ConnectorError, ConnectorResult};
use crate::source::CdcTableSnapshotSplit;
use crate::source::cdc::CdcSourceType;
use crate::source::cdc::external::mock_external_table::MockExternalTableReader;
use crate::source::cdc::external::mysql::{
    MysqlExternalTable, MysqlExternalTableReader, MysqlOffset,
};
use crate::source::cdc::external::postgres::{PostgresExternalTableReader, PostgresOffset};
use crate::source::cdc::external::sql_server::{
    SqlServerExternalTable, SqlServerExternalTableReader, SqlServerOffset,
};

/// Maps a registry connector to its `PbCdcTableType` variant of the same name.
// TODO(oracle): `PbCdcTableType` has no Oracle value yet; remove the `Oracle` arm once the proto
// adds `CDC_TABLE_TYPE_ORACLE`.
macro_rules! pb_cdc_table_type {
    (Oracle) => {
        PbCdcTableType::Unspecified
    };
    ($cdc_source_type:ident) => {
        PbCdcTableType::$cdc_source_type
    };
}

/// Generates [`ExternalCdcTableType`] with one variant per CDC connector in the registry plus the
/// hardcoded `Unspecified` and `Mock`, together with `from_properties` and the `PbCdcTableType`
/// conversions.
macro_rules! impl_external_cdc_table_type {
    ({$({$cdc_source_type:ident}),*}, {$($_ignore:tt),*}) => {
        #[derive(Debug, Clone, PartialEq, Eq, Hash)]
        pub enum ExternalCdcTableType {
            Unspecified,
            Mock,
            $(
                $cdc_source_type,
            )*
        }

        impl ExternalCdcTableType {
            pub fn from_properties(with_properties: &impl WithPropertiesExt) -> Self {
                let connector = with_properties.get_connector().unwrap_or_default();
                match CdcSourceType::from(connector.as_str()) {
                    $(
                        CdcSourceType::$cdc_source_type => Self::$cdc_source_type,
                    )*
                    CdcSourceType::Unspecified => Self::Unspecified,
                }
            }
        }

        impl From<ExternalCdcTableType> for PbCdcTableType {
            fn from(cdc_table_type: ExternalCdcTableType) -> Self {
                match cdc_table_type {
                    $(
                        ExternalCdcTableType::$cdc_source_type => pb_cdc_table_type!($cdc_source_type),
                    )*
                    ExternalCdcTableType::Unspecified | ExternalCdcTableType::Mock => Self::Unspecified,
                }
            }
        }

        impl From<PbCdcTableType> for ExternalCdcTableType {
            fn from(cdc_table_type: PbCdcTableType) -> Self {
                #[allow(unreachable_patterns)]
                match cdc_table_type {
                    PbCdcTableType::Unspecified => Self::Unspecified,
                    $(
                        pb_cdc_table_type!($cdc_source_type) => Self::$cdc_source_type,
                    )*
                }
            }
        }
    };
}

crate::for_all_classified_sources!(impl_external_cdc_table_type);

impl ExternalCdcTableType {
    pub fn get_cdc_offset_parser(&self) -> ConnectorResult<CdcOffsetParseFunc> {
        match self {
            Self::Mysql => Ok(MysqlExternalTableReader::get_cdc_offset_parser()),
            Self::Postgres => Ok(PostgresExternalTableReader::get_cdc_offset_parser()),
            Self::SqlServer => Ok(SqlServerExternalTableReader::get_cdc_offset_parser()),
            Self::Mock => Ok(MockExternalTableReader::get_cdc_offset_parser()),
            Self::Unspecified | Self::Citus | Self::Mongodb | Self::Oracle => {
                bail!("invalid external table type: {:?}", *self)
            }
        }
    }

    pub async fn create_table_reader(
        &self,
        config: ExternalTableConfig,
        schema: Schema,
        pk_indices: Vec<usize>,
        schema_table_name: SchemaTableName,
        table_id: u32,
    ) -> ConnectorResult<ExternalTableReaderImpl> {
        match self {
            Self::Mysql => Ok(ExternalTableReaderImpl::Mysql(
                MysqlExternalTableReader::new(config, schema, pk_indices).await?,
            )),
            Self::Postgres => Ok(ExternalTableReaderImpl::Postgres(
                PostgresExternalTableReader::new(
                    config,
                    schema,
                    pk_indices,
                    schema_table_name,
                    table_id,
                )
                .await?,
            )),
            Self::SqlServer => Ok(ExternalTableReaderImpl::SqlServer(
                SqlServerExternalTableReader::new(config, schema, pk_indices).await?,
            )),
            // citus is never supported for cdc backfill (create source + create table).
            Self::Mock => Ok(ExternalTableReaderImpl::Mock(MockExternalTableReader::new())),
            Self::Unspecified | Self::Citus | Self::Mongodb | Self::Oracle => {
                bail!("invalid external table type: {:?}", *self)
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct SchemaTableName {
    // namespace of the table, e.g. database in mysql, schema in postgres
    pub schema_name: String,
    pub table_name: String,
}

pub const TABLE_NAME_KEY: &str = "table.name";
pub const SCHEMA_NAME_KEY: &str = "schema.name";
pub const DATABASE_NAME_KEY: &str = "database.name";

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum CdcOffset {
    // The variant name is the JSON tag persisted in the CDC backfill state table,
    // so keep the historical spelling for serde.
    #[serde(rename = "MySql")]
    Mysql(MysqlOffset),
    Postgres(PostgresOffset),
    SqlServer(SqlServerOffset),
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
    pub lsn_commit: Option<u64>,
    pub lsn_proc: Option<u64>,

    // sql server offset
    pub commit_lsn: Option<String>,
    pub change_lsn: Option<String>,
}

pub type CdcOffsetParseFunc = Box<dyn Fn(&str) -> ConnectorResult<CdcOffset> + Send>;

pub trait ExternalTableReader: Sized {
    async fn current_cdc_offset(&self) -> ConnectorResult<CdcOffset>;

    // Currently, MySQL cdc uses a connection pool to manage connections to MySQL, and other CDC processes do not require the disconnect step for now.

    async fn disconnect(self) -> ConnectorResult<()> {
        Ok(())
    }

    fn snapshot_read(
        &self,
        table_name: SchemaTableName,
        start_pk: Option<OwnedRow>,
        primary_keys: Vec<String>,
        limit: u32,
    ) -> BoxStream<'_, ConnectorResult<OwnedRow>>;

    fn get_parallel_cdc_splits(
        &self,
        options: CdcTableSnapshotSplitOption,
    ) -> BoxStream<'_, ConnectorResult<CdcTableSnapshotSplit>>;

    fn split_snapshot_read(
        &self,
        table_name: SchemaTableName,
        left: OwnedRow,
        right: OwnedRow,
        split_columns: Vec<Field>,
    ) -> BoxStream<'_, ConnectorResult<OwnedRow>>;
}

pub struct CdcTableSnapshotSplitOption {
    pub backfill_num_rows_per_split: u64,
    pub backfill_as_even_splits: bool,
    pub backfill_split_pk_column_index: u32,
}

pub enum ExternalTableReaderImpl {
    Mysql(MysqlExternalTableReader),
    Postgres(PostgresExternalTableReader),
    SqlServer(SqlServerExternalTableReader),
    Mock(MockExternalTableReader),
}

#[derive(Debug, Default, Clone, Deserialize)]
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
    #[serde(rename = "ssl.mode", default = "postgres_ssl_mode_default")]
    #[serde(alias = "debezium.database.sslmode")]
    pub ssl_mode: SslMode,

    #[serde(rename = "ssl.root.cert")]
    #[serde(alias = "debezium.database.sslrootcert")]
    pub ssl_root_cert: Option<String>,

    /// `encrypt` specifies whether connect to SQL Server using SSL.
    /// Only "true" means using SSL. All other values are treated as "false".
    #[serde(rename = "database.encrypt", default = "Default::default")]
    pub encrypt: String,
}

fn postgres_ssl_mode_default() -> SslMode {
    // NOTE(StrikeW): Default to `disabled` for backward compatibility
    SslMode::Disabled
}

impl ExternalTableConfig {
    pub fn try_from_btreemap(
        connect_properties: BTreeMap<String, String>,
        secret_refs: BTreeMap<String, PbSecretRef>,
    ) -> ConnectorResult<Self> {
        let options_with_secret =
            LocalSecretManager::global().fill_secrets(connect_properties, secret_refs)?;
        let json_value = serde_json::to_value(options_with_secret)?;
        let config = serde_json::from_value::<ExternalTableConfig>(json_value)?;
        Ok(config)
    }

    /// Project the Postgres-specific subset of this config so it can drive the
    /// shared `create_pg_client` / `PostgresExternalTable` helpers. Only meaningful
    /// for the Postgres CDC connector; other connectors ignore the fields.
    pub fn pg_connection_config(&self) -> ConnectorResult<PgConnectionConfig> {
        let port = self
            .port
            .parse::<u16>()
            .with_context(|| format!("invalid postgres port `{}`", self.port))?;
        Ok(PgConnectionConfig {
            host: self.host.clone(),
            port,
            user: self.username.clone(),
            password: self.password.clone(),
            database: self.database.clone(),
            ssl_mode: self.ssl_mode.clone(),
            ssl_root_cert: self.ssl_root_cert.clone(),
        })
    }
}

impl ExternalTableReader for ExternalTableReaderImpl {
    async fn current_cdc_offset(&self) -> ConnectorResult<CdcOffset> {
        match self {
            ExternalTableReaderImpl::Mysql(mysql) => mysql.current_cdc_offset().await,
            ExternalTableReaderImpl::Postgres(postgres) => postgres.current_cdc_offset().await,
            ExternalTableReaderImpl::SqlServer(sql_server) => sql_server.current_cdc_offset().await,
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

    fn get_parallel_cdc_splits(
        &self,
        options: CdcTableSnapshotSplitOption,
    ) -> BoxStream<'_, ConnectorResult<CdcTableSnapshotSplit>> {
        self.get_parallel_cdc_splits_inner(options)
    }

    fn split_snapshot_read(
        &self,
        table_name: SchemaTableName,
        left: OwnedRow,
        right: OwnedRow,
        split_columns: Vec<Field>,
    ) -> BoxStream<'_, ConnectorResult<OwnedRow>> {
        self.split_snapshot_read_inner(table_name, left, right, split_columns)
    }
}

impl ExternalTableReaderImpl {
    /// Recover comparison semantics for primary-key columns from the live reader.
    pub fn pk_column_comparisons(
        &self,
        pk_names: &[String],
    ) -> ConnectorResult<Vec<CdcKeyComparison>> {
        match self {
            ExternalTableReaderImpl::Mysql(mysql) => mysql.pk_column_comparisons(pk_names),
            ExternalTableReaderImpl::Postgres(_)
            | ExternalTableReaderImpl::SqlServer(_)
            | ExternalTableReaderImpl::Mock(_) => {
                Ok(vec![CdcKeyComparison::Native; pk_names.len()])
            }
        }
    }

    pub fn get_cdc_offset_parser(&self) -> CdcOffsetParseFunc {
        match self {
            ExternalTableReaderImpl::Mysql(_) => MysqlExternalTableReader::get_cdc_offset_parser(),
            ExternalTableReaderImpl::Postgres(_) => {
                PostgresExternalTableReader::get_cdc_offset_parser()
            }
            ExternalTableReaderImpl::SqlServer(_) => {
                SqlServerExternalTableReader::get_cdc_offset_parser()
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
            ExternalTableReaderImpl::Mysql(mysql) => {
                mysql.snapshot_read(table_name, start_pk, primary_keys, limit)
            }
            ExternalTableReaderImpl::Postgres(postgres) => {
                postgres.snapshot_read(table_name, start_pk, primary_keys, limit)
            }
            ExternalTableReaderImpl::SqlServer(sql_server) => {
                sql_server.snapshot_read(table_name, start_pk, primary_keys, limit)
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

    #[try_stream(boxed, ok = CdcTableSnapshotSplit, error = ConnectorError)]
    async fn get_parallel_cdc_splits_inner(&self, options: CdcTableSnapshotSplitOption) {
        let stream = match self {
            ExternalTableReaderImpl::Mysql(e) => e.get_parallel_cdc_splits(options),
            ExternalTableReaderImpl::Postgres(e) => e.get_parallel_cdc_splits(options),
            ExternalTableReaderImpl::SqlServer(e) => e.get_parallel_cdc_splits(options),
            ExternalTableReaderImpl::Mock(e) => e.get_parallel_cdc_splits(options),
        };
        pin_mut!(stream);
        #[for_await]
        for row in stream {
            let row = row?;
            yield row;
        }
    }

    #[try_stream(boxed, ok = OwnedRow, error = ConnectorError)]
    async fn split_snapshot_read_inner(
        &self,
        table_name: SchemaTableName,
        left: OwnedRow,
        right: OwnedRow,
        split_columns: Vec<Field>,
    ) {
        let stream = match self {
            ExternalTableReaderImpl::Mysql(mysql) => {
                mysql.split_snapshot_read(table_name, left, right, split_columns)
            }
            ExternalTableReaderImpl::Postgres(postgres) => {
                postgres.split_snapshot_read(table_name, left, right, split_columns)
            }
            ExternalTableReaderImpl::SqlServer(sql_server) => {
                sql_server.split_snapshot_read(table_name, left, right, split_columns)
            }
            ExternalTableReaderImpl::Mock(mock) => {
                mock.split_snapshot_read(table_name, left, right, split_columns)
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
    Mysql(MysqlExternalTable),
    Postgres(PostgresExternalTable),
    SqlServer(SqlServerExternalTable),
}

impl ExternalTableImpl {
    pub async fn connect(config: ExternalTableConfig) -> ConnectorResult<Self> {
        let cdc_source_type = CdcSourceType::from(config.connector.as_str());
        match cdc_source_type {
            CdcSourceType::Mysql => Ok(ExternalTableImpl::Mysql(
                MysqlExternalTable::connect(config).await?,
            )),
            CdcSourceType::Postgres => {
                let pg_conn = config.pg_connection_config()?;
                Ok(ExternalTableImpl::Postgres(
                    PostgresExternalTable::connect(
                        &pg_conn,
                        &config.schema,
                        &config.table,
                        false,
                        Some("SELECT"),
                    )
                    .await?,
                ))
            }
            CdcSourceType::SqlServer => Ok(ExternalTableImpl::SqlServer(
                SqlServerExternalTable::connect(config).await?,
            )),
            CdcSourceType::Citus
            | CdcSourceType::Mongodb
            | CdcSourceType::Oracle
            | CdcSourceType::Unspecified => {
                Err(anyhow!("Unsupported cdc connector type: {}", config.connector).into())
            }
        }
    }

    pub fn column_descs(&self) -> &Vec<ColumnDesc> {
        match self {
            ExternalTableImpl::Mysql(mysql) => mysql.column_descs(),
            ExternalTableImpl::Postgres(postgres) => postgres.column_descs(),
            ExternalTableImpl::SqlServer(sql_server) => sql_server.column_descs(),
        }
    }

    pub fn pk_names(&self) -> &Vec<String> {
        match self {
            ExternalTableImpl::Mysql(mysql) => mysql.pk_names(),
            ExternalTableImpl::Postgres(postgres) => postgres.pk_names(),
            ExternalTableImpl::SqlServer(sql_server) => sql_server.pk_names(),
        }
    }

    /// Return CDC backfill comparison semantics in the requested `pk_names` order.
    /// Reuses metadata loaded by [`Self::connect`], without querying upstream again.
    ///
    /// MySQL `BIGINT UNSIGNED` keys use `UnsignedInt64` to preserve upstream ordering
    /// when represented as signed `i64` values; other types use `Native`. MySQL names
    /// are matched case-insensitively, and missing upstream PK names return an error.
    /// PostgreSQL and SQL Server return `Native` for every requested name.
    pub fn pk_column_comparisons(
        &self,
        pk_names: &[String],
    ) -> ConnectorResult<Vec<CdcKeyComparison>> {
        match self {
            ExternalTableImpl::Mysql(mysql) => mysql.pk_column_comparisons(pk_names),
            ExternalTableImpl::Postgres(_) | ExternalTableImpl::SqlServer(_) => {
                Ok(vec![CdcKeyComparison::Native; pk_names.len()])
            }
        }
    }

    /// Discover CDC backfill comparison semantics without loading the full table schema.
    /// Used when SQL defines the columns explicitly, so there is no existing table
    /// instance whose metadata can be reused via [`Self::pk_column_comparisons`].
    ///
    /// For MySQL, queries upstream PK names and types and returns the same comparison
    /// modes in the requested `pk_names` order, with case-insensitive name matching and
    /// an error for missing upstream PK names. Other connectors return `Native` for
    /// every requested name without querying upstream.
    pub async fn discover_pk_column_comparisons(
        config: &ExternalTableConfig,
        pk_names: &[String],
    ) -> ConnectorResult<Vec<CdcKeyComparison>> {
        match CdcSourceType::from(config.connector.as_str()) {
            CdcSourceType::Mysql => {
                MysqlExternalTable::discover_pk_column_comparisons(config, pk_names).await
            }
            CdcSourceType::Postgres
            | CdcSourceType::Citus
            | CdcSourceType::Mongodb
            | CdcSourceType::SqlServer
            | CdcSourceType::Oracle
            | CdcSourceType::Unspecified => Ok(vec![CdcKeyComparison::Native; pk_names.len()]),
        }
    }
}

pub const CDC_TABLE_SPLIT_ID_START: i64 = 1;

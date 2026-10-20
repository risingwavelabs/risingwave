// Copyright 2026 RisingWave Labs
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

use std::cmp::Ordering;
use std::collections::BTreeMap;

use anyhow::Context;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt, pin_mut, stream};
use futures_async_stream::try_stream;
use itertools::Itertools;
use risingwave_common::bail;
use risingwave_common::catalog::{ColumnDesc, ColumnId, Field, Schema};
use risingwave_common::license::Feature;
use risingwave_common::row::OwnedRow;
use risingwave_common::types::ScalarImpl;
use risingwave_pb::plan_common::{PbEncodeType, PbFormatType};
use serde::{Deserialize, Serialize};
use tiberius::{Config, Query, QueryItem};

use crate::connector_common::sql_server::mssql_type_to_rw_type;
use crate::error::{ConnectorError, ConnectorResult};
use crate::parser::{
    DebeziumTransactionStatus, ScalarImplTiberiusWrapper, SqlServerRowDecoder, TransactionControl,
};
use crate::sink::sqlserver::SqlServerClient;
use crate::source::CdcTableSnapshotSplit;
use crate::source::cdc::external::{
    CdcOffset, CdcOffsetParseFunc, CdcTableSnapshotSplitOption, DATABASE_NAME_KEY, DebeziumOffset,
    ExternalTable, ExternalTableConfig, ExternalTableReader, SCHEMA_NAME_KEY, SchemaTableName,
    TABLE_NAME_KEY,
};
use crate::source::cdc::split::{CdcSplitBase, CdcSplitTrait, DebeziumCdcSplit};
use crate::source::cdc::{
    CdcDebeziumOffsetInterpreter, CdcDebeziumPayloadInterpreter, CdcExternalTableHandler,
    CdcExternalTableNameParser, CdcSerializedNames, CdcSourceFrontendValidator,
    CdcSourceRuntimeFlags, SqlServer,
    db_name_end_and_table_name_start_from_schema_qualified_topic_suffix,
    derived_cdc_offset_variant, derived_debezium_cdc_split_split_field, parse_debezium_offset,
};

// ------- Persisted names -------

impl CdcSerializedNames for SqlServer {
    const CDC_OFFSET_VARIANT: &'static str = derived_cdc_offset_variant!(SqlServer);
    const DEBEZIUM_CDC_SPLIT_SPLIT_FIELD: &'static str =
        derived_debezium_cdc_split_split_field!(SqlServer);
}

// ------- Frontend validation -------

impl CdcSourceFrontendValidator for SqlServer {
    const ALLOW_ALTER_ON_FLY_FIELDS: &'static [&'static str] = &[
        "cdc.source.wait.streaming.start.timeout",
        "debezium.max.queue.size",
        "debezium.queue.memory.ratio",
        "hostname",
        "port",
        "password",
    ];
    const COMPATIBLE_FORMATS: &'static [(PbFormatType, &'static [PbEncodeType])] = &[
        (PbFormatType::Debezium, &[PbEncodeType::Json]),
        // support source stream job
        (PbFormatType::Plain, &[PbEncodeType::Json]),
    ];
    const LICENSE_FEATURE: Option<Feature> = Some(Feature::SqlServerCdcSource);

    fn fill_and_validate_props(props: &mut BTreeMap<String, String>) -> ConnectorResult<()> {
        // Default schema name is "dbo"
        props
            .entry("schema.name".to_owned())
            .or_insert_with(|| "dbo".to_owned());
        Ok(())
    }
}

// ------- Runtime flags -------

impl CdcSourceRuntimeFlags for SqlServer {
    const CAN_BACKFILL: bool = true;
    const MUST_WAIT_OFFSET_BEFORE_REPORT: bool = true;
}

// ------- Table name -------

impl CdcExternalTableNameParser for SqlServer {
    /// Accepts `schema.table`, or `database.schema.table` where the database must match the
    /// source's `database.name`. A bare table name is rejected because the schema is not known
    /// from the source. The returned name is always `schema.table`, which is what Debezium
    /// reports.
    fn fill_table_name_options(
        options: &mut BTreeMap<String, String>,
        external_table_name: &str,
    ) -> ConnectorResult<String> {
        let source_database_name = options
            .get(DATABASE_NAME_KEY)
            .map(String::as_str)
            .unwrap_or_default();
        let parts: Vec<&str> = external_table_name.split('.').collect();
        let (schema_name, table_name) = match parts.as_slice() {
            [db_name, schema_name, table_name] => {
                if *db_name != source_database_name {
                    bail!(
                        "The database name '{}' in FROM clause does not match the database name '{}' specified in source definition. \
                         You can either use 'schema.table' format (recommended) or ensure the database name matches.",
                        db_name,
                        source_database_name
                    );
                }
                (*schema_name, *table_name)
            }
            [schema_name, table_name] => (*schema_name, *table_name),
            [_] => {
                bail!(
                    "Invalid table name format '{}'. For SQL Server CDC, you must specify the schema name. \
                     Use 'schema.table' format (e.g., 'dbo.{}') or 'database.schema.table' format (e.g., '{}.dbo.{}').",
                    external_table_name,
                    external_table_name,
                    source_database_name,
                    external_table_name
                );
            }
            _ => {
                bail!(
                    "Invalid table name format '{}'. Expected 'schema.table' or 'database.schema.table'.",
                    external_table_name
                );
            }
        };
        options.insert(SCHEMA_NAME_KEY.into(), schema_name.into());
        options.insert(TABLE_NAME_KEY.into(), table_name.into());
        Ok(format!("{}.{}", schema_name, table_name))
    }

    fn schema_table_name_from_options(options: &BTreeMap<String, String>) -> SchemaTableName {
        SchemaTableName {
            schema_name: options.get(SCHEMA_NAME_KEY).cloned().unwrap_or_default(),
            table_name: options.get(TABLE_NAME_KEY).cloned().unwrap_or_default(),
        }
    }

    fn db_name_end_and_table_name_start_from_debezium_topic_suffix(
        topic_suffix: &str,
    ) -> (usize, usize) {
        db_name_end_and_table_name_start_from_schema_qualified_topic_suffix(topic_suffix)
    }
}

// ------- Debezium payload -------

impl CdcDebeziumPayloadInterpreter for SqlServer {
    /// The id is the commit LSN, e.g. `00000027:00000ac0:0002`, and is used as is.
    fn transaction_control_from_transaction_meta(
        status: DebeziumTransactionStatus,
        id: &str,
    ) -> Option<TransactionControl> {
        Some(status.into_control(id))
    }
}

// ------- Split -------

#[derive(Clone, Default, Serialize, Deserialize, Debug, PartialEq, Hash)]
pub struct SqlServerCdcSplit {
    pub inner: CdcSplitBase,
}

impl CdcSplitTrait for SqlServerCdcSplit {
    fn inner(&self) -> &CdcSplitBase {
        &self.inner
    }

    fn inner_mut(&mut self) -> &mut CdcSplitBase {
        &mut self.inner
    }
}

impl SqlServerCdcSplit {
    /// Extract SQL Server `change_lsn` value from the offset JSON string.
    pub fn sql_server_change_lsn(&self) -> Option<u128> {
        let offset_str = self.inner.start_offset.as_ref()?;
        extract_sql_server_change_lsn_from_offset_str(offset_str)
    }

    /// Extract SQL Server `commit_lsn` value from the offset JSON string.
    pub fn sql_server_commit_lsn(&self) -> Option<u128> {
        let offset_str = self.inner.start_offset.as_ref()?;
        extract_sql_server_commit_lsn_from_offset_str(offset_str)
    }
}

impl DebeziumCdcSplit<SqlServer> {
    /// Extract SQL Server CDC `change_lsn` from the current split offset.
    pub fn sql_server_change_lsn(&self) -> Option<u128> {
        self.sql_server_split.as_ref()?.sql_server_change_lsn()
    }

    /// Extract SQL Server CDC `commit_lsn` from the current split offset.
    pub fn sql_server_commit_lsn(&self) -> Option<u128> {
        self.sql_server_split.as_ref()?.sql_server_commit_lsn()
    }
}

/// Parse SQL Server LSN string (`XXXXXXXX:XXXXXXXX:XXXX`) into a comparable integer.
pub fn parse_sql_server_lsn_str(lsn: &str) -> Option<u128> {
    let mut parts = lsn.split(':');
    let part0 = u32::from_str_radix(parts.next()?, 16).ok()? as u128;
    let part1 = u32::from_str_radix(parts.next()?, 16).ok()? as u128;
    let part2 = u16::from_str_radix(parts.next()?, 16).ok()? as u128;
    if parts.next().is_some() {
        return None;
    }

    Some((part0 << 48) | (part1 << 16) | part2)
}

/// Extract SQL Server `change_lsn` from a CDC offset JSON string.
pub fn extract_sql_server_change_lsn_from_offset_str(offset_str: &str) -> Option<u128> {
    let offset = serde_json::from_str::<serde_json::Value>(offset_str).ok()?;
    let source_offset = offset.get("sourceOffset")?;
    let lsn = source_offset.get("change_lsn")?.as_str()?;
    parse_sql_server_lsn_str(lsn)
}

/// Extract SQL Server `commit_lsn` from a CDC offset JSON string.
pub fn extract_sql_server_commit_lsn_from_offset_str(offset_str: &str) -> Option<u128> {
    let offset = serde_json::from_str::<serde_json::Value>(offset_str).ok()?;
    let source_offset = offset.get("sourceOffset")?;
    let lsn = source_offset.get("commit_lsn")?.as_str()?;
    parse_sql_server_lsn_str(lsn)
}

// ------- External table -------

// The maximum commit_lsn value in Sql Server
const MAX_COMMIT_LSN: &str = "ffffffff:ffffffff:ffff";

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct SqlServerOffset {
    // https://learn.microsoft.com/en-us/answers/questions/1328359/how-to-accurately-sequence-change-data-capture-dat
    pub change_lsn: String,
    pub commit_lsn: String,
}

// only compare the lsn field
impl PartialOrd for SqlServerOffset {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self.change_lsn.partial_cmp(&other.change_lsn) {
            Some(Ordering::Equal) => self.commit_lsn.partial_cmp(&other.commit_lsn),
            other => other,
        }
    }
}

/// `sourceOffset` of a SQL Server Debezium offset.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct SqlServerSourceOffset {
    pub snapshot: Option<bool>,
    pub change_lsn: Option<String>,
    pub commit_lsn: Option<String>,
}

impl CdcDebeziumOffsetInterpreter for SqlServer {
    type Offset = SqlServerOffset;
    type SourceOffset = SqlServerSourceOffset;

    /// Debezium sets `snapshot: true` while snapshotting and drops the key once streaming.
    fn snapshot_done_from_debezium_offset(offset: &DebeziumOffset<SqlServerSourceOffset>) -> bool {
        !offset.source_offset.snapshot.unwrap_or(false)
    }

    fn offset_from_debezium_offset(
        offset: DebeziumOffset<SqlServerSourceOffset>,
    ) -> ConnectorResult<SqlServerOffset> {
        Ok(SqlServerOffset {
            change_lsn: offset
                .source_offset
                .change_lsn
                .context("invalid sql server change_lsn")?,
            commit_lsn: offset
                .source_offset
                .commit_lsn
                .context("invalid sql server commit_lsn")?,
        })
    }
}

pub struct SqlServerExternalTable {
    column_descs: Vec<ColumnDesc>,
    pk_names: Vec<String>,
}

/// SQL Server-specific options of [`ExternalTableConfig`].
#[derive(Debug, Default, Clone, Deserialize)]
pub struct SqlServerTableConfig {
    /// Whether to connect with SSL. Only `"true"` enables it; any other value is `"false"`.
    #[serde(rename = "database.encrypt", default)]
    pub encrypt: String,
}

impl SqlServerExternalTable {
    pub async fn connect(config: ExternalTableConfig) -> ConnectorResult<Self> {
        tracing::debug!("connect to sql server");
        let config = config.with_specific::<SqlServerTableConfig>()?;

        let mut client_config = Config::new();

        client_config.host(&config.host);
        client_config.database(&config.database);
        client_config.port(config.port.parse::<u16>().unwrap());
        client_config.authentication(tiberius::AuthMethod::sql_server(
            &config.username,
            &config.password,
        ));
        // TODO(kexiang): use trust_cert_ca, trust_cert is not secure
        if config.specific.encrypt == "true" {
            client_config.encryption(tiberius::EncryptionLevel::Required);
        }
        client_config.trust_cert();

        let mut client = SqlServerClient::new_with_config(client_config).await?;

        let mut column_descs = vec![];
        let mut pk_names = vec![];
        {
            let sql = Query::new(format!(
                "SELECT
                    COLUMN_NAME,
                    DATA_TYPE
                FROM
                    INFORMATION_SCHEMA.COLUMNS
                WHERE
                    TABLE_SCHEMA = '{}'
                    AND TABLE_NAME = '{}'
                ORDER BY
                    ORDINAL_POSITION",
                config.schema.clone(),
                config.table.clone(),
            ));

            let mut stream = sql.query(&mut client.inner_client).await?;
            while let Some(item) = stream.try_next().await? {
                match item {
                    QueryItem::Metadata(_) => {}
                    QueryItem::Row(row) => {
                        let col_name: &str = row.try_get(0)?.unwrap();
                        let col_type: &str = row.try_get(1)?.unwrap();
                        column_descs.push(ColumnDesc::named(
                            col_name,
                            ColumnId::placeholder(),
                            mssql_type_to_rw_type(col_type).with_context(|| {
                                format!(
                                    "unsupported SQL Server data type {col_type:?}, column name: {col_name}"
                                )
                            })?,
                        ));
                    }
                }
            }
        }
        {
            let sql = Query::new(format!(
                "SELECT kcu.COLUMN_NAME
                FROM
                    INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS tc
                JOIN
                    INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS kcu
                    ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME AND
                    tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA AND
                    tc.TABLE_NAME = kcu.TABLE_NAME
                WHERE
                    tc.CONSTRAINT_TYPE = 'PRIMARY KEY' AND
                    tc.TABLE_SCHEMA = '{}' AND tc.TABLE_NAME = '{}'
                ORDER BY
                    kcu.ORDINAL_POSITION",
                config.schema, config.table,
            ));

            let mut stream = sql.query(&mut client.inner_client).await?;
            while let Some(item) = stream.try_next().await? {
                match item {
                    QueryItem::Metadata(_) => {}
                    QueryItem::Row(row) => {
                        let pk_name: &str = row.try_get(0)?.unwrap();
                        pk_names.push(pk_name.to_owned());
                    }
                }
            }
        }

        // The table does not exist
        if column_descs.is_empty() {
            bail!(
                "Sql Server table '{}'.'{}' not found in '{}'",
                config.schema,
                config.table,
                config.database
            );
        }

        Ok(Self {
            column_descs,
            pk_names,
        })
    }
}

impl ExternalTable for SqlServerExternalTable {
    fn column_descs(&self) -> &[ColumnDesc] {
        &self.column_descs
    }

    fn pk_names(&self) -> &[String] {
        &self.pk_names
    }
}

impl CdcExternalTableHandler for SqlServer {
    type TableReader = SqlServerExternalTableReader;

    async fn create_table_reader(
        config: ExternalTableConfig,
        schema: Schema,
        pk_indices: Vec<usize>,
        _schema_table_name: SchemaTableName,
        _table_id: u32,
    ) -> ConnectorResult<SqlServerExternalTableReader> {
        SqlServerExternalTableReader::new(config, schema, pk_indices).await
    }

    async fn connect_external_table(
        config: ExternalTableConfig,
    ) -> ConnectorResult<Box<dyn ExternalTable>> {
        Ok(Box::new(SqlServerExternalTable::connect(config).await?))
    }
}

#[derive(Debug)]
pub struct SqlServerExternalTableReader {
    rw_schema: Schema,
    pk_indices: Vec<usize>,
    field_names: String,
    client: tokio::sync::Mutex<SqlServerClient>,
}

impl ExternalTableReader for SqlServerExternalTableReader {
    async fn current_cdc_offset(&self) -> ConnectorResult<CdcOffset> {
        let mut client = self.client.lock().await;
        // start a transaction to read max start_lsn.
        let row = client
            .inner_client
            .simple_query(String::from("SELECT sys.fn_cdc_get_max_lsn()"))
            .await?
            .into_row()
            .await?
            .expect("No result returned by `SELECT sys.fn_cdc_get_max_lsn()`");
        // An example of change_lsn or commit_lsn: "00000027:00000ac0:0002" from debezium
        // sys.fn_cdc_get_max_lsn() returns a 10 bytes array, we convert it to a hex string here.
        let max_lsn = match row.try_get::<&[u8], usize>(0)? {
            Some(bytes) => {
                let mut hex_string = String::with_capacity(bytes.len() * 2 + 2);
                assert_eq!(
                    bytes.len(),
                    10,
                    "sys.fn_cdc_get_max_lsn() should return a 10 bytes array."
                );
                for byte in &bytes[0..4] {
                    hex_string.push_str(&format!("{:02x}", byte));
                }
                hex_string.push(':');
                for byte in &bytes[4..8] {
                    hex_string.push_str(&format!("{:02x}", byte));
                }
                hex_string.push(':');
                for byte in &bytes[8..10] {
                    hex_string.push_str(&format!("{:02x}", byte));
                }
                hex_string
            }
            None => bail!(
                "None is returned by `SELECT sys.fn_cdc_get_max_lsn()`, please ensure Sql Server Agent is running."
            ),
        };

        tracing::debug!("current max_lsn: {}", max_lsn);

        Ok(CdcOffset::SqlServer(SqlServerOffset {
            change_lsn: max_lsn,
            commit_lsn: MAX_COMMIT_LSN.into(),
        }))
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
        _options: CdcTableSnapshotSplitOption,
    ) -> BoxStream<'_, ConnectorResult<CdcTableSnapshotSplit>> {
        // TODO(zw): feat: impl
        stream::empty::<ConnectorResult<CdcTableSnapshotSplit>>().boxed()
    }

    fn split_snapshot_read(
        &self,
        _table_name: SchemaTableName,
        _left: OwnedRow,
        _right: OwnedRow,
        _split_columns: Vec<Field>,
    ) -> BoxStream<'_, ConnectorResult<OwnedRow>> {
        todo!("implement SqlServer CDC parallelized backfill")
    }
}

impl SqlServerExternalTableReader {
    pub async fn new(
        config: ExternalTableConfig,
        rw_schema: Schema,
        pk_indices: Vec<usize>,
    ) -> ConnectorResult<Self> {
        tracing::info!(
            ?rw_schema,
            ?pk_indices,
            "create sql server external table reader"
        );
        let config = config.with_specific::<SqlServerTableConfig>()?;
        let mut client_config = Config::new();

        client_config.host(&config.host);
        client_config.database(&config.database);
        client_config.port(config.port.parse::<u16>().unwrap());
        client_config.authentication(tiberius::AuthMethod::sql_server(
            &config.username,
            &config.password,
        ));
        // TODO(kexiang): use trust_cert_ca, trust_cert is not secure
        if config.specific.encrypt == "true" {
            client_config.encryption(tiberius::EncryptionLevel::Required);
        }
        client_config.trust_cert();

        let client = SqlServerClient::new_with_config(client_config).await?;

        let field_names = rw_schema
            .fields
            .iter()
            .map(|f| Self::quote_column(&f.name))
            .join(",");

        Ok(Self {
            rw_schema,
            pk_indices,
            field_names,
            client: tokio::sync::Mutex::new(client),
        })
    }

    pub fn get_cdc_offset_parser() -> CdcOffsetParseFunc {
        Box::new(move |offset| {
            Ok(CdcOffset::SqlServer(parse_debezium_offset::<SqlServer>(
                offset,
            )?))
        })
    }

    #[try_stream(boxed, ok = OwnedRow, error = ConnectorError)]
    async fn snapshot_read_inner(
        &self,
        table_name: SchemaTableName,
        start_pk_row: Option<OwnedRow>,
        primary_keys: Vec<String>,
        limit: u32,
    ) {
        let order_key = primary_keys
            .iter()
            .map(|col| Self::quote_column(col))
            .join(",");
        let mut sql = Query::new(if start_pk_row.is_none() {
            format!(
                "SELECT {} FROM {} ORDER BY {} OFFSET 0 ROWS FETCH NEXT {limit} ROWS ONLY",
                self.field_names,
                Self::get_normalized_table_name(&table_name),
                order_key,
            )
        } else {
            let filter_expr = Self::filter_expression(&primary_keys);
            format!(
                "SELECT {} FROM {} WHERE {} ORDER BY {} OFFSET 0 ROWS FETCH NEXT {limit} ROWS ONLY",
                self.field_names,
                Self::get_normalized_table_name(&table_name),
                filter_expr,
                order_key,
            )
        });

        let mut client = self.client.lock().await;

        // FIXME(kexiang): Set session timezone to UTC
        if let Some(pk_row) = start_pk_row {
            let params: Vec<Option<ScalarImpl>> = pk_row.into_iter().collect();
            for (index, param) in params.into_iter().enumerate() {
                let param = param.with_context(|| {
                    format!(
                        "SQL Server snapshot primary-key position at index {index} cannot be NULL"
                    )
                })?;
                sql.bind(ScalarImplTiberiusWrapper::from(param));
            }
        }

        let mut query_stream = sql.query(&mut client.inner_client).await?;
        let decoder = SqlServerRowDecoder::new(
            query_stream
                .columns()
                .await?
                .context("SQL Server snapshot query returned no result metadata")?,
            &self.rw_schema,
        )?;
        let stream = query_stream.into_row_stream();

        let row_stream = stream.map(|res| {
            // convert sql server row into OwnedRow
            let mut row = res?;
            decoder
                .decode_with_strict_pk(&mut row, &self.pk_indices)
                .map_err(ConnectorError::from)
        });

        pin_mut!(row_stream);

        #[for_await]
        for row in row_stream {
            let row = row?;
            yield row;
        }
    }

    pub fn get_normalized_table_name(table_name: &SchemaTableName) -> String {
        format!(
            "\"{}\".\"{}\"",
            table_name.schema_name, table_name.table_name
        )
    }

    // sql server cannot leverage the given key to narrow down the range of scan,
    // we need to rewrite the comparison conditions by our own.
    // (a, b) > (x, y) => ("a" > @P1) OR (("a" = @P1) AND ("b" > @P2))
    fn filter_expression(columns: &[String]) -> String {
        let mut conditions = vec![];
        // push the first condition
        conditions.push(format!("({} > @P{})", Self::quote_column(&columns[0]), 1));
        for i in 2..=columns.len() {
            // '=' condition
            let mut condition = String::new();
            for (j, col) in columns.iter().enumerate().take(i - 1) {
                if j == 0 {
                    condition.push_str(&format!("({} = @P{})", Self::quote_column(col), j + 1));
                } else {
                    condition.push_str(&format!(
                        " AND ({} = @P{})",
                        Self::quote_column(col),
                        j + 1
                    ));
                }
            }
            // '>' condition
            condition.push_str(&format!(
                " AND ({} > @P{})",
                Self::quote_column(&columns[i - 1]),
                i
            ));
            conditions.push(format!("({})", condition));
        }
        if columns.len() > 1 {
            conditions.join(" OR ")
        } else {
            conditions.join("")
        }
    }

    fn quote_column(column: &str) -> String {
        format!("\"{}\"", column)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_sql_server_lsn_str() {
        let lsn = "00000027:00000ac0:0002";
        let parsed = parse_sql_server_lsn_str(lsn).unwrap();
        let expected = ((0x00000027_u128) << 48) | ((0x00000ac0_u128) << 16) | (0x0002_u128);
        assert_eq!(parsed, expected);
    }

    #[test]
    fn test_extract_sql_server_lsn_from_offset_str() {
        let offset = r#"{
            "sourcePartition": {"server":"RW_CDC_1001"},
            "sourceOffset": {
                "change_lsn":"00000027:00000ac0:0001",
                "commit_lsn":"00000027:00000ac0:0002"
            },
            "isHeartbeat": false
        }"#;

        let change_lsn = extract_sql_server_change_lsn_from_offset_str(offset).unwrap();
        let commit_lsn = extract_sql_server_commit_lsn_from_offset_str(offset).unwrap();
        assert!(change_lsn < commit_lsn);
    }

    #[test]
    fn test_sql_server_filter_expr() {
        let cols = vec!["id".to_owned()];
        let expr = SqlServerExternalTableReader::filter_expression(&cols);
        assert_eq!(expr, "(\"id\" > @P1)");

        let cols = vec!["aa".to_owned(), "bb".to_owned(), "cc".to_owned()];
        let expr = SqlServerExternalTableReader::filter_expression(&cols);
        assert_eq!(
            expr,
            "(\"aa\" > @P1) OR ((\"aa\" = @P1) AND (\"bb\" > @P2)) OR ((\"aa\" = @P1) AND (\"bb\" = @P2) AND (\"cc\" > @P3))"
        );
    }
}

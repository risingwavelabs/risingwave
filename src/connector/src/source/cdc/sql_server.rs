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

use std::collections::BTreeMap;

use risingwave_common::bail;
use risingwave_common::license::Feature;
use risingwave_pb::plan_common::{PbEncodeType, PbFormatType};
use serde::{Deserialize, Serialize};

use crate::error::ConnectorResult;
use crate::parser::{DebeziumTransactionStatus, TransactionControl};
use crate::source::cdc::external::{
    DATABASE_NAME_KEY, SCHEMA_NAME_KEY, SchemaTableName, TABLE_NAME_KEY,
};
use crate::source::cdc::split::{CdcSplitBase, CdcSplitTrait, DebeziumCdcSplit};
use crate::source::cdc::{
    CdcDebeziumPayloadInterpreter, CdcExternalTableNameParser, CdcSerializedFieldNames,
    CdcSourceFrontendValidator, CdcSourceRuntimeFlags, SqlServer,
    db_name_end_and_table_name_start_from_schema_qualified_topic_suffix,
};

// ------- Persisted names -------

impl CdcSerializedFieldNames for SqlServer {}

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
}

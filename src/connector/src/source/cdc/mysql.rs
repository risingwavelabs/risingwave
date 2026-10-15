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

use rand::Rng;
use risingwave_common::bail;
use risingwave_common::types::DataType;
use risingwave_pb::plan_common::{PbEncodeType, PbFormatType};
use serde::{Deserialize, Serialize};
use thiserror_ext::AsReport;

use crate::error::ConnectorResult;
use crate::parser::schema_change::{TableChangeColumn, TableChangeContext};
use crate::parser::{
    AccessError, AccessResult, DebeziumTransactionStatus, TransactionControl, jsonb_access_field,
};
use crate::source::cdc::external::mysql::{
    mysql_type_to_rw_type, timestamp_val_to_timestamptz, type_name_to_mysql_type,
};
use crate::source::cdc::external::{DATABASE_NAME_KEY, SchemaTableName, TABLE_NAME_KEY};
use crate::source::cdc::split::{CdcSplitBase, CdcSplitTrait, DebeziumCdcSplit};
use crate::source::cdc::{
    CdcDebeziumPayloadInterpreter, CdcExternalTableNameParser, CdcSerializedFieldNames,
    CdcSourceFrontendValidator, CdcSourceRuntimeFlags, Mysql,
    db_name_end_and_table_name_start_from_database_qualified_topic_suffix,
};

// ------- Persisted names -------

impl CdcSerializedFieldNames for Mysql {}

// ------- Frontend validation -------

impl CdcSourceFrontendValidator for Mysql {
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

    fn fill_and_validate_props(props: &mut BTreeMap<String, String>) -> ConnectorResult<()> {
        // Generate a random server id for mysql cdc source if needed
        // `server.id` (in the range from 1 to 2^32 - 1). This value MUST be unique across whole replication
        // group (that is, different from any other server id being used by any master or slave)
        props
            .entry("server.id".to_owned())
            .or_insert_with(|| rand::rng().random_range(1..u32::MAX).to_string());
        Ok(())
    }
}

// ------- Runtime flags -------

impl CdcSourceRuntimeFlags for Mysql {
    const CAN_BACKFILL: bool = true;
    const ENABLE_TRANSACTION_METADATA: bool = true;
    const MUST_WAIT_OFFSET_BEFORE_REPORT: bool = true;
}

// ------- Table name -------

impl CdcExternalTableNameParser for Mysql {
    /// MySQL has no schemas and forbids `.` in database and table names, so the clause is
    /// `database.table`. The database must be one of those listed in the source's
    /// `database.name`, and replaces that list in the table's options.
    fn fill_table_name_options(
        options: &mut BTreeMap<String, String>,
        external_table_name: &str,
    ) -> ConnectorResult<String> {
        let Some((db_name, table_name)) = external_table_name.split_once('.') else {
            bail!(
                "The upstream table name must contain database name prefix, e.g. 'database.table'"
            );
        };
        let source_database_name = options
            .get(DATABASE_NAME_KEY)
            .map(String::as_str)
            .unwrap_or_default();
        if !source_database_name
            .split(',')
            .map(str::trim)
            .any(|name| name == db_name)
        {
            bail!(
                "The database name `{}` in the FROM clause is not included in the database name `{}` in source definition",
                db_name,
                source_database_name
            );
        }
        options.insert(DATABASE_NAME_KEY.into(), db_name.into());
        options.insert(TABLE_NAME_KEY.into(), table_name.into());
        Ok(external_table_name.to_owned())
    }

    fn schema_table_name_from_options(options: &BTreeMap<String, String>) -> SchemaTableName {
        SchemaTableName {
            schema_name: options.get(DATABASE_NAME_KEY).cloned().unwrap_or_default(),
            table_name: options.get(TABLE_NAME_KEY).cloned().unwrap_or_default(),
        }
    }

    fn db_name_end_and_table_name_start_from_debezium_topic_suffix(
        topic_suffix: &str,
    ) -> (usize, usize) {
        db_name_end_and_table_name_start_from_database_qualified_topic_suffix(topic_suffix)
    }
}

// ------- Debezium payload -------

impl CdcDebeziumPayloadInterpreter for Mysql {
    /// The id is `source_id:transaction_id`, e.g. `3E11FA47-71CA-11E1-9E33-C80AA9429562:23`,
    /// and is used as is.
    fn transaction_control_from_transaction_meta(
        status: DebeziumTransactionStatus,
        id: &str,
    ) -> Option<TransactionControl> {
        Some(status.into_control(id))
    }

    /// Types come from `type_name_to_mysql_type`. A `timestamp` default is interpreted in UTC
    /// because MySQL `timestamp` maps to `timestamptz`.
    async fn columns_from_table_change(
        ctx: &TableChangeContext<'_>,
    ) -> AccessResult<Vec<TableChangeColumn>> {
        let mut columns = vec![];
        for col in ctx.columns.array_elements().unwrap() {
            let name: String = jsonb_access_field!(col, "name", string);
            let type_name: String = jsonb_access_field!(col, "typeName", string);
            let data_type = match type_name_to_mysql_type(type_name.as_str()) {
                Some(ty) => match mysql_type_to_rw_type(&ty) {
                    Ok(data_type) => data_type,
                    Err(err) => {
                        tracing::warn!(error=%err.as_report(), "unsupported mysql type in schema change message");
                        return Err(ctx.unsupported_type(type_name));
                    }
                },
                None => return Err(ctx.unsupported_type(type_name)),
            };

            let default_value_expression = col
                .access_object_field("defaultValueExpression")
                .filter(|v| !v.is_jsonb_null())
                .map(|v| v.as_str().unwrap().to_owned());
            let default_value_text = match &default_value_expression {
                Some(expr) if data_type == DataType::Timestamptz => {
                    Some(timestamp_val_to_timestamptz(expr).map_err(|err| {
                        tracing::error!(target: "auto_schema_change", error=%err.as_report(), "failed to convert timestamp value to timestamptz");
                        AccessError::TypeError {
                            expected: "timestamp in YYYY-MM-DD HH:MM:SS".into(),
                            got: data_type.to_string(),
                            value: expr.clone(),
                        }
                    })?)
                }
                Some(expr) => Some(expr.clone()),
                None => None,
            };
            columns.push(TableChangeColumn {
                name,
                data_type,
                default_value_expression,
                default_value_text,
            });
        }
        Ok(columns)
    }
}

// ------- Split -------

#[derive(Clone, Default, Serialize, Deserialize, Debug, PartialEq, Hash)]
pub struct MysqlCdcSplit {
    pub inner: CdcSplitBase,
}

impl CdcSplitTrait for MysqlCdcSplit {
    fn inner(&self) -> &CdcSplitBase {
        &self.inner
    }

    fn inner_mut(&mut self) -> &mut CdcSplitBase {
        &mut self.inner
    }
}

impl MysqlCdcSplit {
    /// Extract MySQL CDC binlog offset (file sequence and position) from the offset JSON string.
    ///
    /// MySQL binlog offset format:
    /// ```json
    /// {
    ///   "sourcePartition": { "server": "..." },
    ///   "sourceOffset": {
    ///     "file": "binlog.000123",
    ///     "pos": 456789,
    ///     ...
    ///   }
    /// }
    /// ```
    ///
    /// Returns `Some((file_seq, position))` where:
    /// - `file_seq`: the numeric part of binlog filename (e.g., 123 from "binlog.000123")
    /// - `position`: the byte offset within the binlog file
    pub fn mysql_binlog_offset(&self) -> Option<(u64, u64)> {
        let offset_str = self.inner.start_offset.as_ref()?;
        let offset = serde_json::from_str::<serde_json::Value>(offset_str).ok()?;
        let source_offset = offset.get("sourceOffset")?;

        let file = source_offset.get("file")?.as_str()?;
        let pos = source_offset.get("pos")?.as_u64()?;

        let file_seq = extract_binlog_file_seq(file)?;

        Some((file_seq, pos))
    }
}

impl DebeziumCdcSplit<Mysql> {
    /// Extract MySQL CDC binlog offset (file sequence and position) from the current split offset.
    ///
    /// Returns `Some((file_seq, position))` where:
    /// - `file_seq`: the numeric part of binlog filename (e.g., 123 from "binlog.000123")
    /// - `position`: the byte offset within the binlog file
    pub fn mysql_binlog_offset(&self) -> Option<(u64, u64)> {
        self.mysql_split.as_ref()?.mysql_binlog_offset()
    }
}

/// Extract the numeric sequence from a MySQL binlog file name `<basename>.<sequence>`.
///
/// The basename is configurable (`binlog`, `mysql-bin`, `mysql-bin-changelog` on RDS/Aurora, ...),
/// so we take the number after the last `.` rather than assuming a fixed prefix.
pub fn extract_binlog_file_seq(file_name: &str) -> Option<u64> {
    file_name.rsplit('.').next()?.parse::<u64>().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_binlog_file_seq() {
        // default MySQL 8.0 basename
        assert_eq!(extract_binlog_file_seq("binlog.000123"), Some(123));
        // `--log-bin=mysql-bin`
        assert_eq!(extract_binlog_file_seq("mysql-bin.000123"), Some(123));
        // `<hostname>-bin` on older versions
        assert_eq!(extract_binlog_file_seq("my-host-bin.000001"), Some(1));
        // RDS / Aurora MySQL
        assert_eq!(
            extract_binlog_file_seq("mysql-bin-changelog.037568"),
            Some(37568)
        );
        // invalid suffix
        assert_eq!(extract_binlog_file_seq("binlog.index"), None);
        assert_eq!(extract_binlog_file_seq("no-extension"), None);
    }

    #[test]
    fn test_mysql_binlog_offset() {
        let offset = r#"{
            "sourcePartition": {"server": "test"},
            "sourceOffset": {
                "file": "mysql-bin-changelog.037568",
                "pos": 12345
            },
            "isHeartbeat": false
        }"#;
        let split = MysqlCdcSplit::new(1, Some(offset.to_owned()));
        assert_eq!(split.mysql_binlog_offset(), Some((37568, 12345)));
    }
}

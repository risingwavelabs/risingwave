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
use risingwave_pb::plan_common::{PbEncodeType, PbFormatType};
use serde::{Deserialize, Serialize};

use crate::error::ConnectorResult;
use crate::source::cdc::split::{CdcSplitBase, CdcSplitTrait, DebeziumCdcSplit};
use crate::source::cdc::{CdcSerializedFieldNames, CdcSourceFrontendValidator, Mysql};

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

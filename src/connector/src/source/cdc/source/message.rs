// Copyright 2025 RisingWave Labs
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

use risingwave_common::types::{DatumRef, ScalarRefImpl, Timestamptz};
use risingwave_pb::connector_service::{CdcMessage, cdc_message};

use crate::source::SourceMeta;
use crate::source::base::SourceMessage;

#[derive(Clone, Debug)]
pub enum CdcMessageType {
    Unspecified,
    Heartbeat,
    Data,
    TransactionMeta,
    SchemaChange,
}

impl From<cdc_message::CdcMessageType> for CdcMessageType {
    fn from(msg_type: cdc_message::CdcMessageType) -> Self {
        match msg_type {
            cdc_message::CdcMessageType::Data => CdcMessageType::Data,
            cdc_message::CdcMessageType::Heartbeat => CdcMessageType::Heartbeat,
            cdc_message::CdcMessageType::TransactionMeta => CdcMessageType::TransactionMeta,
            cdc_message::CdcMessageType::SchemaChange => CdcMessageType::SchemaChange,
            cdc_message::CdcMessageType::Unspecified => CdcMessageType::Unspecified,
        }
    }
}

#[derive(Debug, Clone)]
pub struct DebeziumCdcMeta {
    db_name_prefix_len: usize,

    pub full_table_name: String,
    // extracted from `payload.source.ts_ms`, the time that the change event was made in the database
    pub source_ts_ms: i64,
    pub msg_type: CdcMessageType,
}

impl DebeziumCdcMeta {
    // These `extract_xxx` methods are used to support the `INCLUDE TIMESTAMP/DATABASE_NAME/TABLE_NAME` feature
    pub fn extract_timestamp(&self) -> DatumRef<'_> {
        Some(ScalarRefImpl::Timestamptz(
            Timestamptz::from_millis(self.source_ts_ms).unwrap(),
        ))
    }

    pub fn extract_database_name(&self) -> DatumRef<'_> {
        Some(ScalarRefImpl::Utf8(
            &self.full_table_name.as_str()[0..self.db_name_prefix_len],
        ))
    }

    pub fn extract_table_name(&self) -> DatumRef<'_> {
        Some(ScalarRefImpl::Utf8(if self.db_name_prefix_len == 0 {
            // No dot found, return the entire string
            self.full_table_name.as_str()
        } else {
            // Skip the database name and the dot
            &self.full_table_name.as_str()[self.db_name_prefix_len + 1..]
        }))
    }

    /// Calculate the prefix length to skip when extracting table name from `full_table_name`.
    ///
    /// # Background
    ///
    /// Debezium sends `full_table_name` in different formats depending on the CDC source type:
    ///
    /// - **MySQL**: `"database.table"` (1 dot, 2 parts)
    ///   - Example: `"risedev.orders"`
    ///   - User provides: `"risedev.orders"` (same format)
    ///   - Result: `database_name` = "risedev", `table_name` = "orders"
    ///   - Note: MySQL CDC typically doesn't use the `database_name` column separately
    ///
    /// - **`MongoDB`**: `"database.collection"` (1 dot, 2 parts)
    ///   - Example: `"random_data.users"`
    ///   - User can include: `DATABASE_NAME` and `COLLECTION_NAME`
    ///   - Result: `database_name` = `"random_data"`, collection parsed from payload
    ///
    /// - **Postgres**: `"database.schema.table"` (2 dots, 3 parts)
    ///   - Example: `"mydb.public.users"`
    ///   - User provides: `"public.users"` (database already in source config)
    ///   - Result: `database_name` = "mydb", `table_name` = "public.users"
    ///
    /// - **SQL Server**: `"database.schema.table"` (2 dots, 3 parts)
    ///   - Example: `"mydb.dbo.orders"`
    ///   - User provides: `"dbo.orders"` (database already in source config)
    ///   - Result: `database_name` = "mydb", `table_name` = "dbo.orders"
    ///
    /// # Returns
    ///
    /// The position of the first dot in `full_table_name`, which separates the database name
    /// from the rest. Used by `extract_database_name()` as the end position and by
    /// `extract_table_name()` to skip the database portion.
    fn extract_db_name_prefix_len_from_full_table_name(full_table_name: &str) -> usize {
        // Return position of the first dot (not including the dot itself)
        // This works for all formats:
        // - MySQL: "database.table" → database_name = "database", table_name = "table"
        // - MongoDB: "database.collection" → database_name = "database"
        // - Postgres/SQL Server: "database.schema.table" → database_name = "database", table_name = "schema.table"
        // If no dot found, return 0 (should not happen in practice for valid CDC messages)
        full_table_name.find('.').unwrap_or_default()
    }

    pub fn new(
        full_table_name: String,
        source_ts_ms: i64,
        msg_type: cdc_message::CdcMessageType,
    ) -> Self {
        let db_name_prefix_len =
            Self::extract_db_name_prefix_len_from_full_table_name(&full_table_name);
        Self {
            db_name_prefix_len,
            full_table_name,
            source_ts_ms,
            msg_type: msg_type.into(),
        }
    }
}

impl From<CdcMessage> for SourceMessage {
    fn from(message: CdcMessage) -> Self {
        let msg_type = message.get_msg_type().expect("invalid message type");
        SourceMessage {
            key: if message.key.is_empty() {
                None // only data message has key
            } else {
                Some(message.key.as_bytes().to_vec())
            },
            payload: if message.payload.is_empty() {
                None // heartbeat message
            } else {
                Some(message.payload.as_bytes().to_vec())
            },
            offset: message.offset,
            split_id: message.partition.into(),
            meta: SourceMeta::DebeziumCdc(DebeziumCdcMeta::new(
                message.full_table_name,
                message.source_ts_ms,
                msg_type,
            )),
        }
    }
}

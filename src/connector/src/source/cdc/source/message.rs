// Copyright 2022 RisingWave Labs
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
        Some(ScalarRefImpl::Utf8(
            &self.full_table_name.as_str()[self.db_name_prefix_len..],
        ))
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
    ///   - Result: Keep entire string → `prefix_len` = 0
    ///
    /// - **Postgres**: `"database.schema.table"` (2 dots, 3 parts)
    ///   - Example: `"mydb.public.users"`
    ///   - User provides: `"public.users"` (database already in source config)
    ///   - Result: Skip "mydb." → `prefix_len` = 5
    ///
    /// - **SQL Server**: `"database.schema.table"` (2 dots, 3 parts)
    ///   - Example: `"mydb.dbo.orders"`
    ///   - User provides: `"dbo.orders"` (database already in source config)
    ///   - Result: Skip "mydb." → `prefix_len` = 5
    ///
    /// # Why the difference?
    ///
    /// - **MySQL** has no schema concept, so the "table name" IS `database.table`
    /// - **Postgres/SQL Server** have schemas, and the database is specified in the source,
    ///   so the "table name" is just `schema.table`
    ///
    /// # Returns
    ///
    /// The number of characters to skip from the start of `full_table_name` when calling
    /// `extract_table_name()`. This value is used to strip the database prefix for
    /// Postgres/SQL Server while keeping the full name for MySQL.
    fn extract_db_name_prefix_len_from_full_table_name(full_table_name: &str) -> usize {
        if let Some(first_dot) = full_table_name.find('.') {
            // Check if there's a second dot after the first one
            if full_table_name[first_dot + 1..].find('.').is_some() {
                // Found 2 dots (3 parts): Postgres/SQL Server format "database.schema.table"
                // Return position after first dot to skip "database." part
                first_dot + 1
            } else {
                // Found 1 dot (2 parts): MySQL format "database.table"
                // Return 0 to keep the entire string (no prefix to skip)
                0
            }
        } else {
            // No dot found (should not happen in practice for valid CDC messages)
            0
        }
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

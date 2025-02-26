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
        Some(ScalarRefImpl::Utf8(
            &self.full_table_name.as_str()[self.db_name_prefix_len..],
        ))
    }

    pub fn new(
        full_table_name: String,
        source_ts_ms: i64,
        msg_type: cdc_message::CdcMessageType,
    ) -> Self {
        // full_table_name is in the format of `database_name.table_name`
        let db_name_prefix_len = full_table_name.as_str().find('.').unwrap_or(0);
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

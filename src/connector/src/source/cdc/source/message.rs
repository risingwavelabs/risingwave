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

use risingwave_pb::connector_service::CdcMessage;

use crate::source::base::SourceMessage;
use crate::source::SourceMeta;

#[derive(Debug, Clone)]
pub struct DebeziumCdcMeta {
    pub full_table_name: String,
}

impl From<CdcMessage> for SourceMessage {
    fn from(message: CdcMessage) -> Self {
        SourceMessage {
            key: None,
            payload: if message.payload.is_empty() {
                None // heartbeat message
            } else {
                Some(message.payload.as_bytes().to_vec())
            },
            offset: message.offset,
            split_id: message.partition.into(),
            meta: SourceMeta::DebeziumCdc(DebeziumCdcMeta {
                full_table_name: message.full_table_name,
            }),
        }
    }
}

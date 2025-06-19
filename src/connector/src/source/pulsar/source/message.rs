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

use pulsar::consumer::Message;
use pulsar_prost::Message as PulsarProstMessage;

use crate::source::{SourceMessage, SourceMeta};

#[derive(Debug, Clone)]
pub struct PulsarMeta {
    pub schema_version: Option<Vec<u8>>,
    pub ack_message_id: Option<Vec<u8>>,
}

impl From<Message<Vec<u8>>> for SourceMessage {
    fn from(msg: Message<Vec<u8>>) -> Self {
        let message_id = msg.message_id.id;
        let ack_data_bytes = message_id.encode_to_vec();
        tracing::info!("ack message id original: {:?}", ack_data_bytes);

        SourceMessage {
            key: msg.payload.metadata.partition_key.clone().map(|k| k.into()),
            payload: Some(msg.payload.data),
            offset: format!(
                "{}:{}:{}:{}",
                message_id.ledger_id,
                message_id.entry_id,
                message_id.partition.unwrap_or(-1),
                message_id.batch_index.unwrap_or(-1)
            ),
            split_id: msg.topic.into(),
            meta: SourceMeta::Pulsar(PulsarMeta {
                schema_version: msg.payload.metadata.schema_version.clone(),
                ack_message_id: Some(ack_data_bytes),
            }),
        }
    }
}

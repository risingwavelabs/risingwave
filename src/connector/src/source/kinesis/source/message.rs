// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use aws_sdk_kinesis::model::Record;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::source::SourceMessage;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct KinesisMessage {
    pub shard_id: String,
    pub sequence_number: String,
    pub partition_key: String,
    pub payload: Option<Vec<u8>>,
}

impl From<KinesisMessage> for SourceMessage {
    fn from(msg: KinesisMessage) -> Self {
        SourceMessage {
            payload: msg
                .payload
                .as_ref()
                .map(|payload| Bytes::copy_from_slice(payload)),
            offset: msg.sequence_number.clone(),
            split_id: msg.shard_id,
        }
    }
}

impl KinesisMessage {
    pub fn new(shard_id: String, message: Record) -> Self {
        KinesisMessage {
            shard_id,
            sequence_number: message.sequence_number.unwrap(),
            partition_key: message.partition_key.unwrap(),
            payload: Some(message.data.unwrap().into_inner()),
        }
    }
}

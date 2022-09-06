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

use crate::source::{SourceMessage, SplitId};

#[derive(Clone, Debug)]
pub struct KinesisMessage {
    pub shard_id: SplitId,
    pub sequence_number: String,
    pub partition_key: String,
    pub payload: Bytes,
}

impl From<KinesisMessage> for SourceMessage {
    fn from(msg: KinesisMessage) -> Self {
        SourceMessage {
            payload: Some(msg.payload),
            offset: msg.sequence_number.clone(),
            split_id: msg.shard_id,
        }
    }
}

impl KinesisMessage {
    pub fn new(shard_id: SplitId, message: Record) -> Self {
        KinesisMessage {
            shard_id,
            sequence_number: message.sequence_number.unwrap(),
            partition_key: message.partition_key.unwrap(),
            payload: message.data.unwrap().into_inner().into(),
        }
    }
}

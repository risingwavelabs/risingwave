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

use std::sync::Arc;

use aws_sdk_kinesis::types::Record;

use crate::source::{SourceMessage, SourceMeta, SplitId};

#[derive(Clone, Debug)]
pub struct KinesisMessage {
    pub shard_id: SplitId,
    pub sequence_number: String,
    pub partition_key: String,
    pub payload: Vec<u8>,
}

impl From<KinesisMessage> for SourceMessage {
    fn from(msg: KinesisMessage) -> Self {
        SourceMessage {
            key: Some(msg.partition_key.into_bytes()),
            payload: Some(msg.payload),
            offset: msg.sequence_number.clone(),
            split_id: msg.shard_id,
            meta: SourceMeta::Empty,
        }
    }
}

#[derive(Clone, Debug)]
pub struct KinesisMeta {
    // from `approximate_arrival_timestamp` of type `Option<aws_smithy_types::DateTime>`
    timestamp: Option<i64>,
}

pub fn from_kinesis_record(value: &Record, split_id: SplitId) -> SourceMessage {
    SourceMessage {
        key: Some(value.partition_key.into_bytes()),
        payload: Some(value.data.into_inner()),
        offset: value.sequence_number.clone(),
        split_id,
        meta: SourceMeta::Kinesis(KinesisMeta {
            timestamp: value
                .approximate_arrival_timestamp
                // todo: review if safe to unwrap
                .map(|dt| dt.to_millis().unwrap()),
        }),
    }
}

impl KinesisMessage {
    pub fn new(shard_id: SplitId, message: Record) -> Self {
        KinesisMessage {
            shard_id,
            sequence_number: message.sequence_number,
            partition_key: message.partition_key,
            payload: message.data.into_inner(),
        }
    }
}

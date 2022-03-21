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
//
use anyhow::anyhow;
use bytes::Bytes;
use rdkafka::message::BorrowedMessage;
use rdkafka::Message;
use serde::{Deserialize, Serialize};

use crate::base::{InnerMessage, SourceMessage, SourceOffset};

#[derive(Clone, Serialize, Deserialize)]
pub struct KafkaMessage {
    partition: i32,
    offset: i64,
    payload: Option<Vec<u8>>,
    key: Option<Vec<u8>>,
}

impl SourceMessage for KafkaMessage {
    fn payload(&self) -> anyhow::Result<Option<&[u8]>> {
        Ok(self.payload.as_ref().map(|payload| payload.as_ref()))
    }

    fn offset(&self) -> anyhow::Result<Option<SourceOffset>> {
        Ok(Some(SourceOffset::Number(self.offset)))
    }

    fn serialize(&self) -> anyhow::Result<String> {
        serde_json::to_string(self).map_err(|e| anyhow!(e))
    }
}

impl<'a> From<BorrowedMessage<'a>> for InnerMessage {
    fn from(message: BorrowedMessage<'a>) -> Self {
        InnerMessage {
            payload: message.payload().map(Bytes::copy_from_slice),
            offset: message.offset().to_string(),
            split_id: message.partition().to_string(),
        }
    }
}

// Copyright 2024 RisingWave Labs
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

use rdkafka::message::BorrowedMessage;
use rdkafka::Message;

use crate::source::base::SourceMessage;
use crate::source::SourceMeta;

#[derive(Debug, Clone)]
pub struct KafkaMeta {
    // timestamp(milliseconds) of message append in mq
    pub timestamp: Option<i64>,
}

impl SourceMessage {
    pub fn from_kafka_message(message: &BorrowedMessage<'_>) -> Self {
        SourceMessage {
            // TODO(TaoWu): Possible performance improvement: avoid memory copying here.
            key: message.key().map(|p| p.to_vec()),
            payload: message.payload().map(|p| p.to_vec()),
            offset: message.offset().to_string(),
            split_id: message.partition().to_string().into(),
            meta: SourceMeta::Kafka(KafkaMeta {
                timestamp: message.timestamp().to_millis(),
            }),
        }
    }
}

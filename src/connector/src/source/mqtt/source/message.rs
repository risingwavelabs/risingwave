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

use rumqttc::v5::mqttbytes::v5::Publish;

use crate::source::SourceMeta;
use crate::source::base::SourceMessage;

#[derive(Clone, Debug)]
pub struct MqttMessage {
    pub topic: String,
    pub sequence_number: String,
    pub payload: Vec<u8>,
}

impl From<MqttMessage> for SourceMessage {
    fn from(message: MqttMessage) -> Self {
        SourceMessage {
            key: None,
            payload: Some(message.payload),
            // For nats jetstream, use sequence id as offset
            offset: message.sequence_number,
            split_id: message.topic.into(),
            meta: SourceMeta::Empty,
        }
    }
}

impl MqttMessage {
    pub fn new(message: Publish) -> Self {
        MqttMessage {
            topic: String::from_utf8_lossy(&message.topic).to_string(),
            sequence_number: message.pkid.to_string(),
            payload: message.payload.to_vec(),
        }
    }
}

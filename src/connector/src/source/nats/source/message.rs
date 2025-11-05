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

use async_nats::jetstream::Message;
use risingwave_common::types::{DatumRef, ScalarRefImpl};

use crate::source::base::SourceMessage;
use crate::source::{SourceMeta, SplitId};

#[derive(Debug, Clone)]
pub struct NatsMeta {
    pub subject: String,
}

impl NatsMeta {
    pub fn extract_subject(&self) -> DatumRef<'_> {
        Some(ScalarRefImpl::Utf8(self.subject.as_str()))
    }
}

#[derive(Clone, Debug)]
pub struct NatsMessage {
    pub split_id: SplitId,
    pub sequence_number: String,
    pub payload: Vec<u8>,
    pub reply_subject: Option<String>,
    pub subject: String,
}

impl From<NatsMessage> for SourceMessage {
    fn from(message: NatsMessage) -> Self {
        SourceMessage {
            key: None,
            payload: Some(message.payload),
            // For nats jetstream, use sequence id as offset
            //
            // DEPRECATED: no longer use sequence id as offset, let nats broker handle failover
            // use reply_subject as offset for ack use, we just check the persisted state for whether this is the first run
            offset: message.reply_subject.unwrap_or_default(),
            split_id: message.split_id,
            meta: SourceMeta::Nats(NatsMeta {
                subject: message.subject,
            }),
        }
    }
}

impl NatsMessage {
    pub fn new(split_id: SplitId, message: Message) -> Self {
        NatsMessage {
            split_id,
            sequence_number: message.info().unwrap().stream_sequence.to_string(),
            payload: message.message.payload.to_vec(),
            reply_subject: message
                .message
                .reply
                .map(|subject| subject.as_str().to_owned()),
            subject: message.message.subject.as_str().to_owned(),
        }
    }
}

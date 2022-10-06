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

use bytes::Bytes;
use chrono::{TimeZone, Utc};
use google_cloud_pubsub::subscriber::ReceivedMessage;

use crate::source::SourceMessage;

pub(crate) struct TaggedReceivedMessage(pub(crate) String, pub(crate) ReceivedMessage);

impl From<TaggedReceivedMessage> for SourceMessage {
    fn from(tagged_message: TaggedReceivedMessage) -> Self {
        let TaggedReceivedMessage(split_id, message) = tagged_message;

        let timestamp = message
            .message
            .publish_time
            .map(|t| Utc.timestamp(t.seconds, u32::try_from(t.nanos).unwrap_or_default()))
            .unwrap_or_default();

        Self {
            payload: {
                let payload = message.message.data;
                match payload.len() {
                    0 => None,
                    _ => Some(Bytes::from(payload)),
                }
            },

            offset: timestamp.to_string(),

            split_id: split_id.into(),
        }
    }
}

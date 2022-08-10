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
use serde::{Deserialize, Serialize};

use crate::source::nexmark::source::event::Event;
use crate::source::SourceMessage;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NexmarkMessage {
    pub shard_id: String,
    pub sequence_number: String,
    pub payload: Option<Vec<u8>>,
}

impl From<NexmarkMessage> for SourceMessage {
    fn from(msg: NexmarkMessage) -> Self {
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

impl NexmarkMessage {
    pub fn new(shard_id: String, offset: u64, event: Event) -> Self {
        NexmarkMessage {
            shard_id,
            sequence_number: offset.to_string(),
            payload: Some(event.to_json().as_bytes().to_vec()),
        }
    }
}

// Copyright 2026 RisingWave Labs
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

use crate::source::base::SourceMessage;
use crate::source::{SourceMeta, SplitId};

#[derive(Clone, Debug)]
pub struct WebsocketMessage {
    pub split_id: SplitId,
    /// A per-reader counter of received messages, used as the message offset.
    /// A `WebSocket` stream has no server-side offsets and cannot be replayed.
    pub sequence_number: u64,
    /// Payload of a text or binary frame.
    pub payload: Vec<u8>,
}

impl From<WebsocketMessage> for SourceMessage {
    fn from(message: WebsocketMessage) -> Self {
        SourceMessage {
            key: None,
            payload: Some(message.payload),
            offset: message.sequence_number.to_string(),
            split_id: message.split_id,
            meta: SourceMeta::Empty,
        }
    }
}

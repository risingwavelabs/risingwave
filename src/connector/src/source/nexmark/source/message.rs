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

use crate::source::nexmark::source::event::Event;
use crate::source::{SourceMessage, SplitId};

#[derive(Clone, Debug)]
pub struct NexmarkMessage {
    pub split_id: SplitId,
    pub sequence_number: String,
    pub payload: Bytes,
}

impl From<NexmarkMessage> for SourceMessage {
    fn from(msg: NexmarkMessage) -> Self {
        SourceMessage {
            payload: Some(msg.payload),
            offset: msg.sequence_number.clone(),
            split_id: msg.split_id,
        }
    }
}

impl NexmarkMessage {
    pub fn new(split_id: SplitId, offset: u64, event: Event) -> Self {
        NexmarkMessage {
            split_id,
            sequence_number: offset.to_string(),
            payload: event.to_json().into(),
        }
    }
}

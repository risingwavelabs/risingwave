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
use std::time::{SystemTime, UNIX_EPOCH};

use nexmark::event::Event;
use risingwave_common::types::{Datum, ScalarImpl};

use crate::impl_source_meta_extract_func;
use crate::source::nexmark::source::combined_event::CombinedEvent;
use crate::source::{SourceMessage, SourceMeta, SplitId};
#[derive(Clone, Debug)]
pub struct NexmarkMeta {
    pub split_id: SplitId,
    pub timestamp: Option<i64>,
    pub offset: i64,
}
#[derive(Clone, Debug)]
pub struct NexmarkMessage {
    pub split_id: SplitId,
    pub sequence_number: i64,
    pub payload: Vec<u8>,
}

impl From<NexmarkMessage> for SourceMessage {
    fn from(msg: NexmarkMessage) -> Self {
        SourceMessage {
            key: None,
            payload: Some(msg.payload),
            meta: SourceMeta::Nexmark(NexmarkMeta {
                split_id: msg.split_id,
                offset: msg.sequence_number,
                timestamp: Some(
                    SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as i64,
                ),
            }),
        }
    }
}

impl_source_meta_extract_func!(NexmarkMeta, Int64, offset, split_id);

impl NexmarkMessage {
    pub fn new_single_event(split_id: SplitId, offset: u64, event: Event) -> Self {
        NexmarkMessage {
            split_id,
            sequence_number: offset as i64,
            payload: match &event {
                Event::Person(p) => serde_json::to_vec(p),
                Event::Auction(a) => serde_json::to_vec(a),
                Event::Bid(b) => serde_json::to_vec(b),
            }
            .unwrap(),
        }
    }

    pub fn new_combined_event(split_id: SplitId, offset: u64, event: Event) -> Self {
        let combined_event = match event {
            Event::Person(p) => CombinedEvent::person(p),
            Event::Auction(a) => CombinedEvent::auction(a),
            Event::Bid(b) => CombinedEvent::bid(b),
        };
        let combined_event = serde_json::to_string(&combined_event).unwrap();
        NexmarkMessage {
            split_id,
            sequence_number: offset as i64,
            payload: combined_event.into(),
        }
    }
}

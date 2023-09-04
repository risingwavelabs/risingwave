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

use async_nats;

use crate::source::base::SourceMessage;
use crate::source::SourceMeta;

impl SourceMessage {
    pub fn from_nats_message(message: async_nats::Message) -> Self {
        SourceMessage {
            key: None,
            payload: Some(message.payload.to_vec()),
            // Nats message doesn't have offset
            offset: "".to_string(),
            split_id: Arc::from(""),
            meta: SourceMeta::Empty,
        }
    }
}

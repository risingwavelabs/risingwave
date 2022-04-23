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

use pulsar::consumer::Message;

use crate::base::SourceMessage;

impl From<Message<Vec<u8>>> for SourceMessage {
    fn from(msg: Message<Vec<u8>>) -> Self {
        SourceMessage {
            payload: Some(bytes::Bytes::from(msg.payload.data)),
            offset: msg.message_id.id.entry_id.to_string(),
            split_id: msg.topic,
        }
    }
}

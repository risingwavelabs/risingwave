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

use crate::source::SourceMessage;

impl From<Message<Vec<u8>>> for SourceMessage {
    fn from(msg: Message<Vec<u8>>) -> Self {
        let message_id = msg.message_id.id;

        SourceMessage {
            payload: Some(msg.payload.data.into()),
            offset: format!(
                "{}:{}:{}:{}",
                message_id.ledger_id,
                message_id.entry_id,
                message_id.partition.unwrap_or(-1),
                message_id.batch_index.unwrap_or(-1)
            ),
            split_id: msg.topic.into(),
        }
    }
}

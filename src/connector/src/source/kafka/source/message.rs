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
use rdkafka::message::BorrowedMessage;
use rdkafka::Message;

use crate::source::base::SourceMessage;

impl<'a> From<BorrowedMessage<'a>> for SourceMessage {
    fn from(message: BorrowedMessage<'a>) -> Self {
        SourceMessage {
            // TODO(TaoWu): Possible performance improvement: avoid memory copying here.
            payload: message.payload().map(Bytes::copy_from_slice),
            offset: message.offset().to_string(),
            split_id: message.partition().to_string().into(),
        }
    }
}

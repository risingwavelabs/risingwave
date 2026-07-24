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

mod message;
mod reader;

pub(crate) use message::RabbitmqAckData;
pub use message::RabbitmqMeta;
pub use reader::RabbitmqSplitReader;
pub(crate) use reader::{RABBITMQ_ACK_SENDER_REGISTRY, RabbitmqAckRequest};

// Copyright 2025 RisingWave Labs
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

use await_tree::InstrumentAwait;
use educe::Educe;

use super::error::ExchangeChannelClosed;
use super::permit::Sender;
use crate::error::StreamResult;
use crate::executor::DispatcherMessageBatch as Message;
use crate::task::ActorId;

/// `LocalOutput` sends data to a local channel.
#[derive(Educe)]
#[educe(Debug)]
pub struct Output {
    actor_id: ActorId,

    #[educe(Debug(ignore))]
    span: await_tree::Span,

    #[educe(Debug(ignore))]
    ch: Sender,
}

impl Output {
    pub fn new(actor_id: ActorId, ch: Sender) -> Self {
        Self {
            actor_id,
            span: await_tree::span!("Output (actor {:?})", actor_id).verbose(),
            ch,
        }
    }
}

impl Output {
    pub async fn send(&mut self, message: Message) -> StreamResult<()> {
        self.ch
            .send(message)
            .instrument_await(self.span.clone())
            .await
            .map_err(|_| ExchangeChannelClosed::output(self.actor_id).into())
    }

    pub fn actor_id(&self) -> ActorId {
        self.actor_id
    }
}

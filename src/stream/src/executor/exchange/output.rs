// Copyright 2022 RisingWave Labs
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

use std::pin::pin;

use await_tree::InstrumentAwait;
use educe::Educe;
use futures::future::poll_immediate;
use risingwave_common::metrics::LabelGuardedIntCounter;
use tokio::time::Instant;

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

    /// Time spent waiting on a full channel; shared by all outputs of a dispatcher.
    #[educe(Debug(ignore))]
    channel_blocking_ns: LabelGuardedIntCounter,
}

impl Output {
    pub fn new(actor_id: ActorId, ch: Sender, channel_blocking_ns: LabelGuardedIntCounter) -> Self {
        Self {
            actor_id,
            span: await_tree::span!("Output (actor {:?})", actor_id).verbose(),
            ch,
            channel_blocking_ns,
        }
    }

    #[cfg(test)]
    pub fn for_test(actor_id: ActorId, ch: Sender) -> Self {
        Self::new(
            actor_id,
            ch,
            LabelGuardedIntCounter::test_int_counter::<3>(),
        )
    }
}

impl Output {
    pub async fn send(&mut self, message: Message) -> StreamResult<()> {
        let mut fut = pin!(self.ch.send(message).instrument_await(self.span.clone()));
        // Fast path: the channel has room, so this is the same single poll as a plain `.await`.
        match poll_immediate(&mut fut).await {
            Some(res) => res,
            None => {
                let start = Instant::now();
                let res = fut.await;
                self.channel_blocking_ns
                    .inc_by(start.elapsed().as_nanos() as u64);
                res
            }
        }
        .map_err(|_| ExchangeChannelClosed::output(self.actor_id).into())
    }

    pub fn actor_id(&self) -> ActorId {
        self.actor_id
    }
}

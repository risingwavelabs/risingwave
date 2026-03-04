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

use await_tree::InstrumentAwait;
use tokio::sync::mpsc;

use super::error::ExchangeChannelClosed;
use super::permit::Sender;
use crate::error::StreamResult;
use crate::executor::{DispatcherBarrier, DispatcherMessageBatch as Message};
use crate::task::ActorId;

/// Output sends data to downstream actors. It has two variants:
///
/// - `Direct`: both data and barriers go through the same permit-based channel.
///   Used for non-multiplexed exchanges (both local and remote).
///
/// - `CoalescedBarrier`: data goes through a per-actor permit-based channel,
///   while barriers are sent to a coalescer for barrier coalescing.
///   The coalescer tracks per-actor data message counts and sends coalesced
///   barriers with those counts on a separate barrier-only channel.
///   Used for multiplexed remote exchanges.
pub enum Output {
    /// Normal output: data and barriers go through the same channel.
    Direct {
        actor_id: ActorId,
        span: await_tree::Span,
        ch: Sender,
    },
    /// Coalesced barrier output: data goes through per-actor channel,
    /// barriers go to coalescer with message count.
    CoalescedBarrier {
        actor_id: ActorId,
        span: await_tree::Span,
        /// Per-actor data channel (same type as Direct's ch).
        data_ch: Sender,
        /// Channel to send barriers to the coalescer.
        /// Tuple: (`actor_id`, barriers, `epoch_msg_count`)
        barrier_tx: mpsc::UnboundedSender<(ActorId, Vec<DispatcherBarrier>, u64)>,
        /// Count of data messages (chunks + watermarks) sent in the current epoch.
        epoch_msg_count: u64,
    },
}

impl std::fmt::Debug for Output {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Output")
            .field("actor_id", &self.actor_id())
            .finish_non_exhaustive()
    }
}

impl Output {
    pub fn new(actor_id: ActorId, ch: Sender) -> Self {
        Self::Direct {
            actor_id,
            span: await_tree::span!("Output (actor {:?})", actor_id).verbose(),
            ch,
        }
    }

    pub fn new_coalesced_barrier(
        actor_id: ActorId,
        data_ch: Sender,
        barrier_tx: mpsc::UnboundedSender<(ActorId, Vec<DispatcherBarrier>, u64)>,
    ) -> Self {
        Self::CoalescedBarrier {
            actor_id,
            span: await_tree::span!("Output (actor {:?}, coalesced barrier)", actor_id).verbose(),
            data_ch,
            barrier_tx,
            epoch_msg_count: 0,
        }
    }
}

impl Output {
    pub async fn send(&mut self, message: Message) -> StreamResult<()> {
        match self {
            Self::Direct { actor_id, span, ch } => ch
                .send(message)
                .instrument_await(span.clone())
                .await
                .map_err(|_| ExchangeChannelClosed::output(*actor_id).into()),

            Self::CoalescedBarrier {
                actor_id,
                span,
                data_ch,
                barrier_tx,
                epoch_msg_count,
            } => match message {
                Message::BarrierBatch(barriers) => {
                    // Send barriers to the coalescer with the current epoch message count.
                    let count = *epoch_msg_count;
                    *epoch_msg_count = 0; // Reset for next epoch.
                    barrier_tx
                        .send((*actor_id, barriers, count))
                        .map_err(|_| ExchangeChannelClosed::output(*actor_id))?;
                    Ok(())
                }
                msg @ (Message::Chunk(_) | Message::Watermark(_)) => {
                    // Increment message count and send data through the data channel.
                    *epoch_msg_count += 1;
                    data_ch
                        .send(msg)
                        .instrument_await(span.clone())
                        .await
                        .map_err(|_| ExchangeChannelClosed::output(*actor_id).into())
                }
            },
        }
    }

    pub fn actor_id(&self) -> ActorId {
        match self {
            Self::Direct { actor_id, .. } | Self::CoalescedBarrier { actor_id, .. } => *actor_id,
        }
    }
}

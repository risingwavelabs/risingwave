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
        /// Downstream actor ID. Used for dispatch matching (hash mapping, `remove_outputs`, etc.).
        actor_id: ActorId,
        /// Upstream (self) actor ID. Used for coalescer submission so the coalescer
        /// knows which upstream actor submitted this barrier.
        upstream_actor_id: ActorId,
        span: await_tree::Span,
        /// Per-actor data channel (same type as Direct's ch).
        data_ch: Sender,
        /// Channel to send barriers to the coalescer.
        /// Tuple: (`upstream_actor_id`, single barrier, `epoch_msg_count`)
        ///
        /// Each barrier is sent individually (not as a batch) to ensure the
        /// coalescer's FIFO queue stays aligned across actors even when different
        /// actors batch barriers differently via `try_batch_barriers`.
        barrier_tx: mpsc::UnboundedSender<(ActorId, DispatcherBarrier, u64)>,
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
        downstream_actor_id: ActorId,
        upstream_actor_id: ActorId,
        data_ch: Sender,
        barrier_tx: mpsc::UnboundedSender<(ActorId, DispatcherBarrier, u64)>,
    ) -> Self {
        Self::CoalescedBarrier {
            actor_id: downstream_actor_id,
            upstream_actor_id,
            span: await_tree::span!(
                "Output (actor {:?} -> {:?}, coalesced barrier)",
                upstream_actor_id,
                downstream_actor_id
            )
            .verbose(),
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
                upstream_actor_id,
                span,
                data_ch,
                barrier_tx,
                epoch_msg_count,
            } => match message {
                Message::BarrierBatch(barriers) => {
                    // Split the barrier batch into individual submissions to the coalescer.
                    //
                    // This is critical for correctness: `try_batch_barriers` may batch
                    // consecutive barriers differently for different actors (e.g., Actor A1
                    // batches [b1, b2] while Actor A2 sends b1 and b2 separately). If we
                    // submit the entire batch as one coalescer entry, the FIFO queue pairing
                    // becomes misaligned between actors, causing wrong epoch-count
                    // associations and potential deadlocks.
                    //
                    // Since barrier batching only groups consecutive barriers with NO data
                    // between them, the first barrier carries the accumulated data count
                    // and subsequent barriers carry count=0.
                    let mut first = true;
                    for barrier in barriers {
                        let count = if first {
                            first = false;
                            std::mem::take(epoch_msg_count)
                        } else {
                            0
                        };
                        // Use upstream_actor_id for the coalescer (it tracks by upstream actor).
                        barrier_tx
                            .send((*upstream_actor_id, barrier, count))
                            .map_err(|_| ExchangeChannelClosed::output(*actor_id))?;
                    }
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

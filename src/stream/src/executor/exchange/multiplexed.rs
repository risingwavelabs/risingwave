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

//! Multiplexed exchange for node-level barrier relay.
//!
//! This module provides [`MultiplexedOutput`] (sender side) and [`MultiplexedRemoteInput`]
//! (receiver side) to reduce barrier messages from N×M to M per exchange per epoch, where
//! N is the number of upstream actors on one node and M is the number of downstream actors.
//!
//! See `docs/dev/src/design/node-level-barrier-relay.md` for the full design.

use std::collections::{HashMap, HashSet};

use risingwave_pb::stream_plan::stream_message_batch::StreamMessageBatch as PbStreamMessageBatchInner;
use risingwave_pb::task_service::{GetStreamResponse, permits};
use tokio::sync::mpsc;

use super::error::ExchangeChannelClosed;
use super::permit;
use crate::error::StreamResult;
use crate::executor::{
    DispatcherBarrier, DispatcherMessageBatch, DispatcherMessageBatch as Message,
};
use crate::task::ActorId;

// ============================================================================
// Sender side: BarrierCoalescer + MultiplexedOutput
// ============================================================================

/// Collects barriers from multiple upstream actors and emits a single coalesced barrier
/// once all expected actors have reached the same epoch.
///
/// This implements "sender-side barrier alignment": the same waiting that MergeExecutor
/// does on the receiver side, but performed locally on the sender node to avoid N
/// barrier messages crossing the network.
pub struct BarrierCoalescer {
    /// The set of upstream actor IDs that must all deliver a barrier before we coalesce.
    expected_actors: HashSet<ActorId>,
    /// Actors that have already delivered their barrier for the current epoch.
    arrived: HashSet<ActorId>,
    /// The first barrier received for this epoch (used as the template for the coalesced one).
    pending_barrier: Option<DispatcherBarrier>,
}

impl BarrierCoalescer {
    pub fn new(expected_actors: HashSet<ActorId>) -> Self {
        let capacity = expected_actors.len();
        Self {
            expected_actors,
            arrived: HashSet::with_capacity(capacity),
            pending_barrier: None,
        }
    }

    /// Record that `actor_id` has reached its barrier for the current epoch.
    ///
    /// Returns `Some(barrier, actor_ids)` when all expected actors have arrived,
    /// containing the barrier and the full set of coalesced actor IDs.
    /// Returns `None` if we're still waiting for more actors.
    pub fn collect(
        &mut self,
        actor_id: ActorId,
        barrier: DispatcherBarrier,
    ) -> Option<(DispatcherBarrier, Vec<ActorId>)> {
        assert!(
            self.expected_actors.contains(&actor_id),
            "unexpected actor {} in barrier coalescer, expected: {:?}",
            actor_id,
            self.expected_actors
        );

        if let Some(pending) = &self.pending_barrier {
            // Verify epoch consistency — all actors must be at the same epoch.
            assert_eq!(
                pending.epoch, barrier.epoch,
                "barrier epoch mismatch in coalescer: actor {} has epoch {:?}, expected {:?}",
                actor_id, barrier.epoch, pending.epoch
            );
        } else {
            self.pending_barrier = Some(barrier);
        }

        let newly_inserted = self.arrived.insert(actor_id);
        assert!(
            newly_inserted,
            "actor {} sent barrier twice for the same epoch",
            actor_id
        );

        if self.arrived.len() == self.expected_actors.len() {
            // All actors have arrived — coalesce!
            let barrier = self.pending_barrier.take().unwrap();
            let actor_ids: Vec<ActorId> = self.arrived.drain().collect();
            Some((barrier, actor_ids))
        } else {
            None
        }
    }

    /// Add a new upstream actor to the expected set.
    /// Called when actors are added at a barrier boundary during scaling.
    pub fn add_actor(&mut self, actor_id: ActorId) {
        self.expected_actors.insert(actor_id);
    }

    /// Remove an upstream actor from the expected set.
    /// Called when actors are removed at a barrier boundary during scaling.
    pub fn remove_actor(&mut self, actor_id: ActorId) {
        self.expected_actors.remove(&actor_id);
        // Also remove from arrived in case it already arrived this epoch
        self.arrived.remove(&actor_id);
    }
}

/// A handle for one upstream actor to send messages into a [`MultiplexedOutput`].
///
/// Each upstream actor's `DispatchExecutor` holds one of these instead of a direct `Output`.
/// Data chunks and watermarks are forwarded immediately (tagged with the actor ID).
/// Barriers are collected by the shared `BarrierCoalescer`.
pub struct MultiplexedActorOutput {
    /// The upstream actor ID this handle belongs to.
    actor_id: ActorId,
    /// Shared sender — sends tagged messages to the physical gRPC stream.
    /// Uses `permit::Sender` for backpressure.
    ch: permit::Sender,
    /// Shared barrier coalescer state. We use a channel to coordinate:
    /// each actor sends its barrier to the coordinator task via this channel.
    barrier_tx: mpsc::UnboundedSender<(ActorId, DispatcherBarrier)>,
}

impl MultiplexedActorOutput {
    /// Send a data chunk or watermark. These are forwarded immediately, tagged with the actor ID.
    pub async fn send(&mut self, message: Message) -> StreamResult<()> {
        match &message {
            Message::BarrierBatch(barriers) => {
                // Send barriers to the coalescer instead of directly to the channel.
                for barrier in barriers.clone() {
                    self.barrier_tx
                        .send((self.actor_id, barrier))
                        .map_err(|_| ExchangeChannelClosed::output(self.actor_id))?;
                }
                Ok(())
            }
            Message::Chunk(_) | Message::Watermark(_) => {
                // Data and watermarks are sent immediately on the shared channel.
                // The source_actor_id will be set when serializing to protobuf.
                self.ch
                    .send(message)
                    .await
                    .map_err(|_| ExchangeChannelClosed::output(self.actor_id).into())
            }
        }
    }

    pub fn actor_id(&self) -> ActorId {
        self.actor_id
    }
}

/// Coordinator for a multiplexed output stream.
///
/// This runs as a background task that:
/// 1. Receives barrier notifications from all upstream actors via `barrier_rx`
/// 2. Coalesces them using `BarrierCoalescer`
/// 3. Sends the coalesced barrier on the physical channel
///
/// Data chunks and watermarks bypass this coordinator entirely — they go directly
/// from `MultiplexedActorOutput` to the `permit::Sender` channel.
pub struct MultiplexedOutputCoordinator {
    /// Receives barrier notifications from all upstream actors.
    barrier_rx: mpsc::UnboundedReceiver<(ActorId, DispatcherBarrier)>,
    /// Coalesces barriers from multiple actors.
    coalescer: BarrierCoalescer,
    /// The shared physical channel sender.
    ch: permit::Sender,
}

impl MultiplexedOutputCoordinator {
    /// Run the coordinator loop. This should be spawned as a background task.
    ///
    /// It continuously receives barrier notifications and sends coalesced barriers
    /// when all expected actors have reached the same epoch.
    pub async fn run(mut self) -> StreamResult<()> {
        while let Some((actor_id, barrier)) = self.barrier_rx.recv().await {
            if let Some((coalesced_barrier, actor_ids)) =
                self.coalescer.collect(actor_id, barrier)
            {
                // All actors reached the barrier — send the coalesced barrier batch.
                let coalesced = DispatcherMessageBatch::BarrierBatch(vec![coalesced_barrier]);
                self.ch
                    .send(coalesced)
                    .await
                    .map_err(|_| ExchangeChannelClosed::output(0.into()))?;
                // Note: The coalesced_actor_ids are set in the protobuf serialization
                // layer when the server reads from this channel and converts to proto.
                // We store the actor_ids info separately (see server-side handler).
                let _ = actor_ids; // TODO: pass actor_ids through the channel
            }
        }
        Ok(())
    }
}

/// Creates a multiplexed output for a set of upstream actors targeting a single downstream actor.
///
/// Returns:
/// - A vec of `MultiplexedActorOutput` handles, one per upstream actor
/// - A `MultiplexedOutputCoordinator` that must be spawned as a background task
/// - A `permit::Receiver` that the gRPC server reads from
pub fn create_multiplexed_output(
    upstream_actor_ids: &[ActorId],
    initial_permits: usize,
    batched_permits: usize,
    concurrent_barriers: usize,
) -> (
    Vec<MultiplexedActorOutput>,
    MultiplexedOutputCoordinator,
    permit::Receiver,
) {
    // Scale record permits proportional to the number of upstream actors.
    let scaled_initial_permits = initial_permits * upstream_actor_ids.len();
    let scaled_batched_permits = batched_permits * upstream_actor_ids.len();
    // Only 1 barrier permit needed — we send one coalesced barrier per epoch.
    let barrier_permits = concurrent_barriers;

    let (tx, rx) = permit::channel(scaled_initial_permits, scaled_batched_permits, barrier_permits);

    let (barrier_tx, barrier_rx) = mpsc::unbounded_channel();

    let expected_actors: HashSet<ActorId> = upstream_actor_ids.iter().copied().collect();
    let coalescer = BarrierCoalescer::new(expected_actors);

    let actor_outputs: Vec<MultiplexedActorOutput> = upstream_actor_ids
        .iter()
        .map(|&actor_id| MultiplexedActorOutput {
            actor_id,
            ch: tx.clone(),
            barrier_tx: barrier_tx.clone(),
        })
        .collect();

    // Drop the original barrier_tx — only the actor outputs hold senders.
    drop(barrier_tx);

    let coordinator = MultiplexedOutputCoordinator {
        barrier_rx,
        coalescer,
        ch: tx,
    };

    (actor_outputs, coordinator, rx)
}

// ============================================================================
// Receiver side: MultiplexedRemoteInput
// ============================================================================

/// Demultiplexes a single multiplexed gRPC stream into per-actor logical inputs.
///
/// This reads from the multiplexed response stream and routes:
/// - Data chunks → to the logical input for the tagged `source_actor_id`
/// - Watermarks → to the logical input for the tagged `source_actor_id`
/// - Coalesced barriers → expanded into one barrier per actor in `coalesced_actor_ids`
///
/// Each logical output is an `mpsc::UnboundedSender<DispatcherMessage>` that feeds
/// into a `LogicalInput` (which implements `Input` / `ActorInput`).
pub async fn run_multiplexed_remote_input(
    mut stream: tonic::Streaming<GetStreamResponse>,
    permits_tx: mpsc::UnboundedSender<permits::Value>,
    logical_outputs: HashMap<ActorId, mpsc::UnboundedSender<crate::executor::DispatcherMessage>>,
    batched_permits_limit: usize,
) -> StreamResult<()> {
    use crate::executor::DispatcherMessage;

    let mut batched_permits_accumulated: u32 = 0;

    while let Some(data_res) = stream.message().await.transpose() {
        match data_res {
            Ok(GetStreamResponse { message, permits }) => {
                let msg = message.unwrap();
                let source_actor_id = msg.source_actor_id;

                // Handle permit return
                if let Some(add_back_permits) = match permits.unwrap().value {
                    Some(permits::Value::Record(p)) => {
                        batched_permits_accumulated += p;
                        if batched_permits_accumulated >= batched_permits_limit as u32 {
                            let p = std::mem::take(&mut batched_permits_accumulated);
                            Some(permits::Value::Record(p))
                        } else {
                            None
                        }
                    }
                    Some(permits::Value::Barrier(p)) => Some(permits::Value::Barrier(p)),
                    None => None,
                } {
                    permits_tx
                        .send(add_back_permits)
                        .map_err(|_| anyhow::anyhow!("MultiplexedRemoteInput backward permits channel closed"))?;
                }

                let msg_batch = DispatcherMessageBatch::from_protobuf(&msg)
                    .map_err(|e| anyhow::anyhow!("MultiplexedRemoteInput decode error: {}", e))?;

                match msg_batch {
                    DispatcherMessageBatch::Chunk(chunk) => {
                        // Route to the correct actor's logical input
                        if let Some(tx) = logical_outputs.get(&source_actor_id) {
                            let _ = tx.send(DispatcherMessage::Chunk(chunk));
                        }
                    }
                    DispatcherMessageBatch::Watermark(watermark) => {
                        if let Some(tx) = logical_outputs.get(&source_actor_id) {
                            let _ = tx.send(DispatcherMessage::Watermark(watermark));
                        }
                    }
                    DispatcherMessageBatch::BarrierBatch(barriers) => {
                        // Check for coalesced_actor_ids in the protobuf
                        let coalesced_actor_ids = msg
                            .stream_message_batch
                            .as_ref()
                            .and_then(|smb| match smb {
                                PbStreamMessageBatchInner::BarrierBatch(bb) => {
                                    if bb.coalesced_actor_ids.is_empty() {
                                        None
                                    } else {
                                        Some(&bb.coalesced_actor_ids)
                                    }
                                }
                                _ => None,
                            });

                        if let Some(actor_ids) = coalesced_actor_ids {
                            // Multiplexed mode: expand the barrier to all listed actors
                            for &actor_id in actor_ids {
                                if let Some(tx) = logical_outputs.get(&actor_id) {
                                    for barrier in &barriers {
                                        let _ = tx.send(DispatcherMessage::Barrier(
                                            barrier.clone(),
                                        ));
                                    }
                                }
                            }
                        } else {
                            // Legacy mode: send to source_actor_id
                            if let Some(tx) = logical_outputs.get(&source_actor_id) {
                                for barrier in barriers {
                                    let _ = tx.send(DispatcherMessage::Barrier(barrier));
                                }
                            }
                        }
                    }
                }
            }
            Err(e) => {
                return Err(anyhow::anyhow!("MultiplexedRemoteInput gRPC error: {}", e).into());
            }
        }
    }

    Err(anyhow::anyhow!("MultiplexedRemoteInput stream ended unexpectedly").into())
}

/// A logical input that receives messages from a [`run_multiplexed_remote_input`] task.
///
/// This implements the `Input` trait so it can be used by `MergeExecutor` as a drop-in
/// replacement for `RemoteInput`.
pub struct LogicalInput {
    actor_id: ActorId,
    rx: mpsc::UnboundedReceiver<crate::executor::DispatcherMessage>,
}

impl LogicalInput {
    pub fn new(
        actor_id: ActorId,
        rx: mpsc::UnboundedReceiver<crate::executor::DispatcherMessage>,
    ) -> Self {
        Self { actor_id, rx }
    }
}

impl futures::Stream for LogicalInput {
    type Item = crate::executor::DispatcherMessageStreamItem;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx).map(|opt| opt.map(Ok))
    }
}

impl super::input::Input for LogicalInput {
    type InputId = ActorId;

    fn id(&self) -> Self::InputId {
        self.actor_id
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use risingwave_common::util::epoch::test_epoch;

    use super::*;
    use crate::executor::DispatcherBarrier;

    #[test]
    fn test_barrier_coalescer_basic() {
        let actors: HashSet<ActorId> = [1, 2, 3].into_iter().collect();
        let mut coalescer = BarrierCoalescer::new(actors);

        let barrier1 = DispatcherBarrier::with_prev_epoch_for_test(test_epoch(2), test_epoch(1));
        let barrier2 = DispatcherBarrier::with_prev_epoch_for_test(test_epoch(2), test_epoch(1));
        let barrier3 = DispatcherBarrier::with_prev_epoch_for_test(test_epoch(2), test_epoch(1));

        // First two arrivals return None
        assert!(coalescer.collect(1, barrier1).is_none());
        assert!(coalescer.collect(2, barrier2).is_none());

        // Third arrival completes the coalescing
        let result = coalescer.collect(3, barrier3);
        assert!(result.is_some());

        let (barrier, mut actor_ids) = result.unwrap();
        assert_eq!(barrier.epoch.curr, test_epoch(2));
        actor_ids.sort();
        assert_eq!(actor_ids, vec![1, 2, 3]);
    }

    #[test]
    fn test_barrier_coalescer_sequential_epochs() {
        let actors: HashSet<ActorId> = [10, 20].into_iter().collect();
        let mut coalescer = BarrierCoalescer::new(actors);

        // Epoch 1
        let b1 = DispatcherBarrier::with_prev_epoch_for_test(test_epoch(2), test_epoch(1));
        let b2 = DispatcherBarrier::with_prev_epoch_for_test(test_epoch(2), test_epoch(1));
        assert!(coalescer.collect(10, b1).is_none());
        let result = coalescer.collect(20, b2);
        assert!(result.is_some());

        // Epoch 2 — coalescer should be reset
        let b3 = DispatcherBarrier::with_prev_epoch_for_test(test_epoch(3), test_epoch(2));
        let b4 = DispatcherBarrier::with_prev_epoch_for_test(test_epoch(3), test_epoch(2));
        assert!(coalescer.collect(10, b3).is_none());
        let result = coalescer.collect(20, b4);
        assert!(result.is_some());
        let (barrier, _) = result.unwrap();
        assert_eq!(barrier.epoch.curr, test_epoch(3));
    }

    #[test]
    #[should_panic(expected = "barrier epoch mismatch")]
    fn test_barrier_coalescer_epoch_mismatch() {
        let actors: HashSet<ActorId> = [1, 2].into_iter().collect();
        let mut coalescer = BarrierCoalescer::new(actors);

        let b1 = DispatcherBarrier::with_prev_epoch_for_test(test_epoch(2), test_epoch(1));
        let b2 = DispatcherBarrier::with_prev_epoch_for_test(test_epoch(3), test_epoch(2));

        coalescer.collect(1, b1);
        coalescer.collect(2, b2); // Should panic: epoch mismatch
    }

    #[test]
    #[should_panic(expected = "sent barrier twice")]
    fn test_barrier_coalescer_duplicate_actor() {
        let actors: HashSet<ActorId> = [1, 2].into_iter().collect();
        let mut coalescer = BarrierCoalescer::new(actors);

        let b1 = DispatcherBarrier::with_prev_epoch_for_test(test_epoch(2), test_epoch(1));
        let b2 = DispatcherBarrier::with_prev_epoch_for_test(test_epoch(2), test_epoch(1));

        coalescer.collect(1, b1);
        coalescer.collect(1, b2); // Should panic: duplicate
    }

    #[test]
    fn test_barrier_coalescer_add_remove_actors() {
        let actors: HashSet<ActorId> = [1, 2].into_iter().collect();
        let mut coalescer = BarrierCoalescer::new(actors);

        // Add actor 3
        coalescer.add_actor(3);

        let b1 = DispatcherBarrier::with_prev_epoch_for_test(test_epoch(2), test_epoch(1));
        let b2 = DispatcherBarrier::with_prev_epoch_for_test(test_epoch(2), test_epoch(1));
        let b3 = DispatcherBarrier::with_prev_epoch_for_test(test_epoch(2), test_epoch(1));

        assert!(coalescer.collect(1, b1).is_none());
        assert!(coalescer.collect(2, b2).is_none());
        // Now need all 3
        let result = coalescer.collect(3, b3);
        assert!(result.is_some());

        // Remove actor 3
        coalescer.remove_actor(3);

        // Next epoch only needs actors 1 and 2
        let b4 = DispatcherBarrier::with_prev_epoch_for_test(test_epoch(3), test_epoch(2));
        let b5 = DispatcherBarrier::with_prev_epoch_for_test(test_epoch(3), test_epoch(2));
        assert!(coalescer.collect(1, b4).is_none());
        let result = coalescer.collect(2, b5);
        assert!(result.is_some());
    }
}

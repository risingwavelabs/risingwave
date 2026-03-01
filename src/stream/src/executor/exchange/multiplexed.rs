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

//! Multiplexed exchange for node-level barrier relay.
//!
//! This module provides [`MultiplexedActorOutput`] (sender side) and
//! [`run_multiplexed_remote_input`] (receiver side) to reduce barrier messages from N×M to
//! M per exchange per epoch, where N is the number of upstream actors on one node and M is
//! the number of downstream actors.
//!
//! See `docs/dev/src/design/node-level-barrier-relay.md` for the full design.

use std::collections::{HashMap, HashSet};

use anyhow::Context;
use itertools::Itertools;
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

/// Collects barrier batches from multiple upstream actors and emits a single coalesced
/// barrier batch once all expected actors have reached the same set of epochs.
///
/// This implements "sender-side barrier alignment": the same waiting that `MergeExecutor`
/// does on the receiver side, but performed locally on the sender node to avoid N
/// barrier messages crossing the network.
///
/// **Batch-aware**: each actor sends its entire `Vec<DispatcherBarrier>` batch atomically.
/// The coalescer waits for all actors to submit the same batch (verified by epoch list),
/// then emits one copy of the batch along with the full set of actor IDs.
pub struct BarrierCoalescer {
    /// The set of upstream actor IDs that must all deliver a barrier batch before we coalesce.
    expected_actors: HashSet<ActorId>,
    /// Actors that have already delivered their barrier batch for the current epoch(s).
    arrived: HashSet<ActorId>,
    /// The first barrier batch received (used as the template for the coalesced output).
    pending_batch: Option<Vec<DispatcherBarrier>>,
}

impl BarrierCoalescer {
    pub fn new(expected_actors: HashSet<ActorId>) -> Self {
        let capacity = expected_actors.len();
        Self {
            expected_actors,
            arrived: HashSet::with_capacity(capacity),
            pending_batch: None,
        }
    }

    /// Record that `actor_id` has reached its barrier batch for the current epoch(s).
    ///
    /// Returns `Some((batch, actor_ids))` when all expected actors have arrived,
    /// containing the barrier batch and the full set of coalesced actor IDs.
    /// Returns `None` if we're still waiting for more actors.
    pub fn collect(
        &mut self,
        actor_id: ActorId,
        barriers: Vec<DispatcherBarrier>,
    ) -> Option<(Vec<DispatcherBarrier>, Vec<ActorId>)> {
        assert!(
            self.expected_actors.contains(&actor_id),
            "unexpected actor {} in barrier coalescer, expected: {:?}",
            actor_id,
            self.expected_actors
        );

        if let Some(pending) = &self.pending_batch {
            // Verify batch consistency — all actors must have the same barrier epochs.
            assert_eq!(
                pending.len(),
                barriers.len(),
                "barrier batch length mismatch in coalescer: actor {} has {} barriers, expected {}",
                actor_id,
                barriers.len(),
                pending.len()
            );
            for (p, b) in pending.iter().zip_eq(barriers.iter()) {
                assert_eq!(
                    p.epoch, b.epoch,
                    "barrier epoch mismatch in coalescer: actor {} has epoch {:?}, expected {:?}",
                    actor_id, b.epoch, p.epoch
                );
            }
        } else {
            self.pending_batch = Some(barriers);
        }

        let newly_inserted = self.arrived.insert(actor_id);
        assert!(
            newly_inserted,
            "actor {} sent barrier batch twice for the same epoch(s)",
            actor_id
        );

        if self.arrived.len() == self.expected_actors.len() {
            // All actors have arrived — coalesce!
            let batch = self.pending_batch.take().unwrap();
            let actor_ids: Vec<ActorId> = self.arrived.drain().collect();
            Some((batch, actor_ids))
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

/// A handle for one upstream actor to send messages into a multiplexed output channel.
///
/// Each upstream actor's `DispatchExecutor` holds one of these instead of a direct `Output`.
/// Data chunks and watermarks are forwarded immediately (tagged with the actor ID).
/// Barriers are collected by the shared `BarrierCoalescer`.
///
/// **Ordering invariant**: before sending ANY message (including a new barrier), the actor
/// waits until its previously submitted barrier has been coalesced and committed to the
/// shared channel. This guarantees:
/// 1. No post-barrier data overtakes the barrier (snapshot consistency).
/// 2. No barrier N+1 reaches the coalescer before barrier N finishes coalescing across
///    all actors (prevents epoch mismatch when `in_flight_barrier_nums > 1`).
pub struct MultiplexedActorOutput {
    /// The upstream actor ID this handle belongs to.
    actor_id: ActorId,
    /// Shared sender — sends tagged messages to the physical gRPC stream.
    /// Uses `permit::Sender` for backpressure.
    ch: permit::Sender,
    /// Channel to send barrier batches to the coordinator for coalescing.
    barrier_tx: mpsc::UnboundedSender<(ActorId, Vec<DispatcherBarrier>)>,
    /// Receives the epoch number of the latest coalesced barrier that was committed
    /// to the shared channel. Used as a gate: after sending a barrier, this actor
    /// must wait here until the coordinator confirms the coalesced barrier is in the
    /// shared channel before sending any more data.
    barrier_committed_rx: tokio::sync::watch::Receiver<u64>,
    /// The epoch of the barrier this actor is waiting for. `Some(epoch)` means we sent
    /// a barrier and must wait for the coordinator to commit it before sending more data.
    pending_barrier_epoch: Option<u64>,
}

impl MultiplexedActorOutput {
    /// Wait until the previously submitted barrier has been coalesced and committed
    /// to the shared channel. This prevents:
    /// - Data/watermarks from overtaking a pending barrier (snapshot consistency).
    /// - A new barrier from reaching the coalescer before the previous one completes
    ///   (which would cause an epoch mismatch panic, since the coalescer expects all
    ///   actors to submit the same epoch before any actor advances).
    async fn wait_for_pending_barrier(&mut self) -> StreamResult<()> {
        if let Some(epoch) = self.pending_barrier_epoch {
            loop {
                if *self.barrier_committed_rx.borrow() >= epoch {
                    break;
                }
                self.barrier_committed_rx
                    .changed()
                    .await
                    .map_err(|_| ExchangeChannelClosed::output(self.actor_id))?;
            }
            self.pending_barrier_epoch = None;
        }
        Ok(())
    }

    /// Send a message to the downstream via the shared channel.
    ///
    /// For ALL message types: waits for any pending barrier to be committed first.
    /// This is critical for barriers too — with `in_flight_barrier_nums > 1`, an actor
    /// can receive consecutive barriers without data between them. Without the gate,
    /// barrier N+1 could reach the coalescer before barrier N finishes coalescing
    /// (because other actors haven't submitted barrier N yet), causing an epoch mismatch.
    ///
    /// For barriers: additionally sends to the coordinator for coalescing and records
    /// the epoch so subsequent sends will block.
    pub async fn send(&mut self, message: Message) -> StreamResult<()> {
        // Gate: wait for any previously-submitted barrier to complete coalescing.
        self.wait_for_pending_barrier().await?;

        match &message {
            Message::BarrierBatch(barriers) => {
                // Send the entire barrier batch atomically to the coalescer.
                // The coalescer collects full batches from all actors before coalescing.
                let last_epoch = barriers.last().map(|b| b.epoch.curr);
                self.barrier_tx
                    .send((self.actor_id, barriers.clone()))
                    .map_err(|_| ExchangeChannelClosed::output(self.actor_id))?;
                // Record the epoch — the next `send()` call will block until the
                // coordinator has committed this barrier batch to the shared channel.
                if let Some(epoch) = last_epoch {
                    self.pending_barrier_epoch = Some(epoch);
                }
                Ok(())
            }
            Message::Chunk(_) | Message::Watermark(_) => {
                // Data and watermarks are sent immediately on the shared channel,
                // tagged with the source actor ID so the receiver can route them.
                self.ch
                    .send_tagged(message, self.actor_id, vec![])
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
/// 4. **Broadcasts the committed epoch** to ungate all actors
///
/// Data chunks and watermarks bypass this coordinator entirely — they go directly
/// from `MultiplexedActorOutput` to the `permit::Sender` channel. However, actors
/// gate themselves after sending a barrier, waiting for this coordinator to confirm
/// the coalesced barrier is in the shared channel before sending more data.
pub struct MultiplexedOutputCoordinator {
    /// Receives barrier batch notifications from all upstream actors.
    barrier_rx: mpsc::UnboundedReceiver<(ActorId, Vec<DispatcherBarrier>)>,
    /// Coalesces barrier batches from multiple actors.
    coalescer: BarrierCoalescer,
    /// The shared physical channel sender.
    ch: permit::Sender,
    /// Broadcasts the epoch of the coalesced barrier after it has been written to `ch`.
    /// All `MultiplexedActorOutput` handles watch this to ungate after a barrier.
    barrier_committed_tx: tokio::sync::watch::Sender<u64>,
}

impl MultiplexedOutputCoordinator {
    /// Run the coordinator loop. This should be spawned as a background task.
    ///
    /// It continuously receives barrier notifications and sends coalesced barriers
    /// when all expected actors have reached the same epoch.
    pub async fn run(mut self) -> StreamResult<()> {
        while let Some((actor_id, barriers)) = self.barrier_rx.recv().await {
            if let Some((coalesced_batch, actor_ids)) = self.coalescer.collect(actor_id, barriers) {
                let last_epoch = coalesced_batch
                    .last()
                    .expect("barrier batch should not be empty")
                    .epoch
                    .curr;

                // All actors reached the barrier batch — send the coalesced batch,
                // tagged with the full set of coalesced actor IDs.
                //
                // Use `send_barrier_bypassing_permits` instead of `send_tagged` to avoid
                // deadlock: all upstream actors are gated on the coordinator via
                // `wait_for_pending_barrier()`, so if we blocked here on barrier permits
                // (which are returned by the downstream), and the downstream is itself
                // waiting for these actors' data to make progress, we'd deadlock.
                let coalesced = DispatcherMessageBatch::BarrierBatch(coalesced_batch);
                self.ch
                    .send_barrier_bypassing_permits(coalesced, actor_ids)
                    .map_err(|_| ExchangeChannelClosed::output(0.into()))?;

                // Ungate all actors: the coalesced barrier batch is now in the shared
                // channel, so actors can safely send post-barrier data.
                let _ = self.barrier_committed_tx.send(last_epoch);
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
    // Barrier permits control how many coalesced barriers can be in-flight simultaneously
    // in the shared channel. This value comes from the `concurrent_barriers` config
    // (`stream_exchange_concurrent_barriers`). At high parallelism, consider raising it
    // (e.g. to 10) to avoid serializing barrier propagation through the coordinator.
    let barrier_permits = concurrent_barriers;

    let (tx, rx) = permit::channel(
        scaled_initial_permits,
        scaled_batched_permits,
        barrier_permits,
    );

    let (barrier_tx, barrier_rx) = mpsc::unbounded_channel();

    // Watch channel for the barrier gate: coordinator broadcasts the committed epoch,
    // actors wait for it before sending post-barrier data.
    let (barrier_committed_tx, barrier_committed_rx) = tokio::sync::watch::channel(0u64);

    let expected_actors: HashSet<ActorId> = upstream_actor_ids.iter().copied().collect();
    let coalescer = BarrierCoalescer::new(expected_actors);

    let actor_outputs: Vec<MultiplexedActorOutput> = upstream_actor_ids
        .iter()
        .map(|&actor_id| MultiplexedActorOutput {
            actor_id,
            ch: tx.clone(),
            barrier_tx: barrier_tx.clone(),
            barrier_committed_rx: barrier_committed_rx.clone(),
            pending_barrier_epoch: None,
        })
        .collect();

    // Drop the original barrier_tx — only the actor outputs hold senders.
    drop(barrier_tx);

    let coordinator = MultiplexedOutputCoordinator {
        barrier_rx,
        coalescer,
        ch: tx,
        barrier_committed_tx,
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
                    permits_tx.send(add_back_permits).map_err(|_| {
                        anyhow::anyhow!("MultiplexedRemoteInput backward permits channel closed")
                    })?;
                }

                let msg_batch = DispatcherMessageBatch::from_protobuf(&msg)
                    .context("MultiplexedRemoteInput decode error")?;

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
                        let coalesced_actor_ids =
                            msg.stream_message_batch.as_ref().and_then(|smb| match smb {
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
                                        let _ =
                                            tx.send(DispatcherMessage::Barrier(barrier.clone()));
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
                return Err(anyhow::anyhow!(e)
                    .context("MultiplexedRemoteInput gRPC error")
                    .into());
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
        let actors: HashSet<ActorId> = [1u32, 2, 3].into_iter().map(ActorId::new).collect();
        let mut coalescer = BarrierCoalescer::new(actors);

        let barrier1 = DispatcherBarrier::with_prev_epoch_for_test(test_epoch(2), test_epoch(1));
        let barrier2 = DispatcherBarrier::with_prev_epoch_for_test(test_epoch(2), test_epoch(1));
        let barrier3 = DispatcherBarrier::with_prev_epoch_for_test(test_epoch(2), test_epoch(1));

        // First two arrivals return None
        assert!(coalescer.collect(1.into(), vec![barrier1]).is_none());
        assert!(coalescer.collect(2.into(), vec![barrier2]).is_none());

        // Third arrival completes the coalescing
        let result = coalescer.collect(3.into(), vec![barrier3]);
        assert!(result.is_some());

        let (batch, mut actor_ids) = result.unwrap();
        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0].epoch.curr, test_epoch(2));
        actor_ids.sort();
        assert_eq!(
            actor_ids,
            vec![ActorId::new(1), ActorId::new(2), ActorId::new(3)]
        );
    }

    #[test]
    fn test_barrier_coalescer_sequential_epochs() {
        let actors: HashSet<ActorId> = [10u32, 20].into_iter().map(ActorId::new).collect();
        let mut coalescer = BarrierCoalescer::new(actors);

        // Epoch 1
        let b1 = DispatcherBarrier::with_prev_epoch_for_test(test_epoch(2), test_epoch(1));
        let b2 = DispatcherBarrier::with_prev_epoch_for_test(test_epoch(2), test_epoch(1));
        assert!(coalescer.collect(10.into(), vec![b1]).is_none());
        let result = coalescer.collect(20.into(), vec![b2]);
        assert!(result.is_some());

        // Epoch 2 — coalescer should be reset
        let b3 = DispatcherBarrier::with_prev_epoch_for_test(test_epoch(3), test_epoch(2));
        let b4 = DispatcherBarrier::with_prev_epoch_for_test(test_epoch(3), test_epoch(2));
        assert!(coalescer.collect(10.into(), vec![b3]).is_none());
        let result = coalescer.collect(20.into(), vec![b4]);
        assert!(result.is_some());
        let (batch, _) = result.unwrap();
        assert_eq!(batch[0].epoch.curr, test_epoch(3));
    }

    #[test]
    #[should_panic(expected = "barrier epoch mismatch")]
    fn test_barrier_coalescer_epoch_mismatch() {
        let actors: HashSet<ActorId> = [1u32, 2].into_iter().map(ActorId::new).collect();
        let mut coalescer = BarrierCoalescer::new(actors);

        let b1 = DispatcherBarrier::with_prev_epoch_for_test(test_epoch(2), test_epoch(1));
        let b2 = DispatcherBarrier::with_prev_epoch_for_test(test_epoch(3), test_epoch(2));

        coalescer.collect(1.into(), vec![b1]);
        coalescer.collect(2.into(), vec![b2]); // Should panic: epoch mismatch
    }

    #[test]
    #[should_panic(expected = "sent barrier batch twice")]
    fn test_barrier_coalescer_duplicate_actor() {
        let actors: HashSet<ActorId> = [1u32, 2].into_iter().map(ActorId::new).collect();
        let mut coalescer = BarrierCoalescer::new(actors);

        let b1 = DispatcherBarrier::with_prev_epoch_for_test(test_epoch(2), test_epoch(1));
        let b2 = DispatcherBarrier::with_prev_epoch_for_test(test_epoch(2), test_epoch(1));

        coalescer.collect(1.into(), vec![b1]);
        coalescer.collect(1.into(), vec![b2]); // Should panic: duplicate
    }

    #[test]
    fn test_barrier_coalescer_multi_barrier_batch() {
        let actors: HashSet<ActorId> = [1u32, 2].into_iter().map(ActorId::new).collect();
        let mut coalescer = BarrierCoalescer::new(actors);

        // Each actor sends a batch of two barriers
        let b1_e1 = DispatcherBarrier::with_prev_epoch_for_test(test_epoch(2), test_epoch(1));
        let b1_e2 = DispatcherBarrier::with_prev_epoch_for_test(test_epoch(3), test_epoch(2));
        let b2_e1 = DispatcherBarrier::with_prev_epoch_for_test(test_epoch(2), test_epoch(1));
        let b2_e2 = DispatcherBarrier::with_prev_epoch_for_test(test_epoch(3), test_epoch(2));

        assert!(coalescer.collect(1.into(), vec![b1_e1, b1_e2]).is_none());
        let result = coalescer.collect(2.into(), vec![b2_e1, b2_e2]);
        assert!(result.is_some());

        let (batch, _) = result.unwrap();
        assert_eq!(batch.len(), 2);
        assert_eq!(batch[0].epoch.curr, test_epoch(2));
        assert_eq!(batch[1].epoch.curr, test_epoch(3));
    }

    #[test]
    fn test_barrier_coalescer_add_remove_actors() {
        let actors: HashSet<ActorId> = [1u32, 2].into_iter().map(ActorId::new).collect();
        let mut coalescer = BarrierCoalescer::new(actors);

        // Add actor 3
        coalescer.add_actor(3.into());

        let b1 = DispatcherBarrier::with_prev_epoch_for_test(test_epoch(2), test_epoch(1));
        let b2 = DispatcherBarrier::with_prev_epoch_for_test(test_epoch(2), test_epoch(1));
        let b3 = DispatcherBarrier::with_prev_epoch_for_test(test_epoch(2), test_epoch(1));

        assert!(coalescer.collect(1.into(), vec![b1]).is_none());
        assert!(coalescer.collect(2.into(), vec![b2]).is_none());
        // Now need all 3
        let result = coalescer.collect(3.into(), vec![b3]);
        assert!(result.is_some());

        // Remove actor 3
        coalescer.remove_actor(3.into());

        // Next epoch only needs actors 1 and 2
        let b4 = DispatcherBarrier::with_prev_epoch_for_test(test_epoch(3), test_epoch(2));
        let b5 = DispatcherBarrier::with_prev_epoch_for_test(test_epoch(3), test_epoch(2));
        assert!(coalescer.collect(1.into(), vec![b4]).is_none());
        let result = coalescer.collect(2.into(), vec![b5]);
        assert!(result.is_some());
    }
}

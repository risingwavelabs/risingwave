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

//! Multiplexed exchange for node-level barrier relay with epoch-tagged pipelining.
//!
//! This module provides [`MultiplexedActorOutput`] (sender side) and
//! [`run_multiplexed_remote_input`] (receiver side) to reduce barrier messages from N×M to
//! M per exchange per epoch, where N is the number of upstream actors on one node and M is
//! the number of downstream actors.
//!
//! **Epoch-tagged pipelining**: Unlike the blocking approach where actors wait for barrier
//! coalescing before sending more data, each actor tags data with its current epoch and
//! sends it immediately. The receiver buffers ahead-of-barrier data and flushes it once
//! the coalesced barrier arrives. This preserves correctness while eliminating sender-side
//! head-of-line blocking.
//!
//! See `docs/dev/src/design/node-level-barrier-relay.md` for the full design.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

use anyhow::Context;
use itertools::Itertools;
use risingwave_pb::stream_plan::stream_message_batch::StreamMessageBatch as PbStreamMessageBatchInner;
use risingwave_pb::task_service::{GetStreamResponse, permits};
use tokio::sync::{Semaphore, SemaphorePermit, mpsc};

use super::error::ExchangeChannelClosed;
use super::permit;
use crate::error::StreamResult;
use crate::executor::monitor::StreamingMetrics;
use crate::executor::{
    DispatcherBarrier, DispatcherMessageBatch, DispatcherMessageBatch as Message,
};
use crate::task::{ActorId, FragmentId};

// ============================================================================
// Sender side: BarrierCoalescer + MultiplexedOutput
// ============================================================================

/// Queue-based barrier coalescer that collects barrier batches from multiple upstream actors
/// and emits a single coalesced barrier batch once all expected actors have reached the same
/// set of epochs.
///
/// With epoch-tagged pipelining, fast actors can submit barriers for epoch E+1 before slow
/// actors have submitted epoch E. The coalescer queues per-actor barriers and coalesces them
/// in FIFO order (epoch E before epoch E+1).
///
/// **Batch-aware**: each actor sends its `Vec<DispatcherBarrier>` batch atomically.
/// The coalescer waits for all actors to submit the same batch (verified by epoch list),
/// then emits one copy of the batch along with the full set of actor IDs.
pub struct BarrierCoalescer {
    /// The set of upstream actor IDs that must all deliver a barrier batch before we coalesce.
    expected_actors: HashSet<ActorId>,
    /// Per-actor queue of barrier batches. Fast actors can queue multiple batches ahead of
    /// slow actors. The coalescer only coalesces the front of each queue.
    queued: HashMap<ActorId, VecDeque<Vec<DispatcherBarrier>>>,
}

impl BarrierCoalescer {
    pub fn new(expected_actors: HashSet<ActorId>) -> Self {
        let queued = expected_actors
            .iter()
            .map(|&id| (id, VecDeque::new()))
            .collect();
        Self {
            expected_actors,
            queued,
        }
    }

    /// Record that `actor_id` has reached its barrier batch for some epoch(s).
    ///
    /// The barrier is queued. If all expected actors now have at least one queued barrier,
    /// and the front barriers all match in epoch, the coalesced result is returned.
    ///
    /// Returns `Some((batch, actor_ids))` when all expected actors have arrived for the
    /// same epoch. Returns `None` if we're still waiting for more actors.
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

        self.queued.get_mut(&actor_id).unwrap().push_back(barriers);

        self.try_coalesce()
    }

    /// Try to produce a coalesced barrier batch from the front of all queues.
    ///
    /// Returns `Some` if all expected actors have at least one queued barrier and the
    /// front barriers all have matching epochs. Returns `None` otherwise.
    pub fn try_coalesce(&mut self) -> Option<(Vec<DispatcherBarrier>, Vec<ActorId>)> {
        // Check if all expected actors have at least one queued barrier.
        if !self
            .expected_actors
            .iter()
            .all(|id| self.queued.get(id).is_some_and(|q| !q.is_empty()))
        {
            return None;
        }

        // Verify epoch consistency across all actors' front barriers.
        let mut template: Option<&Vec<DispatcherBarrier>> = None;
        for id in &self.expected_actors {
            let front = self.queued.get(id).unwrap().front().unwrap();
            if let Some(t) = &template {
                assert_eq!(
                    t.len(),
                    front.len(),
                    "barrier batch length mismatch in coalescer: actor {} has {} barriers, expected {}",
                    id,
                    front.len(),
                    t.len()
                );
                for (p, b) in t.iter().zip_eq(front.iter()) {
                    assert_eq!(
                        p.epoch, b.epoch,
                        "barrier epoch mismatch in coalescer: actor {} has epoch {:?}, expected {:?}",
                        id, b.epoch, p.epoch
                    );
                }
            } else {
                template = Some(front);
            }
        }

        // All actors match — dequeue from all and emit the coalesced batch.
        let actor_ids: Vec<ActorId> = self.expected_actors.iter().copied().collect();
        let mut batch = None;
        for id in &actor_ids {
            let barriers = self.queued.get_mut(id).unwrap().pop_front().unwrap();
            if batch.is_none() {
                batch = Some(barriers);
            }
        }

        Some((batch.unwrap(), actor_ids))
    }

    /// Add a new upstream actor to the expected set.
    /// Called when actors are added at a barrier boundary during scaling.
    pub fn add_actor(&mut self, actor_id: ActorId) {
        self.expected_actors.insert(actor_id);
        self.queued.entry(actor_id).or_default();
    }

    /// Remove an upstream actor from the expected set.
    /// Called when actors are removed at a barrier boundary during scaling.
    pub fn remove_actor(&mut self, actor_id: ActorId) {
        self.expected_actors.remove(&actor_id);
        self.queued.remove(&actor_id);
    }
}

/// A handle for one upstream actor to send messages into a multiplexed output channel.
///
/// Each upstream actor's `DispatchExecutor` holds one of these instead of a direct `Output`.
/// Data chunks and watermarks are forwarded immediately (tagged with the actor ID and epoch).
/// Barriers are sent to the shared `BarrierCoalescer` for coalescing.
///
/// **Epoch-tagged pipelining**: after sending a barrier, the actor immediately continues
/// sending data for the next epoch. The actor is never blocked by other actors. Data is
/// tagged with `current_epoch` so the receiver can buffer ahead-of-barrier data.
///
/// **Per-actor record permits**: each actor has its own record semaphore, preventing fast
/// actors from starving slow actors of backpressure permits.
pub struct MultiplexedActorOutput {
    /// The upstream actor ID this handle belongs to.
    actor_id: ActorId,
    /// Shared sender — sends tagged messages to the physical gRPC stream.
    ch: permit::Sender,
    /// Per-actor record permit semaphore. This actor acquires record permits from its own
    /// semaphore instead of the shared one, ensuring fair backpressure across actors.
    record_permits: Arc<Semaphore>,
    /// The maximum permits that a single chunk can acquire, equal to
    /// `initial_permits_per_actor - batched_permits`.
    max_chunk_permits: usize,
    /// Channel to send barrier batches to the coordinator for coalescing.
    barrier_tx: mpsc::UnboundedSender<(ActorId, Vec<DispatcherBarrier>)>,
    /// The current epoch this actor is in. Data sent after the last barrier is tagged
    /// with this epoch. Initialized to 0 (sentinel for "before any real barrier") and
    /// updated to `barrier.epoch.curr` after each barrier is sent to the coalescer.
    current_epoch: u64,
}

impl MultiplexedActorOutput {
    /// Send a message to the downstream via the shared channel.
    ///
    /// For barriers: decomposes the batch and sends each barrier individually to the
    /// coordinator for coalescing. Updates `current_epoch` after each barrier. The actor
    /// does NOT wait for coalescing to complete — it continues immediately.
    ///
    /// For data/watermarks: acquires record permits from this actor's per-actor semaphore,
    /// then sends on the shared channel tagged with the source actor ID and epoch.
    pub async fn send(&mut self, message: Message) -> StreamResult<()> {
        match message {
            Message::BarrierBatch(barriers) => {
                // Send each barrier individually to the coordinator. This ensures all
                // actors submit one epoch at a time, preventing batch length or epoch
                // mismatch panics in the coalescer.
                for barrier in barriers {
                    let next_epoch = barrier.epoch.curr;
                    self.barrier_tx
                        .send((self.actor_id, vec![barrier]))
                        .map_err(|_| ExchangeChannelClosed::output(self.actor_id))?;
                    // Update epoch: subsequent data belongs to the new epoch.
                    self.current_epoch = next_epoch;
                }
                Ok(())
            }
            Message::Chunk(ref chunk) => {
                // Acquire record permits from this actor's dedicated semaphore.
                let card = chunk.cardinality().clamp(1, self.max_chunk_permits);
                if card == self.max_chunk_permits {
                    tracing::warn!(
                        cardinality = chunk.cardinality(),
                        "large chunk in multiplexed exchange"
                    );
                }
                let permits_val = permits::Value::Record(card as u32);

                // Acquire from per-actor semaphore (blocks if this actor has too much
                // in-flight data, without affecting other actors).
                if self
                    .record_permits
                    .acquire_many(card as u32)
                    .await
                    .map(SemaphorePermit::forget)
                    .is_err()
                {
                    return Err(ExchangeChannelClosed::output(self.actor_id).into());
                }

                // Send with pre-acquired permits (no shared semaphore acquisition).
                self.ch
                    .send_preacquired(
                        message,
                        Some(permits_val),
                        self.actor_id,
                        vec![],
                        self.current_epoch,
                    )
                    .map_err(|_| ExchangeChannelClosed::output(self.actor_id))?;
                Ok(())
            }
            Message::Watermark(_) => {
                // Watermarks don't require permits — send immediately.
                self.ch
                    .send_preacquired(message, None, self.actor_id, vec![], self.current_epoch)
                    .map_err(|_| ExchangeChannelClosed::output(self.actor_id))?;
                Ok(())
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
/// 2. Coalesces them using `BarrierCoalescer` (queue-based, handles fast actors)
/// 3. Sends the coalesced barrier on the physical channel
///
/// With epoch-tagged pipelining, there is no watch channel for ungating actors.
/// Actors send data independently without waiting for the coordinator.
pub struct MultiplexedOutputCoordinator {
    /// The downstream actor ID this coordinator targets (for error reporting).
    downstream_actor_id: ActorId,
    /// Receives barrier batch notifications from all upstream actors.
    barrier_rx: mpsc::UnboundedReceiver<(ActorId, Vec<DispatcherBarrier>)>,
    /// Queue-based coalescer for barrier batches from multiple actors.
    coalescer: BarrierCoalescer,
    /// The shared physical channel sender.
    ch: permit::Sender,
}

impl MultiplexedOutputCoordinator {
    /// Send a coalesced barrier batch on the shared channel.
    async fn send_coalesced(
        &self,
        coalesced_batch: Vec<DispatcherBarrier>,
        actor_ids: Vec<ActorId>,
    ) -> StreamResult<()> {
        let coalesced = DispatcherMessageBatch::BarrierBatch(coalesced_batch);
        self.ch
            .send_tagged(coalesced, 0.into(), actor_ids, 0)
            .await
            .map_err(|_| ExchangeChannelClosed::output(self.downstream_actor_id))?;
        Ok(())
    }

    /// Run the coordinator loop. This should be spawned as a background task.
    ///
    /// It continuously receives barrier notifications and sends coalesced barriers
    /// when all expected actors have reached the same epoch. After each coalescing,
    /// it drains any additional ready batches (in case a slow actor catching up
    /// unblocks multiple epochs).
    pub async fn run(mut self) -> StreamResult<()> {
        while let Some((actor_id, barriers)) = self.barrier_rx.recv().await {
            if let Some((coalesced_batch, actor_ids)) = self.coalescer.collect(actor_id, barriers) {
                self.send_coalesced(coalesced_batch, actor_ids).await?;

                // After sending one coalesced barrier, check if more are ready.
                // This can happen when a slow actor catches up and multiple epochs
                // become coalesceable in sequence.
                while let Some((coalesced_batch, actor_ids)) = self.coalescer.try_coalesce() {
                    self.send_coalesced(coalesced_batch, actor_ids).await?;
                }
            }
        }
        Ok(())
    }
}

/// Creates a multiplexed output for a set of upstream actors targeting a single downstream actor.
///
/// Each actor gets its own record permit semaphore with `initial_permits` permits, preventing
/// fast actors from starving slow actors. Barrier permits remain shared since barriers are
/// coalesced by the coordinator.
///
/// Returns:
/// - A vec of `MultiplexedActorOutput` handles, one per upstream actor
/// - A `MultiplexedOutputCoordinator` that must be spawned as a background task
/// - A `permit::Receiver` that the gRPC server reads from
pub fn create_multiplexed_output(
    upstream_actor_ids: &[ActorId],
    downstream_actor_id: ActorId,
    initial_permits: usize,
    batched_permits: usize,
    concurrent_barriers: usize,
) -> (
    Vec<MultiplexedActorOutput>,
    MultiplexedOutputCoordinator,
    permit::Receiver,
) {
    // Create channel with per-actor record permits. Each actor gets `initial_permits` worth
    // of record capacity in its own semaphore. Returned permits are distributed fairly
    // across all actors.
    let num_actors = upstream_actor_ids.len();
    let (tx, rx, per_actor_semaphores) = permit::channel_multiplexed(
        initial_permits,
        batched_permits,
        concurrent_barriers,
        num_actors,
    );

    let max_chunk_permits = tx.max_chunk_permits();
    let (barrier_tx, barrier_rx) = mpsc::unbounded_channel();

    let expected_actors: HashSet<ActorId> = upstream_actor_ids.iter().copied().collect();
    let coalescer = BarrierCoalescer::new(expected_actors);

    let actor_outputs: Vec<MultiplexedActorOutput> = upstream_actor_ids
        .iter()
        .zip_eq(per_actor_semaphores)
        .map(|(&actor_id, record_permits)| MultiplexedActorOutput {
            actor_id,
            ch: tx.clone(),
            record_permits,
            max_chunk_permits,
            barrier_tx: barrier_tx.clone(),
            current_epoch: 0,
        })
        .collect();

    // Drop the original barrier_tx — only the actor outputs hold senders.
    drop(barrier_tx);

    let coordinator = MultiplexedOutputCoordinator {
        downstream_actor_id,
        barrier_rx,
        coalescer,
        ch: tx,
    };

    (actor_outputs, coordinator, rx)
}

// ============================================================================
// Receiver side: MultiplexedRemoteInput with epoch-tagged buffering
// ============================================================================

/// Flush buffered messages that are now deliverable after a barrier advanced the epoch.
///
/// Pops all entries from the front of the buffer whose epoch is <= `delivered_epoch`
/// and sends them to the logical input channel.
fn flush_buffer(
    buf: &mut VecDeque<(u64, crate::executor::DispatcherMessage)>,
    delivered_epoch: u64,
    tx: &mpsc::UnboundedSender<crate::executor::DispatcherMessage>,
) {
    while let Some(&(epoch, _)) = buf.front() {
        if epoch <= delivered_epoch {
            let (_, msg) = buf.pop_front().unwrap();
            let _ = tx.send(msg);
        } else {
            break;
        }
    }
}

/// Demultiplexes a single multiplexed gRPC stream into per-actor logical inputs.
///
/// With epoch-tagged pipelining, this function also handles buffering: when data for
/// epoch E+1 arrives from a fast actor before the coalesced barrier for epoch E has
/// arrived, the data is buffered. Once the barrier arrives, buffered data is flushed
/// to the logical input in the correct order.
///
/// This reads from the multiplexed response stream and routes:
/// - Data chunks → to the logical input for the tagged `source_actor_id` (buffered if ahead)
/// - Watermarks → to the logical input for the tagged `source_actor_id` (buffered if ahead)
/// - Coalesced barriers → expanded into one barrier per actor, then buffered data flushed
///
/// Each logical output is an `mpsc::UnboundedSender<DispatcherMessage>` that feeds
/// into a `LogicalInput` (which implements `Input` / `ActorInput`).
pub async fn run_multiplexed_remote_input(
    mut stream: tonic::Streaming<GetStreamResponse>,
    permits_tx: mpsc::UnboundedSender<permits::Value>,
    logical_outputs: HashMap<ActorId, mpsc::UnboundedSender<crate::executor::DispatcherMessage>>,
    batched_permits_limit: usize,
    up_fragment_id: FragmentId,
    down_fragment_id: FragmentId,
    metrics: Arc<StreamingMetrics>,
) -> StreamResult<()> {
    use crate::executor::DispatcherMessage;

    let exchange_frag_recv_size_metrics = metrics
        .exchange_frag_recv_size
        .with_guarded_label_values(&[&up_fragment_id.to_string(), &down_fragment_id.to_string()]);

    let mut batched_permits_accumulated: u32 = 0;

    // Per-actor epoch tracking for epoch-tagged pipelining.
    // `delivered_barrier_curr[actor]`: the `curr` epoch of the last barrier delivered to
    // this actor's logical input. Data with epoch <= this value can be forwarded
    // immediately; data with epoch > this value must be buffered.
    // Initialized to 0 (matches sender's initial `current_epoch`).
    let mut delivered_barrier_curr: HashMap<ActorId, u64> =
        logical_outputs.keys().map(|&id| (id, 0)).collect();

    // Per-actor buffer for data that arrived ahead of its barrier.
    let mut buffered: HashMap<ActorId, VecDeque<(u64, DispatcherMessage)>> = logical_outputs
        .keys()
        .map(|&id| (id, VecDeque::new()))
        .collect();

    while let Some(data_res) = stream.message().await.transpose() {
        match data_res {
            Ok(GetStreamResponse { message, permits }) => {
                let msg = message.unwrap();
                let bytes = DispatcherMessageBatch::get_encoded_len(&msg);
                exchange_frag_recv_size_metrics.inc_by(bytes as u64);
                let source_actor_id = msg.source_actor_id;
                let data_epoch = msg.epoch;

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
                        let delivered = delivered_barrier_curr.get(&source_actor_id).copied()
                            .ok_or_else(|| {
                                anyhow::anyhow!(
                                    "MultiplexedRemoteInput received chunk for unknown source actor {}",
                                    source_actor_id
                                )
                            })?;

                        let tx = logical_outputs.get(&source_actor_id).unwrap();
                        if data_epoch <= delivered {
                            // Data belongs to current or past epoch — deliver immediately.
                            let _ = tx.send(DispatcherMessage::Chunk(chunk));
                        } else {
                            // Data is ahead of the barrier — buffer it.
                            buffered
                                .get_mut(&source_actor_id)
                                .unwrap()
                                .push_back((data_epoch, DispatcherMessage::Chunk(chunk)));
                        }
                    }
                    DispatcherMessageBatch::Watermark(watermark) => {
                        let delivered = delivered_barrier_curr.get(&source_actor_id).copied()
                            .ok_or_else(|| {
                                anyhow::anyhow!(
                                    "MultiplexedRemoteInput received watermark for unknown source actor {}",
                                    source_actor_id
                                )
                            })?;

                        let tx = logical_outputs.get(&source_actor_id).unwrap();
                        if data_epoch <= delivered {
                            let _ = tx.send(DispatcherMessage::Watermark(watermark));
                        } else {
                            buffered
                                .get_mut(&source_actor_id)
                                .unwrap()
                                .push_back((data_epoch, DispatcherMessage::Watermark(watermark)));
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

                        let last_barrier_curr = barriers
                            .last()
                            .expect("barrier batch should not be empty")
                            .epoch
                            .curr;

                        if let Some(actor_ids) = coalesced_actor_ids {
                            // Multiplexed mode: expand the barrier to all listed actors,
                            // then flush any buffered ahead-of-barrier data.
                            for &actor_id in actor_ids {
                                let tx = logical_outputs.get(&actor_id).ok_or_else(|| {
                                    anyhow::anyhow!(
                                        "MultiplexedRemoteInput received coalesced barrier for unknown actor {}",
                                        actor_id
                                    )
                                })?;
                                for barrier in &barriers {
                                    let _ = tx.send(DispatcherMessage::Barrier(barrier.clone()));
                                }
                                // Update delivered epoch for this actor.
                                *delivered_barrier_curr.get_mut(&actor_id).unwrap() =
                                    last_barrier_curr;
                                // Flush buffered data that is now deliverable.
                                flush_buffer(
                                    buffered.get_mut(&actor_id).unwrap(),
                                    last_barrier_curr,
                                    tx,
                                );
                            }
                        } else {
                            // Legacy mode: send to source_actor_id
                            let tx = logical_outputs.get(&source_actor_id).ok_or_else(|| {
                                anyhow::anyhow!(
                                    "MultiplexedRemoteInput received barrier for unknown source actor {}",
                                    source_actor_id
                                )
                            })?;
                            for barrier in barriers {
                                let _ = tx.send(DispatcherMessage::Barrier(barrier));
                            }
                            *delivered_barrier_curr.get_mut(&source_actor_id).unwrap() =
                                last_barrier_curr;
                            flush_buffer(
                                buffered.get_mut(&source_actor_id).unwrap(),
                                last_barrier_curr,
                                tx,
                            );
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
        use std::task::Poll;

        match self.rx.poll_recv(cx) {
            Poll::Ready(Some(msg)) => Poll::Ready(Some(Ok(msg))),
            Poll::Ready(None) => {
                // The underlying channel was closed (e.g. the demuxer task exited).
                // Return an error instead of ending the stream, matching the behavior
                // of `LocalInput` and `RemoteInput`. Downstream executors like
                // `MergeExecutor` assume inputs never end and would panic on `None`.
                Poll::Ready(Some(Err(ExchangeChannelClosed::multiplexed_input(
                    self.actor_id,
                )
                .into())))
            }
            Poll::Pending => Poll::Pending,
        }
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

        // Epoch 2 — coalescer should handle it after draining epoch 1
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
        coalescer.collect(2.into(), vec![b2]); // Should panic: epoch mismatch at front
    }

    #[test]
    fn test_barrier_coalescer_queued_barriers() {
        // Test epoch-tagged pipelining: actor 1 (fast) submits epochs E1 and E2 before
        // actor 2 (slow) submits E1.
        let actors: HashSet<ActorId> = [1u32, 2].into_iter().map(ActorId::new).collect();
        let mut coalescer = BarrierCoalescer::new(actors);

        let b1_e1 = DispatcherBarrier::with_prev_epoch_for_test(test_epoch(2), test_epoch(1));
        let b1_e2 = DispatcherBarrier::with_prev_epoch_for_test(test_epoch(3), test_epoch(2));
        let b2_e1 = DispatcherBarrier::with_prev_epoch_for_test(test_epoch(2), test_epoch(1));
        let b2_e2 = DispatcherBarrier::with_prev_epoch_for_test(test_epoch(3), test_epoch(2));

        // Fast actor submits E1 — not enough
        assert!(coalescer.collect(1.into(), vec![b1_e1]).is_none());
        // Fast actor submits E2 — still not enough (slow actor hasn't submitted E1)
        assert!(coalescer.collect(1.into(), vec![b1_e2]).is_none());

        // Slow actor submits E1 — now E1 can coalesce
        let result = coalescer.collect(2.into(), vec![b2_e1]);
        assert!(result.is_some());
        let (batch, _) = result.unwrap();
        assert_eq!(batch[0].epoch.curr, test_epoch(2));

        // E2 is not ready yet (only actor 1 has it)
        assert!(coalescer.try_coalesce().is_none());

        // Slow actor submits E2 — now E2 can coalesce
        let result = coalescer.collect(2.into(), vec![b2_e2]);
        assert!(result.is_some());
        let (batch, _) = result.unwrap();
        assert_eq!(batch[0].epoch.curr, test_epoch(3));
    }

    #[test]
    fn test_barrier_coalescer_queued_drain_multiple() {
        // Test that when a slow actor catches up, multiple epochs can be drained.
        let actors: HashSet<ActorId> = [1u32, 2].into_iter().map(ActorId::new).collect();
        let mut coalescer = BarrierCoalescer::new(actors);

        let b1_e1 = DispatcherBarrier::with_prev_epoch_for_test(test_epoch(2), test_epoch(1));
        let b1_e2 = DispatcherBarrier::with_prev_epoch_for_test(test_epoch(3), test_epoch(2));
        let b2_e1 = DispatcherBarrier::with_prev_epoch_for_test(test_epoch(2), test_epoch(1));
        let b2_e2 = DispatcherBarrier::with_prev_epoch_for_test(test_epoch(3), test_epoch(2));

        // Fast actor queues E1 and E2
        assert!(coalescer.collect(1.into(), vec![b1_e1]).is_none());
        assert!(coalescer.collect(1.into(), vec![b1_e2]).is_none());

        // Slow actor queues E1 — coalesces E1
        let result = coalescer.collect(2.into(), vec![b2_e1]);
        assert!(result.is_some());

        // Slow actor queues E2 — coalesces E2
        let result = coalescer.collect(2.into(), vec![b2_e2]);
        assert!(result.is_some());
        let (batch, _) = result.unwrap();
        assert_eq!(batch[0].epoch.curr, test_epoch(3));
    }

    #[test]
    fn test_barrier_coalescer_multi_barrier_batch() {
        let actors: HashSet<ActorId> = [1u32, 2].into_iter().map(ActorId::new).collect();
        let mut coalescer = BarrierCoalescer::new(actors);

        // Each actor sends a batch of two barriers (decomposed by MultiplexedActorOutput,
        // but tested here as atomic batches for the coalescer)
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

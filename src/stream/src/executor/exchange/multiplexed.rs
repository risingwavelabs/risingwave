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

//! Multiplexed exchange with barrier coalescing and message counting.
//!
//! When multiple upstream actors on the same node dispatch to the same downstream actor,
//! barrier messages can be coalesced from N to 1 per epoch. Data flows through per-actor
//! channels (same as non-multiplexed mode) while barriers take a separate coalesced path.
//!
//! # Architecture
//!
//! ```text
//! SENDER NODE                                RECEIVER NODE
//! ===========                                =============
//! Actor A1 ──data──> [per-actor ch] ──gRPC──> RemoteInput ──> CountingMergeInput ──> MergeExecutor
//!          ──barrier──> Coalescer                                   ↑ barrier_rx
//! Actor A2 ──data──> [per-actor ch] ──gRPC──> RemoteInput ──> CountingMergeInput ──> MergeExecutor
//!          ──barrier──> Coalescer                                   ↑ barrier_rx
//!                         │                                         │
//!                         └── coalesced barrier ──> [barrier ch] ──gRPC──> run_barrier_receiver
//!                             (with actor_data_counts)                     (distributes to CountingMergeInputs)
//! ```
//!
//! # Message Counting
//!
//! The sender tracks data messages (chunks + watermarks) per epoch per actor. The count is
//! embedded in the coalesced barrier. The receiver verifies the count before delivering the
//! barrier, ensuring all epoch E data arrives before barrier E.

use std::collections::{HashMap, HashSet, VecDeque};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use anyhow::Context as _;
use futures::stream::StreamExt;
use futures::{Stream, pin_mut};
use pin_project::pin_project;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_pb::id::ActorId;
use risingwave_pb::task_service::{GetStreamResponse, permits};
use tokio::sync::mpsc;

use super::error::ExchangeChannelClosed;
use super::input::{BoxedActorInput, Input};
use super::permit;
use crate::error::StreamResult;
use crate::executor::monitor::StreamingMetrics;
use crate::executor::prelude::try_stream;
use crate::executor::{
    DispatcherBarrier, DispatcherMessage, DispatcherMessageBatch, DispatcherMessageStreamItem,
    StreamExecutorError,
};
use crate::task::UpDownFragmentIds;

/// Result of a successful barrier coalesce: `(barrier, actor_ids, per_actor_data_counts)`.
type CoalescedBarrierResult = (DispatcherBarrier, Vec<ActorId>, HashMap<ActorId, u64>);

/// Coalesces barriers from multiple upstream actors into a single barrier.
///
/// Uses a queue-based approach: fast actors can submit barriers for future epochs before
/// slow actors catch up. When all actors have submitted for an epoch, the coalesced result
/// is returned.
///
/// Each submission is a single barrier (not a batch). The sender splits batched barriers
/// into individual submissions to maintain FIFO alignment across actors.
pub struct BarrierCoalescer {
    /// Set of actor IDs participating in coalescing.
    actor_ids: HashSet<ActorId>,
    /// Queued barriers per actor: each actor's `VecDeque` holds
    /// `(barrier, msg_count)` for each epoch in submission order.
    queued: HashMap<ActorId, VecDeque<(DispatcherBarrier, u64)>>,
}

impl BarrierCoalescer {
    pub fn new(actor_ids: impl IntoIterator<Item = ActorId>) -> Self {
        let actor_ids: HashSet<_> = actor_ids.into_iter().collect();
        let queued = actor_ids.iter().map(|&id| (id, VecDeque::new())).collect();
        Self { actor_ids, queued }
    }

    /// Submit a single barrier from an actor. Returns the coalesced result if all actors
    /// have submitted for the oldest pending epoch.
    ///
    /// Each barrier must be submitted individually (not as a batch) to ensure correct
    /// FIFO alignment across actors. The sender splits `BarrierBatch` into individual
    /// submissions with the first barrier carrying the epoch's data count and subsequent
    /// barriers (from the same batch) carrying count=0.
    ///
    /// # Returns
    /// `Some((barrier, actor_ids, actor_data_counts))` if all actors have submitted.
    pub fn collect(
        &mut self,
        actor_id: ActorId,
        barrier: DispatcherBarrier,
        msg_count: u64,
    ) -> Option<CoalescedBarrierResult> {
        assert!(
            self.actor_ids.contains(&actor_id),
            "unknown actor {} in coalescer",
            actor_id
        );

        self.queued
            .get_mut(&actor_id)
            .unwrap()
            .push_back((barrier, msg_count));

        self.try_drain()
    }

    /// Try to drain one complete epoch from the front of all queues.
    fn try_drain(&mut self) -> Option<CoalescedBarrierResult> {
        // Check if all actors have at least one entry queued.
        if self.queued.values().any(|q| q.is_empty()) {
            return None;
        }

        // All actors have at least one entry: drain the front.
        let mut coalesced_barrier = None;
        let mut coalesced_actor_ids = Vec::with_capacity(self.actor_ids.len());
        let mut actor_data_counts = HashMap::with_capacity(self.actor_ids.len());

        for (&actor_id, queue) in &mut self.queued {
            let (barrier, count) = queue.pop_front().unwrap();
            if coalesced_barrier.is_none() {
                coalesced_barrier = Some(barrier);
            }
            // All actors in the same partial graph will have identical barriers for the
            // same epoch; we keep only one representative barrier.
            coalesced_actor_ids.push(actor_id);
            actor_data_counts.insert(actor_id, count);
        }

        Some((
            coalesced_barrier.unwrap(),
            coalesced_actor_ids,
            actor_data_counts,
        ))
    }

    /// Try to drain all complete epochs. Returns an iterator of results.
    pub fn drain_all(&mut self) -> Vec<CoalescedBarrierResult> {
        let mut results = Vec::new();
        while let Some(result) = self.try_drain() {
            results.push(result);
        }
        results
    }

    /// Add a new actor to the coalescer.
    pub fn add_actor(&mut self, actor_id: ActorId) {
        self.actor_ids.insert(actor_id);
        self.queued.entry(actor_id).or_default();
    }

    /// Remove an actor from the coalescer.
    pub fn remove_actor(&mut self, actor_id: ActorId) {
        self.actor_ids.remove(&actor_id);
        self.queued.remove(&actor_id);
    }

    /// Returns the number of actors in the coalescer.
    pub fn num_actors(&self) -> usize {
        self.actor_ids.len()
    }
}

/// Background coordinator that receives barrier submissions from actors via mpsc channel,
/// coalesces them, and sends coalesced barriers on the barrier-only permit channel.
pub struct MultiplexedOutputCoordinator {
    /// Channel to receive `(actor_id, barrier, msg_count)` from actors.
    /// Each barrier is submitted individually (not as a batch).
    barrier_rx: mpsc::UnboundedReceiver<(ActorId, DispatcherBarrier, u64)>,
    /// The barrier coalescer.
    coalescer: BarrierCoalescer,
    /// The barrier-only channel sender.
    barrier_ch: permit::Sender,
}

impl MultiplexedOutputCoordinator {
    pub fn new(
        barrier_rx: mpsc::UnboundedReceiver<(ActorId, DispatcherBarrier, u64)>,
        actor_ids: impl IntoIterator<Item = ActorId>,
        barrier_ch: permit::Sender,
    ) -> Self {
        Self {
            barrier_rx,
            coalescer: BarrierCoalescer::new(actor_ids),
            barrier_ch,
        }
    }

    /// Run the coordinator loop. This consumes the coordinator.
    pub async fn run(mut self) {
        while let Some((actor_id, barrier, msg_count)) = self.barrier_rx.recv().await {
            // Submit to coalescer. This may return one coalesced result if all actors
            // have submitted for the oldest pending epoch.
            if let Some(result) = self.coalescer.collect(actor_id, barrier, msg_count)
                && self.send_coalesced_barrier(result).await.is_err()
            {
                return;
            }

            // Drain any additional ready epochs (fast actors may have queued multiple).
            for result in self.coalescer.drain_all() {
                if self.send_coalesced_barrier(result).await.is_err() {
                    return;
                }
            }
        }
    }

    async fn send_coalesced_barrier(
        &self,
        (barrier, actor_ids, data_counts): CoalescedBarrierResult,
    ) -> Result<(), ()> {
        let message = DispatcherMessageBatch::BarrierBatch(vec![barrier]);
        self.barrier_ch
            .send_coalesced(message, actor_ids, data_counts)
            .await
            .map_err(|_| {
                tracing::warn!("barrier channel closed in MultiplexedOutputCoordinator");
            })
    }
}

/// Per-actor channels for constructing `Output::CoalescedBarrier` in the `DispatchExecutor`.
pub struct CoalescedBarrierChannels {
    /// Upstream actor ID for coalescer submission.
    pub upstream_actor_id: ActorId,
    /// Per-actor data channel sender.
    pub data_tx: permit::Sender,
    /// Shared channel to send barriers to the coalescer.
    pub barrier_tx: mpsc::UnboundedSender<(ActorId, DispatcherBarrier, u64)>,
}

/// Create the multiplexed output setup: per-actor channel sets,
/// a `MultiplexedOutputCoordinator`, and the barrier channel receiver.
///
/// # Arguments
/// - `upstream_actor_ids`: The actor IDs that will participate in barrier coalescing.
/// - `data_senders`: Pre-created per-actor data channel senders (one per actor, same order).
/// - `barrier_concurrent`: Max concurrent barriers on the barrier-only channel.
///
/// # Returns
/// - `Vec<CoalescedBarrierChannels>`: Per-actor channel sets for building
///   `NewOutputRequest::CoalescedBarrierRemote`.
/// - `MultiplexedOutputCoordinator`: Background task to run.
/// - `permit::Receiver`: Barrier channel receiver.
pub fn create_multiplexed_output(
    upstream_actor_ids: &[ActorId],
    data_senders: Vec<permit::Sender>,
    barrier_concurrent: usize,
) -> (
    Vec<CoalescedBarrierChannels>,
    MultiplexedOutputCoordinator,
    permit::Receiver,
) {
    assert_eq!(upstream_actor_ids.len(), data_senders.len());

    // Create barrier-only channel. Records semaphore is 0 (barriers only).
    let (barrier_tx, barrier_rx) = permit::channel(0, 0, barrier_concurrent);

    // Create the shared barrier submission channel (individual barriers, not batches).
    let (coalescer_tx, coalescer_rx) = mpsc::unbounded_channel();

    // Create per-actor channel sets.
    let channels: Vec<CoalescedBarrierChannels> = upstream_actor_ids
        .iter()
        .copied()
        .zip_eq_fast(data_senders)
        .map(|(actor_id, data_tx)| CoalescedBarrierChannels {
            upstream_actor_id: actor_id,
            data_tx,
            barrier_tx: coalescer_tx.clone(),
        })
        .collect();

    let coordinator = MultiplexedOutputCoordinator::new(
        coalescer_rx,
        upstream_actor_ids.iter().copied(),
        barrier_tx,
    );

    (channels, coordinator, barrier_rx)
}

/// `CountingMergeInput` wraps an inner `BoxedActorInput` (typically a `RemoteInput`)
/// and injects barriers from a separate barrier channel using message counting.
///
/// The inner input provides data messages (chunks + watermarks) only.
/// Barriers arrive on a separate channel with an expected data message count.
/// The `CountingMergeInput` ensures that exactly `expected_count` data messages
/// are delivered before the barrier, preserving the ordering invariant:
/// `[epoch E data] → barrier E → [epoch E+1 data]`.
///
/// Each barrier is received individually (not as a batch), ensuring correct 1:1
/// alignment with the coalescer's per-epoch entries.
#[pin_project]
pub struct CountingMergeInput {
    #[pin]
    inner: CountingMergeInputStreamInner,
    actor_id: ActorId,
}

type CountingMergeInputStreamInner = impl crate::executor::DispatcherMessageStream;

impl CountingMergeInput {
    #[define_opaque(CountingMergeInputStreamInner)]
    pub fn new(
        actor_id: ActorId,
        inner: BoxedActorInput,
        barrier_rx: mpsc::UnboundedReceiver<(DispatcherBarrier, u64)>,
    ) -> Self {
        Self {
            inner: run_counting_merge(actor_id, inner, barrier_rx),
            actor_id,
        }
    }
}

#[try_stream(ok = DispatcherMessage, error = StreamExecutorError)]
async fn run_counting_merge(
    actor_id: ActorId,
    mut inner: BoxedActorInput,
    mut barrier_rx: mpsc::UnboundedReceiver<(DispatcherBarrier, u64)>,
) {
    let mut msg_count: u64 = 0;

    // Wait for the first barrier before forwarding any data messages.
    // The streaming protocol requires the first message to be a barrier.
    // Since data and barriers arrive on separate channels, data may arrive
    // before the first barrier if we don't explicitly wait for it.
    match barrier_rx.recv().await {
        Some((first_barrier, expected_count)) => {
            debug_assert_eq!(
                expected_count, 0,
                "first barrier should always have expected_count=0 because the \
                 Output::CoalescedBarrier starts with epoch_msg_count=0"
            );
            yield DispatcherMessage::Barrier(first_barrier);
        }
        None => {
            return Err(ExchangeChannelClosed::remote_input(actor_id, None).into());
        }
    }

    // State machine: when a barrier arrives with expected_count > msg_count,
    // we set `pending_barrier` and drain data from `inner` until the count matches.
    let mut pending_barrier: Option<(DispatcherBarrier, u64)> = None;

    loop {
        // Drain pending barrier: wait for remaining data messages from `inner`.
        if let Some((barrier, expected_count)) = pending_barrier.take() {
            while msg_count < expected_count {
                match inner.next().await {
                    Some(Ok(msg)) => {
                        msg_count += 1;
                        yield msg;
                    }
                    Some(Err(e)) => return Err(e),
                    None => {
                        return Err(ExchangeChannelClosed::remote_input(actor_id, None).into());
                    }
                }
            }
            yield DispatcherMessage::Barrier(barrier);
            // Carry over excess: data from the next epoch may have arrived before
            // this barrier (data and barriers travel on separate gRPC streams).
            msg_count -= expected_count;
        }

        // No pending barrier. Try non-blocking barrier receive first (biased toward barriers).
        match barrier_rx.try_recv() {
            Ok((barrier, expected_count)) => {
                if msg_count >= expected_count {
                    yield DispatcherMessage::Barrier(barrier);
                    msg_count -= expected_count;
                } else {
                    pending_barrier = Some((barrier, expected_count));
                }
                continue;
            }
            Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                return Err(ExchangeChannelClosed::remote_input(actor_id, None).into());
            }
            Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {}
        }

        // Race barrier channel vs data channel.
        // (tokio::select! is not usable inside #[try_stream], so we use future::select.)
        use futures::future::{self, Either as FutEither};
        let barrier_fut = std::pin::pin!(barrier_rx.recv());
        let data_fut = std::pin::pin!(inner.next());

        match future::select(barrier_fut, data_fut).await {
            FutEither::Left((Some((barrier, expected_count)), _)) => {
                if msg_count >= expected_count {
                    yield DispatcherMessage::Barrier(barrier);
                    msg_count -= expected_count;
                } else {
                    pending_barrier = Some((barrier, expected_count));
                }
            }
            FutEither::Left((None, _)) => {
                return Err(ExchangeChannelClosed::remote_input(actor_id, None).into());
            }
            FutEither::Right((Some(Ok(msg)), _)) => {
                msg_count += 1;
                yield msg;
            }
            FutEither::Right((Some(Err(e)), _)) => return Err(e),
            FutEither::Right((None, _)) => {
                return Err(ExchangeChannelClosed::remote_input(actor_id, None).into());
            }
        }
    }
}

impl Stream for CountingMergeInput {
    type Item = DispatcherMessageStreamItem;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.project().inner.poll_next(cx)
    }
}

impl Input for CountingMergeInput {
    type InputId = ActorId;

    fn id(&self) -> Self::InputId {
        self.actor_id
    }
}

/// Distribute a barrier to per-actor channels, removing actors whose channels have
/// been closed. Returns `true` if all actors have been removed (caller should terminate).
fn distribute_barrier_to_actors(
    barrier: &DispatcherBarrier,
    actor_data_counts: &HashMap<u32, u64>,
    actor_barrier_txs: &mut HashMap<ActorId, mpsc::UnboundedSender<(DispatcherBarrier, u64)>>,
) -> bool {
    let mut closed_actors = Vec::new();
    for (&actor_id_raw, &expected_count) in actor_data_counts {
        let actor_id: ActorId = actor_id_raw.into();
        if let Some(tx) = actor_barrier_txs.get(&actor_id)
            && tx.send((barrier.clone(), expected_count)).is_err()
        {
            closed_actors.push(actor_id);
        }
    }
    for id in &closed_actors {
        tracing::info!(
            actor_id = %id,
            "removing closed actor from barrier receiver"
        );
        actor_barrier_txs.remove(id);
    }
    actor_barrier_txs.is_empty()
}

/// Background task that reads coalesced barriers from a barrier-only gRPC stream
/// and distributes them to per-actor `CountingMergeInput` channels.
///
/// For each coalesced barrier message:
/// 1. Returns permits to the upstream (same batching logic as `RemoteInput`).
/// 2. Decodes the `BarrierBatch` and extracts `actor_data_counts`.
/// 3. For each `(actor_id, expected_count)`, sends `(barrier, expected_count)` to
///    that actor's barrier channel.
///
/// Each coalesced message contains exactly one barrier (the coalescer now sends
/// individual barriers, not batches) plus per-actor data counts.
pub async fn run_barrier_receiver(
    stream: tonic::Streaming<GetStreamResponse>,
    permits_tx: mpsc::UnboundedSender<permits::Value>,
    mut actor_barrier_txs: HashMap<ActorId, mpsc::UnboundedSender<(DispatcherBarrier, u64)>>,
    up_down_frag: UpDownFragmentIds,
    metrics: Arc<StreamingMetrics>,
    batched_permits_limit: usize,
) -> StreamResult<()> {
    let up_fragment_id = up_down_frag.0.to_string();
    let down_fragment_id = up_down_frag.1.to_string();
    let exchange_frag_recv_size_metrics = metrics
        .exchange_frag_recv_size
        .with_guarded_label_values(&[&up_fragment_id, &down_fragment_id]);

    let _span = await_tree::span!("BarrierReceiver (frag {up_fragment_id} -> {down_fragment_id})")
        .verbose();

    let mut batched_permits_accumulated: u32 = 0;

    pin_mut!(stream);
    while let Some(data_res) = stream.next().await {
        match data_res {
            Ok(GetStreamResponse { message, permits }) => {
                let msg = message.unwrap();
                let bytes = DispatcherMessageBatch::get_encoded_len(&msg);
                exchange_frag_recv_size_metrics.inc_by(bytes as u64);

                // Return permits.
                if let Some(add_back_permits) = match permits.unwrap().value {
                    Some(permits::Value::Record(p)) => {
                        batched_permits_accumulated += p;
                        if batched_permits_accumulated >= batched_permits_limit as u32 {
                            let permits = std::mem::take(&mut batched_permits_accumulated);
                            Some(permits::Value::Record(permits))
                        } else {
                            None
                        }
                    }
                    Some(permits::Value::Barrier(p)) => Some(permits::Value::Barrier(p)),
                    None => None,
                } {
                    permits_tx
                        .send(add_back_permits)
                        .context("BarrierReceiver backward permits channel closed.")?;
                }

                // Decode the message. Should always be a BarrierBatch with exactly one barrier.
                use risingwave_pb::stream_plan::stream_message_batch::StreamMessageBatch;
                match msg.stream_message_batch {
                    Some(StreamMessageBatch::BarrierBatch(bb)) => {
                        assert_eq!(
                            bb.barriers.len(),
                            1,
                            "coalesced barrier batch should contain exactly one barrier"
                        );
                        let barrier =
                            DispatcherBarrier::from_protobuf_inner(&bb.barriers[0], |mutation| {
                                if mutation.is_some() {
                                    if cfg!(debug_assertions) {
                                        panic!(
                                            "should not receive message of barrier with mutation"
                                        );
                                    } else {
                                        tracing::warn!(
                                            barrier = ?bb.barriers[0],
                                            "receive message of barrier with mutation"
                                        );
                                    }
                                }
                                Ok(())
                            })?;

                        // Distribute to each actor's `CountingMergeInput`,
                        // removing actors whose channels have been closed.
                        if distribute_barrier_to_actors(
                            &barrier,
                            &bb.actor_data_counts,
                            &mut actor_barrier_txs,
                        ) {
                            tracing::info!(
                                "all actor barrier channels closed, terminating barrier receiver"
                            );
                            return Ok(());
                        }
                    }
                    other => {
                        tracing::warn!(
                            ?other,
                            "unexpected non-barrier message on barrier-only stream"
                        );
                    }
                }
            }
            Err(e) => {
                return Err(ExchangeChannelClosed::remote_input(0u32.into(), Some(e)).into());
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::StreamChunk;

    use super::*;
    use crate::executor::DispatcherBarrier as Barrier;

    fn aid(id: u32) -> ActorId {
        ActorId::from(id)
    }

    fn test_barrier(epoch: u64) -> Barrier {
        Barrier::with_prev_epoch_for_test(epoch * 10 + 1, epoch * 10)
    }

    #[test]
    fn test_barrier_coalescer_basic() {
        let mut coalescer = BarrierCoalescer::new([aid(1), aid(2), aid(3)]);

        // Actor 1 submits
        let result = coalescer.collect(aid(1), test_barrier(1), 5);
        assert!(result.is_none());

        // Actor 2 submits
        let result = coalescer.collect(aid(2), test_barrier(1), 3);
        assert!(result.is_none());

        // Actor 3 submits — should trigger coalescence
        let result = coalescer.collect(aid(3), test_barrier(1), 0);
        let (barrier, actor_ids, counts) = result.unwrap();

        assert_eq!(barrier.epoch.curr, 11); // test_barrier(1) has curr = 1*10+1 = 11
        assert_eq!(actor_ids.len(), 3);
        assert_eq!(counts[&aid(1)], 5);
        assert_eq!(counts[&aid(2)], 3);
        assert_eq!(counts[&aid(3)], 0);
    }

    #[test]
    fn test_barrier_coalescer_queued() {
        let mut coalescer = BarrierCoalescer::new([aid(1), aid(2)]);

        // Actor 1 submits epoch 1 and epoch 2
        assert!(coalescer.collect(aid(1), test_barrier(1), 10).is_none());
        assert!(coalescer.collect(aid(1), test_barrier(2), 20).is_none());

        // Actor 2 submits epoch 1 — epoch 1 should be ready
        let result = coalescer.collect(aid(2), test_barrier(1), 15);
        let (_barrier, _actor_ids, counts) = result.unwrap();
        assert_eq!(counts[&aid(1)], 10);
        assert_eq!(counts[&aid(2)], 15);

        // Drain remaining: epoch 2 is not ready yet (actor 2 hasn't submitted)
        assert!(coalescer.drain_all().is_empty());

        // Actor 2 submits epoch 2
        let result = coalescer.collect(aid(2), test_barrier(2), 25);
        let (_barrier, _, counts) = result.unwrap();
        assert_eq!(counts[&aid(1)], 20);
        assert_eq!(counts[&aid(2)], 25);
    }

    #[test]
    fn test_barrier_coalescer_add_remove_actor() {
        let mut coalescer = BarrierCoalescer::new([aid(1), aid(2)]);
        assert_eq!(coalescer.num_actors(), 2);

        coalescer.add_actor(aid(3));
        assert_eq!(coalescer.num_actors(), 3);

        coalescer.remove_actor(aid(2));
        assert_eq!(coalescer.num_actors(), 2);

        // Now only actors 1 and 3
        assert!(coalescer.collect(aid(1), test_barrier(1), 5).is_none());
        let result = coalescer.collect(aid(3), test_barrier(1), 7);
        assert!(result.is_some());
    }

    /// Tests that barriers submitted individually (as if split from different batch sizes)
    /// maintain correct FIFO alignment. This is the scenario that previously caused deadlocks:
    /// Actor A1 batches [b1, b2] as one entry, Actor A2 sends b1 and b2 separately.
    /// With the fix (splitting at the sender), both submit individual barriers, keeping
    /// the queue aligned.
    #[test]
    fn test_barrier_coalescer_split_batches_alignment() {
        let mut coalescer = BarrierCoalescer::new([aid(1), aid(2)]);

        // Simulate: Actor A1 had barriers [b1, b2] batched together.
        // After the split fix, A1 submits: b1 with count=5, then b2 with count=0.
        assert!(coalescer.collect(aid(1), test_barrier(1), 5).is_none());
        assert!(coalescer.collect(aid(1), test_barrier(2), 0).is_none());

        // Actor A2 had barriers [b1] and [b2] sent separately.
        // A2 submits: b1 with count=3.
        let result = coalescer.collect(aid(2), test_barrier(1), 3);
        let (_barrier, _actor_ids, counts) = result.unwrap();
        // Epoch 1 should pair correctly: A1 count=5, A2 count=3
        assert_eq!(counts[&aid(1)], 5);
        assert_eq!(counts[&aid(2)], 3);

        // A2 submits: b2 with count=7.
        let result = coalescer.collect(aid(2), test_barrier(2), 7);
        let (_barrier, _actor_ids, counts) = result.unwrap();
        // Epoch 2 should pair correctly: A1 count=0 (no data between b1 and b2), A2 count=7
        assert_eq!(counts[&aid(1)], 0);
        assert_eq!(counts[&aid(2)], 7);
    }

    /// Helper to create a `CountingMergeInput` with a channel-backed inner stream.
    /// Returns `(pinned_counting, data_tx, barrier_tx)`.
    fn make_counting_input_raw(
        actor_id: ActorId,
    ) -> (
        Pin<Box<CountingMergeInput>>,
        mpsc::UnboundedSender<DispatcherMessage>,
        mpsc::UnboundedSender<(DispatcherBarrier, u64)>,
    ) {
        let (data_tx, data_rx) = mpsc::unbounded_channel();
        let (barrier_tx, barrier_rx) = mpsc::unbounded_channel();

        let inner_stream = tokio_stream::wrappers::UnboundedReceiverStream::new(data_rx)
            .map(|msg: DispatcherMessage| Ok(msg));

        struct SimpleInput {
            inner: Pin<Box<dyn Stream<Item = DispatcherMessageStreamItem> + Send>>,
            id: ActorId,
        }
        impl Stream for SimpleInput {
            type Item = DispatcherMessageStreamItem;

            fn poll_next(
                mut self: Pin<&mut Self>,
                cx: &mut Context<'_>,
            ) -> Poll<Option<Self::Item>> {
                self.inner.as_mut().poll_next(cx)
            }
        }
        impl Input for SimpleInput {
            type InputId = ActorId;

            fn id(&self) -> Self::InputId {
                self.id
            }
        }

        let simple_input = SimpleInput {
            inner: Box::pin(inner_stream),
            id: actor_id,
        };

        let counting = Box::pin(CountingMergeInput::new(
            actor_id,
            simple_input.boxed_input(),
            barrier_rx,
        ));
        (counting, data_tx, barrier_tx)
    }

    /// Helper that creates a `CountingMergeInput` and sends+consumes the required
    /// initial barrier (`expected_count=0`). Tests can focus on steady-state behavior.
    async fn make_counting_input(
        actor_id: ActorId,
    ) -> (
        Pin<Box<CountingMergeInput>>,
        mpsc::UnboundedSender<DispatcherMessage>,
        mpsc::UnboundedSender<(DispatcherBarrier, u64)>,
    ) {
        let (mut counting, data_tx, barrier_tx) = make_counting_input_raw(actor_id);

        // Send the required initial barrier with expected_count=0.
        barrier_tx.send((test_barrier(0), 0)).unwrap();

        // Consume the initial barrier.
        let msg = counting.next().await.unwrap().unwrap();
        assert!(
            matches!(msg, DispatcherMessage::Barrier(_)),
            "first message must be the initial barrier"
        );

        (counting, data_tx, barrier_tx)
    }

    #[tokio::test]
    async fn test_counting_merge_first_barrier() {
        // Verify that the first message yielded is always a barrier,
        // even if data messages arrive on the data channel first.
        let (mut counting, data_tx, barrier_tx) = make_counting_input_raw(aid(42));

        // Send data BEFORE the first barrier.
        data_tx
            .send(DispatcherMessage::Chunk(StreamChunk::default()))
            .unwrap();

        // Now send the first barrier with expected_count=0.
        // (The data message sent above is NOT counted toward this barrier's epoch.)
        barrier_tx.send((test_barrier(0), 0)).unwrap();

        // The first message MUST be the barrier, not the data.
        let msg = counting.next().await.unwrap().unwrap();
        assert!(
            matches!(msg, DispatcherMessage::Barrier(_)),
            "first message must be a barrier"
        );
    }

    #[tokio::test]
    async fn test_counting_merge_input_basic() {
        let (mut counting, data_tx, barrier_tx) = make_counting_input(aid(42)).await;

        // Send 3 data messages
        for _ in 0..3 {
            data_tx
                .send(DispatcherMessage::Chunk(StreamChunk::default()))
                .unwrap();
        }

        // Send barrier with expected_count=3
        barrier_tx.send((test_barrier(1), 3)).unwrap();

        // Read 3 data messages
        for _ in 0..3 {
            let msg = counting.next().await.unwrap().unwrap();
            assert!(matches!(msg, DispatcherMessage::Chunk(_)));
        }

        // Read barrier
        let msg = counting.next().await.unwrap().unwrap();
        assert!(matches!(msg, DispatcherMessage::Barrier(_)));
    }

    #[tokio::test]
    async fn test_counting_merge_input_idle_actor() {
        let (mut counting, _data_tx, barrier_tx) = make_counting_input(aid(42)).await;

        // Send barrier with expected_count=0 (idle actor)
        barrier_tx.send((test_barrier(1), 0)).unwrap();

        // Barrier should be delivered immediately
        let msg = counting.next().await.unwrap().unwrap();
        assert!(matches!(msg, DispatcherMessage::Barrier(_)));
    }

    #[tokio::test]
    async fn test_counting_merge_input_barrier_before_data() {
        let (mut counting, data_tx, barrier_tx) = make_counting_input(aid(42)).await;

        // Send barrier first with expected_count=2
        barrier_tx.send((test_barrier(1), 2)).unwrap();

        // Now send 2 data messages (arriving after barrier was received)
        data_tx
            .send(DispatcherMessage::Chunk(StreamChunk::default()))
            .unwrap();
        data_tx
            .send(DispatcherMessage::Chunk(StreamChunk::default()))
            .unwrap();

        // Should get 2 data messages first, then barrier
        let msg = counting.next().await.unwrap().unwrap();
        assert!(matches!(msg, DispatcherMessage::Chunk(_)));
        let msg = counting.next().await.unwrap().unwrap();
        assert!(matches!(msg, DispatcherMessage::Chunk(_)));
        let msg = counting.next().await.unwrap().unwrap();
        assert!(matches!(msg, DispatcherMessage::Barrier(_)));
    }

    #[tokio::test]
    async fn test_counting_merge_input_multiple_epochs() {
        let (mut counting, data_tx, barrier_tx) = make_counting_input(aid(42)).await;

        // Epoch 1: 2 data messages
        data_tx
            .send(DispatcherMessage::Chunk(StreamChunk::default()))
            .unwrap();
        data_tx
            .send(DispatcherMessage::Chunk(StreamChunk::default()))
            .unwrap();
        barrier_tx.send((test_barrier(1), 2)).unwrap();

        // Epoch 2: 1 data message
        data_tx
            .send(DispatcherMessage::Chunk(StreamChunk::default()))
            .unwrap();
        barrier_tx.send((test_barrier(2), 1)).unwrap();

        // Read epoch 1
        assert!(matches!(
            counting.next().await.unwrap().unwrap(),
            DispatcherMessage::Chunk(_)
        ));
        assert!(matches!(
            counting.next().await.unwrap().unwrap(),
            DispatcherMessage::Chunk(_)
        ));
        assert!(matches!(
            counting.next().await.unwrap().unwrap(),
            DispatcherMessage::Barrier(_)
        ));

        // Read epoch 2
        assert!(matches!(
            counting.next().await.unwrap().unwrap(),
            DispatcherMessage::Chunk(_)
        ));
        assert!(matches!(
            counting.next().await.unwrap().unwrap(),
            DispatcherMessage::Barrier(_)
        ));
    }

    /// End-to-end test: verifies that the full pipeline from `create_multiplexed_output()`
    /// through `Output::send()` to the coordinator produces coalesced barriers with the
    /// correct upstream actor IDs and data counts.
    ///
    /// This test also verifies the dual-actor-id fix: `Output::actor_id()` returns the
    /// **downstream** actor ID (for dispatch matching), while the coalescer submission
    /// uses the **upstream** actor ID internally.
    ///
    /// Regression test for: <https://github.com/risingwavelabs/risingwave/pull/24951>
    #[tokio::test]
    async fn test_output_sends_upstream_actor_id_to_coalescer() {
        use super::super::output::Output;

        let upstream_ids = [aid(10), aid(20)];
        let downstream_id = aid(99);

        // Create per-actor data channels.
        let mut data_senders = Vec::new();
        let mut _data_receivers = Vec::new();
        for _ in &upstream_ids {
            let (tx, rx) = permit::channel_for_test();
            data_senders.push(tx);
            _data_receivers.push(rx);
        }

        // Create the multiplexed output setup.
        let (channels, coordinator, mut barrier_rx) =
            create_multiplexed_output(&upstream_ids, data_senders, 4);

        // Spawn the coordinator.
        tokio::spawn(coordinator.run());

        // Build Output handles from the returned channels.
        // In production, this happens in dispatch.rs::resolve_output().
        let mut output_map: HashMap<ActorId, Output> = channels
            .into_iter()
            .map(|chs| {
                let up_id = chs.upstream_actor_id;
                let output = Output::new_coalesced_barrier(
                    downstream_id,
                    chs.upstream_actor_id,
                    chs.data_tx,
                    chs.barrier_tx,
                );
                (up_id, output)
            })
            .collect();

        // Verify that Output::actor_id() returns the DOWNSTREAM actor ID (for dispatch matching).
        assert_eq!(output_map[&aid(10)].actor_id(), downstream_id);
        assert_eq!(output_map[&aid(20)].actor_id(), downstream_id);

        // Send data through actor 10: 2 chunks.
        let out10 = output_map.get_mut(&aid(10)).unwrap();
        out10
            .send(DispatcherMessageBatch::Chunk(StreamChunk::default()))
            .await
            .unwrap();
        out10
            .send(DispatcherMessageBatch::Chunk(StreamChunk::default()))
            .await
            .unwrap();

        // Send data through actor 20: 1 chunk.
        let out20 = output_map.get_mut(&aid(20)).unwrap();
        out20
            .send(DispatcherMessageBatch::Chunk(StreamChunk::default()))
            .await
            .unwrap();

        // Send barrier from both actors.
        output_map
            .get_mut(&aid(10))
            .unwrap()
            .send(DispatcherMessageBatch::BarrierBatch(vec![test_barrier(1)]))
            .await
            .unwrap();
        output_map
            .get_mut(&aid(20))
            .unwrap()
            .send(DispatcherMessageBatch::BarrierBatch(vec![test_barrier(1)]))
            .await
            .unwrap();

        // Read the coalesced barrier from the barrier channel.
        let msg = barrier_rx.recv_raw().await.unwrap();

        // Verify coalesced_actor_ids contains UPSTREAM actor IDs (10, 20), not downstream (99).
        assert!(msg.coalesced_actor_ids.contains(&aid(10)));
        assert!(msg.coalesced_actor_ids.contains(&aid(20)));
        assert!(!msg.coalesced_actor_ids.contains(&downstream_id));

        // Verify actor_data_counts has correct counts keyed by upstream actor IDs.
        assert_eq!(msg.actor_data_counts[&aid(10)], 2);
        assert_eq!(msg.actor_data_counts[&aid(20)], 1);

        // Verify the barrier itself is present.
        assert!(matches!(
            msg.message,
            DispatcherMessageBatch::BarrierBatch(_)
        ));
    }

    /// Test that `CountingMergeInput` returns an **error** (not `None`) when the barrier
    /// channel disconnects unexpectedly. This simulates `run_barrier_receiver` failing
    /// (e.g., gRPC broken pipe).
    ///
    /// Test that disconnecting the barrier channel before ANY barrier is sent results
    /// in an error (not a silent stream close).
    ///
    /// Regression test for: `barrier_rx` disconnect must be an error, not silent stream end.
    #[tokio::test]
    async fn test_counting_merge_input_errors_on_barrier_disconnect() {
        let (mut counting, _data_tx, barrier_tx) = make_counting_input_raw(aid(42));

        // Drop barrier_tx immediately — the first `barrier_rx.recv()` returns None.
        drop(barrier_tx);

        // The CountingMergeInput should return an error during first-barrier wait.
        let result = counting.next().await;
        assert!(
            matches!(result, Some(Err(_))),
            "CountingMergeInput should return error when barrier channel disconnects, \
             got None (silent stream close)"
        );
    }

    /// Test that `CountingMergeInput` returns an error when the barrier channel disconnects
    /// while data is actively flowing. This is a realistic scenario: data arrives normally,
    /// but the barrier receiver task has failed, so no barrier will ever come.
    ///
    /// Without the fix, after yielding buffered data, the next poll returns `None` instead
    /// of `Err`, causing the `MergeExecutor` to see a normal input end.
    #[tokio::test]
    async fn test_counting_merge_input_errors_on_barrier_disconnect_with_data() {
        let (mut counting, data_tx, barrier_tx) = make_counting_input_raw(aid(42));

        // Send the required first barrier so we can get past initialization.
        barrier_tx.send((test_barrier(0), 0)).unwrap();
        let msg = counting.next().await.unwrap().unwrap();
        assert!(matches!(msg, DispatcherMessage::Barrier(_)));

        // Send data while barrier_tx is still alive.
        data_tx
            .send(DispatcherMessage::Chunk(StreamChunk::default()))
            .unwrap();

        // Read the data message (barrier_tx still alive, so try_recv → Empty, not Disconnected).
        let msg = counting.next().await.unwrap().unwrap();
        assert!(matches!(msg, DispatcherMessage::Chunk(_)));

        // NOW drop barrier_tx to simulate run_barrier_receiver failure.
        drop(barrier_tx);

        // Next poll: try_recv → Disconnected. Should be an error.
        let result = counting.next().await;
        assert!(
            matches!(result, Some(Err(_))),
            "CountingMergeInput should return error when barrier channel disconnects mid-stream"
        );
    }

    /// Test the barrier distribution helper: closed actor channels are removed, and
    /// the function returns `true` when all actors are gone.
    #[test]
    fn test_distribute_barrier_removes_closed_actors() {
        let (tx1, mut rx1) = mpsc::unbounded_channel();
        let (tx2, rx2) = mpsc::unbounded_channel();

        let mut txs: HashMap<ActorId, mpsc::UnboundedSender<(Barrier, u64)>> = HashMap::new();
        txs.insert(aid(1), tx1);
        txs.insert(aid(2), tx2);

        // Close actor 2's receiving end.
        drop(rx2);

        let counts: HashMap<u32, u64> = [(1, 5), (2, 3)].into_iter().collect();
        let all_closed = distribute_barrier_to_actors(&test_barrier(1), &counts, &mut txs);

        // Actor 2 should be removed, actor 1 remains.
        assert!(!all_closed);
        assert_eq!(txs.len(), 1);
        assert!(txs.contains_key(&aid(1)));

        // Actor 1 should have received the barrier with correct count.
        let (_, count) = rx1.try_recv().unwrap();
        assert_eq!(count, 5);
    }

    /// Test that barrier distribution returns `true` (all closed) when all actors are gone.
    #[test]
    fn test_distribute_barrier_all_actors_closed() {
        let (tx1, rx1) = mpsc::unbounded_channel();
        let (tx2, rx2) = mpsc::unbounded_channel();

        let mut txs: HashMap<ActorId, mpsc::UnboundedSender<(Barrier, u64)>> = HashMap::new();
        txs.insert(aid(1), tx1);
        txs.insert(aid(2), tx2);

        // Close both receiving ends.
        drop(rx1);
        drop(rx2);

        let counts: HashMap<u32, u64> = [(1, 5), (2, 3)].into_iter().collect();
        let all_closed = distribute_barrier_to_actors(&test_barrier(1), &counts, &mut txs);

        assert!(all_closed);
        assert!(txs.is_empty());
    }

    /// Regression test for the deadlock caused by data arriving before barriers.
    ///
    /// Data and barriers flow through separate gRPC streams. The barrier path goes
    /// through the coalescer (waits for ALL actors), making it slower. So data from
    /// epoch E+1 can arrive at `CountingMergeInput` BEFORE barrier E.
    ///
    /// The bug: when `msg_count >= expected_count`, the code reset `msg_count = 0`,
    /// losing track of excess messages already consumed from the next epoch. The next
    /// barrier's `expected_count` could then never be satisfied → deadlock.
    ///
    /// The fix: `msg_count -= expected_count` (carry over excess).
    #[tokio::test]
    async fn test_counting_merge_data_arrives_before_barrier() {
        let (mut counting, data_tx, barrier_tx) = make_counting_input(aid(42)).await;

        // Send all data for epochs 1 AND 2 before any barrier arrives.
        // Epoch 1: 5 data messages, Epoch 2: 3 data messages.
        for _ in 0..8 {
            data_tx
                .send(DispatcherMessage::Chunk(StreamChunk::default()))
                .unwrap();
        }

        // Now send barrier 1 with expected_count=5.
        barrier_tx.send((test_barrier(1), 5)).unwrap();
        // Then barrier 2 with expected_count=3.
        barrier_tx.send((test_barrier(2), 3)).unwrap();

        // Read all messages. Should get: 5 chunks, barrier 1, 3 chunks, barrier 2.
        // test_barrier(n) produces epoch curr = n*10+1.
        let mut chunks_before_b1 = 0;
        loop {
            let msg = counting.next().await.unwrap().unwrap();
            match msg {
                DispatcherMessage::Chunk(_) => chunks_before_b1 += 1,
                DispatcherMessage::Barrier(b) => {
                    assert_eq!(b.epoch.curr, 11);
                    break;
                }
                _ => panic!("unexpected message"),
            }
        }
        assert_eq!(
            chunks_before_b1, 5,
            "should get exactly 5 chunks before barrier 1"
        );

        let mut chunks_before_b2 = 0;
        loop {
            let msg = counting.next().await.unwrap().unwrap();
            match msg {
                DispatcherMessage::Chunk(_) => chunks_before_b2 += 1,
                DispatcherMessage::Barrier(b) => {
                    assert_eq!(b.epoch.curr, 21);
                    break;
                }
                _ => panic!("unexpected message"),
            }
        }
        assert_eq!(
            chunks_before_b2, 3,
            "should get exactly 3 chunks before barrier 2"
        );
    }

    /// Similar to `test_counting_merge_data_arrives_before_barrier`, but with more
    /// epochs and varying data counts to ensure carry-over works across many epochs.
    #[tokio::test]
    async fn test_counting_merge_carry_over_multiple_epochs() {
        let (mut counting, data_tx, barrier_tx) = make_counting_input(aid(42)).await;

        // Send all data for 4 epochs upfront: 2, 0, 7, 1 messages respectively.
        let counts = [2u64, 0, 7, 1];
        let total: u64 = counts.iter().sum();
        for _ in 0..total {
            data_tx
                .send(DispatcherMessage::Chunk(StreamChunk::default()))
                .unwrap();
        }

        // Send all barriers.
        for (i, &count) in counts.iter().enumerate() {
            barrier_tx
                .send((test_barrier((i + 1) as u64), count))
                .unwrap();
        }

        // Read and verify ordering.
        // test_barrier(n) produces epoch curr = n*10+1.
        for (i, &expected_chunks) in counts.iter().enumerate() {
            let expected_epoch_curr = ((i + 1) as u64) * 10 + 1;
            let mut chunk_count = 0u64;
            loop {
                let msg = counting.next().await.unwrap().unwrap();
                match msg {
                    DispatcherMessage::Chunk(_) => chunk_count += 1,
                    DispatcherMessage::Barrier(b) => {
                        assert_eq!(b.epoch.curr, expected_epoch_curr, "barrier epoch mismatch");
                        break;
                    }
                    _ => panic!("unexpected message"),
                }
            }
            assert_eq!(
                chunk_count,
                expected_chunks,
                "epoch {}: expected {} chunks, got {}",
                i + 1,
                expected_chunks,
                chunk_count
            );
        }
    }

    /// Regression test: if `Output::CoalescedBarrier` is constructed with the wrong
    /// `upstream_actor_id`, the coalescer panics with "unknown actor".
    ///
    /// This simulates a hypothetical bug where the wrong actor ID is used for coalescer
    /// submission. The coalescer is keyed by upstream actor IDs, so submitting with an
    /// unknown ID causes a panic.
    ///
    /// Skipped under madsim: madsim intercepts panics in spawned tasks via `catch_unwind`,
    /// so `JoinHandle::await` does not return `Err(JoinError::Panic)` as real tokio does.
    #[cfg(not(madsim))]
    #[tokio::test]
    async fn test_output_wrong_actor_id_panics_in_coalescer() {
        use super::super::output::Output;

        let upstream_ids = [aid(10), aid(20)];
        let downstream_id = aid(50);
        let wrong_upstream_id = aid(99); // Not in the coalescer's actor set!

        // Create per-actor data channels.
        let mut data_senders = Vec::new();
        for _ in &upstream_ids {
            let (tx, _rx) = permit::channel_for_test();
            data_senders.push(tx);
        }

        // Create the multiplexed output setup (coalescer keyed by upstream_ids [10, 20]).
        let (channels, coordinator, _barrier_rx) =
            create_multiplexed_output(&upstream_ids, data_senders, 4);

        // Spawn the coordinator.
        let handle = tokio::spawn(coordinator.run());

        // Take one channel set and recreate the Output with a wrong upstream_actor_id.
        let chs = channels.into_iter().next().unwrap();

        // Construct Output with wrong upstream_actor_id (99 instead of 10).
        let mut wrong_output = Output::new_coalesced_barrier(
            downstream_id,
            wrong_upstream_id,
            chs.data_tx,
            chs.barrier_tx,
        );

        // Sending a barrier through the wrong output sends (wrong_upstream_id=99, barrier, 0)
        // to the coalescer, which only knows about actors 10 and 20 → panic.
        wrong_output
            .send(DispatcherMessageBatch::BarrierBatch(vec![test_barrier(1)]))
            .await
            .unwrap();

        // The coordinator task should have panicked.
        let result = handle.await;
        assert!(
            result.is_err(),
            "coordinator should have panicked due to unknown actor ID in coalescer"
        );
    }
}

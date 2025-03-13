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

use futures::channel::{mpsc, oneshot};
use futures::stream;
use futures::stream::select_with_strategy;

use crate::executor::prelude::*;
use crate::task::CreateMviewProgressReporter;

/// `ChainExecutor` is an executor that enables synchronization between the existing stream and
/// newly appended executors. Currently, `ChainExecutor` is mainly used to implement MV on MV
/// feature. It pipes new data of existing MVs to newly created MV only all of the old data in the
/// existing MVs are dispatched.
///
/// [`RearrangedChainExecutor`] resolves the latency problem when creating MV with a huge amount of
/// existing data, by rearranging the barrier from the upstream. Check the design doc for details.
pub struct RearrangedChainExecutor {
    snapshot: Executor,

    upstream: Executor,

    progress: CreateMviewProgressReporter,

    actor_id: ActorId,
}

#[derive(Debug)]
enum RearrangedMessage {
    RearrangedBarrier(Barrier),
    PhantomBarrier(Barrier),
    Chunk(StreamChunk),
    // This watermark is just a place holder.
    Watermark,
}

impl RearrangedMessage {
    fn phantom_into(self) -> Option<Message> {
        match self {
            RearrangedMessage::RearrangedBarrier(_) | RearrangedMessage::Watermark => None,
            RearrangedMessage::PhantomBarrier(barrier) => Message::Barrier(barrier).into(),
            RearrangedMessage::Chunk(chunk) => Message::Chunk(chunk).into(),
        }
    }
}

impl RearrangedMessage {
    fn rearranged_from(msg: Message) -> Self {
        match msg {
            Message::Watermark(_) => RearrangedMessage::Watermark,
            Message::Chunk(chunk) => RearrangedMessage::Chunk(chunk),
            Message::Barrier(barrier) => RearrangedMessage::RearrangedBarrier(barrier),
        }
    }

    fn phantom_from(msg: Message) -> Self {
        match msg {
            Message::Watermark(_) => RearrangedMessage::Watermark,
            Message::Chunk(chunk) => RearrangedMessage::Chunk(chunk),
            Message::Barrier(barrier) => RearrangedMessage::PhantomBarrier(barrier),
        }
    }
}

impl RearrangedChainExecutor {
    pub fn new(
        snapshot: Executor,
        upstream: Executor,
        progress: CreateMviewProgressReporter,
    ) -> Self {
        Self {
            snapshot,
            upstream,
            actor_id: progress.actor_id(),
            progress,
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(mut self) {
        let mut upstream = pin!(self.upstream.execute());

        // 1. Poll the upstream to get the first barrier.
        let first_barrier = expect_first_barrier(&mut upstream).await?;
        let create_epoch = first_barrier.epoch;

        // If the barrier is a conf change of creating this mview, init snapshot from its epoch
        // and begin to consume the snapshot.
        // Otherwise, it means we've recovered and the snapshot is already consumed.
        let to_consume_snapshot = first_barrier.is_newly_added(self.actor_id);

        // The first barrier message should be propagated.
        yield Message::Barrier(first_barrier.clone());

        if to_consume_snapshot {
            // If we need to consume the snapshot ...
            // We will spawn a background task to poll the upstream actively, in order to get the
            // barrier as soon as possible and then to rearrange(steal) it.
            // The upstream after transforming the barriers to phantom barriers.
            let (upstream_tx, upstream_rx) = mpsc::unbounded();
            // When we catch-up the progress, notify the task to stop.
            let (stop_rearrange_tx, stop_rearrange_rx) = oneshot::channel();

            // 2. Actually, the `first_msg` is rearranged too. So we need to put a phantom barrier.
            upstream_tx
                .unbounded_send(RearrangedMessage::PhantomBarrier(first_barrier))
                .unwrap();

            let mut processed_rows: u64 = 0;

            {
                // 3. Rearrange stream, will yield the barriers polled from upstream to rearrange.
                let rearranged_barrier = pin!(
                    Self::rearrange_barrier(&mut upstream, upstream_tx, stop_rearrange_rx)
                        .map(|result| result.map(RearrangedMessage::RearrangedBarrier)),
                );

                // 4. Init the snapshot with reading epoch.
                let snapshot = self.snapshot.execute_with_epoch(create_epoch.prev);

                // Chain the `snapshot` and `upstream_rx` to get a unified `rearranged_chunks`
                // stream.
                let rearranged_chunks = snapshot
                    .map(|result| result.map(RearrangedMessage::rearranged_from))
                    .chain(upstream_rx.map(Ok));

                // 5. Merge the rearranged barriers with chunks, with the priority of barrier.
                let mut rearranged =
                    select_with_strategy(rearranged_barrier, rearranged_chunks, |_: &mut ()| {
                        stream::PollNext::Left
                    });

                // Record the epoch of the last rearranged barrier we received.
                let mut last_rearranged_epoch = create_epoch;
                let mut stop_rearrange_tx = Some(stop_rearrange_tx);

                #[for_await]
                for rearranged_msg in &mut rearranged {
                    match rearranged_msg? {
                        // If we received a phantom barrier, update the progress and check whether
                        // we catches up with the progress of upstream MV.
                        //
                        // Note that there's no phantom barrier in the snapshot. So we must have
                        // already consumed the whole snapshot and be on the
                        // upstream now.
                        RearrangedMessage::PhantomBarrier(barrier) => {
                            // Update the progress since we've consumed all chunks before this
                            // phantom.
                            self.progress.update(
                                last_rearranged_epoch,
                                barrier.epoch.curr,
                                processed_rows,
                            );

                            if barrier.epoch.curr >= last_rearranged_epoch.curr {
                                // Stop the background rearrangement task.
                                stop_rearrange_tx.take().unwrap().send(()).map_err(|_| {
                                    StreamExecutorError::channel_closed("stop rearrange")
                                })?;
                                break;
                            }
                        }

                        // If we received a message, yield it.
                        RearrangedMessage::RearrangedBarrier(barrier) => {
                            last_rearranged_epoch = barrier.epoch;
                            yield Message::Barrier(barrier);
                        }
                        RearrangedMessage::Chunk(chunk) => {
                            processed_rows += chunk.cardinality() as u64;
                            yield Message::Chunk(chunk)
                        }
                        RearrangedMessage::Watermark => {
                            // Ignore watermark during snapshot consumption.
                        }
                    }
                }

                // 7. Rearranged task finished.
                // The reason for finish must be that we told it to stop.
                tracing::trace!("rearranged task finished");
                if stop_rearrange_tx.is_some() {
                    tracing::error!("rearrangement finished passively");
                }

                // 8. Consume remainings.
                // Note that there may still be some messages in `rearranged`. However the
                // rearranged barriers must be ignored, we should take the phantoms.
                #[for_await]
                for msg in rearranged {
                    let msg: RearrangedMessage = msg?;
                    let Some(msg) = msg.phantom_into() else {
                        continue;
                    };
                    if let Some(barrier) = msg.as_barrier() {
                        self.progress.finish(barrier.epoch, processed_rows);
                    }
                    yield msg;
                }
            }

            // Consume remaining upstream.
            tracing::trace!("begin to consume remaining upstream");

            #[for_await]
            for msg in upstream {
                let msg: Message = msg?;
                if let Some(barrier) = msg.as_barrier() {
                    self.progress.finish(barrier.epoch, processed_rows);
                }
                yield msg;
            }
        } else {
            // If there's no need to consume the snapshot ...
            // We directly forward the messages from the upstream.

            #[for_await]
            for msg in upstream {
                yield msg?;
            }
        }
    }

    /// Rearrangement stream. The `upstream: U` will be taken out from the mutex, then put back
    /// after stopped.
    ///
    /// Check `execute_inner` for more details.
    #[try_stream(ok = Barrier, error = StreamExecutorError)]
    async fn rearrange_barrier<U>(
        upstream: &mut U,
        upstream_tx: mpsc::UnboundedSender<RearrangedMessage>,
        mut stop_rearrange_rx: oneshot::Receiver<()>,
    ) where
        U: MessageStream + std::marker::Unpin,
    {
        loop {
            use futures::future::{Either, select};

            // Stop when `stop_rearrange_rx` is received.
            match select(&mut stop_rearrange_rx, upstream.next()).await {
                Either::Left((Ok(_), _)) => break,
                Either::Left((Err(_e), _)) => {
                    return Err(StreamExecutorError::channel_closed("stop rearrange"));
                }

                Either::Right((Some(msg), _)) => {
                    let msg = msg?;

                    // If we polled a barrier, rearrange it by yielding and leave a phantom barrier
                    // with `RearrangedMessage::phantom_from` in-place.
                    // If we polled a chunk, simply put it to the `upstream_tx`.
                    if let Some(barrier) = msg.as_barrier().cloned() {
                        yield barrier;
                    }
                    upstream_tx
                        .unbounded_send(RearrangedMessage::phantom_from(msg))
                        .map_err(|_| StreamExecutorError::channel_closed("rearranged upstream"))?;
                }
                Either::Right((None, _)) => {
                    Err(StreamExecutorError::channel_closed("upstream"))?;
                }
            }
        }
    }
}

impl Execute for RearrangedChainExecutor {
    fn execute(self: Box<Self>) -> super::BoxedMessageStream {
        self.execute_inner().boxed()
    }
}

// TODO: add new unit tests for rearranged chain

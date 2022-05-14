// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use either::Either;
use futures::channel::{mpsc, oneshot};
use futures::stream::select_with_strategy;
use futures::{stream, FutureExt, StreamExt};
use futures_async_stream::{for_await, try_stream};
use risingwave_common::array::StreamChunk;
use risingwave_common::catalog::Schema;

use super::error::{StreamExecutorError, StreamExecutorResult};
use super::{Barrier, BoxedExecutor, Executor, ExecutorInfo, Message, MessageStream};
use crate::task::{ActorId, FinishCreateMviewNotifier};

/// `ChainExecutor` is an executor that enables synchronization between the existing stream and
/// newly appended executors. Currently, `ChainExecutor` is mainly used to implement MV on MV
/// feature. It pipes new data of existing MVs to newly created MV only all of the old data in the
/// existing MVs are dispatched.
///
/// [`RearrangedChainExecutor`] resolves the latency problem when creating MV with a huge amount of
/// existing data, by rearranging the barrier from the upstream. Check the design doc for details.
pub struct RearrangedChainExecutor {
    snapshot: BoxedExecutor,

    upstream: BoxedExecutor,

    upstream_indices: Arc<[usize]>,

    notifier: FinishCreateMviewNotifier,

    actor_id: ActorId,

    info: ExecutorInfo,
}

fn mapping(upstream_indices: &[usize], msg: Message) -> Message {
    match msg {
        Message::Chunk(chunk) => {
            let (ops, columns, visibility) = chunk.into_inner();
            let mapped_columns = upstream_indices
                .iter()
                .map(|&i| columns[i].clone())
                .collect();
            Message::Chunk(StreamChunk::new(ops, mapped_columns, visibility))
        }
        _ => msg,
    }
}

#[derive(Debug)]
enum RearrangedMessage {
    RearrangedBarrier(Barrier),
    PhantomBarrier(Barrier),
    Chunk(StreamChunk),
}

impl RearrangedMessage {
    fn into_message_ignore_rearranged(self) -> Option<Message> {
        match self {
            RearrangedMessage::RearrangedBarrier(_) => None,
            RearrangedMessage::PhantomBarrier(barrier) => Message::Barrier(barrier).into(),
            RearrangedMessage::Chunk(chunk) => Message::Chunk(chunk).into(),
        }
    }
}

impl From<Message> for RearrangedMessage {
    fn from(msg: Message) -> Self {
        match msg {
            Message::Chunk(chunk) => RearrangedMessage::Chunk(chunk),
            Message::Barrier(barrier) => RearrangedMessage::RearrangedBarrier(barrier),
        }
    }
}

impl RearrangedChainExecutor {
    pub fn new(
        snapshot: BoxedExecutor,
        upstream: BoxedExecutor,
        upstream_indices: Vec<usize>,
        notifier: FinishCreateMviewNotifier,
        schema: Schema,
    ) -> Self {
        Self {
            info: ExecutorInfo {
                schema,
                pk_indices: upstream.pk_indices().to_owned(),
                identity: "RearrangedChain".to_owned(),
            },
            snapshot,
            upstream,
            upstream_indices: upstream_indices.into(),
            actor_id: notifier.actor_id,
            notifier,
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(self) {
        // 0. Project the upstream with `upstream_indices`.
        let upstream_indices = self.upstream_indices.clone();
        let mut upstream = self
            .upstream
            .execute()
            .map(move |result| result.map(|msg| mapping(&upstream_indices, msg)));

        // 1. Poll the upstream to get the first barrier.
        let first_msg = upstream.next().await.unwrap()?;
        let first_barrier = first_msg
            .as_barrier()
            .cloned()
            .expect("the first message received by chain must be a barrier");
        let create_epoch = first_barrier.epoch;

        // If the barrier is a conf change of creating this mview, init snapshot from its epoch
        // and begin to consume the snapshot.
        // Otherwise, it means we've recovered and the snapshot is already consumed.
        let to_consume_snapshot = first_barrier.is_to_add_output(self.actor_id);

        // The first barrier message should be propagated.
        yield first_msg;

        if to_consume_snapshot {
            // If we need to consume the snapshot ...
            // We will spawn a background task to poll the upstream actively, in order to get the
            // barrier as soon as possible and then to rearrange(steal) it.

            // The barriers polled from upstream to rearrange.
            let (rearranged_barrier_tx, rearranged_barrier_rx) = mpsc::unbounded();
            // The upstream after transforming the barriers to phantom barriers.
            let (upstream_tx, upstream_rx) = mpsc::unbounded();
            // When we catch-up the progress, notify the task to stop.
            let (stop_rearrange_tx, stop_rearrange_rx) = oneshot::channel();

            // 2. Actually, the `first_msg` is rearranged too. So we need to put a phantom barrier.
            upstream_tx
                .unbounded_send(RearrangedMessage::PhantomBarrier(first_barrier))
                .unwrap();

            // 3. Spawn the background task.
            let upstream_poll_handle = madsim::task::spawn(Self::rearrange(
                upstream,
                upstream_tx,
                rearranged_barrier_tx,
                stop_rearrange_rx,
            ));

            // 4. Init the snapshot with reading epoch.
            let snapshot = self.snapshot.execute_with_epoch(create_epoch.prev);

            // Chain the `snapshot` and `upstream_rx` to get a unified `rearranged_chunks` stream.
            let rearranged_chunks = snapshot
                .map(|result| result.map(RearrangedMessage::from))
                .chain(upstream_rx.map(Ok));

            // 5. Merge the rearranged barriers with chunks, with the priority of barrier.
            let mut rearranged = select_with_strategy(
                rearranged_barrier_rx.map(Ok),
                rearranged_chunks,
                |_state: &mut ()| stream::PollNext::Left,
            );

            // Record the epoch of the last rearranged barrier we received.
            let mut last_rearranged_epoch = create_epoch;
            let mut stop_rearrange_tx = Some(stop_rearrange_tx);

            // 6. Consume the merged `rearranged` stream.
            #[for_await]
            for rearranged_msg in &mut rearranged {
                match rearranged_msg? {
                    // If we received a phantom barrier, check whether we catches up with the
                    // progress of upstream MV.
                    //
                    // Note that there's no phantom barrier in the snapshot. So we must have already
                    // consumed the whole snapshot and be on the upstream now.
                    RearrangedMessage::PhantomBarrier(barrier) => {
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
                    RearrangedMessage::Chunk(chunk) => yield Message::Chunk(chunk),
                }
            }

            // 7. Rearranged task finished.
            // The reason for finish must be that we told it to stop.
            tracing::debug!(actor = self.actor_id, "rearranged task finished");
            if stop_rearrange_tx.is_some() {
                tracing::error!(actor = self.actor_id, "rearrangement finished passively");
            }

            // Now we take back the remaining upstream.
            // If there's any error in the rearrangement task, it will be thrown.
            let remaining_upstream = upstream_poll_handle.await?;

            // 8. Assemble the remaining messages.
            // Note that there may still be some messages in `rearranged`. However the rearranged
            // barriers must be ignored, we should take the phantoms.
            let remaining_rearranged = Box::pin(rearranged.filter_map(|result| async {
                result
                    .map(RearrangedMessage::into_message_ignore_rearranged)
                    .transpose()
            }));

            // Chain the remainings.
            let remaining = remaining_rearranged.chain(remaining_upstream);

            // 9. Begin to consume remainings.
            tracing::debug!(actor = self.actor_id, "begin to consume remainings");

            let mut notifier = Some(self.notifier);

            #[for_await]
            for msg in remaining {
                let msg: Message = msg?;
                let is_barrier = msg.as_barrier().is_some();
                yield msg;

                if is_barrier && let Some(notifier) = notifier.take()  {
                    // Notify about the finish after the first barrier.
                    notifier.notify(create_epoch.curr);
                }
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

    /// Background rearrangement task. Check `execute_inner` for more details.
    async fn rearrange(
        upstream: impl MessageStream + std::marker::Unpin,
        upstream_tx: mpsc::UnboundedSender<RearrangedMessage>,
        rearranged_barrier_tx: mpsc::UnboundedSender<RearrangedMessage>,
        stop_rearrange_rx: oneshot::Receiver<()>,
    ) -> StreamExecutorResult<impl MessageStream> {
        // Stop when `stop_rearrange_rx` is received.
        // TODO(bugen): clearer way to select from both, since the left side is an oneshot
        let mut selected = select_with_strategy(
            stop_rearrange_rx.into_stream().map(Either::Left),
            upstream.map(Either::Right),
            |_: &mut ()| stream::PollNext::Left,
        );

        #[for_await]
        for either in &mut selected {
            match either {
                // Future `stop_rx` ready.
                Either::Left(Ok(_)) => break,
                Either::Left(Err(_e)) => {
                    return Err(StreamExecutorError::channel_closed("stop rearrange"));
                }

                // Receive a message from the upstream.
                Either::Right(msg) => match msg? {
                    // If we polled a chunk, simply put it to the `upstream_tx`.
                    Message::Chunk(chunk) => {
                        upstream_tx
                            .unbounded_send(RearrangedMessage::Chunk(chunk))
                            .map_err(|_| {
                                StreamExecutorError::channel_closed("rearranged upstream")
                            })?;
                    }

                    // If we polled a barrier, rearrange it to `rearranged_barrier_tx` and leave
                    // a phantom barrier in-place.
                    Message::Barrier(barrier) => {
                        rearranged_barrier_tx
                            .unbounded_send(RearrangedMessage::RearrangedBarrier(barrier.clone()))
                            .map_err(|_| {
                                StreamExecutorError::channel_closed("rearranged barrier")
                            })?;
                        upstream_tx
                            .unbounded_send(RearrangedMessage::PhantomBarrier(barrier))
                            .map_err(|_| {
                                StreamExecutorError::channel_closed("rearranged upstream")
                            })?;
                    }
                },
            }
        }

        // Resume the upstream from `selected`.
        let upstream = selected.map(Either::unwrap_right);
        Ok(upstream)
    }
}

impl Executor for RearrangedChainExecutor {
    fn execute(self: Box<Self>) -> super::BoxedMessageStream {
        self.execute_inner().boxed()
    }

    fn schema(&self) -> &Schema {
        &self.info.schema
    }

    fn pk_indices(&self) -> super::PkIndicesRef {
        &self.info.pk_indices
    }

    fn identity(&self) -> &str {
        &self.info.identity
    }
}

// TODO: add new unit tests for rearranged chain

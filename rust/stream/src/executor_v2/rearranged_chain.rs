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

use futures::channel::{mpsc, oneshot};
use futures::stream::select_with_strategy;
use futures::{stream, StreamExt};
use futures_async_stream::try_stream;
use risingwave_common::array::StreamChunk;
use risingwave_common::catalog::Schema;

use super::error::TracedStreamExecutorError;
use super::{Barrier, BoxedExecutor, Executor, ExecutorInfo, Message, Mutation};
use crate::executor::Epoch;
use crate::task::{ActorId, FinishCreateMviewNotifier};

/// [`ChainExecutor`] is an executor that enables synchronization between the existing
/// stream and newly appended executors. Currently, [`ChainExecutor`] is mainly used to implement MV
/// on MV feature. It pipes new data of existing MVs to newly created MV only all of the old data in
/// the existing MVs are dispatched.
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
            let columns = upstream_indices
                .iter()
                .map(|i| chunk.columns()[*i].clone())
                .collect();
            Message::Chunk(StreamChunk::new(
                chunk.ops().to_vec(),
                columns,
                chunk.visibility().clone(),
            ))
        }
        _ => msg,
    }
}

#[derive(Debug)]
enum RearrangedMessage {
    RearrangedBarrier(Barrier),
    PhantomBarrier(Epoch),
    Chunk(StreamChunk),
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
        actor_id: ActorId,
        info: ExecutorInfo,
    ) -> Self {
        Self {
            snapshot,
            upstream,
            upstream_indices: upstream_indices.into(),
            notifier,
            actor_id,
            info,
        }
    }

    #[try_stream(ok = Message, error = TracedStreamExecutorError)]
    async fn execute_inner(self) {
        // 0. Project the upstream with `upstream_indices`.
        let upstream_indices = self.upstream_indices.clone();
        let mut upstream = self
            .upstream
            .execute()
            .map(move |result| result.map(|msg| mapping(&upstream_indices, msg)));

        // 1. Poll the upstream to get the first barrier.
        //
        let first_msg = upstream.next().await.unwrap()?;
        let barrier = first_msg
            .as_barrier()
            .expect("the first message received by chain must be a barrier");
        let create_epoch = barrier.epoch;

        let to_consume_snapshot = match barrier.mutation.as_ref().cloned().as_deref() {
            // If the barrier is a conf change of creating this mview, init snapshot from its epoch
            // and begin to consume the snapshot.
            Some(Mutation::AddOutput(map)) => map
                .values()
                .flatten()
                .any(|info| info.actor_id == self.actor_id),

            // If the barrier is not a conf change, it means we've recovered and the snapshot is
            // already consumed.
            _ => false,
        };

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
            let (stop_rearrange_tx, mut stop_rearrange_rx) = oneshot::channel();

            // 2. Actually, the `first_msg` is rearranged too. So we need to put a phantom barrier.
            //
            upstream_tx
                .unbounded_send(RearrangedMessage::PhantomBarrier(create_epoch))
                .unwrap();

            // 3. Spawn the background task.
            //
            let upstream_poll_handle = tokio::spawn(async move {
                #[for_await]
                for msg in &mut upstream {
                    let msg = msg?;
                    match msg {
                        // If we polled a chunk, simply put it to the `upstream_tx`.
                        Message::Chunk(chunk) => {
                            info!("polled chunk");

                            upstream_tx
                                .unbounded_send(RearrangedMessage::Chunk(chunk))
                                .unwrap();
                        }

                        // If we polled a barrier, rearrange it to `rearranged_barrier_tx` and leave
                        // a phantom barrier in-place.
                        Message::Barrier(barrier) => {
                            info!("polled barrier, rearrange");

                            upstream_tx
                                .unbounded_send(RearrangedMessage::PhantomBarrier(barrier.epoch))
                                .unwrap();
                            rearranged_barrier_tx
                                .unbounded_send(RearrangedMessage::RearrangedBarrier(barrier))
                                .unwrap();
                        }
                    };

                    // Check that whether we should stop.
                    if stop_rearrange_rx.try_recv().unwrap().is_some() {
                        info!("rearrange stop");
                        break;
                    }
                }

                // Return the remaining upstream back. There's no need to rearrange it any more.
                Ok::<_, TracedStreamExecutorError>(upstream)
            });

            // 4. Init the snapshot with reading epoch.
            //
            let snapshot = self.snapshot.execute_with_epoch(create_epoch.prev);

            // Chain the `snapshot` and `upstream_rx` to get a unified `rearranged_chunks` stream.
            let rearranged_chunks = snapshot
                .map(|result| result.map(RearrangedMessage::from))
                .chain(upstream_rx.map(Ok));

            // 5. Merge the rearranged barriers with chunks, with the priority of barrier.
            //
            let rearranged = select_with_strategy(
                rearranged_barrier_rx.map(Ok),
                rearranged_chunks,
                |_state: &mut ()| stream::PollNext::Left,
            );

            // Record the epoch of the last rearranged barrier we received.
            let mut last_rearranged_epoch = create_epoch;
            // When we stop the rearrangement task, we should also tell the barrier manager that
            // we've finish the creation.
            let mut rearrange_finish_tx = Some((stop_rearrange_tx, self.notifier));

            // 6. Consume the merged `rearranged` stream.
            //
            #[for_await]
            for rearranged_msg in rearranged {
                info!("recv rearranged: {:?}", rearranged_msg);

                match rearranged_msg? {
                    // If we received a phantom barrier, check whether we catches up with the
                    // progress of upstream MV.
                    //
                    // Note that there's no phantom barrier in the snapshot. So we must have already
                    // consumed the whole snapshot and be on the upstream now.
                    RearrangedMessage::PhantomBarrier(epoch) => {
                        if epoch.curr >= last_rearranged_epoch.curr {
                            if let Some((stop_tx, create_notifier)) = rearrange_finish_tx.take() {
                                // Stop the background rearrangement task.
                                stop_tx.send(()).unwrap();
                                // Notify about the finish.
                                create_notifier.notify(create_epoch.curr);

                                info!("notified");
                            }
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

            // 7. Rearranged stream finished. Now we take back the remaining upstream.
            //
            let remaining_upstream = upstream_poll_handle
                .await
                .unwrap()
                .expect("rearranging failed");

            info!("begin to consume remaining upstream");

            // 8. Begin to consume remaining upstream.
            //
            #[for_await]
            for msg in remaining_upstream {
                let msg = msg?;
                yield msg;
            }
        } else {
            // If there's no need to consume the snapshot ...
            // We directly forward the messages from the upstream.

            #[for_await]
            for msg in upstream {
                let msg = msg?;
                yield msg;
            }
        }
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

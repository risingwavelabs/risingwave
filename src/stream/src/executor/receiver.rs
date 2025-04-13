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

use itertools::Itertools;
use tokio::sync::mpsc;
use tokio::time::Instant;

use super::exchange::input::{BarrierReceiver, BoxedInput};
use crate::executor::DispatcherMessage;
use crate::executor::prelude::*;
use crate::task::{FragmentId, LocalBarrierManager};

/// `ReceiverExecutor` is used along with a channel. After creating a mpsc channel,
/// there should be a `ReceiverExecutor` running in the background, so as to push
/// messages down to the executors.
pub struct ReceiverExecutor {
    /// Input from upstream.
    input: BoxedInput,

    /// The context of the actor.
    actor_context: ActorContextRef,

    /// Belonged fragment id.
    fragment_id: FragmentId,

    /// Metrics
    metrics: Arc<StreamingMetrics>,

    barrier_rx: BarrierReceiver,
}

impl std::fmt::Debug for ReceiverExecutor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReceiverExecutor").finish()
    }
}

impl ReceiverExecutor {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        ctx: ActorContextRef,
        fragment_id: FragmentId,
        upstream_fragment_id: FragmentId,
        input: BoxedInput,
        local_barrier_manager: LocalBarrierManager,
        metrics: Arc<StreamingMetrics>,
        barrier_rx: mpsc::UnboundedReceiver<Barrier>,
    ) -> Self {
        let barrier_rx = BarrierReceiver::new(
            barrier_rx,
            ctx.id,
            ctx.fragment_id,
            upstream_fragment_id,
            local_barrier_manager,
            metrics.clone(),
        );
        Self {
            input,
            actor_context: ctx,
            metrics,
            fragment_id,
            barrier_rx,
        }
    }

    #[cfg(test)]
    pub fn for_test(
        actor_id: ActorId,
        input: super::exchange::permit::Receiver,
        local_barrier_manager: crate::task::LocalBarrierManager,
    ) -> Self {
        use super::exchange::input::LocalInput;
        use crate::executor::exchange::input::Input;

        let barrier_rx = local_barrier_manager.subscribe_barrier(actor_id);

        Self::new(
            ActorContext::for_test(actor_id),
            514,
            1919,
            LocalInput::new(input, 0).boxed_input(),
            local_barrier_manager,
            StreamingMetrics::unused().into(),
            barrier_rx,
        )
    }
}

impl Execute for ReceiverExecutor {
    fn execute(mut self: Box<Self>) -> BoxedMessageStream {
        let actor_id = self.actor_context.id;

        let mut metrics = self.metrics.new_actor_input_metrics(
            actor_id,
            self.fragment_id,
            self.barrier_rx.upstream_fragment_id,
        );

        let stream = #[try_stream]
        async move {
            let mut start_time = Instant::now();
            while let Some(msg) = self.barrier_rx.run_future(self.input.next()).await? {
                metrics
                    .actor_input_buffer_blocking_duration_ns
                    .inc_by(start_time.elapsed().as_nanos() as u64);
                let msg = match msg? {
                    DispatcherMessage::Watermark(watermark) => {
                        // Do nothing.
                        Message::Watermark(watermark)
                    }
                    DispatcherMessage::Chunk(chunk) => {
                        metrics.actor_in_record_cnt.inc_by(chunk.cardinality() as _);
                        Message::Chunk(chunk)
                    }
                    DispatcherMessage::Barrier(barrier) => {
                        tracing::debug!(
                            target: "events::stream::barrier::path",
                            actor_id = actor_id,
                            "receiver receives barrier from path: {:?}",
                            barrier.passed_actors
                        );
                        let (barrier, new_inputs) =
                            self.barrier_rx.process_dispatcher_barrier(barrier).await?;

                        if let Some(update) = barrier.as_update_merge(
                            self.actor_context.id,
                            self.barrier_rx.upstream_fragment_id,
                        ) {
                            let new_inputs = new_inputs.expect("should be Some on update_merge");
                            let new_upstream_fragment_id = update
                                .new_upstream_fragment_id
                                .unwrap_or(self.barrier_rx.upstream_fragment_id);
                            let removed_upstream_actor_id: Vec<_> =
                                if update.new_upstream_fragment_id.is_some() {
                                    vec![self.input.actor_id()]
                                } else {
                                    update.removed_upstream_actor_id.clone()
                                };

                            assert_eq!(
                                removed_upstream_actor_id,
                                vec![self.input.actor_id()],
                                "the removed upstream actor should be the same as the current input"
                            );
                            let upstream_actor = update
                                .added_upstream_actors
                                .iter()
                                .exactly_one()
                                .expect("receiver should have exactly one upstream");

                            // Create new upstream receiver.
                            let (upstream_actor_id, new_upstream) = new_inputs
                                .into_iter()
                                .exactly_one()
                                .expect("receiver should have exactly one upstream");

                            assert_eq!(upstream_actor_id, upstream_actor.actor_id);

                            // Replace the input.
                            self.input = new_upstream;

                            self.barrier_rx.upstream_fragment_id = new_upstream_fragment_id;
                            metrics = self.metrics.new_actor_input_metrics(
                                actor_id,
                                self.fragment_id,
                                new_upstream_fragment_id,
                            );
                        }
                        Message::Barrier(barrier)
                    }
                };

                yield msg;
                start_time = Instant::now();
            }
        };

        stream.boxed()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use futures::{FutureExt, pin_mut};
    use risingwave_common::util::epoch::test_epoch;
    use risingwave_pb::stream_plan::update_mutation::MergeUpdate;

    use super::*;
    use crate::executor::exchange::input::new_input;
    use crate::executor::{MessageInner as Message, UpdateMutation};
    use crate::task::barrier_test_utils::LocalBarrierTestEnv;
    use crate::task::test_utils::helper_make_local_actor;

    #[tokio::test]
    async fn test_configuration_change() {
        let actor_id = 233;
        let (old, new) = (114, 514); // old and new upstream actor id

        let barrier_test_env = LocalBarrierTestEnv::for_test().await;

        let ctx = barrier_test_env.shared_context.clone();
        let metrics = Arc::new(StreamingMetrics::unused());

        // 1. Register info in context.

        // old -> actor_id
        // new -> actor_id

        let (upstream_fragment_id, fragment_id) = (10, 18);

        // 4. Send a configuration change barrier.
        let merge_updates = maplit::hashmap! {
            (actor_id, upstream_fragment_id) => MergeUpdate {
                actor_id,
                upstream_fragment_id,
                new_upstream_fragment_id: None,
                added_upstream_actors: vec![helper_make_local_actor(new)],
                removed_upstream_actor_id: vec![old],
            }
        };

        let b1 = Barrier::new_test_barrier(test_epoch(1)).with_mutation(Mutation::Update(
            UpdateMutation {
                dispatchers: Default::default(),
                merges: merge_updates,
                vnode_bitmaps: Default::default(),
                dropped_actors: Default::default(),
                actor_splits: Default::default(),
                actor_new_dispatchers: Default::default(),
            },
        ));

        barrier_test_env.inject_barrier(&b1, [actor_id]);
        barrier_test_env.flush_all_events().await;

        let input = new_input(
            &barrier_test_env.local_barrier_manager,
            metrics.clone(),
            actor_id,
            fragment_id,
            &helper_make_local_actor(old),
            upstream_fragment_id,
        )
        .unwrap();

        let receiver = ReceiverExecutor::new(
            ActorContext::for_test(actor_id),
            fragment_id,
            upstream_fragment_id,
            input,
            barrier_test_env.local_barrier_manager.clone(),
            metrics.clone(),
            barrier_test_env
                .local_barrier_manager
                .subscribe_barrier(actor_id),
        )
        .boxed()
        .execute();

        pin_mut!(receiver);

        // 2. Take downstream receivers.
        let txs = [old, new]
            .into_iter()
            .map(|id| (id, ctx.take_sender(&(id, actor_id)).unwrap()))
            .collect::<HashMap<_, _>>();
        macro_rules! send {
            ($actors:expr, $msg:expr) => {
                for actor in $actors {
                    txs.get(&actor).unwrap().send($msg).await.unwrap();
                }
            };
        }
        macro_rules! send_error {
            ($actors:expr, $msg:expr) => {
                for actor in $actors {
                    txs.get(&actor).unwrap().send($msg).await.unwrap_err();
                }
            };
        }
        macro_rules! assert_recv_pending {
            () => {
                assert!(
                    receiver
                        .next()
                        .now_or_never()
                        .flatten()
                        .transpose()
                        .unwrap()
                        .is_none()
                );
            };
        }

        macro_rules! recv {
            () => {
                receiver.next().await.transpose().unwrap()
            };
        }

        // 3. Send a chunk.
        send!([old], Message::Chunk(StreamChunk::default()).into());
        recv!().unwrap().as_chunk().unwrap(); // We should be able to receive the chunk.
        assert_recv_pending!();

        send!([new], Message::Barrier(b1.clone().into_dispatcher()).into());
        assert_recv_pending!(); // We should not receive the barrier, as new is not the upstream.

        send!([old], Message::Barrier(b1.clone().into_dispatcher()).into());
        recv!().unwrap().as_barrier().unwrap(); // We should now receive the barrier.

        // 5. Send a chunk to the removed upstream.
        send_error!([old], Message::Chunk(StreamChunk::default()).into());
        assert_recv_pending!();

        // 6. Send a chunk to the added upstream.
        send!([new], Message::Chunk(StreamChunk::default()).into());
        recv!().unwrap().as_chunk().unwrap(); // We should be able to receive the chunk.
        assert_recv_pending!();
    }
}

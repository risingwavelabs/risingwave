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

use crate::executor::merge::{MergeExecutorInner, SingletonUpstream};

/// `ReceiverExecutor` receives data from a single upstream actor. It's a special case of
/// `MergeExecutor` with only one upstream.
pub type ReceiverExecutor = MergeExecutorInner<SingletonUpstream>;

impl ReceiverExecutor {
    #[cfg(test)]
    pub fn for_test(
        actor_id: impl Into<risingwave_pb::id::ActorId>,
        input: super::exchange::permit::Receiver,
        local_barrier_manager: crate::task::LocalBarrierManager,
    ) -> Self {
        use super::exchange::input::LocalInput;
        use crate::executor::ActorContext;
        use crate::executor::exchange::input::ActorInput;
        use crate::executor::prelude::StreamingMetrics;

        let actor_id = actor_id.into();

        let barrier_rx = local_barrier_manager.subscribe_barrier(actor_id);

        Self::new(
            ActorContext::for_test(actor_id),
            514.into(),
            1919.into(),
            LocalInput::new(input, 0.into()).boxed_input(),
            local_barrier_manager,
            StreamingMetrics::unused().into(),
            barrier_rx,
        )
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use futures::{FutureExt, StreamExt, pin_mut};
    use risingwave_common::array::StreamChunk;
    use risingwave_common::util::epoch::test_epoch;
    use risingwave_pb::stream_plan::update_mutation::MergeUpdate;

    use super::*;
    use crate::executor::exchange::input::new_input;
    use crate::executor::prelude::StreamingMetrics;
    use crate::executor::{
        ActorContext, Barrier, Execute as _, MessageInner as Message, Mutation, UpdateMutation,
    };
    use crate::task::NewOutputRequest;
    use crate::task::barrier_test_utils::LocalBarrierTestEnv;
    use crate::task::test_utils::helper_make_local_actor;

    #[tokio::test]
    async fn test_configuration_change() {
        let actor_id = 233.into();
        let (old, new) = (114.into(), 514.into()); // old and new upstream actor id

        let barrier_test_env = LocalBarrierTestEnv::for_test().await;

        let metrics = Arc::new(StreamingMetrics::unused());

        // 1. Register info in context.

        // old -> actor_id
        // new -> actor_id

        let (upstream_fragment_id, fragment_id) = (10.into(), 18.into());

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
                actor_cdc_table_snapshot_splits: Default::default(),
                sink_schema_change: Default::default(),
            },
        ));

        barrier_test_env.inject_barrier(&b1, [actor_id]);
        barrier_test_env.flush_all_events().await;

        let actor_ctx = ActorContext::for_test(actor_id);

        let input = new_input(
            &barrier_test_env.local_barrier_manager,
            metrics.clone(),
            actor_id,
            fragment_id,
            &helper_make_local_actor(old),
            upstream_fragment_id,
            actor_ctx.config.clone(),
        )
        .await
        .unwrap();

        let receiver = ReceiverExecutor::new(
            actor_ctx.clone(),
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

        let mut txs = HashMap::new();
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

        macro_rules! collect_upstream_tx {
            ($actors:expr) => {
                for upstream_id in $actors {
                    let mut output_requests = barrier_test_env
                        .take_pending_new_output_requests(upstream_id.into())
                        .await;
                    assert_eq!(output_requests.len(), 1);
                    let (downstream_actor_id, request) = output_requests.pop().unwrap();
                    assert_eq!(downstream_actor_id, actor_id);
                    let NewOutputRequest::Local(tx) = request else {
                        unreachable!()
                    };
                    txs.insert(upstream_id, tx);
                }
            };
        }

        assert_recv_pending!();
        barrier_test_env.flush_all_events().await;

        // 2. Take downstream receivers.
        collect_upstream_tx!([old]);

        // 3. Send a chunk.
        send!([old], Message::Chunk(StreamChunk::default()).into());
        recv!().unwrap().as_chunk().unwrap(); // We should be able to receive the chunk.
        assert_recv_pending!();

        send!([old], Message::Barrier(b1.clone().into_dispatcher()).into());
        assert_recv_pending!(); // We should not receive the barrier, as new is not the upstream.

        collect_upstream_tx!([new]);

        send!([new], Message::Barrier(b1.clone().into_dispatcher()).into());
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

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

use std::future::{Future, poll_fn};
use std::iter::once;
use std::pin::pin;
use std::task::Poll;

use futures::FutureExt;
use futures::future::join_all;
use risingwave_common::util::epoch::{EpochExt, test_epoch};

use super::*;
use crate::task::barrier_test_utils::LocalBarrierTestEnv;

#[tokio::test]
async fn test_managed_barrier_collection() -> StreamResult<()> {
    let mut test_env = LocalBarrierTestEnv::for_test().await;

    let manager = &test_env.local_barrier_manager;

    let register_sender = |actor_id: u32| {
        let barrier_rx = test_env.local_barrier_manager.subscribe_barrier(actor_id);
        (actor_id, barrier_rx)
    };

    // Register actors
    let actor_ids = vec![233, 234, 235];

    // Send a barrier to all actors
    let curr_epoch = test_epoch(2);
    let barrier = Barrier::new_test_barrier(curr_epoch);
    let epoch = barrier.epoch.prev;

    test_env.inject_barrier(&barrier, actor_ids.clone());

    test_env.flush_all_events().await;

    let count = actor_ids.len();
    let mut rxs = actor_ids
        .clone()
        .into_iter()
        .map(register_sender)
        .collect_vec();

    // Collect barriers from actors
    let collected_barriers = join_all(rxs.iter_mut().map(|(actor_id, rx)| async move {
        let barrier = rx.recv().await.unwrap();
        assert_eq!(barrier.epoch.prev, epoch);
        (*actor_id, barrier)
    }))
    .await;

    let mut await_epoch_future = pin!(test_env.response_rx.recv().map(|result| {
        let resp: StreamingControlStreamResponse = result.unwrap().unwrap();
        let resp = resp.response.unwrap();
        match resp {
            streaming_control_stream_response::Response::CompleteBarrier(_complete_barrier) => {}
            _ => unreachable!(),
        }
    }));

    // Report to local barrier manager
    for (i, (actor_id, barrier)) in collected_barriers.into_iter().enumerate() {
        manager.collect(actor_id, &barrier);
        LocalBarrierTestEnv::flush_all_events_impl(&test_env.actor_op_tx).await;
        let notified =
            poll_fn(|cx| Poll::Ready(await_epoch_future.as_mut().poll(cx).is_ready())).await;
        assert_eq!(notified, i == count - 1);
    }

    Ok(())
}

#[tokio::test]
async fn test_managed_barrier_collection_separately() -> StreamResult<()> {
    let mut test_env = LocalBarrierTestEnv::for_test().await;

    let manager = &test_env.local_barrier_manager;

    let register_sender = |actor_id: u32| {
        let barrier_rx = test_env.local_barrier_manager.subscribe_barrier(actor_id);
        (actor_id, barrier_rx)
    };

    let actor_ids_to_send = vec![233, 234, 235];
    let extra_actor_id = 666;
    let actor_ids_to_collect = actor_ids_to_send
        .iter()
        .cloned()
        .chain(once(extra_actor_id))
        .collect_vec();

    // Prepare the barrier
    let curr_epoch = test_epoch(2);
    let barrier = Barrier::new_test_barrier(curr_epoch).with_stop();

    test_env.inject_barrier(&barrier, actor_ids_to_collect.clone());

    test_env.flush_all_events().await;

    // Register actors
    let count = actor_ids_to_send.len();
    let mut rxs = actor_ids_to_send
        .clone()
        .into_iter()
        .map(register_sender)
        .collect_vec();

    let mut barrier_subscriber = manager.subscribe_barrier(extra_actor_id);

    // Read the mutation after receiving the barrier from remote input.
    let mut mutation_reader = pin!(barrier_subscriber.recv());
    assert!(poll_fn(|cx| Poll::Ready(mutation_reader.as_mut().poll(cx).is_pending())).await);

    let recv_barrier = mutation_reader.await.unwrap();
    assert_eq!(
        (recv_barrier.epoch, &recv_barrier.mutation),
        (barrier.epoch, &barrier.mutation)
    );

    // Collect a barrier before sending
    manager.collect(extra_actor_id, &barrier);

    // Collect barriers from actors
    let collected_barriers = join_all(rxs.iter_mut().map(|(actor_id, rx)| async move {
        let barrier = rx.recv().await.unwrap();
        assert_eq!(barrier.epoch, recv_barrier.epoch);
        (*actor_id, barrier)
    }))
    .await;

    let mut await_epoch_future = pin!(test_env.response_rx.recv().map(|result| {
        let resp: StreamingControlStreamResponse = result.unwrap().unwrap();
        let resp = resp.response.unwrap();
        match resp {
            streaming_control_stream_response::Response::CompleteBarrier(_complete_barrier) => {}
            _ => unreachable!(),
        }
    }));

    // Report to local barrier manager
    for (i, (actor_id, barrier)) in collected_barriers.into_iter().enumerate() {
        manager.collect(actor_id, &barrier);
        LocalBarrierTestEnv::flush_all_events_impl(&test_env.actor_op_tx).await;
        let notified =
            poll_fn(|cx| Poll::Ready(await_epoch_future.as_mut().poll(cx).is_ready())).await;
        assert_eq!(notified, i == count - 1);
    }

    Ok(())
}

#[tokio::test]
async fn test_late_register_barrier_sender() -> StreamResult<()> {
    let mut test_env = LocalBarrierTestEnv::for_test().await;

    let manager = &test_env.local_barrier_manager;

    let register_sender = |actor_id: u32| {
        let barrier_rx = test_env.local_barrier_manager.subscribe_barrier(actor_id);
        (actor_id, barrier_rx)
    };

    let actor_ids_to_send = vec![233, 234, 235];
    let extra_actor_id = 666;
    let actor_ids_to_collect = actor_ids_to_send
        .iter()
        .cloned()
        .chain(once(extra_actor_id))
        .collect_vec();

    // Register actors
    let count = actor_ids_to_send.len();

    // Prepare the barrier
    let epoch1 = test_epoch(2);
    let barrier1 = Barrier::new_test_barrier(epoch1);

    let epoch2 = epoch1.next_epoch();
    let barrier2 = Barrier::new_test_barrier(epoch2).with_stop();

    test_env.inject_barrier(&barrier1, actor_ids_to_collect.clone());
    test_env.inject_barrier(&barrier2, actor_ids_to_collect.clone());

    test_env.flush_all_events().await;

    // register sender after inject barrier
    let mut rxs = actor_ids_to_send
        .clone()
        .into_iter()
        .map(register_sender)
        .collect_vec();

    // Collect barriers from actors
    let collected_barriers = join_all(rxs.iter_mut().map(|(actor_id, rx)| async move {
        let barrier1 = rx.recv().await.unwrap();
        assert_eq!(barrier1.epoch.curr, epoch1);
        let barrier2 = rx.recv().await.unwrap();
        assert_eq!(barrier2.epoch.curr, epoch2);
        manager.collect(*actor_id, &barrier1);
        (*actor_id, barrier2)
    }))
    .await;

    // Collect a barrier before sending
    manager.collect(extra_actor_id, &barrier1);

    let resp = test_env.response_rx.recv().await.unwrap().unwrap();
    match resp.response.unwrap() {
        streaming_control_stream_response::Response::CompleteBarrier(complete_barrier) => {
            assert_eq!(complete_barrier.epoch, barrier1.epoch.prev);
        }
        _ => unreachable!(),
    }

    manager.collect(extra_actor_id, &barrier2);

    let mut await_epoch_future = pin!(test_env.response_rx.recv().map(|result| {
        let resp: StreamingControlStreamResponse = result.unwrap().unwrap();
        let resp = resp.response.unwrap();
        match resp {
            streaming_control_stream_response::Response::CompleteBarrier(complete_barrier) => {
                assert_eq!(complete_barrier.epoch, barrier2.epoch.prev);
            }
            _ => unreachable!(),
        }
    }));

    // Report to local barrier manager
    for (i, (actor_id, barrier)) in collected_barriers.into_iter().enumerate() {
        manager.collect(actor_id, &barrier);
        LocalBarrierTestEnv::flush_all_events_impl(&test_env.actor_op_tx).await;
        let notified =
            poll_fn(|cx| Poll::Ready(await_epoch_future.as_mut().poll(cx).is_ready())).await;
        assert_eq!(notified, i == count - 1);
    }

    Ok(())
}

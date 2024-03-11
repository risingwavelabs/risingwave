// Copyright 2024 RisingWave Labs
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

use std::future::{poll_fn, Future};
use std::iter::once;
use std::pin::pin;
use std::task::Poll;

use assert_matches::assert_matches;
use futures::future::join_all;
use futures::FutureExt;
use itertools::Itertools;
use risingwave_common::util::epoch::test_epoch;
use risingwave_pb::stream_service::{streaming_control_stream_request, InjectBarrierRequest};
use tokio::sync::mpsc::unbounded_channel;
use tokio_stream::wrappers::UnboundedReceiverStream;

use super::*;

#[tokio::test]
async fn test_managed_barrier_collection() -> StreamResult<()> {
    let actor_op_tx = LocalBarrierManager::spawn_for_test();

    let (request_tx, request_rx) = unbounded_channel();
    let (response_tx, mut response_rx) = unbounded_channel();

    actor_op_tx.send_event(LocalActorOperation::NewControlStream {
        handle: ControlStreamHandle::new(
            response_tx,
            UnboundedReceiverStream::new(request_rx).boxed(),
        ),
        init_request: InitRequest { prev_epoch: 0 },
    });

    assert_matches!(
        response_rx.recv().await.unwrap().unwrap().response.unwrap(),
        streaming_control_stream_response::Response::Init(_)
    );

    let context = actor_op_tx
        .send_and_await(LocalActorOperation::GetCurrentSharedContext)
        .await
        .unwrap();

    let manager = &context.local_barrier_manager;

    let register_sender = |actor_id: u32| {
        let (barrier_tx, barrier_rx) = unbounded_channel();
        manager.register_sender(actor_id, barrier_tx);
        (actor_id, barrier_rx)
    };

    // Register actors
    let actor_ids = vec![233, 234, 235];
    let count = actor_ids.len();
    let mut rxs = actor_ids
        .clone()
        .into_iter()
        .map(register_sender)
        .collect_vec();

    // Send a barrier to all actors
    let curr_epoch = test_epoch(2);
    let barrier = Barrier::new_test_barrier(curr_epoch);
    let epoch = barrier.epoch.prev;

    request_tx
        .send(Ok(StreamingControlStreamRequest {
            request: Some(streaming_control_stream_request::Request::InjectBarrier(
                InjectBarrierRequest {
                    request_id: "".to_string(),
                    barrier: Some(barrier.to_protobuf()),
                    actor_ids_to_send: actor_ids.clone(),
                    actor_ids_to_collect: actor_ids,
                },
            )),
        }))
        .unwrap();

    // Collect barriers from actors
    let collected_barriers = join_all(rxs.iter_mut().map(|(actor_id, rx)| async move {
        let barrier = rx.recv().await.unwrap();
        assert_eq!(barrier.epoch.prev, epoch);
        (*actor_id, barrier)
    }))
    .await;

    let mut await_epoch_future = pin!(response_rx.recv().map(|result| {
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
        manager.flush_all_events().await;
        let notified =
            poll_fn(|cx| Poll::Ready(await_epoch_future.as_mut().poll(cx).is_ready())).await;
        assert_eq!(notified, i == count - 1);
    }

    Ok(())
}

#[tokio::test]
async fn test_managed_barrier_collection_before_send_request() -> StreamResult<()> {
    let actor_op_tx = LocalBarrierManager::spawn_for_test();

    let (request_tx, request_rx) = unbounded_channel();
    let (response_tx, mut response_rx) = unbounded_channel();

    actor_op_tx.send_event(LocalActorOperation::NewControlStream {
        handle: ControlStreamHandle::new(
            response_tx,
            UnboundedReceiverStream::new(request_rx).boxed(),
        ),
        init_request: InitRequest { prev_epoch: 0 },
    });

    assert_matches!(
        response_rx.recv().await.unwrap().unwrap().response.unwrap(),
        streaming_control_stream_response::Response::Init(_)
    );

    let context = actor_op_tx
        .send_and_await(LocalActorOperation::GetCurrentSharedContext)
        .await
        .unwrap();

    let manager = &context.local_barrier_manager;

    let register_sender = |actor_id: u32| {
        let (barrier_tx, barrier_rx) = unbounded_channel();
        manager.register_sender(actor_id, barrier_tx);
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
    let mut rxs = actor_ids_to_send
        .clone()
        .into_iter()
        .map(register_sender)
        .collect_vec();

    // Prepare the barrier
    let curr_epoch = test_epoch(2);
    let barrier = Barrier::new_test_barrier(curr_epoch);
    let epoch = barrier.epoch.prev;

    // Collect a barrier before sending
    manager.collect(extra_actor_id, &barrier);

    request_tx
        .send(Ok(StreamingControlStreamRequest {
            request: Some(streaming_control_stream_request::Request::InjectBarrier(
                InjectBarrierRequest {
                    request_id: "".to_string(),
                    barrier: Some(barrier.to_protobuf()),
                    actor_ids_to_send,
                    actor_ids_to_collect,
                },
            )),
        }))
        .unwrap();

    // Collect barriers from actors
    let collected_barriers = join_all(rxs.iter_mut().map(|(actor_id, rx)| async move {
        let barrier = rx.recv().await.unwrap();
        assert_eq!(barrier.epoch.prev, epoch);
        (*actor_id, barrier)
    }))
    .await;

    let mut await_epoch_future = pin!(response_rx.recv().map(|result| {
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
        manager.flush_all_events().await;
        let notified =
            poll_fn(|cx| Poll::Ready(await_epoch_future.as_mut().poll(cx).is_ready())).await;
        assert_eq!(notified, i == count - 1);
    }

    Ok(())
}

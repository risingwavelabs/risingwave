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

use std::collections::HashSet;
use std::future::{Future, poll_fn};
use std::iter::once;
use std::pin::pin;
use std::task::Poll;

use futures::FutureExt;
use futures::future::join_all;
use risingwave_common::util::epoch::{EpochExt, test_epoch};
use risingwave_pb::id::IcebergCompactionTaskId;
use risingwave_pb::stream_service::streaming_control_stream_request::{
    ControlCompactionWriterRequest, control_compaction_writer_request,
};
use risingwave_pb::stream_service::{
    StreamingControlStreamRequest, streaming_control_stream_request,
};

use super::*;
use crate::executor::{IcebergPkIndexBarrierPhase, Mutation};
use crate::task::TEST_PARTIAL_GRAPH_ID;
use crate::task::barrier_test_utils::LocalBarrierTestEnv;

#[tokio::test]
async fn test_actor_local_compaction_gate_projection() {
    let test_env = LocalBarrierTestEnv::for_test().await;
    let target_actor_id = ActorId::new(233);
    let unrelated_actor_id = ActorId::new(234);
    let actor_ids = vec![target_actor_id, unrelated_actor_id];

    let pause = Barrier::new_test_barrier(test_epoch(2)).with_mutation(
        Mutation::IcebergPkIndexBarrier(crate::executor::IcebergPkIndexBarrierMutation {
            sink_id: risingwave_common::id::SinkId::new(7),
            task_id: IcebergCompactionTaskId::new(42),
            phase: IcebergPkIndexBarrierPhase::Pause,
            gated_actor_ids: HashSet::from([target_actor_id]),
        }),
    );
    let resume = Barrier::new_test_barrier(test_epoch(3)).with_mutation(
        Mutation::IcebergPkIndexBarrier(crate::executor::IcebergPkIndexBarrierMutation {
            sink_id: risingwave_common::id::SinkId::new(7),
            task_id: IcebergCompactionTaskId::new(42),
            phase: IcebergPkIndexBarrierPhase::Resume,
            gated_actor_ids: HashSet::from([target_actor_id]),
        }),
    );

    let database_pause = Barrier::new_test_barrier(test_epoch(4)).with_mutation(Mutation::Pause);

    test_env.inject_barrier(&pause, actor_ids.clone());
    test_env.inject_barrier(&resume, actor_ids.clone());
    test_env.inject_barrier(&database_pause, actor_ids);
    test_env.flush_all_events().await;

    let mut target_rx = test_env
        .local_barrier_manager
        .subscribe_barrier(target_actor_id);
    let mut unrelated_rx = test_env
        .local_barrier_manager
        .subscribe_barrier(unrelated_actor_id);

    let target_pause = target_rx.recv().await.unwrap();
    let target_resume = target_rx.recv().await.unwrap();
    let target_database_pause = target_rx.recv().await.unwrap();
    let unrelated_pause = unrelated_rx.recv().await.unwrap();
    let unrelated_resume = unrelated_rx.recv().await.unwrap();
    let unrelated_database_pause = unrelated_rx.recv().await.unwrap();

    assert!(matches!(
        target_pause.mutation.as_deref(),
        Some(Mutation::Pause)
    ));
    assert!(matches!(
        target_resume.mutation.as_deref(),
        Some(Mutation::Resume)
    ));
    assert!(unrelated_pause.mutation.is_none());
    assert!(unrelated_resume.mutation.is_none());
    assert_eq!(target_pause.epoch, unrelated_pause.epoch);
    assert_eq!(target_resume.epoch, unrelated_resume.epoch);
    assert!(matches!(
        target_database_pause.mutation.as_deref(),
        Some(Mutation::Pause)
    ));
    assert!(matches!(
        unrelated_database_pause.mutation.as_deref(),
        Some(Mutation::Pause)
    ));
    assert!(target_database_pause.iceberg_pk_index_barrier().is_none());
    assert!(
        unrelated_database_pause
            .iceberg_pk_index_barrier()
            .is_none()
    );
    for barrier in [
        &target_pause,
        &target_resume,
        &unrelated_pause,
        &unrelated_resume,
    ] {
        let context = barrier
            .iceberg_pk_index_barrier()
            .expect("actor-local barrier should retain raw pk-index context");
        assert_eq!(context.sink_id, risingwave_common::id::SinkId::new(7));
        assert_eq!(context.task_id, IcebergCompactionTaskId::new(42));
        assert_eq!(context.gated_actor_ids, HashSet::from([target_actor_id]));
    }
}

#[tokio::test]
async fn test_compaction_writer_control_routes_both_stages_to_target_actor_and_sink() {
    let test_env = LocalBarrierTestEnv::for_test().await;
    let actor_id = ActorId::new(233);
    let sink_id = risingwave_common::id::SinkId::new(7);
    let mut control_rx = test_env
        .local_barrier_manager
        .subscribe_iceberg_pk_index_writer_control(actor_id, sink_id);
    test_env.flush_all_events().await;

    for (stage, expected) in [
        (
            control_compaction_writer_request::Stage::SealReady,
            IcebergPkIndexWriterControl::SealReady {
                task_id: IcebergCompactionTaskId::new(42),
                epoch: 100,
            },
        ),
        (
            control_compaction_writer_request::Stage::Committed,
            IcebergPkIndexWriterControl::Committed {
                task_id: IcebergCompactionTaskId::new(42),
                epoch: 100,
            },
        ),
    ] {
        test_env
            .request_tx
            .send(Ok(StreamingControlStreamRequest {
                request: Some(
                    streaming_control_stream_request::Request::ControlCompactionWriter(
                        ControlCompactionWriterRequest {
                            partial_graph_id: TEST_PARTIAL_GRAPH_ID,
                            sink_id,
                            task_id: IcebergCompactionTaskId::new(42),
                            epoch: 100,
                            actor_ids: vec![actor_id],
                            stage: stage.into(),
                        },
                    ),
                ),
            }))
            .unwrap();

        let control = control_rx
            .recv()
            .await
            .expect("writer control should arrive");
        assert_eq!(control, expected);
    }
}

#[tokio::test]
async fn test_compaction_writer_control_does_not_panic_for_suspended_graph() {
    let mut test_env = LocalBarrierTestEnv::for_test().await;
    let actor_id = ActorId::new(233);
    let sink_id = risingwave_common::id::SinkId::new(7);
    let mut control_rx = test_env
        .local_barrier_manager
        .subscribe_iceberg_pk_index_writer_control(actor_id, sink_id);
    test_env.flush_all_events().await;

    test_env
        .local_barrier_manager
        .notify_failure(actor_id, StreamError::from(anyhow!("actor failed")));
    let failure = test_env.response_rx.recv().await.unwrap().unwrap();
    assert!(matches!(
        failure.response,
        Some(streaming_control_stream_response::Response::ReportPartialGraphFailure(_))
    ));

    test_env
        .request_tx
        .send(Ok(StreamingControlStreamRequest {
            request: Some(
                streaming_control_stream_request::Request::ControlCompactionWriter(
                    ControlCompactionWriterRequest {
                        partial_graph_id: TEST_PARTIAL_GRAPH_ID,
                        sink_id,
                        task_id: IcebergCompactionTaskId::new(42),
                        epoch: 100,
                        actor_ids: vec![actor_id],
                        stage: control_compaction_writer_request::Stage::SealReady.into(),
                    },
                ),
            ),
        }))
        .unwrap();
    test_env.flush_all_events().await;

    assert!(matches!(
        control_rx.try_recv(),
        Err(tokio::sync::mpsc::error::TryRecvError::Empty)
    ));
}

#[tokio::test]
async fn test_managed_barrier_collection() -> StreamResult<()> {
    let mut test_env = LocalBarrierTestEnv::for_test().await;

    let manager = &test_env.local_barrier_manager;

    let register_sender = |actor_id: ActorId| {
        let barrier_rx = test_env.local_barrier_manager.subscribe_barrier(actor_id);
        (actor_id, barrier_rx)
    };

    // Register actors
    let actor_ids = vec![233.into(), 234.into(), 235.into()];

    // Send a barrier to all actors
    let curr_epoch = test_epoch(2);
    let barrier = Barrier::new_test_barrier(curr_epoch);
    let epoch = barrier.epoch.prev;

    test_env.inject_barrier(&barrier, actor_ids.clone());

    test_env.flush_all_events().await;

    let count = actor_ids.len();
    let mut rxs = actor_ids.iter().copied().map(register_sender).collect_vec();

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

    let register_sender = |actor_id: ActorId| {
        let barrier_rx = test_env.local_barrier_manager.subscribe_barrier(actor_id);
        (actor_id, barrier_rx)
    };

    let actor_ids_to_send = vec![233.into(), 234.into(), 235.into()];
    let extra_actor_id = 666.into();
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

    let register_sender = |actor_id: ActorId| {
        let barrier_rx = test_env.local_barrier_manager.subscribe_barrier(actor_id);
        (actor_id, barrier_rx)
    };

    let actor_ids_to_send = vec![233.into(), 234.into(), 235.into()];
    let extra_actor_id = 666.into();
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

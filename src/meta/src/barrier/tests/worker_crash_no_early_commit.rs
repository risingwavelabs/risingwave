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

use std::collections::{HashMap, HashSet};
use std::future::pending;
use std::sync::Arc;

use futures::StreamExt;
use risingwave_common::catalog::{DatabaseId, TableId};
use risingwave_common::util::epoch::test_epoch;
use risingwave_meta_model::fragment::DistributionType;
use risingwave_pb::catalog::Database;
use risingwave_pb::common::{HostAddress, PbWorkerType, WorkerNode, worker_node};
use risingwave_pb::hummock::HummockVersionStats;
use risingwave_pb::stream_service::streaming_control_stream_request::{PbInitRequest, Request};
use risingwave_pb::stream_service::streaming_control_stream_response::Response;
use risingwave_pb::stream_service::{
    BarrierCompleteResponse, StreamingControlStreamRequest, StreamingControlStreamResponse,
};
use risingwave_rpc_client::{StreamingControlHandle, UnboundedBidiStreamHandle};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::{mpsc, oneshot};
use tokio::task::yield_now;
use tokio_stream::wrappers::UnboundedReceiverStream;

use crate::MetaResult;
use crate::barrier::command::CommandContext;
use crate::barrier::context::GlobalBarrierWorkerContext;
use crate::barrier::info::InflightStreamingJobInfo;
use crate::barrier::progress::TrackingJob;
use crate::barrier::schedule::MarkReadyOptions;
use crate::barrier::worker::GlobalBarrierWorker;
use crate::barrier::{
    BarrierWorkerRuntimeInfoSnapshot, DatabaseRuntimeInfoSnapshot, RecoveryReason, Scheduled,
};
use crate::controller::fragment::{InflightActorInfo, InflightFragmentInfo};
use crate::hummock::CommitEpochInfo;
use crate::manager::sink_coordination::SinkCoordinatorManager;
use crate::manager::{ActiveStreamingWorkerNodes, MetaOpts, MetaSrvEnv};
use crate::model::StreamActor;

enum ContextRequest {
    AbortAndMarkBlocked(RecoveryReason),
    MarkReady,
    ReloadRuntimeInfo(oneshot::Sender<BarrierWorkerRuntimeInfoSnapshot>),
    NewControlStream(WorkerNode, oneshot::Sender<StreamingControlHandle>),
}

struct MockBarrierWorkerContext(UnboundedSender<ContextRequest>);
impl GlobalBarrierWorkerContext for MockBarrierWorkerContext {
    async fn commit_epoch(&self, _commit_info: CommitEpochInfo) -> MetaResult<HummockVersionStats> {
        unreachable!()
    }

    async fn next_scheduled(&self) -> Scheduled {
        pending().await
    }

    fn abort_and_mark_blocked(
        &self,
        database_id: Option<DatabaseId>,
        recovery_reason: RecoveryReason,
    ) {
        assert_eq!(database_id, None);
        self.0
            .send(ContextRequest::AbortAndMarkBlocked(recovery_reason))
            .unwrap();
    }

    fn mark_ready(&self, options: MarkReadyOptions) {
        let MarkReadyOptions::Global { blocked_databases } = options else {
            unreachable!()
        };
        assert!(blocked_databases.is_empty());
        self.0.send(ContextRequest::MarkReady).unwrap();
    }

    async fn post_collect_command<'a>(&'a self, _command: &'a CommandContext) -> MetaResult<()> {
        unreachable!()
    }

    async fn notify_creating_job_failed(&self, database_id: Option<DatabaseId>, _err: String) {
        assert_eq!(database_id, None);
    }

    async fn finish_creating_job(&self, _job: TrackingJob) -> MetaResult<()> {
        unreachable!()
    }

    async fn new_control_stream(
        &self,
        node: &WorkerNode,
        _init_request: &PbInitRequest,
    ) -> MetaResult<StreamingControlHandle> {
        let (tx, rx) = oneshot::channel();
        self.0
            .send(ContextRequest::NewControlStream(node.clone(), tx))
            .unwrap();
        Ok(rx.await.unwrap())
    }

    async fn reload_runtime_info(&self) -> MetaResult<BarrierWorkerRuntimeInfoSnapshot> {
        let (tx, rx) = oneshot::channel();
        self.0.send(ContextRequest::ReloadRuntimeInfo(tx)).unwrap();
        Ok(rx.await.unwrap())
    }

    async fn reload_database_runtime_info(
        &self,
        _database_id: DatabaseId,
    ) -> MetaResult<Option<DatabaseRuntimeInfoSnapshot>> {
        unreachable!()
    }
}

#[tokio::test]
async fn test_barrier_manager_worker_crash_no_early_commit() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let context = Arc::new(MockBarrierWorkerContext(tx));
    let env = MetaSrvEnv::for_test_opts(MetaOpts::test(true), |param| {
        param.per_database_isolation = Some(false);
    })
    .await;
    let (_request_tx, request_rx) = mpsc::unbounded_channel();
    let sink_manager = SinkCoordinatorManager::for_test();
    let mut worker = GlobalBarrierWorker::new_inner(env, sink_manager, request_rx, context).await;
    let (_shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();

    let _join_handle = tokio::spawn(async move {
        worker.recovery(false, RecoveryReason::Bootstrap).await;
        worker.run_inner(shutdown_rx).await
    });

    // bootstrap recovery
    let ContextRequest::AbortAndMarkBlocked(RecoveryReason::Bootstrap) = rx.recv().await.unwrap()
    else {
        unreachable!()
    };
    let ContextRequest::ReloadRuntimeInfo(tx) = rx.recv().await.unwrap() else {
        unreachable!()
    };
    let database_id = DatabaseId::new(233);
    let job_id = TableId::new(234);
    let worker_node = |id| WorkerNode {
        id,
        r#type: PbWorkerType::ComputeNode as i32,
        host: Some(HostAddress {
            host: format!("host{}", id),
            port: 0,
        }),
        state: worker_node::State::Running as i32,
        property: None,
        transactional_id: None,
        resource: None,
        started_at: None,
    };
    let worker1 = worker_node(1);
    let worker2 = worker_node(2);
    // two actors on two singleton fragments
    let new_actor = |actor_id| StreamActor {
        actor_id,
        fragment_id: actor_id,
        vnode_bitmap: None,
        mview_definition: "".to_owned(),
        expr_context: None,
    };
    let table1 = TableId::new(1);
    let table2 = TableId::new(2);
    let actor1 = new_actor(1);
    let actor2 = new_actor(2);
    let initial_epoch = test_epoch(100);

    tx.send(BarrierWorkerRuntimeInfoSnapshot {
        active_streaming_nodes: ActiveStreamingWorkerNodes::for_test(HashMap::from_iter([
            (worker1.id as _, worker1.clone()),
            (worker2.id as _, worker2.clone()),
        ])),
        database_job_infos: HashMap::from_iter([(
            database_id,
            HashMap::from_iter([(
                job_id,
                InflightStreamingJobInfo {
                    job_id,
                    fragment_infos: HashMap::from_iter([
                        (
                            actor1.fragment_id,
                            InflightFragmentInfo {
                                fragment_id: actor1.fragment_id,
                                distribution_type: DistributionType::Single,
                                nodes: Default::default(),
                                actors: HashMap::from_iter([(
                                    actor1.actor_id as _,
                                    InflightActorInfo {
                                        worker_id: worker1.id as _,
                                        vnode_bitmap: None,
                                    },
                                )]),
                                state_table_ids: HashSet::from_iter([table1]),
                            },
                        ),
                        (
                            actor2.fragment_id,
                            InflightFragmentInfo {
                                fragment_id: actor2.fragment_id,
                                distribution_type: DistributionType::Single,
                                nodes: Default::default(),
                                actors: HashMap::from_iter([(
                                    actor2.actor_id as _,
                                    InflightActorInfo {
                                        worker_id: worker2.id as _,
                                        vnode_bitmap: None,
                                    },
                                )]),
                                state_table_ids: HashSet::from_iter([table2]),
                            },
                        ),
                    ]),
                },
            )]),
        )]),
        state_table_committed_epochs: HashMap::from_iter([
            (table1, initial_epoch),
            (table2, initial_epoch),
        ]),
        state_table_log_epochs: Default::default(),
        subscription_infos: Default::default(),
        stream_actors: HashMap::from_iter([
            (actor1.actor_id as _, actor1.clone()),
            (actor2.actor_id as _, actor2.clone()),
        ]),
        fragment_relations: Default::default(),
        source_splits: Default::default(),
        background_jobs: Default::default(),
        hummock_version_stats: Default::default(),
        database_infos: vec![Database {
            id: database_id.database_id,
            name: "".to_owned(),
            owner: 0,
            resource_group: "test".to_owned(),
            barrier_interval_ms: None,
            checkpoint_frequency: None,
        }],
    })
    .unwrap();
    let make_control_stream_handle = || {
        let (request_tx, request_rx) = mpsc::unbounded_channel();
        let (response_tx, response_rx) = mpsc::unbounded_channel();
        let handle = UnboundedBidiStreamHandle {
            request_sender: request_tx,
            response_stream: UnboundedReceiverStream::new(response_rx).boxed(),
        };
        (request_rx, response_tx, Some(handle))
    };
    let (mut request_rx1, response_tx1, mut handle1) = make_control_stream_handle();
    let (mut request_rx2, response_tx2, mut handle2) = make_control_stream_handle();

    for _ in 0..2 {
        let ContextRequest::NewControlStream(worker, tx) = rx.recv().await.unwrap() else {
            unreachable!()
        };
        if worker == worker1 {
            tx.send(handle1.take().unwrap()).unwrap()
        } else if worker == worker2 {
            tx.send(handle2.take().unwrap()).unwrap()
        } else {
            unreachable!()
        }
    }

    let handle_initial_barrier = async |request_rx: &mut UnboundedReceiver<
        StreamingControlStreamRequest,
    >,
                                        response_tx: &UnboundedSender<_>,
                                        worker: &WorkerNode| {
        let Request::CreatePartialGraph(req) = request_rx.recv().await.unwrap().request.unwrap()
        else {
            unreachable!()
        };
        assert_eq!(req.database_id, database_id.database_id);
        let partial_graph_id = req.partial_graph_id;
        let Request::InjectBarrier(init_inject_request) =
            request_rx.recv().await.unwrap().request.unwrap()
        else {
            unreachable!()
        };
        let epoch = init_inject_request.barrier.unwrap().epoch.unwrap();
        assert_eq!(epoch.prev, initial_epoch);
        assert_eq!(partial_graph_id, init_inject_request.partial_graph_id);
        assert_eq!(init_inject_request.database_id, database_id.database_id);
        response_tx
            .send(Ok(StreamingControlStreamResponse {
                response: Some(Response::CompleteBarrier(BarrierCompleteResponse {
                    worker_id: worker.id as _,
                    partial_graph_id,
                    epoch: epoch.prev,
                    database_id: database_id.database_id,
                    ..Default::default()
                })),
            }))
            .unwrap();
        (partial_graph_id, epoch)
    };

    let (partial_graph_id, initial_epoch) =
        handle_initial_barrier(&mut request_rx1, &response_tx1, &worker1).await;
    assert_eq!(
        handle_initial_barrier(&mut request_rx2, &response_tx2, &worker2).await,
        (partial_graph_id, initial_epoch)
    );

    let ContextRequest::MarkReady = rx.recv().await.unwrap() else {
        unreachable!()
    };

    // correctly collect from worker1
    let Request::InjectBarrier(init_inject_request) =
        request_rx1.recv().await.unwrap().request.unwrap()
    else {
        unreachable!()
    };
    let epoch1 = init_inject_request.barrier.unwrap().epoch.unwrap();
    assert_eq!(epoch1.prev, initial_epoch.curr);
    assert_eq!(partial_graph_id, init_inject_request.partial_graph_id);
    assert_eq!(init_inject_request.database_id, database_id.database_id);
    response_tx1
        .send(Ok(StreamingControlStreamResponse {
            response: Some(Response::CompleteBarrier(BarrierCompleteResponse {
                worker_id: worker1.id as _,
                partial_graph_id,
                epoch: epoch1.prev,
                database_id: database_id.database_id,
                ..Default::default()
            })),
        }))
        .unwrap();

    // yield to ensure that the worker has processed the response
    for _ in 0..10 {
        yield_now().await;
    }

    // receive from worker2
    {
        let Request::InjectBarrier(init_inject_request) =
            request_rx2.recv().await.unwrap().request.unwrap()
        else {
            unreachable!()
        };
        let epoch = init_inject_request.barrier.unwrap().epoch.unwrap();
        assert_eq!(epoch.prev, initial_epoch.curr);
        assert_eq!(partial_graph_id, init_inject_request.partial_graph_id);
        assert_eq!(init_inject_request.database_id, database_id.database_id);
    }

    // worker2 crashes before sending the response
    drop(response_tx2);

    // enter recovery
    let ContextRequest::AbortAndMarkBlocked(RecoveryReason::Failover(_)) = rx.recv().await.unwrap()
    else {
        unreachable!()
    };
}

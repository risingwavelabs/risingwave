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
use std::error::Error;
use std::fmt::{Debug, Formatter};
use std::future::poll_fn;
use std::task::{Context, Poll};
use std::time::Duration;

use anyhow::anyhow;
use fail::fail_point;
use futures::StreamExt;
use futures::future::join_all;
use itertools::Itertools;
use risingwave_common::catalog::{DatabaseId, TableId};
use risingwave_common::util::epoch::Epoch;
use risingwave_common::util::tracing::TracingContext;
use risingwave_connector::source::SplitImpl;
use risingwave_meta_model::WorkerId;
use risingwave_pb::common::{ActorInfo, WorkerNode};
use risingwave_pb::hummock::HummockVersionStats;
use risingwave_pb::stream_plan::barrier_mutation::Mutation;
use risingwave_pb::stream_plan::{
    AddMutation, Barrier, BarrierMutation, StreamNode, SubscriptionUpstreamInfo,
};
use risingwave_pb::stream_service::inject_barrier_request::build_actor_info::UpstreamActors;
use risingwave_pb::stream_service::inject_barrier_request::{
    BuildActorInfo, FragmentBuildActorInfo,
};
use risingwave_pb::stream_service::streaming_control_stream_request::{
    CreatePartialGraphRequest, PbDatabaseInitialPartialGraph, PbInitRequest, PbInitialPartialGraph,
    RemovePartialGraphRequest, ResetDatabaseRequest,
};
use risingwave_pb::stream_service::{
    BarrierCompleteResponse, InjectBarrierRequest, StreamingControlStreamRequest,
    streaming_control_stream_request, streaming_control_stream_response,
};
use risingwave_rpc_client::StreamingControlHandle;
use thiserror_ext::AsReport;
use tokio::time::sleep;
use tokio_retry::strategy::ExponentialBackoff;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use super::{BarrierKind, Command, InflightSubscriptionInfo, TracedEpoch};
use crate::barrier::checkpoint::{BarrierWorkerState, DatabaseCheckpointControl};
use crate::barrier::context::{GlobalBarrierWorkerContext, GlobalBarrierWorkerContextImpl};
use crate::barrier::info::{BarrierInfo, InflightDatabaseInfo, InflightStreamingJobInfo};
use crate::barrier::progress::CreateMviewProgressTracker;
use crate::barrier::utils::{NodeToCollect, is_valid_after_worker_err};
use crate::controller::fragment::InflightFragmentInfo;
use crate::manager::MetaSrvEnv;
use crate::model::{
    ActorId, FragmentId, StreamActorWithDispatchers, StreamActorWithUpDownstreams,
    StreamJobActorsToCreate, StreamJobFragments,
};
use crate::stream::build_actor_connector_splits;
use crate::{MetaError, MetaResult};

fn to_partial_graph_id(job_id: Option<TableId>) -> u32 {
    job_id
        .map(|table| {
            assert_ne!(table.table_id, u32::MAX);
            table.table_id
        })
        .unwrap_or(u32::MAX)
}

pub(super) fn from_partial_graph_id(partial_graph_id: u32) -> Option<TableId> {
    if partial_graph_id == u32::MAX {
        None
    } else {
        Some(TableId::new(partial_graph_id))
    }
}

struct ControlStreamNode {
    worker: WorkerNode,
    handle: StreamingControlHandle,
}

pub(super) struct ControlStreamManager {
    nodes: HashMap<WorkerId, ControlStreamNode>,
    pub(crate) env: MetaSrvEnv,
}

impl ControlStreamManager {
    pub(super) fn new(env: MetaSrvEnv) -> Self {
        Self {
            nodes: Default::default(),
            env,
        }
    }

    pub(super) fn contains_worker(&self, worker_id: WorkerId) -> bool {
        self.nodes.contains_key(&worker_id)
    }

    pub(super) async fn try_add_worker(
        &mut self,
        node: WorkerNode,
        inflight_infos: impl Iterator<
            Item = (
                DatabaseId,
                &InflightSubscriptionInfo,
                &InflightDatabaseInfo,
                impl Iterator<Item = &InflightStreamingJobInfo>,
            ),
        >,
        term_id: String,
        context: &impl GlobalBarrierWorkerContext,
    ) {
        let node_id = node.id as WorkerId;
        if self.nodes.contains_key(&node_id) {
            warn!(id = node.id, host = ?node.host, "node already exists");
            return;
        }
        let node_host = node.host.clone().unwrap();

        let init_request = self.collect_init_request(inflight_infos, term_id);
        match context.new_control_stream(&node, &init_request).await {
            Ok(handle) => {
                assert!(
                    self.nodes
                        .insert(
                            node_id,
                            ControlStreamNode {
                                worker: node.clone(),
                                handle,
                            }
                        )
                        .is_none()
                );
                info!(?node_host, "add control stream worker");
            }
            Err(e) => {
                error!(err = %e.as_report(), ?node_host, "fail to create worker node");
            }
        }
    }

    pub(super) async fn add_worker(
        &mut self,
        node: WorkerNode,
        inflight_infos: impl Iterator<
            Item = (
                DatabaseId,
                &InflightSubscriptionInfo,
                &InflightDatabaseInfo,
                impl Iterator<Item = &InflightStreamingJobInfo>,
            ),
        >,
        term_id: String,
        context: &impl GlobalBarrierWorkerContext,
    ) {
        let node_id = node.id as WorkerId;
        if self.nodes.contains_key(&node_id) {
            warn!(id = node.id, host = ?node.host, "node already exists");
            return;
        }
        let node_host = node.host.clone().unwrap();
        let mut backoff = ExponentialBackoff::from_millis(100)
            .max_delay(Duration::from_secs(3))
            .factor(5);
        let init_request = self.collect_init_request(inflight_infos, term_id);
        const MAX_RETRY: usize = 5;
        for i in 1..=MAX_RETRY {
            match context.new_control_stream(&node, &init_request).await {
                Ok(handle) => {
                    assert!(
                        self.nodes
                            .insert(
                                node_id,
                                ControlStreamNode {
                                    worker: node.clone(),
                                    handle,
                                }
                            )
                            .is_none()
                    );
                    info!(?node_host, "add control stream worker");
                    return;
                }
                Err(e) => {
                    // It may happen that the dns information of newly registered worker node
                    // has not been propagated to the meta node and cause error. Wait for a while and retry
                    let delay = backoff.next().unwrap();
                    error!(attempt = i, backoff_delay = ?delay, err = %e.as_report(), ?node_host, "fail to resolve worker node address");
                    sleep(delay).await;
                }
            }
        }
        error!(?node_host, "fail to create worker node after retry");
    }

    pub(super) async fn reset(
        &mut self,
        nodes: &HashMap<WorkerId, WorkerNode>,
        term_id: String,
        context: &impl GlobalBarrierWorkerContext,
    ) -> HashSet<WorkerId> {
        let init_request = PbInitRequest {
            databases: vec![],
            term_id,
        };
        let init_request = &init_request;
        let nodes = join_all(nodes.iter().map(|(worker_id, node)| async move {
            let result = context.new_control_stream(node, init_request).await;
            (*worker_id, node.clone(), result)
        }))
        .await;
        self.nodes.clear();
        let mut failed_workers = HashSet::new();
        for (worker_id, node, result) in nodes {
            match result {
                Ok(handle) => {
                    assert!(
                        self.nodes
                            .insert(
                                worker_id,
                                ControlStreamNode {
                                    worker: node,
                                    handle
                                }
                            )
                            .is_none()
                    );
                }
                Err(e) => {
                    failed_workers.insert(worker_id);
                    warn!(
                        e = %e.as_report(),
                        worker_id,
                        ?node,
                        "failed to connect to node"
                    )
                }
            }
        }

        failed_workers
    }

    /// Clear all nodes and response streams in the manager.
    pub(super) fn clear(&mut self) {
        *self = Self::new(self.env.clone());
    }

    fn poll_next_response(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<(
        WorkerId,
        MetaResult<streaming_control_stream_response::Response>,
    )> {
        if self.nodes.is_empty() {
            return Poll::Pending;
        }
        let mut poll_result: Poll<(WorkerId, MetaResult<_>)> = Poll::Pending;
        {
            for (worker_id, node) in &mut self.nodes {
                match node.handle.response_stream.poll_next_unpin(cx) {
                    Poll::Ready(result) => {
                        poll_result = Poll::Ready((
                            *worker_id,
                            result
                                .ok_or_else(|| anyhow!("end of stream").into())
                                .and_then(|result| {
                                    result.map_err(Into::<MetaError>::into).and_then(|resp| {
                                        match resp
                                            .response
                                            .ok_or_else(||anyhow!("empty response"))?
                                        {
                                            streaming_control_stream_response::Response::Shutdown(_) => Err(anyhow!(
                                                "worker node {worker_id} is shutting down"
                                            )
                                            .into()),
                                            streaming_control_stream_response::Response::Init(_) => {
                                                // This arm should be unreachable.
                                                Err(anyhow!("get unexpected init response").into())
                                            }
                                            resp => Ok(resp),
                                        }
                                    })
                                })
                        ));
                        break;
                    }
                    Poll::Pending => {
                        continue;
                    }
                }
            }
        };

        if let Poll::Ready((worker_id, Err(err))) = &poll_result {
            let node = self
                .nodes
                .remove(worker_id)
                .expect("should exist when get shutdown resp");
            warn!(node = ?node.worker, err = %err.as_report(), "get error from response stream");
        }

        poll_result
    }

    pub(super) async fn next_response(
        &mut self,
    ) -> (
        WorkerId,
        MetaResult<streaming_control_stream_response::Response>,
    ) {
        poll_fn(|cx| self.poll_next_response(cx)).await
    }

    fn collect_init_request(
        &self,
        initial_inflight_infos: impl Iterator<
            Item = (
                DatabaseId,
                &InflightSubscriptionInfo,
                &InflightDatabaseInfo,
                impl Iterator<Item = &InflightStreamingJobInfo>,
            ),
        >,
        term_id: String,
    ) -> PbInitRequest {
        PbInitRequest {
            databases: initial_inflight_infos
                .map(
                    |(
                        database_id,
                        subscriptions,
                        database_inflight_info,
                        creating_job_inflight_info,
                    )| {
                        fn actor_infos(
                            nodes: &HashMap<WorkerId, ControlStreamNode>,
                            fragment_infos: impl Iterator<Item = &InflightFragmentInfo>,
                        ) -> Vec<ActorInfo> {
                            {
                                fragment_infos
                                    .flat_map(|fragment| {
                                        fragment.actors.iter().map(|(actor_id, actor)| {
                                            let host_addr = nodes
                                                .get(&actor.worker_id)
                                                .expect("worker should exist for inflight actor")
                                                .worker
                                                .host
                                                .clone()
                                                .expect("should exist");
                                            ActorInfo {
                                                actor_id: *actor_id,
                                                host: Some(host_addr),
                                            }
                                        })
                                    })
                                    .collect()
                            }
                        }
                        let mut graphs = vec![PbInitialPartialGraph {
                            partial_graph_id: to_partial_graph_id(None),
                            subscriptions: subscriptions.into_iter().collect_vec(),
                            actor_infos: actor_infos(
                                &self.nodes,
                                database_inflight_info.fragment_infos(),
                            ),
                        }];
                        graphs.extend(creating_job_inflight_info.map(|job| {
                            PbInitialPartialGraph {
                                partial_graph_id: to_partial_graph_id(Some(job.job_id)),
                                subscriptions: vec![],
                                actor_infos: actor_infos(&self.nodes, job.fragment_infos()),
                            }
                        }));
                        PbDatabaseInitialPartialGraph {
                            database_id: database_id.database_id,
                            graphs,
                        }
                    },
                )
                .collect(),
            term_id,
        }
    }
}

pub(super) struct DatabaseInitialBarrierCollector {
    database_id: DatabaseId,
    node_to_collect: NodeToCollect,
    database_state: BarrierWorkerState,
    create_mview_tracker: CreateMviewProgressTracker,
    committed_epoch: u64,
}

impl Debug for DatabaseInitialBarrierCollector {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DatabaseInitialBarrierCollector")
            .field("database_id", &self.database_id)
            .field("node_to_collect", &self.node_to_collect)
            .finish()
    }
}

impl DatabaseInitialBarrierCollector {
    pub(super) fn is_collected(&self) -> bool {
        self.node_to_collect.is_empty()
    }

    pub(super) fn database_state(&self) -> &BarrierWorkerState {
        &self.database_state
    }

    pub(super) fn collect_resp(&mut self, resp: BarrierCompleteResponse) {
        assert_eq!(self.database_id.database_id, resp.database_id);
        assert_eq!(resp.epoch, self.committed_epoch);
        assert!(
            self.node_to_collect
                .remove(&(resp.worker_id as _))
                .is_some()
        );
    }

    pub(super) fn finish(self) -> DatabaseCheckpointControl {
        assert!(self.is_collected());
        DatabaseCheckpointControl::recovery(
            self.database_id,
            self.create_mview_tracker,
            self.database_state,
            self.committed_epoch,
        )
    }

    pub(super) fn is_valid_after_worker_err(&mut self, worker_id: WorkerId) -> bool {
        is_valid_after_worker_err(&mut self.node_to_collect, worker_id)
    }
}

impl ControlStreamManager {
    /// Extract information from the loaded runtime barrier worker snapshot info, and inject the initial barrier.
    pub(super) fn inject_database_initial_barrier(
        &mut self,
        database_id: DatabaseId,
        info: InflightDatabaseInfo,
        state_table_committed_epochs: &mut HashMap<TableId, u64>,
        stream_actors: &mut HashMap<ActorId, StreamActorWithDispatchers>,
        source_splits: &mut HashMap<ActorId, Vec<SplitImpl>>,
        background_jobs: &mut HashMap<TableId, (String, StreamJobFragments)>,
        subscription_info: InflightSubscriptionInfo,
        is_paused: bool,
        hummock_version_stats: &HummockVersionStats,
    ) -> MetaResult<DatabaseInitialBarrierCollector> {
        self.add_partial_graph(database_id, None);
        let source_split_assignments = info
            .fragment_infos()
            .flat_map(|info| info.actors.keys())
            .filter_map(|actor_id| {
                let actor_id = *actor_id as ActorId;
                source_splits
                    .remove(&actor_id)
                    .map(|splits| (actor_id, splits))
            })
            .collect();
        let mutation = Mutation::Add(AddMutation {
            // Actors built during recovery is not treated as newly added actors.
            actor_dispatchers: Default::default(),
            added_actors: Default::default(),
            actor_splits: build_actor_connector_splits(&source_split_assignments),
            pause: is_paused,
            subscriptions_to_add: Default::default(),
        });

        let mut epochs = info.existing_table_ids().map(|table_id| {
            (
                table_id,
                state_table_committed_epochs
                    .remove(&table_id)
                    .expect("should exist"),
            )
        });
        let (first_table_id, prev_epoch) = epochs.next().expect("non-empty");
        for (table_id, epoch) in epochs {
            assert_eq!(
                prev_epoch, epoch,
                "{} has different committed epoch to {}",
                first_table_id, table_id
            );
        }
        let prev_epoch = TracedEpoch::new(Epoch(prev_epoch));
        // Use a different `curr_epoch` for each recovery attempt.
        let curr_epoch = prev_epoch.next();
        let barrier_info = BarrierInfo {
            prev_epoch,
            curr_epoch,
            kind: BarrierKind::Initial,
        };

        let mut stream_actors: HashMap<_, _> = info
            .fragment_infos()
            .flat_map(|fragment_info| fragment_info.actors.keys())
            .map(|actor_id| {
                let (stream_actor, dispatchers) =
                    stream_actors.remove(actor_id).expect("should exist");
                (stream_actor.actor_id, (stream_actor, dispatchers))
            })
            .collect();

        let mut actor_upstreams = Command::collect_actor_upstreams(
            stream_actors.values().map(|(actor, dispatchers)| {
                (actor.actor_id, actor.fragment_id, dispatchers.as_slice())
            }),
            None,
        );

        let mut node_actors: HashMap<
            _,
            HashMap<FragmentId, (StreamNode, Vec<StreamActorWithUpDownstreams>)>,
        > = HashMap::new();
        for fragment_info in info.fragment_infos() {
            for (actor_id, actor) in &fragment_info.actors {
                let worker_id = actor.worker_id as WorkerId;
                let actor_id = *actor_id as ActorId;
                let (stream_actor, dispatchers) =
                    stream_actors.remove(&actor_id).expect("should exist");
                let upstream = actor_upstreams.remove(&actor_id).unwrap_or_default();
                node_actors
                    .entry(worker_id)
                    .or_default()
                    .entry(fragment_info.fragment_id)
                    .or_insert_with(|| (fragment_info.nodes.clone(), vec![]))
                    .1
                    .push((stream_actor, upstream, dispatchers));
            }
        }

        let background_mviews = info.job_ids().filter_map(|job_id| {
            background_jobs
                .get(&job_id)
                .map(|(definition, stream_job_fragments)| {
                    (job_id, (definition.clone(), stream_job_fragments))
                })
        });
        let tracker = CreateMviewProgressTracker::recover(background_mviews, hummock_version_stats);

        let node_to_collect = self.inject_barrier(
            database_id,
            None,
            Some(mutation),
            &barrier_info,
            info.fragment_infos(),
            info.fragment_infos(),
            Some(node_actors),
            (&subscription_info).into_iter().collect(),
            vec![],
        )?;
        debug!(
            ?node_to_collect,
            database_id = database_id.database_id,
            "inject initial barrier"
        );

        let committed_epoch = barrier_info.prev_epoch();
        let new_epoch = barrier_info.curr_epoch;
        let database_state =
            BarrierWorkerState::recovery(new_epoch, info, subscription_info, is_paused);
        Ok(DatabaseInitialBarrierCollector {
            database_id,
            node_to_collect,
            database_state,
            create_mview_tracker: tracker,
            committed_epoch,
        })
    }

    pub(super) fn inject_command_ctx_barrier(
        &mut self,
        database_id: DatabaseId,
        command: Option<&Command>,
        barrier_info: &BarrierInfo,
        is_paused: bool,
        pre_applied_graph_info: &InflightDatabaseInfo,
        applied_graph_info: &InflightDatabaseInfo,
    ) -> MetaResult<NodeToCollect> {
        let mutation = command.and_then(|c| c.to_mutation(is_paused));
        let subscriptions_to_add = if let Some(Mutation::Add(add)) = &mutation {
            add.subscriptions_to_add.clone()
        } else {
            vec![]
        };
        let subscriptions_to_remove = if let Some(Mutation::DropSubscriptions(drop)) = &mutation {
            drop.info.clone()
        } else {
            vec![]
        };
        self.inject_barrier(
            database_id,
            None,
            mutation,
            barrier_info,
            pre_applied_graph_info.fragment_infos(),
            applied_graph_info.fragment_infos(),
            command
                .as_ref()
                .map(|command| command.actors_to_create(pre_applied_graph_info))
                .unwrap_or_default(),
            subscriptions_to_add,
            subscriptions_to_remove,
        )
    }

    pub(super) fn inject_barrier<'a>(
        &mut self,
        database_id: DatabaseId,
        creating_table_id: Option<TableId>,
        mutation: Option<Mutation>,
        barrier_info: &BarrierInfo,
        pre_applied_graph_info: impl IntoIterator<Item = &InflightFragmentInfo>,
        applied_graph_info: impl IntoIterator<Item = &'a InflightFragmentInfo> + 'a,
        mut new_actors: Option<StreamJobActorsToCreate>,
        subscriptions_to_add: Vec<SubscriptionUpstreamInfo>,
        subscriptions_to_remove: Vec<SubscriptionUpstreamInfo>,
    ) -> MetaResult<NodeToCollect> {
        fail_point!("inject_barrier_err", |_| risingwave_common::bail!(
            "inject_barrier_err"
        ));

        let partial_graph_id = to_partial_graph_id(creating_table_id);

        let node_actors = InflightFragmentInfo::actor_ids_to_collect(pre_applied_graph_info);

        for worker_id in node_actors.keys() {
            if !self.nodes.contains_key(worker_id) {
                return Err(anyhow!("unconnected worker node {}", worker_id).into());
            }
        }

        let table_ids_to_sync: HashSet<_> =
            InflightFragmentInfo::existing_table_ids(applied_graph_info)
                .map(|table_id| table_id.table_id)
                .collect();

        let mut node_need_collect = HashMap::new();
        let new_actors_location_to_broadcast = new_actors
            .iter()
            .flatten()
            .flat_map(|(worker_id, actor_infos)| {
                actor_infos
                    .iter()
                    .flat_map(|(_, (_, actors))| actors.iter())
                    .map(|actor_info| ActorInfo {
                        actor_id: actor_info.0.actor_id,
                        host: self
                            .nodes
                            .get(worker_id)
                            .expect("have checked exist previously")
                            .worker
                            .host
                            .clone(),
                    })
            })
            .collect_vec();

        self.nodes
            .iter()
            .try_for_each(|(node_id, node)| {
                let actor_ids_to_collect = node_actors
                    .get(node_id)
                    .map(|actors| actors.iter().cloned())
                    .into_iter()
                    .flatten()
                    .collect_vec();
                let is_empty = actor_ids_to_collect.is_empty();
                {
                    let mutation = mutation.clone();
                    let barrier = Barrier {
                        epoch: Some(risingwave_pb::data::Epoch {
                            curr: barrier_info.curr_epoch.value().0,
                            prev: barrier_info.prev_epoch(),
                        }),
                        mutation: mutation.clone().map(|_| BarrierMutation { mutation }),
                        tracing_context: TracingContext::from_span(barrier_info.curr_epoch.span())
                            .to_protobuf(),
                        kind: barrier_info.kind.to_protobuf() as i32,
                        passed_actors: vec![],
                    };

                    node.handle
                        .request_sender
                        .send(StreamingControlStreamRequest {
                            request: Some(
                                streaming_control_stream_request::Request::InjectBarrier(
                                    InjectBarrierRequest {
                                        request_id: Uuid::new_v4().to_string(),
                                        barrier: Some(barrier),
                                        database_id: database_id.database_id,
                                        actor_ids_to_collect,
                                        table_ids_to_sync: table_ids_to_sync
                                            .iter()
                                            .cloned()
                                            .collect(),
                                        partial_graph_id,
                                        broadcast_info: new_actors_location_to_broadcast.clone(),
                                        actors_to_build: new_actors
                                            .as_mut()
                                            .map(|new_actors| new_actors.remove(&(*node_id as _)))
                                            .into_iter()
                                            .flatten()
                                            .flatten()
                                            .map(|(fragment_id, (node, actors))| {
                                                FragmentBuildActorInfo {
                                                    fragment_id,
                                                    node: Some(node),
                                                    actors: actors
                                                        .into_iter()
                                                        .map(|(actor, upstreams, dispatchers)| {
                                                            BuildActorInfo {
                                                                actor_id: actor.actor_id,
                                                                fragment_upstreams: upstreams
                                                                    .into_iter()
                                                                    .map(|(fragment_id, upstreams)| {
                                                                        (
                                                                            fragment_id,
                                                                            UpstreamActors {
                                                                                actors: upstreams
                                                                                    .into_iter()
                                                                                    .collect(),
                                                                            },
                                                                        )
                                                                    })
                                                                    .collect(),
                                                                dispatchers,
                                                                vnode_bitmap: actor.vnode_bitmap.map(|bitmap| bitmap.to_protobuf()),
                                                                mview_definition: actor.mview_definition,
                                                                expr_context: actor.expr_context,
                                                            }
                                                        })
                                                        .collect(),
                                                }
                                            })
                                            .collect(),
                                        subscriptions_to_add: subscriptions_to_add.clone(),
                                        subscriptions_to_remove: subscriptions_to_remove.clone(),
                                    },
                                ),
                            ),
                        })
                        .map_err(|_| {
                            MetaError::from(anyhow!(
                                "failed to send request to {} {:?}",
                                node.worker.id,
                                node.worker.host
                            ))
                        })?;

                    node_need_collect.insert(*node_id as WorkerId, is_empty);
                    Result::<_, MetaError>::Ok(())
                }
            })
            .inspect_err(|e| {
                // Record failure in event log.
                use risingwave_pb::meta::event_log;
                let event = event_log::EventInjectBarrierFail {
                    prev_epoch: barrier_info.prev_epoch(),
                    cur_epoch: barrier_info.curr_epoch.value().0,
                    error: e.to_report_string(),
                };
                self.env
                    .event_log_manager_ref()
                    .add_event_logs(vec![event_log::Event::InjectBarrierFail(event)]);
            })?;
        Ok(node_need_collect)
    }

    pub(super) fn add_partial_graph(
        &mut self,
        database_id: DatabaseId,
        creating_job_id: Option<TableId>,
    ) {
        let partial_graph_id = to_partial_graph_id(creating_job_id);
        self.nodes.iter().for_each(|(_, node)| {
            if node
                .handle
                .request_sender
                .send(StreamingControlStreamRequest {
                    request: Some(
                        streaming_control_stream_request::Request::CreatePartialGraph(
                            CreatePartialGraphRequest {
                                database_id: database_id.database_id,
                                partial_graph_id,
                            },
                        ),
                    ),
                }).is_err() {
                warn!(%database_id, ?creating_job_id, worker_id = node.worker.id, "fail to add partial graph to worker")
            }
        });
    }

    pub(super) fn remove_partial_graph(
        &mut self,
        database_id: DatabaseId,
        creating_job_ids: Vec<TableId>,
    ) {
        if creating_job_ids.is_empty() {
            return;
        }
        let partial_graph_ids = creating_job_ids
            .into_iter()
            .map(|job_id| to_partial_graph_id(Some(job_id)))
            .collect_vec();
        self.nodes.iter().for_each(|(_, node)| {
            if node.handle
                .request_sender
                .send(StreamingControlStreamRequest {
                    request: Some(
                        streaming_control_stream_request::Request::RemovePartialGraph(
                            RemovePartialGraphRequest {
                                partial_graph_ids: partial_graph_ids.clone(),
                                database_id: database_id.database_id,
                            },
                        ),
                    ),
                })
                .is_err()
            {
                warn!(worker_id = node.worker.id,node = ?node.worker.host,"failed to send remove partial graph request");
            }
        })
    }

    pub(super) fn reset_database(
        &mut self,
        database_id: DatabaseId,
        reset_request_id: u32,
    ) -> HashSet<WorkerId> {
        self.nodes.iter().filter_map(|(worker_id, node)| {
            if node.handle
                .request_sender
                .send(StreamingControlStreamRequest {
                    request: Some(
                        streaming_control_stream_request::Request::ResetDatabase(
                            ResetDatabaseRequest {
                                database_id: database_id.database_id,
                                reset_request_id,
                            },
                        ),
                    ),
                })
                .is_err()
            {
                warn!(worker_id, node = ?node.worker.host,"failed to send reset database request");
                None
            } else {
                Some(*worker_id)
            }
        }).collect()
    }
}

impl GlobalBarrierWorkerContextImpl {
    pub(super) async fn new_control_stream_impl(
        &self,
        node: &WorkerNode,
        init_request: &PbInitRequest,
    ) -> MetaResult<StreamingControlHandle> {
        let handle = self
            .env
            .stream_client_pool()
            .get(node)
            .await?
            .start_streaming_control(init_request.clone())
            .await?;
        Ok(handle)
    }
}

pub(super) fn merge_node_rpc_errors<E: Error + Send + Sync + 'static>(
    message: &str,
    errors: impl IntoIterator<Item = (WorkerId, E)>,
) -> MetaError {
    use std::error::request_value;
    use std::fmt::Write;

    use risingwave_common::error::tonic::extra::Score;

    let errors = errors.into_iter().collect_vec();

    if errors.is_empty() {
        return anyhow!(message.to_owned()).into();
    }

    // Create the error from the single error.
    let single_error = |(worker_id, e)| {
        anyhow::Error::from(e)
            .context(format!("{message}, in worker node {worker_id}"))
            .into()
    };

    if errors.len() == 1 {
        return single_error(errors.into_iter().next().unwrap());
    }

    // Find the error with the highest score.
    let max_score = errors
        .iter()
        .filter_map(|(_, e)| request_value::<Score>(e))
        .max();

    if let Some(max_score) = max_score {
        let mut errors = errors;
        let max_scored = errors
            .extract_if(.., |(_, e)| request_value::<Score>(e) == Some(max_score))
            .next()
            .unwrap();

        return single_error(max_scored);
    }

    // The errors do not have scores, so simply concatenate them.
    let concat: String = errors
        .into_iter()
        .fold(format!("{message}: "), |mut s, (w, e)| {
            write!(&mut s, " in worker node {}, {};", w, e.as_report()).unwrap();
            s
        });
    anyhow!(concat).into()
}

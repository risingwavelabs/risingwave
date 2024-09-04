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

use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::future::Future;
use std::time::Duration;

use anyhow::anyhow;
use fail::fail_point;
use futures::future::try_join_all;
use futures::stream::{BoxStream, FuturesUnordered};
use futures::{pin_mut, FutureExt, StreamExt};
use itertools::Itertools;
use risingwave_common::catalog::TableId;
use risingwave_common::hash::ActorId;
use risingwave_common::util::tracing::TracingContext;
use risingwave_hummock_sdk::HummockVersionId;
use risingwave_pb::common::{ActorInfo, WorkerNode};
use risingwave_pb::stream_plan::barrier_mutation::Mutation;
use risingwave_pb::stream_plan::{Barrier, BarrierMutation};
use risingwave_pb::stream_service::build_actor_info::SubscriptionIds;
use risingwave_pb::stream_service::streaming_control_stream_request::RemovePartialGraphRequest;
use risingwave_pb::stream_service::{
    streaming_control_stream_request, streaming_control_stream_response, BarrierCompleteResponse,
    BuildActorInfo, DropActorsRequest, InjectBarrierRequest, StreamingControlStreamRequest,
    StreamingControlStreamResponse,
};
use risingwave_rpc_client::error::RpcError;
use risingwave_rpc_client::StreamClient;
use rw_futures_util::pending_on_none;
use thiserror_ext::AsReport;
use tokio::sync::mpsc::UnboundedSender;
use tokio::time::{sleep, timeout};
use tokio_retry::strategy::ExponentialBackoff;
use tracing::{error, info, warn};
use uuid::Uuid;

use super::command::CommandContext;
use super::{BarrierKind, GlobalBarrierManagerContext, TracedEpoch};
use crate::barrier::info::InflightGraphInfo;
use crate::manager::{MetaSrvEnv, WorkerId};
use crate::{MetaError, MetaResult};

const COLLECT_ERROR_TIMEOUT: Duration = Duration::from_secs(3);

struct ControlStreamNode {
    worker: WorkerNode,
    sender: UnboundedSender<StreamingControlStreamRequest>,
}

fn into_future(
    worker_id: WorkerId,
    stream: BoxStream<
        'static,
        risingwave_rpc_client::error::Result<StreamingControlStreamResponse>,
    >,
) -> ResponseStreamFuture {
    stream.into_future().map(move |(opt, stream)| {
        (
            worker_id,
            stream,
            opt.ok_or_else(|| anyhow!("end of stream").into())
                .and_then(|result| result.map_err(|e| e.into())),
        )
    })
}

type ResponseStreamFuture = impl Future<
        Output = (
            WorkerId,
            BoxStream<
                'static,
                risingwave_rpc_client::error::Result<StreamingControlStreamResponse>,
            >,
            MetaResult<StreamingControlStreamResponse>,
        ),
    > + 'static;

pub(super) struct ControlStreamManager {
    context: GlobalBarrierManagerContext,
    nodes: HashMap<WorkerId, ControlStreamNode>,
    response_streams: FuturesUnordered<ResponseStreamFuture>,
}

impl ControlStreamManager {
    pub(super) fn new(context: GlobalBarrierManagerContext) -> Self {
        Self {
            context,
            nodes: Default::default(),
            response_streams: FuturesUnordered::new(),
        }
    }

    pub(super) async fn add_worker(&mut self, node: WorkerNode) {
        if self.nodes.contains_key(&node.id) {
            warn!(id = node.id, host = ?node.host, "node already exists");
            return;
        }
        let version_id = self
            .context
            .hummock_manager
            .on_current_version(|version| version.id)
            .await;
        let node_id = node.id;
        let node_host = node.host.clone().unwrap();
        let mut backoff = ExponentialBackoff::from_millis(100)
            .max_delay(Duration::from_secs(3))
            .factor(5);
        const MAX_RETRY: usize = 5;
        for i in 1..=MAX_RETRY {
            match self
                .context
                .new_control_stream_node(node.clone(), version_id)
                .await
            {
                Ok((stream_node, response_stream)) => {
                    let _ = self.nodes.insert(node_id, stream_node);
                    self.response_streams
                        .push(into_future(node_id, response_stream));
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
        version_id: HummockVersionId,
        nodes: &HashMap<WorkerId, WorkerNode>,
    ) -> MetaResult<()> {
        let nodes = try_join_all(nodes.iter().map(|(worker_id, node)| async {
            let node = self
                .context
                .new_control_stream_node(node.clone(), version_id)
                .await?;
            Result::<_, MetaError>::Ok((*worker_id, node))
        }))
        .await?;
        self.nodes.clear();
        self.response_streams.clear();
        for (worker_id, (node, response_stream)) in nodes {
            self.nodes.insert(worker_id, node);
            self.response_streams
                .push(into_future(worker_id, response_stream));
        }

        Ok(())
    }

    /// Clear all nodes and response streams in the manager.
    pub(super) fn clear(&mut self) {
        *self = Self::new(self.context.clone());
    }

    async fn next_response(
        &mut self,
    ) -> Option<(WorkerId, MetaResult<StreamingControlStreamResponse>)> {
        let (worker_id, response_stream, result) = self.response_streams.next().await?;

        match result.as_ref().map(|r| r.response.as_ref().unwrap()) {
            Ok(streaming_control_stream_response::Response::Shutdown(_)) | Err(_) => {
                // Do not add it back to the `response_streams` so that it will not be polled again.
            }
            _ => {
                self.response_streams
                    .push(into_future(worker_id, response_stream));
            }
        }

        Some((worker_id, result))
    }

    pub(super) async fn next_complete_barrier_response(
        &mut self,
    ) -> (WorkerId, MetaResult<BarrierCompleteResponse>) {
        use streaming_control_stream_response::Response;

        {
            let (worker_id, result) = pending_on_none(self.next_response()).await;
            let result = match result {
                Ok(resp) => match resp.response.unwrap() {
                    Response::CompleteBarrier(resp) => {
                        assert_eq!(worker_id, resp.worker_id);
                        Ok(resp)
                    }
                    Response::Shutdown(_) => {
                        Err(anyhow!("worker node {worker_id} is shutting down").into())
                    }
                    Response::Init(_) => {
                        // This arm should be unreachable.
                        Err(anyhow!("get unexpected init response").into())
                    }
                },
                Err(err) => Err(err),
            };
            if let Err(err) = &result {
                let node = self
                    .nodes
                    .remove(&worker_id)
                    .expect("should exist when get shutdown resp");
                warn!(node = ?node.worker, err = %err.as_report(), "get error from response stream");
            }
            (worker_id, result)
        }
    }

    pub(super) async fn collect_errors(
        &mut self,
        worker_id: WorkerId,
        first_err: MetaError,
    ) -> Vec<(WorkerId, MetaError)> {
        let mut errors = vec![(worker_id, first_err)];
        #[cfg(not(madsim))]
        {
            let _ = timeout(COLLECT_ERROR_TIMEOUT, async {
                while let Some((worker_id, result)) = self.next_response().await {
                    if let Err(e) = result {
                        errors.push((worker_id, e));
                    }
                }
            })
            .await;
        }
        tracing::debug!(?errors, "collected stream errors");
        errors
    }
}

impl ControlStreamManager {
    pub(super) fn inject_command_ctx_barrier(
        &mut self,
        command_ctx: &CommandContext,
        pre_applied_graph_info: &InflightGraphInfo,
        applied_graph_info: Option<&InflightGraphInfo>,
        actor_ids_to_pre_sync_mutation: HashMap<WorkerId, Vec<ActorId>>,
    ) -> MetaResult<HashSet<WorkerId>> {
        self.inject_barrier(
            None,
            command_ctx.to_mutation(),
            (&command_ctx.curr_epoch, &command_ctx.prev_epoch),
            &command_ctx.kind,
            pre_applied_graph_info,
            applied_graph_info,
            actor_ids_to_pre_sync_mutation,
            command_ctx
                .command
                .actors_to_create()
                .map(|actors_to_create| {
                    actors_to_create
                        .into_iter()
                        .map(|(worker_id, actors)| {
                            (
                                worker_id,
                                actors
                                    .into_iter()
                                    .map(|actor| BuildActorInfo {
                                        actor: Some(actor),
                                        // TODO: consider subscriber of backfilling mv
                                        related_subscriptions: command_ctx
                                            .subscription_info
                                            .mv_depended_subscriptions
                                            .iter()
                                            .map(|(table_id, subscriptions)| {
                                                (
                                                    table_id.table_id,
                                                    SubscriptionIds {
                                                        subscription_ids: subscriptions
                                                            .keys()
                                                            .cloned()
                                                            .collect(),
                                                    },
                                                )
                                            })
                                            .collect(),
                                    })
                                    .collect_vec(),
                            )
                        })
                        .collect()
                }),
        )
    }

    pub(super) fn inject_barrier(
        &mut self,
        creating_table_id: Option<TableId>,
        mutation: Option<Mutation>,
        (curr_epoch, prev_epoch): (&TracedEpoch, &TracedEpoch),
        kind: &BarrierKind,
        pre_applied_graph_info: &InflightGraphInfo,
        applied_graph_info: Option<&InflightGraphInfo>,
        actor_ids_to_pre_sync_mutation: HashMap<WorkerId, Vec<ActorId>>,
        mut new_actors: Option<HashMap<WorkerId, Vec<BuildActorInfo>>>,
    ) -> MetaResult<HashSet<WorkerId>> {
        fail_point!("inject_barrier_err", |_| risingwave_common::bail!(
            "inject_barrier_err"
        ));

        let partial_graph_id = creating_table_id
            .map(|table_id| {
                assert!(actor_ids_to_pre_sync_mutation.is_empty());
                table_id.table_id
            })
            .unwrap_or(u32::MAX);

        for worker_id in pre_applied_graph_info
            .worker_ids()
            .chain(
                applied_graph_info
                    .into_iter()
                    .flat_map(|info| info.worker_ids()),
            )
            .chain(
                new_actors
                    .iter()
                    .flat_map(|new_actors| new_actors.keys().cloned()),
            )
        {
            if !self.nodes.contains_key(&worker_id) {
                return Err(anyhow!("unconnected worker node {}", worker_id).into());
            }
        }

        let mut node_need_collect = HashSet::new();
        let new_actors_location_to_broadcast = new_actors
            .iter()
            .flatten()
            .flat_map(|(worker_id, actor_infos)| {
                actor_infos.iter().map(|actor_info| ActorInfo {
                    actor_id: actor_info.actor.as_ref().unwrap().actor_id,
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
            .iter_mut()
            .map(|(node_id, node)| {
                let actor_ids_to_collect: Vec<_> = pre_applied_graph_info
                    .actor_ids_to_collect(*node_id)
                    .collect();
                let table_ids_to_sync = if let Some(graph_info) = applied_graph_info {
                    graph_info
                        .existing_table_ids()
                        .map(|table_id| table_id.table_id)
                        .collect()
                } else {
                    Default::default()
                };

                {
                    let mutation = mutation.clone();
                    let barrier = Barrier {
                        epoch: Some(risingwave_pb::data::Epoch {
                            curr: curr_epoch.value().0,
                            prev: prev_epoch.value().0,
                        }),
                        mutation: mutation.clone().map(|_| BarrierMutation { mutation }),
                        tracing_context: TracingContext::from_span(curr_epoch.span()).to_protobuf(),
                        kind: kind.to_protobuf() as i32,
                        passed_actors: vec![],
                    };

                    node.sender
                        .send(StreamingControlStreamRequest {
                            request: Some(
                                streaming_control_stream_request::Request::InjectBarrier(
                                    InjectBarrierRequest {
                                        request_id: StreamRpcManager::new_request_id(),
                                        barrier: Some(barrier),
                                        actor_ids_to_collect,
                                        table_ids_to_sync,
                                        partial_graph_id,
                                        actor_ids_to_pre_sync_barrier_mutation:
                                            actor_ids_to_pre_sync_mutation
                                                .get(node_id)
                                                .into_iter()
                                                .flatten()
                                                .cloned()
                                                .collect(),
                                        broadcast_info: new_actors_location_to_broadcast.clone(),
                                        actors_to_build: new_actors
                                            .as_mut()
                                            .map(|new_actors| new_actors.remove(node_id))
                                            .into_iter()
                                            .flatten()
                                            .flatten()
                                            .collect(),
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

                    node_need_collect.insert(*node_id);
                    Result::<_, MetaError>::Ok(())
                }
            })
            .try_collect()
            .inspect_err(|e| {
                // Record failure in event log.
                use risingwave_pb::meta::event_log;
                let event = event_log::EventInjectBarrierFail {
                    prev_epoch: prev_epoch.value().0,
                    cur_epoch: curr_epoch.value().0,
                    error: e.to_report_string(),
                };
                self.context
                    .env
                    .event_log_manager_ref()
                    .add_event_logs(vec![event_log::Event::InjectBarrierFail(event)]);
            })?;
        Ok(node_need_collect)
    }

    pub(super) fn remove_partial_graph(&mut self, partial_graph_ids: Vec<u32>) {
        self.nodes.retain(|_, node| {
            if node
                .sender
                .send(StreamingControlStreamRequest {
                    request: Some(
                        streaming_control_stream_request::Request::RemovePartialGraph(
                            RemovePartialGraphRequest { partial_graph_ids: partial_graph_ids.clone() },
                        ),
                    ),
                })
                .is_ok()
            {
                true
            } else {
                warn!(id = node.worker.id, host = ?node.worker.host, ?partial_graph_ids, "fail to remove_partial_graph request, node removed");
                false
            }
        })
    }
}

impl GlobalBarrierManagerContext {
    async fn new_control_stream_node(
        &self,
        node: WorkerNode,
        initial_version_id: HummockVersionId,
    ) -> MetaResult<(
        ControlStreamNode,
        BoxStream<'static, risingwave_rpc_client::error::Result<StreamingControlStreamResponse>>,
    )> {
        let handle = self
            .env
            .stream_client_pool()
            .get(&node)
            .await?
            .start_streaming_control(initial_version_id)
            .await?;
        Ok((
            ControlStreamNode {
                worker: node.clone(),
                sender: handle.request_sender,
            },
            handle.response_stream,
        ))
    }

    /// Send barrier-complete-rpc and wait for responses from all CNs
    pub(super) fn report_collect_failure(
        &self,
        command_context: &CommandContext,
        error: &MetaError,
    ) {
        // Record failure in event log.
        use risingwave_pb::meta::event_log;
        let event = event_log::EventCollectBarrierFail {
            prev_epoch: command_context.prev_epoch.value().0,
            cur_epoch: command_context.curr_epoch.value().0,
            error: error.to_report_string(),
        };
        self.env
            .event_log_manager_ref()
            .add_event_logs(vec![event_log::Event::CollectBarrierFail(event)]);
    }
}

#[derive(Clone)]
pub struct StreamRpcManager {
    env: MetaSrvEnv,
}

impl StreamRpcManager {
    pub fn new(env: MetaSrvEnv) -> Self {
        Self { env }
    }

    async fn make_request<REQ, RSP, Fut: Future<Output = Result<RSP, RpcError>> + 'static>(
        &self,
        request: impl Iterator<Item = (&WorkerNode, REQ)>,
        f: impl Fn(StreamClient, REQ) -> Fut,
    ) -> MetaResult<Vec<RSP>> {
        let pool = self.env.stream_client_pool();
        let f = &f;
        let iters = request.map(|(node, input)| async move {
            let client = pool.get(node).await.map_err(|e| (node.id, e))?;
            f(client, input).await.map_err(|e| (node.id, e))
        });
        let result = try_join_all_with_error_timeout(iters, COLLECT_ERROR_TIMEOUT).await;
        result.map_err(|results_err| merge_node_rpc_errors("merged RPC Error", results_err))
    }

    fn new_request_id() -> String {
        Uuid::new_v4().to_string()
    }

    pub async fn drop_actors(
        &self,
        node_map: &HashMap<WorkerId, WorkerNode>,
        node_actors: impl Iterator<Item = (WorkerId, Vec<ActorId>)>,
    ) -> MetaResult<()> {
        self.make_request(
            node_actors
                .map(|(worker_id, actor_ids)| (node_map.get(&worker_id).unwrap(), actor_ids)),
            |client, actor_ids| async move {
                client
                    .drop_actors(DropActorsRequest {
                        request_id: Self::new_request_id(),
                        actor_ids,
                    })
                    .await
            },
        )
        .await?;
        Ok(())
    }
}

/// This function is similar to `try_join_all`, but it attempts to collect as many error as possible within `error_timeout`.
async fn try_join_all_with_error_timeout<I, RSP, E, F>(
    iters: I,
    error_timeout: Duration,
) -> Result<Vec<RSP>, Vec<E>>
where
    I: IntoIterator<Item = F>,
    F: Future<Output = Result<RSP, E>>,
{
    let stream = FuturesUnordered::from_iter(iters);
    pin_mut!(stream);
    let mut results_ok = vec![];
    let mut results_err = vec![];
    while let Some(result) = stream.next().await {
        match result {
            Ok(rsp) => {
                results_ok.push(rsp);
            }
            Err(err) => {
                results_err.push(err);
                break;
            }
        }
    }
    if results_err.is_empty() {
        return Ok(results_ok);
    }
    let _ = timeout(error_timeout, async {
        while let Some(result) = stream.next().await {
            if let Err(err) = result {
                results_err.push(err);
            }
        }
    })
    .await;
    Err(results_err)
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
            .extract_if(|(_, e)| request_value::<Score>(e) == Some(max_score))
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

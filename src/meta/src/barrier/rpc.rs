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

use std::collections::{HashMap, HashSet, VecDeque};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use anyhow::anyhow;
use fail::fail_point;
use futures::future::try_join_all;
use futures::stream::{BoxStream, FuturesUnordered};
use futures::{pin_mut, FutureExt, StreamExt};
use itertools::Itertools;
use risingwave_common::bail;
use risingwave_common::hash::ActorId;
use risingwave_common::util::tracing::TracingContext;
use risingwave_pb::common::{ActorInfo, WorkerNode};
use risingwave_pb::stream_plan::{Barrier, BarrierMutation, StreamActor};
use risingwave_pb::stream_service::{
    streaming_control_stream_request, streaming_control_stream_response,
    BroadcastActorInfoTableRequest, BuildActorsRequest, DropActorsRequest, InjectBarrierRequest,
    StreamingControlStreamRequest, StreamingControlStreamResponse, UpdateActorsRequest,
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
use super::GlobalBarrierManagerContext;
use crate::manager::{MetaSrvEnv, WorkerId};
use crate::{MetaError, MetaResult};

struct ControlStreamNode {
    worker: WorkerNode,
    sender: UnboundedSender<StreamingControlStreamRequest>,
    // earlier epoch at the front
    inflight_barriers: VecDeque<Arc<CommandContext>>,
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
        let prev_epoch = self
            .context
            .hummock_manager
            .latest_snapshot()
            .committed_epoch;
        let node_id = node.id;
        let node_host = node.host.clone().unwrap();
        let mut backoff = ExponentialBackoff::from_millis(100)
            .max_delay(Duration::from_secs(3))
            .factor(5);
        const MAX_RETRY: usize = 5;
        for i in 1..=MAX_RETRY {
            match self
                .context
                .new_control_stream_node(node.clone(), prev_epoch)
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
        prev_epoch: u64,
        nodes: &HashMap<WorkerId, WorkerNode>,
    ) -> MetaResult<()> {
        let nodes = try_join_all(nodes.iter().map(|(worker_id, node)| async {
            let node = self
                .context
                .new_control_stream_node(node.clone(), prev_epoch)
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

    pub(super) async fn next_response(
        &mut self,
    ) -> MetaResult<(WorkerId, u64, StreamingControlStreamResponse)> {
        loop {
            let (worker_id, response_stream, result) =
                pending_on_none(self.response_streams.next()).await;
            match result {
                Ok(resp) => match &resp.response {
                    Some(streaming_control_stream_response::Response::CompleteBarrier(_)) => {
                        self.response_streams
                            .push(into_future(worker_id, response_stream));
                        let node = self
                            .nodes
                            .get_mut(&worker_id)
                            .expect("should exist when get collect resp");
                        let command = node
                            .inflight_barriers
                            .pop_front()
                            .expect("should exist when get collect resp");
                        break Ok((worker_id, command.prev_epoch.value().0, resp));
                    }
                    resp => {
                        break Err(anyhow!("get unexpected resp: {:?}", resp).into());
                    }
                },
                Err(err) => {
                    let mut node = self
                        .nodes
                        .remove(&worker_id)
                        .expect("should exist when get collect resp");
                    warn!(node = ?node.worker, err = ?err.as_report(), "get error from response stream");
                    if let Some(command) = node.inflight_barriers.pop_front() {
                        self.context.report_collect_failure(&command, &err);
                        break Err(err);
                    } else {
                        // for node with no inflight barrier, simply ignore the error
                        continue;
                    }
                }
            }
        }
    }
}

impl ControlStreamManager {
    /// Send inject-barrier-rpc to stream service and wait for its response before returns.
    pub(super) fn inject_barrier(
        &mut self,
        command_context: Arc<CommandContext>,
    ) -> MetaResult<HashSet<WorkerId>> {
        fail_point!("inject_barrier_err", |_| bail!("inject_barrier_err"));
        let mutation = command_context.to_mutation();
        let info = command_context.info.clone();
        let mut node_need_collect = HashSet::new();

        info.node_map
            .iter()
            .map(|(node_id, worker_node)| {
                let actor_ids_to_send = info.actor_ids_to_send(node_id).collect_vec();
                let actor_ids_to_collect = info.actor_ids_to_collect(node_id).collect_vec();
                if actor_ids_to_collect.is_empty() {
                    // No need to send or collect barrier for this node.
                    assert!(actor_ids_to_send.is_empty());
                    Ok(())
                } else {
                    let Some(node) = self.nodes.get_mut(node_id) else {
                        return Err(
                            anyhow!("unconnected worker node: {:?}", worker_node.host).into()
                        );
                    };
                    let mutation = mutation.clone();
                    let barrier = Barrier {
                        epoch: Some(risingwave_pb::data::Epoch {
                            curr: command_context.curr_epoch.value().0,
                            prev: command_context.prev_epoch.value().0,
                        }),
                        mutation: mutation.clone().map(|_| BarrierMutation { mutation }),
                        tracing_context: TracingContext::from_span(
                            command_context.curr_epoch.span(),
                        )
                        .to_protobuf(),
                        kind: command_context.kind as i32,
                        passed_actors: vec![],
                    };

                    node.sender
                        .send(StreamingControlStreamRequest {
                            request: Some(
                                streaming_control_stream_request::Request::InjectBarrier(
                                    InjectBarrierRequest {
                                        request_id: StreamRpcManager::new_request_id(),
                                        barrier: Some(barrier),
                                        actor_ids_to_send,
                                        actor_ids_to_collect,
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

                    node.inflight_barriers.push_back(command_context.clone());
                    node_need_collect.insert(*node_id);
                    Result::<_, MetaError>::Ok(())
                }
            })
            .try_collect()
            .inspect_err(|e| {
                // Record failure in event log.
                use risingwave_pb::meta::event_log;
                let event = event_log::EventInjectBarrierFail {
                    prev_epoch: command_context.prev_epoch.value().0,
                    cur_epoch: command_context.curr_epoch.value().0,
                    error: e.to_report_string(),
                };
                self.context
                    .env
                    .event_log_manager_ref()
                    .add_event_logs(vec![event_log::Event::InjectBarrierFail(event)]);
            })?;
        Ok(node_need_collect)
    }
}

impl GlobalBarrierManagerContext {
    async fn new_control_stream_node(
        &self,
        node: WorkerNode,
        prev_epoch: u64,
    ) -> MetaResult<(
        ControlStreamNode,
        BoxStream<'static, risingwave_rpc_client::error::Result<StreamingControlStreamResponse>>,
    )> {
        let handle = self
            .env
            .stream_client_pool()
            .get(&node)
            .await?
            .start_streaming_control(prev_epoch)
            .await?;
        Ok((
            ControlStreamNode {
                worker: node.clone(),
                sender: handle.request_sender,
                inflight_barriers: VecDeque::new(),
            },
            handle.response_stream,
        ))
    }

    /// Send barrier-complete-rpc and wait for responses from all CNs
    fn report_collect_failure(&self, command_context: &CommandContext, error: &MetaError) {
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
        let result = try_join_all_with_error_timeout(iters, Duration::from_secs(3)).await;
        result.map_err(|results_err| merge_node_rpc_errors("merged RPC Error", results_err))
    }

    fn new_request_id() -> String {
        Uuid::new_v4().to_string()
    }

    pub async fn build_actors(
        &self,
        node_map: &HashMap<WorkerId, WorkerNode>,
        node_actors: impl Iterator<Item = (WorkerId, Vec<ActorId>)>,
    ) -> MetaResult<()> {
        self.make_request(
            node_actors.map(|(worker_id, actors)| (node_map.get(&worker_id).unwrap(), actors)),
            |client, actors| async move {
                let request_id = Self::new_request_id();
                tracing::debug!(request_id = request_id.as_str(), actors = ?actors, "build actors");
                client
                    .build_actors(BuildActorsRequest {
                        request_id,
                        actor_id: actors,
                    })
                    .await
            },
        )
        .await?;
        Ok(())
    }

    /// Broadcast and update actor info in CN.
    /// `node_actors_to_create` must be a subset of `broadcast_worker_ids`.
    pub async fn broadcast_update_actor_info(
        &self,
        worker_nodes: &HashMap<WorkerId, WorkerNode>,
        broadcast_worker_ids: impl Iterator<Item = WorkerId>,
        actor_infos_to_broadcast: impl Iterator<Item = ActorInfo>,
        node_actors_to_create: impl Iterator<Item = (WorkerId, Vec<StreamActor>)>,
    ) -> MetaResult<()> {
        let actor_infos = actor_infos_to_broadcast.collect_vec();
        let mut node_actors_to_create = node_actors_to_create.collect::<HashMap<_, _>>();
        self.make_request(
            broadcast_worker_ids
                .map(|worker_id| {
                    let node = worker_nodes.get(&worker_id).unwrap();
                    let actors = node_actors_to_create.remove(&worker_id);
                    (node, actors)
                }),
            |client, actors| {
                let info = actor_infos.clone();
                async move {
                    client
                        .broadcast_actor_info_table(BroadcastActorInfoTableRequest { info })
                        .await?;
                    if let Some(actors) = actors {
                        let request_id = Self::new_request_id();
                        let actor_ids = actors.iter().map(|actor| actor.actor_id).collect_vec();
                        tracing::debug!(request_id = request_id.as_str(), actors = ?actor_ids, "update actors");
                        client
                            .update_actors(UpdateActorsRequest { request_id, actors })
                            .await?;
                    }
                    Ok(())
                }
            },
        )
        .await?;
        assert!(
            node_actors_to_create.is_empty(),
            "remaining uncreated actors: {:?}",
            node_actors_to_create
        );
        Ok(())
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

fn merge_node_rpc_errors(
    message: &str,
    errors: impl IntoIterator<Item = (WorkerId, RpcError)>,
) -> MetaError {
    use std::fmt::Write;

    let concat: String = errors
        .into_iter()
        .fold(format!("{message}:"), |mut s, (w, e)| {
            write!(&mut s, " worker node {}, {};", w, e.as_report()).unwrap();
            s
        });
    anyhow::anyhow!(concat).into()
}

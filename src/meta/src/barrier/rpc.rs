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

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use anyhow::anyhow;
use fail::fail_point;
use futures::future::{select, Either};
use futures::stream::FuturesUnordered;
use futures::{pin_mut, FutureExt, StreamExt};
use itertools::Itertools;
use risingwave_common::bail;
use risingwave_common::hash::ActorId;
use risingwave_common::util::tracing::TracingContext;
use risingwave_pb::common::{ActorInfo, WorkerNode};
use risingwave_pb::stream_plan::{Barrier, BarrierMutation, StreamActor};
use risingwave_pb::stream_service::{
    BarrierCompleteRequest, BroadcastActorInfoTableRequest, BuildActorsRequest, DropActorsRequest,
    ForceStopActorsRequest, InjectBarrierRequest, UpdateActorsRequest,
};
use risingwave_rpc_client::error::RpcError;
use risingwave_rpc_client::StreamClient;
use rw_futures_util::pending_on_none;
use tokio::sync::oneshot;
use uuid::Uuid;

use super::command::CommandContext;
use super::{BarrierCompletion, GlobalBarrierManagerContext};
use crate::manager::{MetaSrvEnv, WorkerId};
use crate::{MetaError, MetaResult};

pub(super) struct BarrierRpcManager {
    context: GlobalBarrierManagerContext,

    /// Futures that await on the completion of barrier.
    injected_in_progress_barrier: FuturesUnordered<BarrierCompletionFuture>,
}

impl BarrierRpcManager {
    pub(super) fn new(context: GlobalBarrierManagerContext) -> Self {
        Self {
            context,
            injected_in_progress_barrier: FuturesUnordered::new(),
        }
    }

    pub(super) fn clear(&mut self) {
        self.injected_in_progress_barrier = FuturesUnordered::new();
    }

    pub(super) async fn inject_barrier(&mut self, command_context: Arc<CommandContext>) {
        let await_complete_future = self.context.inject_barrier(command_context).await;
        self.injected_in_progress_barrier
            .push(await_complete_future);
    }

    pub(super) async fn next_complete_barrier(&mut self) -> BarrierCompletion {
        pending_on_none(self.injected_in_progress_barrier.next()).await
    }
}

pub(super) type BarrierCompletionFuture = impl Future<Output = BarrierCompletion> + Send + 'static;

impl GlobalBarrierManagerContext {
    /// Inject a barrier to all CNs and spawn a task to collect it
    pub(super) async fn inject_barrier(
        &self,
        command_context: Arc<CommandContext>,
    ) -> BarrierCompletionFuture {
        let (tx, rx) = oneshot::channel();
        let prev_epoch = command_context.prev_epoch.value().0;
        let result = self
            .stream_rpc_manager
            .inject_barrier(command_context.clone())
            .await;
        match result {
            Ok(node_need_collect) => {
                // todo: the collect handler should be abort when recovery.
                tokio::spawn({
                    let stream_rpc_manager = self.stream_rpc_manager.clone();
                    async move {
                        stream_rpc_manager
                            .collect_barrier(node_need_collect, command_context, tx)
                            .await
                    }
                });
            }
            Err(e) => {
                let _ = tx.send(BarrierCompletion {
                    prev_epoch,
                    result: Err(e),
                });
            }
        }
        rx.map(move |result| match result {
            Ok(completion) => completion,
            Err(_e) => BarrierCompletion {
                prev_epoch,
                result: Err(anyhow!("failed to receive barrier completion result").into()),
            },
        })
    }
}

impl StreamRpcManager {
    /// Send inject-barrier-rpc to stream service and wait for its response before returns.
    async fn inject_barrier(
        &self,
        command_context: Arc<CommandContext>,
    ) -> MetaResult<HashMap<WorkerId, bool>> {
        fail_point!("inject_barrier_err", |_| bail!("inject_barrier_err"));
        let mutation = command_context.to_mutation().await?;
        let info = command_context.info.clone();
        let mut node_need_collect = HashMap::new();
        self.make_request(
            info.node_map.iter().filter_map(|(node_id, node)| {
                let actor_ids_to_send = info.actor_ids_to_send(node_id).collect_vec();
                let actor_ids_to_collect = info.actor_ids_to_collect(node_id).collect_vec();
                if actor_ids_to_collect.is_empty() {
                    // No need to send or collect barrier for this node.
                    assert!(actor_ids_to_send.is_empty());
                    node_need_collect.insert(*node_id, false);
                    None
                } else {
                    node_need_collect.insert(*node_id, true);
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
                    Some((
                        node,
                        InjectBarrierRequest {
                            request_id: Self::new_request_id(),
                            barrier: Some(barrier),
                            actor_ids_to_send,
                            actor_ids_to_collect,
                        },
                    ))
                }
            }),
            |client, request| {
                async move {
                    tracing::debug!(
                        target: "events::meta::barrier::inject_barrier",
                        ?request, "inject barrier request"
                    );

                    // This RPC returns only if this worker node has injected this barrier.
                    client.inject_barrier(request).await
                }
            },
        )
        .await
        .inspect_err(|e| {
            // Record failure in event log.
            use risingwave_pb::meta::event_log;
            use thiserror_ext::AsReport;
            let event = event_log::EventInjectBarrierFail {
                prev_epoch: command_context.prev_epoch.value().0,
                cur_epoch: command_context.curr_epoch.value().0,
                error: e.to_report_string(),
            };
            self.env
                .event_log_manager_ref()
                .add_event_logs(vec![event_log::Event::InjectBarrierFail(event)]);
        })?;
        Ok(node_need_collect)
    }

    /// Send barrier-complete-rpc and wait for responses from all CNs
    async fn collect_barrier(
        &self,
        node_need_collect: HashMap<WorkerId, bool>,
        command_context: Arc<CommandContext>,
        barrier_complete_tx: oneshot::Sender<BarrierCompletion>,
    ) {
        let prev_epoch = command_context.prev_epoch.value().0;
        let tracing_context =
            TracingContext::from_span(command_context.prev_epoch.span()).to_protobuf();

        let info = command_context.info.clone();
        let result = self
            .broadcast(
                info.node_map.iter().filter_map(|(node_id, node)| {
                    if !*node_need_collect.get(node_id).unwrap() {
                        // No need to send or collect barrier for this node.
                        None
                    } else {
                        Some(node)
                    }
                }),
                |client| {
                    let tracing_context = tracing_context.clone();
                    async move {
                        let request = BarrierCompleteRequest {
                            request_id: Self::new_request_id(),
                            prev_epoch,
                            tracing_context,
                        };
                        tracing::debug!(
                            target: "events::meta::barrier::barrier_complete",
                            ?request, "barrier complete"
                        );

                        // This RPC returns only if this worker node has collected this barrier.
                        client.barrier_complete(request).await
                    }
                },
            )
            .await
            .inspect_err(|e| {
                // Record failure in event log.
                use risingwave_pb::meta::event_log;
                use thiserror_ext::AsReport;
                let event = event_log::EventCollectBarrierFail {
                    prev_epoch: command_context.prev_epoch.value().0,
                    cur_epoch: command_context.curr_epoch.value().0,
                    error: e.to_report_string(),
                };
                self.env
                    .event_log_manager_ref()
                    .add_event_logs(vec![event_log::Event::CollectBarrierFail(event)]);
            })
            .map_err(Into::into);
        let _ = barrier_complete_tx
            .send(BarrierCompletion { prev_epoch, result })
            .inspect_err(|_| tracing::warn!(prev_epoch, "failed to notify barrier completion"));
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

        // similar to join_all, but return early if a timeout occurs since the first error.
        let stream = FuturesUnordered::from_iter(iters);
        pin_mut!(stream);
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let mut results_ok = vec![];
        let mut results_err = vec![];
        let mut is_err_timeout = false;
        loop {
            let rx = rx.recv();
            pin_mut!(rx);
            match select(rx, stream.next()).await {
                Either::Left((_, _)) => {
                    break;
                }
                Either::Right((None, _)) => {
                    break;
                }
                Either::Right((Some(Ok(rsp)), _)) => {
                    results_ok.push(rsp);
                }
                Either::Right((Some(Err(err)), _)) => {
                    results_err.push(err);
                    if is_err_timeout {
                        continue;
                    }
                    is_err_timeout = true;
                    let tx = tx.clone();
                    tokio::spawn(async move {
                        tokio::time::sleep(Duration::from_secs(3)).await;
                        let _ = tx.send(());
                    });
                }
            }
        }
        if results_err.is_empty() {
            return Ok(results_ok);
        }
        let merged_error = merge_compute_node_rpc_error("merged RPC Error", results_err);
        Err(merged_error)
    }

    async fn broadcast<RSP, Fut: Future<Output = Result<RSP, RpcError>> + 'static>(
        &self,
        nodes: impl Iterator<Item = &WorkerNode>,
        f: impl Fn(StreamClient) -> Fut,
    ) -> MetaResult<Vec<RSP>> {
        self.make_request(nodes.map(|node| (node, ())), |client, ()| f(client))
            .await
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

    pub async fn force_stop_actors(
        &self,
        nodes: impl Iterator<Item = &WorkerNode>,
    ) -> MetaResult<()> {
        self.broadcast(nodes, |client| async move {
            client
                .force_stop_actors(ForceStopActorsRequest {
                    request_id: Self::new_request_id(),
                })
                .await
        })
        .await?;
        Ok(())
    }
}

fn merge_compute_node_rpc_error(
    message: &str,
    errors: impl IntoIterator<Item = (WorkerId, RpcError)>,
) -> MetaError {
    use std::fmt::Write;

    use thiserror_ext::AsReport;

    let concat: String = errors
        .into_iter()
        .fold(format!("{message}:"), |mut s, (w, e)| {
            write!(&mut s, " worker node {}, {};", w, e.as_report()).unwrap();
            s
        });
    anyhow::anyhow!(concat).into()
}

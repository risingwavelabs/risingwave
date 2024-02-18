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

use anyhow::anyhow;
use fail::fail_point;
use futures::future::try_join_all;
use futures::FutureExt;
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
use tokio::sync::oneshot;
use tracing::Instrument;
use uuid::Uuid;

use super::command::CommandContext;
use super::{BarrierCollectResult, GlobalBarrierManagerContext};
use crate::manager::{MetaSrvEnv, WorkerId};
use crate::MetaResult;

pub(super) type BarrierCollectFuture = impl Future<Output = BarrierCollectResult> + Send + 'static;

impl GlobalBarrierManagerContext {
    /// Inject a barrier to all CNs and spawn a task to collect it
    pub(super) fn inject_barrier(
        &self,
        command_context: Arc<CommandContext>,
        inject_tx: Option<oneshot::Sender<()>>,
        prev_inject_rx: Option<oneshot::Receiver<()>>,
    ) -> BarrierCollectFuture {
        let (tx, rx) = oneshot::channel();
        let prev_epoch = command_context.prev_epoch.value().0;
        let stream_rpc_manager = self.stream_rpc_manager.clone();
        // todo: the collect handler should be abort when recovery.
        let _join_handle = tokio::spawn(async move {
            let span = command_context.span.clone();
            if let Some(prev_inject_rx) = prev_inject_rx {
                if prev_inject_rx.await.is_err() {
                    let _ = tx.send(BarrierCollectResult {
                        prev_epoch,
                        result: Err(anyhow!("prev barrier failed to be injected").into()),
                    });
                    return;
                }
            }
            let result = stream_rpc_manager
                .inject_barrier(command_context.clone())
                .instrument(span.clone())
                .await;
            match result {
                Ok(node_need_collect) => {
                    if let Some(inject_tx) = inject_tx {
                        let _ = inject_tx.send(());
                    }
                    stream_rpc_manager
                        .collect_barrier(node_need_collect, command_context, tx)
                        .instrument(span.clone())
                        .await;
                }
                Err(e) => {
                    let _ = tx.send(BarrierCollectResult {
                        prev_epoch,
                        result: Err(e),
                    });
                }
            }
        });
        rx.map(move |result| match result {
            Ok(completion) => completion,
            Err(_e) => BarrierCollectResult {
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
        barrier_collect_tx: oneshot::Sender<BarrierCollectResult>,
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
        let _ = barrier_collect_tx
            .send(BarrierCollectResult { prev_epoch, result })
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
        Ok(try_join_all(request.map(|(node, input)| async move {
            let client = pool.get(node).await?;
            f(client, input).await
        }))
        .await?)
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

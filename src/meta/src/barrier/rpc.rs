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
use std::ops::Deref;
use std::sync::Arc;

use anyhow::anyhow;
use fail::fail_point;
use futures::future::try_join_all;
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use itertools::Itertools;
use risingwave_common::bail;
use risingwave_common::util::tracing::TracingContext;
use risingwave_pb::stream_plan::{Barrier, BarrierMutation};
use risingwave_pb::stream_service::{BarrierCompleteRequest, InjectBarrierRequest};
use risingwave_rpc_client::StreamClientPoolRef;
use rw_futures_util::pending_on_none;
use tokio::sync::oneshot;
use uuid::Uuid;

use super::command::CommandContext;
use super::{BarrierCompletion, GlobalBarrierManagerContext};
use crate::manager::{MetaSrvEnv, WorkerId};
use crate::MetaResult;

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
        let result = self.inject_barrier_inner(command_context.clone()).await;
        match result {
            Ok(node_need_collect) => {
                // todo: the collect handler should be abort when recovery.
                tokio::spawn(Self::collect_barrier(
                    self.env.clone(),
                    node_need_collect,
                    self.env.stream_client_pool_ref(),
                    command_context,
                    tx,
                ));
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
            Err(e) => BarrierCompletion {
                prev_epoch,
                result: Err(anyhow!("failed to receive barrier completion result: {:?}", e).into()),
            },
        })
    }

    /// Send inject-barrier-rpc to stream service and wait for its response before returns.
    async fn inject_barrier_inner(
        &self,
        command_context: Arc<CommandContext>,
    ) -> MetaResult<HashMap<WorkerId, bool>> {
        fail_point!("inject_barrier_err", |_| bail!("inject_barrier_err"));
        let mutation = command_context.to_mutation().await?;
        let info = command_context.info.clone();
        let mut node_need_collect = HashMap::new();
        let inject_futures = info.node_map.iter().filter_map(|(node_id, node)| {
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
                let request_id = Uuid::new_v4().to_string();
                let barrier = Barrier {
                    epoch: Some(risingwave_pb::data::Epoch {
                        curr: command_context.curr_epoch.value().0,
                        prev: command_context.prev_epoch.value().0,
                    }),
                    mutation: mutation.clone().map(|_| BarrierMutation { mutation }),
                    tracing_context: TracingContext::from_span(command_context.curr_epoch.span())
                        .to_protobuf(),
                    kind: command_context.kind as i32,
                    passed_actors: vec![],
                };
                async move {
                    let client = self.env.stream_client_pool().get(node).await?;

                    let request = InjectBarrierRequest {
                        request_id,
                        barrier: Some(barrier),
                        actor_ids_to_send,
                        actor_ids_to_collect,
                    };
                    tracing::debug!(
                        target: "events::meta::barrier::inject_barrier",
                        ?request, "inject barrier request"
                    );

                    // This RPC returns only if this worker node has injected this barrier.
                    client.inject_barrier(request).await
                }
                .into()
            }
        });
        try_join_all(inject_futures).await.inspect_err(|e| {
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
        env: MetaSrvEnv,
        node_need_collect: HashMap<WorkerId, bool>,
        client_pool_ref: StreamClientPoolRef,
        command_context: Arc<CommandContext>,
        barrier_complete_tx: oneshot::Sender<BarrierCompletion>,
    ) {
        let prev_epoch = command_context.prev_epoch.value().0;
        let tracing_context =
            TracingContext::from_span(command_context.prev_epoch.span()).to_protobuf();

        let info = command_context.info.clone();
        let client_pool = client_pool_ref.deref();
        let collect_futures = info.node_map.iter().filter_map(|(node_id, node)| {
            if !*node_need_collect.get(node_id).unwrap() {
                // No need to send or collect barrier for this node.
                None
            } else {
                let request_id = Uuid::new_v4().to_string();
                let tracing_context = tracing_context.clone();
                async move {
                    let client = client_pool.get(node).await?;
                    let request = BarrierCompleteRequest {
                        request_id,
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
                .into()
            }
        });

        let result = try_join_all(collect_futures)
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
                env.event_log_manager_ref()
                    .add_event_logs(vec![event_log::Event::CollectBarrierFail(event)]);
            })
            .map_err(Into::into);
        let _ = barrier_complete_tx
            .send(BarrierCompletion { prev_epoch, result })
            .inspect_err(|_| tracing::warn!(prev_epoch, "failed to notify barrier completion"));
    }
}

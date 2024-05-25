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

use core::time::Duration;
use std::collections::HashSet;
use std::fmt::Debug;
use std::mem::take;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::Instant;

use anyhow::anyhow;
use async_recursion::async_recursion;
use futures::stream::BoxStream;
use futures::FutureExt;
use itertools::Itertools;
use risingwave_common::bail;
use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::{Field, Schema, TableId};
use risingwave_common::config::MetricLevel;
use risingwave_pb::common::ActorInfo;
use risingwave_pb::stream_plan;
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_plan::StreamNode;
use risingwave_pb::stream_service::streaming_control_stream_request::InitRequest;
use risingwave_pb::stream_service::{
    BuildActorInfo, StreamingControlStreamRequest, StreamingControlStreamResponse,
};
use risingwave_storage::monitor::HummockTraceFutureExt;
use risingwave_storage::{dispatch_state_store, StateStore};
use rw_futures_util::AttachedFuture;
use thiserror_ext::AsReport;
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tonic::Status;

use super::{unique_executor_id, unique_operator_id};
use crate::error::StreamResult;
use crate::executor::exchange::permit::Receiver;
use crate::executor::monitor::StreamingMetrics;
use crate::executor::subtask::SubtaskHandle;
use crate::executor::{
    Actor, ActorContext, ActorContextRef, DispatchExecutor, DispatcherImpl, Executor, ExecutorInfo,
    WrapperExecutor,
};
use crate::from_proto::create_executor;
use crate::task::barrier_manager::{
    ControlStreamHandle, CreateActorOutput, EventSender, LocalActorOperation, LocalBarrierWorker,
};
use crate::task::{
    ActorId, CreateActorContext, FragmentId, LocalBarrierManager, SharedContext,
    StreamActorManager, StreamActorManagerState, StreamEnvironment, UpDownActorIds,
};

#[cfg(test)]
pub static LOCAL_TEST_ADDR: std::sync::LazyLock<risingwave_common::util::addr::HostAddr> =
    std::sync::LazyLock::new(|| "127.0.0.1:2333".parse().unwrap());

pub type ActorHandle = JoinHandle<()>;

pub type AtomicU64Ref = Arc<AtomicU64>;

pub mod await_tree_key {
    /// Await-tree key type for actors.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub struct Actor(pub crate::task::ActorId);

    /// Await-tree key type for barriers.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub struct BarrierAwait {
        pub prev_epoch: u64,
    }
}

/// `LocalStreamManager` manages all stream executors in this project.
#[derive(Clone)]
pub struct LocalStreamManager {
    await_tree_reg: Option<await_tree::Registry>,

    pub env: StreamEnvironment,

    actor_op_tx: EventSender<LocalActorOperation>,
}

/// Report expression evaluation errors to the actor context.
///
/// The struct can be cheaply cloned.
#[derive(Clone)]
pub struct ActorEvalErrorReport {
    pub actor_context: ActorContextRef,
    pub identity: Arc<str>,
}

impl risingwave_expr::expr::EvalErrorReport for ActorEvalErrorReport {
    fn report(&self, err: risingwave_expr::ExprError) {
        self.actor_context.on_compute_error(err, &self.identity);
    }
}

pub struct ExecutorParams {
    pub env: StreamEnvironment,

    /// Basic information about the executor.
    pub info: ExecutorInfo,

    /// Executor id, unique across all actors.
    pub executor_id: u64,

    /// Operator id, unique for each operator in fragment.
    pub operator_id: u64,

    /// Information of the operator from plan node, like `StreamHashJoin { .. }`.
    // TODO: use it for `identity`
    pub op_info: String,

    /// The input executor.
    pub input: Vec<Executor>,

    /// `FragmentId` of the actor
    pub fragment_id: FragmentId,

    /// Metrics
    pub executor_stats: Arc<StreamingMetrics>,

    /// Actor context
    pub actor_context: ActorContextRef,

    /// Vnodes owned by this executor. Represented in bitmap.
    pub vnode_bitmap: Option<Bitmap>,

    /// Used for reporting expression evaluation errors.
    pub eval_error_report: ActorEvalErrorReport,

    /// `watermark_epoch` field in `MemoryManager`
    pub watermark_epoch: AtomicU64Ref,

    pub shared_context: Arc<SharedContext>,

    pub local_barrier_manager: LocalBarrierManager,

    pub create_actor_context: CreateActorContext,
}

impl Debug for ExecutorParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExecutorParams")
            .field("info", &self.info)
            .field("executor_id", &self.executor_id)
            .field("operator_id", &self.operator_id)
            .field("op_info", &self.op_info)
            .field("input", &self.input.len())
            .field("actor_id", &self.actor_context.id)
            .finish_non_exhaustive()
    }
}

impl LocalStreamManager {
    pub fn new(
        env: StreamEnvironment,
        streaming_metrics: Arc<StreamingMetrics>,
        await_tree_config: Option<await_tree::Config>,
        watermark_epoch: AtomicU64Ref,
    ) -> Self {
        if !env.config().unsafe_enable_strict_consistency {
            // If strict consistency is disabled, should disable storage sanity check.
            // Since this is a special config, we have to check it here.
            risingwave_storage::hummock::utils::disable_sanity_check();
        }

        let await_tree_reg = await_tree_config.clone().map(await_tree::Registry::new);

        let (actor_op_tx, actor_op_rx) = unbounded_channel();

        let _join_handle = LocalBarrierWorker::spawn(
            env.clone(),
            streaming_metrics,
            await_tree_reg.clone(),
            watermark_epoch,
            actor_op_rx,
        );
        Self {
            await_tree_reg,
            env,
            actor_op_tx: EventSender(actor_op_tx),
        }
    }

    /// Get the registry of await-trees.
    pub fn await_tree_reg(&self) -> Option<&await_tree::Registry> {
        self.await_tree_reg.as_ref()
    }

    /// Receive a new control stream request from meta. Notify the barrier worker to reset the CN and use the new control stream
    /// to receive control message from meta
    pub fn handle_new_control_stream(
        &self,
        sender: UnboundedSender<Result<StreamingControlStreamResponse, Status>>,
        request_stream: BoxStream<'static, Result<StreamingControlStreamRequest, Status>>,
        init_request: InitRequest,
    ) {
        self.actor_op_tx
            .send_event(LocalActorOperation::NewControlStream {
                handle: ControlStreamHandle::new(sender, request_stream),
                init_request,
            })
    }

    /// Drop the resources of the given actors.
    pub async fn drop_actors(&self, actors: Vec<ActorId>) -> StreamResult<()> {
        self.actor_op_tx
            .send_and_await(|result_sender| LocalActorOperation::DropActors {
                actors,
                result_sender,
            })
            .await
    }

    pub async fn update_actors(&self, actors: Vec<BuildActorInfo>) -> StreamResult<()> {
        self.actor_op_tx
            .send_and_await(|result_sender| LocalActorOperation::UpdateActors {
                actors,
                result_sender,
            })
            .await?
    }

    pub async fn build_actors(&self, actors: Vec<ActorId>) -> StreamResult<()> {
        self.actor_op_tx
            .send_and_await(|result_sender| LocalActorOperation::BuildActors {
                actors,
                result_sender,
            })
            .await?
    }

    pub async fn update_actor_info(&self, new_actor_infos: Vec<ActorInfo>) -> StreamResult<()> {
        self.actor_op_tx
            .send_and_await(|result_sender| LocalActorOperation::UpdateActorInfo {
                new_actor_infos,
                result_sender,
            })
            .await?
    }

    pub async fn take_receiver(&self, ids: UpDownActorIds) -> StreamResult<Receiver> {
        self.actor_op_tx
            .send_and_await(|result_sender| LocalActorOperation::TakeReceiver {
                ids,
                result_sender,
            })
            .await?
    }

    pub async fn inspect_barrier_state(&self) -> StreamResult<String> {
        info!("start inspecting barrier state");
        let start = Instant::now();
        self.actor_op_tx
            .send_and_await(|result_sender| LocalActorOperation::InspectState { result_sender })
            .inspect(|result| {
                info!(
                    ok = result.is_ok(),
                    time = ?start.elapsed(),
                    "finish inspecting barrier state"
                );
            })
            .await
    }
}

impl LocalBarrierWorker {
    /// Drop the resources of the given actors.
    pub(super) fn drop_actors(&mut self, actors: &[ActorId]) {
        self.current_shared_context.drop_actors(actors);
        for &id in actors {
            self.actor_manager_state.drop_actor(id);
        }
        tracing::debug!(actors = ?actors, "drop actors");
    }

    /// Force stop all actors on this worker, and then drop their resources.
    pub(super) async fn reset(&mut self, prev_epoch: u64) {
        let actor_handles = self.actor_manager_state.drain_actor_handles();
        for (actor_id, handle) in &actor_handles {
            tracing::debug!("force stopping actor {}", actor_id);
            handle.abort();
        }
        for (actor_id, handle) in actor_handles {
            tracing::debug!("join actor {}", actor_id);
            let result = handle.await;
            assert!(result.is_ok() || result.unwrap_err().is_cancelled());
        }
        // Clear the join handle of creating actors
        for handle in take(&mut self.actor_manager_state.creating_actors)
            .into_iter()
            .map(|attached_future| attached_future.into_inner().0)
        {
            handle.abort();
            let result = handle.await;
            assert!(result.is_ok() || result.err().unwrap().is_cancelled());
        }
        self.actor_manager_state.clear_state();
        if let Some(m) = self.actor_manager.await_tree_reg.as_ref() {
            m.clear();
        }
        dispatch_state_store!(&self.actor_manager.env.state_store(), store, {
            store.clear_shared_buffer(prev_epoch).await;
        });
        self.reset_state();
        self.actor_manager.env.dml_manager_ref().clear();
    }

    pub(super) fn update_actors(&mut self, actors: Vec<BuildActorInfo>) -> StreamResult<()> {
        self.actor_manager_state.update_actors(actors)
    }

    /// This function could only be called once during the lifecycle of `LocalStreamManager` for
    /// now.
    pub(super) fn start_create_actors(
        &mut self,
        actors: &[ActorId],
        result_sender: oneshot::Sender<StreamResult<()>>,
    ) {
        let actors: Vec<_> = {
            let actor_result = actors
                .iter()
                .map(|actor_id| {
                    self.actor_manager_state
                        .actors
                        .remove(actor_id)
                        .ok_or_else(|| anyhow!("No such actor with actor id:{}", actor_id))
                })
                .try_collect();
            match actor_result {
                Ok(actors) => actors,
                Err(e) => {
                    let _ = result_sender.send(Err(e.into()));
                    return;
                }
            }
        };
        let actor_ids = actors
            .iter()
            .map(|actor| actor.actor.as_ref().unwrap().actor_id)
            .collect();
        let actor_manager = self.actor_manager.clone();
        let create_actors_fut = crate::CONFIG.scope(
            self.actor_manager.env.config().clone(),
            actor_manager.create_actors(actors, self.current_shared_context.clone()),
        );
        let join_handle = self.actor_manager.runtime.spawn(create_actors_fut);
        self.actor_manager_state
            .creating_actors
            .push(AttachedFuture::new(join_handle, (actor_ids, result_sender)));
    }
}

impl StreamActorManager {
    /// Create dispatchers with downstream information registered before
    fn create_dispatcher(
        &self,
        env: StreamEnvironment,
        input: Executor,
        dispatchers: &[stream_plan::Dispatcher],
        actor_id: ActorId,
        fragment_id: FragmentId,
        shared_context: &Arc<SharedContext>,
    ) -> StreamResult<DispatchExecutor> {
        let dispatcher_impls = dispatchers
            .iter()
            .map(|dispatcher| DispatcherImpl::new(shared_context, actor_id, dispatcher))
            .try_collect()?;

        Ok(DispatchExecutor::new(
            input,
            dispatcher_impls,
            actor_id,
            fragment_id,
            shared_context.clone(),
            self.streaming_metrics.clone(),
            env.config().developer.chunk_size,
        ))
    }

    /// Create a chain(tree) of nodes, with given `store`.
    #[allow(clippy::too_many_arguments)]
    #[async_recursion]
    async fn create_nodes_inner(
        &self,
        fragment_id: FragmentId,
        node: &stream_plan::StreamNode,
        env: StreamEnvironment,
        store: impl StateStore,
        actor_context: &ActorContextRef,
        vnode_bitmap: Option<Bitmap>,
        has_stateful: bool,
        subtasks: &mut Vec<SubtaskHandle>,
        shared_context: &Arc<SharedContext>,
        create_actor_context: &CreateActorContext,
    ) -> StreamResult<Executor> {
        // The "stateful" here means that the executor may issue read operations to the state store
        // massively and continuously. Used to decide whether to apply the optimization of subtasks.
        fn is_stateful_executor(stream_node: &StreamNode) -> bool {
            matches!(
                stream_node.get_node_body().unwrap(),
                NodeBody::HashAgg(_)
                    | NodeBody::HashJoin(_)
                    | NodeBody::DeltaIndexJoin(_)
                    | NodeBody::Lookup(_)
                    | NodeBody::StreamScan(_)
                    | NodeBody::StreamCdcScan(_)
                    | NodeBody::DynamicFilter(_)
                    | NodeBody::GroupTopN(_)
                    | NodeBody::Now(_)
            )
        }
        let is_stateful = is_stateful_executor(node);

        // Create the input executor before creating itself
        let mut input = Vec::with_capacity(node.input.iter().len());
        for input_stream_node in &node.input {
            input.push(
                self.create_nodes_inner(
                    fragment_id,
                    input_stream_node,
                    env.clone(),
                    store.clone(),
                    actor_context,
                    vnode_bitmap.clone(),
                    has_stateful || is_stateful,
                    subtasks,
                    shared_context,
                    create_actor_context,
                )
                .await?,
            );
        }

        let op_info = node.get_identity().clone();
        let pk_indices = node
            .get_stream_key()
            .iter()
            .map(|idx| *idx as usize)
            .collect::<Vec<_>>();

        // We assume that the operator_id of different instances from the same RelNode will be the
        // same.
        let executor_id = unique_executor_id(actor_context.id, node.operator_id);
        let operator_id = unique_operator_id(fragment_id, node.operator_id);
        let schema: Schema = node.fields.iter().map(Field::from).collect();

        let identity = format!("{} {:X}", node.get_node_body().unwrap(), executor_id);
        let info = ExecutorInfo {
            schema,
            pk_indices,
            identity,
        };

        let eval_error_report = ActorEvalErrorReport {
            actor_context: actor_context.clone(),
            identity: info.identity.clone().into(),
        };

        // Build the executor with params.
        let executor_params = ExecutorParams {
            env: env.clone(),

            info: info.clone(),
            executor_id,
            operator_id,
            op_info,
            input,
            fragment_id,
            executor_stats: self.streaming_metrics.clone(),
            actor_context: actor_context.clone(),
            vnode_bitmap,
            eval_error_report,
            watermark_epoch: self.watermark_epoch.clone(),
            shared_context: shared_context.clone(),
            local_barrier_manager: shared_context.local_barrier_manager.clone(),
            create_actor_context: create_actor_context.clone(),
        };

        let executor = create_executor(executor_params, node, store).await?;

        // Wrap the executor for debug purpose.
        let wrapped = WrapperExecutor::new(
            executor,
            actor_context.clone(),
            env.config().developer.enable_executor_row_count,
        );
        let executor = (info, wrapped).into();

        // If there're multiple stateful executors in this actor, we will wrap it into a subtask.
        let executor = if has_stateful && is_stateful {
            // TODO(bugen): subtask does not work with tracing spans.
            // let (subtask, executor) = subtask::wrap(executor, actor_context.id);
            // subtasks.push(subtask);
            // executor.boxed()

            let _ = subtasks;
            executor
        } else {
            executor
        };

        Ok(executor)
    }

    /// Create a chain(tree) of nodes and return the head executor.
    #[expect(clippy::too_many_arguments)]
    async fn create_nodes(
        &self,
        fragment_id: FragmentId,
        node: &stream_plan::StreamNode,
        env: StreamEnvironment,
        actor_context: &ActorContextRef,
        vnode_bitmap: Option<Bitmap>,
        shared_context: &Arc<SharedContext>,
        create_actor_context: &CreateActorContext,
    ) -> StreamResult<(Executor, Vec<SubtaskHandle>)> {
        let mut subtasks = vec![];

        let executor = dispatch_state_store!(env.state_store(), store, {
            self.create_nodes_inner(
                fragment_id,
                node,
                env,
                store,
                actor_context,
                vnode_bitmap,
                false,
                &mut subtasks,
                shared_context,
                create_actor_context,
            )
            .await
        })?;

        Ok((executor, subtasks))
    }

    async fn create_actors(
        self: Arc<Self>,
        actors: Vec<BuildActorInfo>,
        shared_context: Arc<SharedContext>,
    ) -> StreamResult<CreateActorOutput> {
        let mut ret = Vec::with_capacity(actors.len());
        let create_actor_context = CreateActorContext::default();
        for actor in actors {
            let BuildActorInfo {
                actor,
                related_subscriptions,
            } = actor;
            let actor = actor.unwrap();
            let actor_id = actor.actor_id;
            let actor_context = ActorContext::create(
                &actor,
                self.env.total_mem_usage(),
                self.streaming_metrics.clone(),
                actor.dispatcher.len(),
                related_subscriptions
                    .into_iter()
                    .map(|(table_id, subscription_ids)| {
                        (
                            TableId::new(table_id),
                            HashSet::from_iter(subscription_ids.subscription_ids),
                        )
                    })
                    .collect(),
            );
            let vnode_bitmap = actor.vnode_bitmap.as_ref().map(|b| b.into());
            let expr_context = actor.expr_context.clone().unwrap();

            let (executor, subtasks) = self
                .create_nodes(
                    actor.fragment_id,
                    actor.get_nodes()?,
                    self.env.clone(),
                    &actor_context,
                    vnode_bitmap,
                    &shared_context,
                    &create_actor_context,
                )
                // If hummock tracing is not enabled, it directly returns wrapped future.
                .may_trace_hummock()
                .await?;

            let dispatcher = self.create_dispatcher(
                self.env.clone(),
                executor,
                &actor.dispatcher,
                actor_id,
                actor.fragment_id,
                &shared_context,
            )?;
            let actor = Actor::new(
                dispatcher,
                subtasks,
                self.streaming_metrics.clone(),
                actor_context.clone(),
                expr_context,
                shared_context.local_barrier_manager.clone(),
            );

            ret.push(actor);
        }
        Ok(CreateActorOutput {
            actors: ret,
            senders: create_actor_context.collect_senders(),
        })
    }
}

impl LocalBarrierWorker {
    pub(super) fn spawn_actors(&mut self, actors: Vec<Actor<DispatchExecutor>>) {
        for actor in actors {
            let monitor = tokio_metrics::TaskMonitor::new();
            let actor_context = actor.actor_context.clone();
            let actor_id = actor_context.id;

            let handle = {
                let trace_span = format!("Actor {actor_id}: `{}`", actor_context.mview_definition);
                let barrier_manager = self.current_shared_context.local_barrier_manager.clone();
                let actor = actor.run().map(move |result| {
                    if let Err(err) = result {
                        // TODO: check error type and panic if it's unexpected.
                        // Intentionally use `?` on the report to also include the backtrace.
                        tracing::error!(actor_id, error = ?err.as_report(), "actor exit with error");
                        barrier_manager.notify_failure(actor_id, err);
                    }
                });
                let traced = match &self.actor_manager.await_tree_reg {
                    Some(m) => m
                        .register(await_tree_key::Actor(actor_id), trace_span)
                        .instrument(actor)
                        .left_future(),
                    None => actor.right_future(),
                };
                let instrumented = monitor.instrument(traced);
                let with_config =
                    crate::CONFIG.scope(self.actor_manager.env.config().clone(), instrumented);

                self.actor_manager.runtime.spawn(with_config)
            };
            self.actor_manager_state.handles.insert(actor_id, handle);

            if self.actor_manager.streaming_metrics.level >= MetricLevel::Debug {
                tracing::info!("Tokio metrics are enabled because metrics_level >= Debug");
                let actor_id_str = actor_id.to_string();
                let metrics = self.actor_manager.streaming_metrics.clone();
                let actor_monitor_task = self.actor_manager.runtime.spawn(async move {
                    loop {
                        let task_metrics = monitor.cumulative();
                        metrics
                            .actor_execution_time
                            .with_label_values(&[&actor_id_str])
                            .set(task_metrics.total_poll_duration.as_secs_f64());
                        metrics
                            .actor_fast_poll_duration
                            .with_label_values(&[&actor_id_str])
                            .set(task_metrics.total_fast_poll_duration.as_secs_f64());
                        metrics
                            .actor_fast_poll_cnt
                            .with_label_values(&[&actor_id_str])
                            .set(task_metrics.total_fast_poll_count as i64);
                        metrics
                            .actor_slow_poll_duration
                            .with_label_values(&[&actor_id_str])
                            .set(task_metrics.total_slow_poll_duration.as_secs_f64());
                        metrics
                            .actor_slow_poll_cnt
                            .with_label_values(&[&actor_id_str])
                            .set(task_metrics.total_slow_poll_count as i64);
                        metrics
                            .actor_poll_duration
                            .with_label_values(&[&actor_id_str])
                            .set(task_metrics.total_poll_duration.as_secs_f64());
                        metrics
                            .actor_poll_cnt
                            .with_label_values(&[&actor_id_str])
                            .set(task_metrics.total_poll_count as i64);
                        metrics
                            .actor_idle_duration
                            .with_label_values(&[&actor_id_str])
                            .set(task_metrics.total_idle_duration.as_secs_f64());
                        metrics
                            .actor_idle_cnt
                            .with_label_values(&[&actor_id_str])
                            .set(task_metrics.total_idled_count as i64);
                        metrics
                            .actor_scheduled_duration
                            .with_label_values(&[&actor_id_str])
                            .set(task_metrics.total_scheduled_duration.as_secs_f64());
                        metrics
                            .actor_scheduled_cnt
                            .with_label_values(&[&actor_id_str])
                            .set(task_metrics.total_scheduled_count as i64);
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                });
                self.actor_manager_state
                    .actor_monitor_tasks
                    .insert(actor_id, actor_monitor_task);
            }
        }
    }
}

impl LocalBarrierWorker {
    /// This function could only be called once during the lifecycle of `LocalStreamManager` for
    /// now.
    pub fn update_actor_info(&self, new_actor_infos: Vec<ActorInfo>) -> StreamResult<()> {
        let mut actor_infos = self.current_shared_context.actor_infos.write();
        for actor in new_actor_infos {
            if let Some(prev_actor) = actor_infos.get(&actor.get_actor_id())
                && &actor != prev_actor
            {
                bail!(
                    "actor info mismatch when broadcasting {}",
                    actor.get_actor_id()
                );
            }
            actor_infos.insert(actor.get_actor_id(), actor);
        }
        Ok(())
    }
}

impl StreamActorManagerState {
    /// `drop_actor` is invoked by meta node via RPC once the stop barrier arrives at the
    /// sink. All the actors in the actors should stop themselves before this method is invoked.
    fn drop_actor(&mut self, actor_id: ActorId) {
        self.actor_monitor_tasks
            .remove(&actor_id)
            .inspect(|handle| handle.abort());
        self.actors.remove(&actor_id);

        // Task should have already stopped when this method is invoked. There might be some
        // clean-up work left (like dropping in-memory data structures), but we don't have to wait
        // for them to finish, in order to make this request non-blocking.
        self.handles.remove(&actor_id);
    }

    fn drain_actor_handles(&mut self) -> Vec<(ActorId, ActorHandle)> {
        self.handles.drain().collect()
    }

    /// `stop_all_actors` is invoked by meta node via RPC for recovery purpose. Different from the
    /// `drop_actor`, the execution of the actors will be aborted.
    fn clear_state(&mut self) {
        self.actors.clear();
        self.actor_monitor_tasks.clear();
    }

    fn update_actors(&mut self, actors: Vec<BuildActorInfo>) -> StreamResult<()> {
        for actor in actors {
            let actor_id = actor.actor.as_ref().unwrap().get_actor_id();
            self.actors
                .try_insert(actor_id, actor)
                .map_err(|_| anyhow!("duplicated actor {}", actor_id))?;
        }

        Ok(())
    }
}

#[cfg(test)]
pub mod test_utils {
    use risingwave_pb::common::HostAddress;

    use super::*;

    pub fn helper_make_local_actor(actor_id: u32) -> ActorInfo {
        ActorInfo {
            actor_id,
            host: Some(HostAddress {
                host: LOCAL_TEST_ADDR.host.clone(),
                port: LOCAL_TEST_ADDR.port as i32,
            }),
        }
    }
}

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

use core::time::Duration;
use std::fmt::Debug;
use std::sync::Arc;

use async_recursion::async_recursion;
use futures::{FutureExt, TryFutureExt};
use itertools::Itertools;
use risingwave_common::bitmap::Bitmap;
use risingwave_common::catalog::{ColumnId, Field, Schema};
use risingwave_common::config::{MetricLevel, StreamingConfig, merge_streaming_config_section};
use risingwave_common::must_match;
use risingwave_common::operator::{unique_executor_id, unique_operator_id};
use risingwave_common::util::runtime::BackgroundShutdownRuntime;
use risingwave_pb::plan_common::StorageTableDesc;
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_plan::{self, StreamNode, StreamScanNode, StreamScanType};
use risingwave_pb::stream_service::inject_barrier_request::BuildActorInfo;
use risingwave_storage::monitor::HummockTraceFutureExt;
use risingwave_storage::table::batch_table::BatchTable;
use risingwave_storage::{StateStore, dispatch_state_store};
use thiserror_ext::AsReport;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::task::JoinHandle;

use crate::common::table::state_table::StateTableBuilder;
use crate::error::StreamResult;
use crate::executor::monitor::StreamingMetrics;
use crate::executor::subtask::SubtaskHandle;
use crate::executor::{
    Actor, ActorContext, ActorContextRef, DispatchExecutor, Execute, Executor, ExecutorInfo,
    MergeExecutorInput, SnapshotBackfillExecutor, TroublemakerExecutor, WrapperExecutor,
};
use crate::from_proto::{MergeExecutorBuilder, create_executor};
use crate::task::{
    ActorEvalErrorReport, ActorId, AtomicU64Ref, FragmentId, LocalBarrierManager, NewOutputRequest,
    StreamEnvironment, await_tree_key,
};

/// Default capacity for `ConfigOverrideCache`.
/// Since we only support per-job config override right now, 256 jobs should be sufficient on a single node.
pub const CONFIG_OVERRIDE_CACHE_DEFAULT_CAPACITY: u64 = 256;
pub type ConfigOverrideCache = moka::sync::Cache<String, Arc<StreamingConfig>>;

/// [Spawning actors](`Self::spawn_actor`), called by [`crate::task::barrier_worker::managed_state::DatabaseManagedBarrierState`].
///
/// See [`crate::task`] for architecture overview.
pub(crate) struct StreamActorManager {
    pub(super) env: StreamEnvironment,
    pub(super) streaming_metrics: Arc<StreamingMetrics>,

    /// Watermark epoch number.
    pub(super) watermark_epoch: AtomicU64Ref,

    /// Manages the await-trees of all actors.
    pub(super) await_tree_reg: Option<await_tree::Registry>,

    /// Runtime for the streaming actors.
    pub(super) runtime: BackgroundShutdownRuntime,

    /// Cache for overridden configuration: `config_override` -> `StreamingConfig`.
    ///
    /// Since the override is based on `env.global_config`, which won't change after creation,
    /// we can use the `config_override` as the only key.
    pub(super) config_override_cache: ConfigOverrideCache,
}

impl StreamActorManager {
    fn get_executor_id(actor_context: &ActorContext, node: &StreamNode) -> u64 {
        // We assume that the operator_id of different instances from the same RelNode will be the
        // same.
        unique_executor_id(actor_context.id, node.operator_id)
    }

    fn get_executor_info(node: &StreamNode, executor_id: u64) -> ExecutorInfo {
        let schema: Schema = node.fields.iter().map(Field::from).collect();

        let stream_key = node
            .get_stream_key()
            .iter()
            .map(|idx| *idx as usize)
            .collect::<Vec<_>>();

        let stream_kind = node.stream_kind();

        let identity = format!("{} {:X}", node.get_node_body().unwrap(), executor_id);

        ExecutorInfo {
            schema,
            stream_key,
            stream_kind,
            identity,
            id: executor_id,
        }
    }

    async fn create_snapshot_backfill_input(
        &self,
        upstream_node: &StreamNode,
        actor_context: &ActorContextRef,
        local_barrier_manager: &LocalBarrierManager,
        chunk_size: usize,
    ) -> StreamResult<MergeExecutorInput> {
        let info = Self::get_executor_info(
            upstream_node,
            Self::get_executor_id(actor_context, upstream_node),
        );

        let upstream_merge = must_match!(upstream_node.get_node_body().unwrap(), NodeBody::Merge(upstream_merge) => {
            upstream_merge
        });

        MergeExecutorBuilder::new_input(
            local_barrier_manager.clone(),
            self.streaming_metrics.clone(),
            actor_context.clone(),
            info,
            upstream_merge,
            chunk_size,
        )
        .await
    }

    async fn create_snapshot_backfill_node(
        &self,
        stream_node: &StreamNode,
        node: &StreamScanNode,
        actor_context: &ActorContextRef,
        vnode_bitmap: Option<Bitmap>,
        local_barrier_manager: &LocalBarrierManager,
        state_store: impl StateStore,
    ) -> StreamResult<Executor> {
        let [upstream_node, _]: &[_; 2] = stream_node.input.as_slice().try_into().unwrap();
        let chunk_size = actor_context.config.developer.chunk_size;
        let upstream = self
            .create_snapshot_backfill_input(
                upstream_node,
                actor_context,
                local_barrier_manager,
                chunk_size,
            )
            .await?;

        let table_desc: &StorageTableDesc = node.get_table_desc()?;

        let output_indices = node
            .output_indices
            .iter()
            .map(|&i| i as usize)
            .collect_vec();

        let column_ids = node
            .upstream_column_ids
            .iter()
            .map(ColumnId::from)
            .collect_vec();

        let progress = local_barrier_manager.register_create_mview_progress(actor_context);

        let vnodes = vnode_bitmap.map(Arc::new);
        let barrier_rx = local_barrier_manager.subscribe_barrier(actor_context.id);

        let upstream_table =
            BatchTable::new_partial(state_store.clone(), column_ids, vnodes.clone(), table_desc);

        let state_table = node.get_state_table()?;
        let state_table = StateTableBuilder::new(state_table, state_store.clone(), vnodes)
            .enable_preload_all_rows_by_config(&actor_context.config)
            .build()
            .await;

        let executor = SnapshotBackfillExecutor::new(
            upstream_table,
            state_table,
            upstream,
            output_indices,
            actor_context.clone(),
            progress,
            chunk_size,
            node.rate_limit.into(),
            barrier_rx,
            self.streaming_metrics.clone(),
            node.snapshot_backfill_epoch,
        )
        .boxed();

        let info = Self::get_executor_info(
            stream_node,
            Self::get_executor_id(actor_context, stream_node),
        );

        if crate::consistency::insane() {
            let mut troubled_info = info.clone();
            troubled_info.identity = format!("{} (troubled)", info.identity);
            Ok((
                info,
                TroublemakerExecutor::new((troubled_info, executor).into(), chunk_size),
            )
                .into())
        } else {
            Ok((info, executor).into())
        }
    }

    /// Create a chain(tree) of nodes, with given `store`.
    #[expect(clippy::too_many_arguments)]
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
        local_barrier_manager: &LocalBarrierManager,
    ) -> StreamResult<Executor> {
        if let NodeBody::StreamScan(stream_scan) = node.get_node_body().unwrap()
            && let Ok(StreamScanType::SnapshotBackfill) = stream_scan.get_stream_scan_type()
        {
            return dispatch_state_store!(env.state_store(), store, {
                self.create_snapshot_backfill_node(
                    node,
                    stream_scan,
                    actor_context,
                    vnode_bitmap,
                    local_barrier_manager,
                    store,
                )
                .await
            });
        }

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
                    local_barrier_manager,
                )
                .await?,
            );
        }

        self.generate_executor_from_inputs(
            fragment_id,
            node,
            env,
            store,
            actor_context,
            vnode_bitmap,
            has_stateful || is_stateful,
            subtasks,
            local_barrier_manager,
            input,
        )
        .await
    }

    #[expect(clippy::too_many_arguments)]
    async fn generate_executor_from_inputs(
        &self,
        fragment_id: FragmentId,
        node: &stream_plan::StreamNode,
        env: StreamEnvironment,
        store: impl StateStore,
        actor_context: &ActorContextRef,
        vnode_bitmap: Option<Bitmap>,
        has_stateful: bool,
        subtasks: &mut Vec<SubtaskHandle>,
        local_barrier_manager: &LocalBarrierManager,
        input: Vec<Executor>,
    ) -> StreamResult<Executor> {
        let op_info = node.get_identity().clone();

        // We assume that the operator_id of different instances from the same RelNode will be the
        // same.
        let executor_id = Self::get_executor_id(actor_context, node);
        let operator_id = unique_operator_id(fragment_id, node.operator_id);

        let info = Self::get_executor_info(node, executor_id);

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
            local_barrier_manager: local_barrier_manager.clone(),
            config: actor_context.config.clone(),
        };

        let executor = create_executor(executor_params, node, store).await?;

        // Wrap the executor for debug purpose.
        let wrapped = WrapperExecutor::new(executor, actor_context.clone());
        let executor = (info, wrapped).into();

        // If there're multiple stateful executors in this actor, we will wrap it into a subtask.
        let executor = if has_stateful {
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
    async fn create_nodes(
        &self,
        fragment_id: FragmentId,
        node: &stream_plan::StreamNode,
        env: StreamEnvironment,
        actor_context: &ActorContextRef,
        vnode_bitmap: Option<Bitmap>,
        local_barrier_manager: &LocalBarrierManager,
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
                local_barrier_manager,
            )
            .await
        })?;

        Ok((executor, subtasks))
    }

    /// Get the overridden configuration for the given `config_override`.
    fn get_overridden_config(
        &self,
        config_override: &str,
        actor_id: ActorId,
    ) -> Arc<StreamingConfig> {
        self.config_override_cache
            .get_with_by_ref(config_override, || {
                let global = self.env.global_config();
                match merge_streaming_config_section(global.as_ref(), config_override) {
                    Ok(Some(config)) => {
                        tracing::info!(%actor_id, "applied configuration override");
                        Arc::new(config)
                    }
                    Ok(None) => global.clone(), // nothing to override
                    Err(e) => {
                        // We should have validated the config override when user specified it for the job.
                        // However, we still tolerate invalid config override here in case there's
                        // any compatibility issue.
                        tracing::error!(
                            error = %e.as_report(),
                            %actor_id,
                            "failed to apply configuration override, use global config instead",
                        );
                        global.clone()
                    }
                }
            })
    }

    async fn create_actor(
        self: Arc<Self>,
        actor: BuildActorInfo,
        fragment_id: FragmentId,
        node: Arc<StreamNode>,
        local_barrier_manager: LocalBarrierManager,
        new_output_request_rx: UnboundedReceiver<(ActorId, NewOutputRequest)>,
    ) -> StreamResult<Actor<DispatchExecutor>> {
        let actor_id = actor.actor_id;
        let actor_config = self.get_overridden_config(&actor.config_override, actor_id);

        let actor_context = ActorContext::create(
            &actor,
            fragment_id,
            self.env.total_mem_usage(),
            self.streaming_metrics.clone(),
            self.env.meta_client(),
            actor_config,
            self.env.clone(),
        );
        let vnode_bitmap = actor.vnode_bitmap.as_ref().map(|b| b.into());
        let expr_context = actor.expr_context.clone().unwrap();

        let (executor, subtasks) = self
            .create_nodes(
                fragment_id,
                &node,
                self.env.clone(),
                &actor_context,
                vnode_bitmap,
                &local_barrier_manager,
            )
            .await?;

        let dispatcher = DispatchExecutor::new(
            executor,
            new_output_request_rx,
            actor.dispatchers,
            actor_id,
            fragment_id,
            local_barrier_manager.clone(),
            self.streaming_metrics.clone(),
        )
        .await?;
        let actor = Actor::new(
            dispatcher,
            subtasks,
            self.streaming_metrics.clone(),
            actor_context.clone(),
            expr_context,
            local_barrier_manager,
        );
        Ok(actor)
    }

    pub(super) fn spawn_actor(
        self: &Arc<Self>,
        actor: BuildActorInfo,
        fragment_id: FragmentId,
        node: Arc<StreamNode>,
        local_barrier_manager: LocalBarrierManager,
        new_output_request_rx: UnboundedReceiver<(ActorId, NewOutputRequest)>,
    ) -> (JoinHandle<()>, Option<JoinHandle<()>>) {
        let monitor = tokio_metrics::TaskMonitor::new();
        let stream_actor_ref = &actor;
        let actor_id = stream_actor_ref.actor_id;
        let handle = {
            let trace_span = format!("Actor {actor_id}: `{}`", stream_actor_ref.mview_definition);
            let barrier_manager = local_barrier_manager;
            // wrap the future of `create_actor` with `boxed` to avoid stack overflow
            let actor = self
                .clone()
                .create_actor(
                    actor,
                    fragment_id,
                    node,
                    barrier_manager.clone(),
                    new_output_request_rx,
                )
                .boxed()
                .and_then(|actor| actor.run())
                .map(move |result| {
                    if let Err(err) = result {
                        // TODO: check error type and panic if it's unexpected.
                        // Intentionally use `?` on the report to also include the backtrace.
                        tracing::error!(%actor_id, error = ?err.as_report(), "actor exit with error");
                        barrier_manager.notify_failure(actor_id, err);
                    }
                });
            let traced = match &self.await_tree_reg {
                Some(m) => m
                    .register(await_tree_key::Actor(actor_id), trace_span)
                    .instrument(actor)
                    .left_future(),
                None => actor.right_future(),
            };
            let instrumented = monitor.instrument(traced);
            // If hummock tracing is not enabled, it directly returns wrapped future.
            let may_track_hummock = instrumented.may_trace_hummock();

            self.runtime.spawn(may_track_hummock)
        };

        let monitor_handle = if self.streaming_metrics.level >= MetricLevel::Debug
            || self
                .env
                .global_config()
                .developer
                .enable_actor_tokio_metrics
        {
            tracing::info!("Tokio metrics are enabled.");
            let streaming_metrics = self.streaming_metrics.clone();
            let actor_monitor_task = self.runtime.spawn(async move {
                let metrics = streaming_metrics.new_actor_metrics(actor_id);
                loop {
                    let task_metrics = monitor.cumulative();
                    metrics
                        .actor_execution_time
                        .set(task_metrics.total_poll_duration.as_secs_f64());
                    metrics
                        .actor_fast_poll_duration
                        .set(task_metrics.total_fast_poll_duration.as_secs_f64());
                    metrics
                        .actor_fast_poll_cnt
                        .set(task_metrics.total_fast_poll_count as i64);
                    metrics
                        .actor_slow_poll_duration
                        .set(task_metrics.total_slow_poll_duration.as_secs_f64());
                    metrics
                        .actor_slow_poll_cnt
                        .set(task_metrics.total_slow_poll_count as i64);
                    metrics
                        .actor_poll_duration
                        .set(task_metrics.total_poll_duration.as_secs_f64());
                    metrics
                        .actor_poll_cnt
                        .set(task_metrics.total_poll_count as i64);
                    metrics
                        .actor_idle_duration
                        .set(task_metrics.total_idle_duration.as_secs_f64());
                    metrics
                        .actor_idle_cnt
                        .set(task_metrics.total_idled_count as i64);
                    metrics
                        .actor_scheduled_duration
                        .set(task_metrics.total_scheduled_duration.as_secs_f64());
                    metrics
                        .actor_scheduled_cnt
                        .set(task_metrics.total_scheduled_count as i64);
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            });
            Some(actor_monitor_task)
        } else {
            None
        };
        (handle, monitor_handle)
    }
}

/// Parameters to construct executors.
/// - [`crate::from_proto::create_executor`]
/// - [`StreamActorManager::create_nodes`]
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

    pub local_barrier_manager: LocalBarrierManager,

    /// The local streaming configuration for this specific actor. Same as `actor_context.config`.
    ///
    /// Compared to `stream_env.global_config`, this config can have some entries overridden by the user.
    pub config: Arc<StreamingConfig>,
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

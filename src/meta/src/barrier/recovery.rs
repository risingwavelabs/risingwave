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

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context};
use itertools::Itertools;
use risingwave_common::catalog::TableId;
use risingwave_hummock_sdk::compaction_group::StateTableId;
use risingwave_meta_model_v2::StreamingParallelism;
use risingwave_pb::common::{ActorInfo, WorkerNode};
use risingwave_pb::meta::table_fragments::State;
use risingwave_pb::meta::PausedReason;
use risingwave_pb::stream_plan::barrier::BarrierKind;
use risingwave_pb::stream_plan::barrier_mutation::Mutation;
use risingwave_pb::stream_plan::AddMutation;
use thiserror_ext::AsReport;
use tokio::sync::oneshot;
use tokio_retry::strategy::{jitter, ExponentialBackoff};
use tracing::{debug, warn, Instrument};

use super::TracedEpoch;
use crate::barrier::command::CommandContext;
use crate::barrier::info::InflightActorInfo;
use crate::barrier::notifier::Notifier;
use crate::barrier::progress::CreateMviewProgressTracker;
use crate::barrier::state::BarrierManagerState;
use crate::barrier::{Command, GlobalBarrierManager, GlobalBarrierManagerContext};
use crate::controller::catalog::ReleaseContext;
use crate::manager::{ActiveStreamingWorkerNodes, LocalNotification, MetadataManager, WorkerId};
use crate::model::{MetadataModel, MigrationPlan, TableFragments, TableParallelism};
use crate::stream::{build_actor_connector_splits, RescheduleOptions, TableResizePolicy};
use crate::MetaResult;

impl GlobalBarrierManager {
    // Retry base interval in milliseconds.
    const RECOVERY_RETRY_BASE_INTERVAL: u64 = 20;
    // Retry max interval.
    const RECOVERY_RETRY_MAX_INTERVAL: Duration = Duration::from_secs(5);

    #[inline(always)]
    /// Initialize a retry strategy for operation in recovery.
    fn get_retry_strategy() -> impl Iterator<Item = Duration> {
        ExponentialBackoff::from_millis(Self::RECOVERY_RETRY_BASE_INTERVAL)
            .max_delay(Self::RECOVERY_RETRY_MAX_INTERVAL)
            .map(jitter)
    }
}

impl GlobalBarrierManagerContext {
    /// Clean catalogs for creating streaming jobs that are in foreground mode or table fragments not persisted.
    async fn clean_dirty_streaming_jobs(&self) -> MetaResult<()> {
        match &self.metadata_manager {
            MetadataManager::V1(mgr) => {
                // Please look at `CatalogManager::clean_dirty_tables` for more details.
                mgr.catalog_manager
                    .clean_dirty_tables(mgr.fragment_manager.clone())
                    .await?;

                // Clean dirty fragments.
                let stream_job_ids = mgr.catalog_manager.list_stream_job_ids().await?;
                let to_drop_table_fragments = mgr
                    .fragment_manager
                    .list_dirty_table_fragments(|tf| {
                        !stream_job_ids.contains(&tf.table_id().table_id)
                    })
                    .await;
                let to_drop_streaming_ids = to_drop_table_fragments
                    .iter()
                    .map(|t| t.table_id())
                    .collect();
                debug!("clean dirty table fragments: {:?}", to_drop_streaming_ids);

                let unregister_table_ids = mgr
                    .fragment_manager
                    .drop_table_fragments_vec(&to_drop_streaming_ids)
                    .await?;

                // unregister compaction group for dirty table fragments.
                self.env
                    .notification_manager()
                    .notify_local_subscribers(LocalNotification::UnregisterTablesFromHummock(
                        unregister_table_ids,
                    ))
                    .await;

                // clean up source connector dirty changes.
                self.source_manager
                    .drop_source_fragments(&to_drop_table_fragments)
                    .await;
            }
            MetadataManager::V2(mgr) => {
                let ReleaseContext {
                    state_table_ids,
                    source_ids,
                    ..
                } = mgr.catalog_controller.clean_dirty_creating_jobs().await?;

                // unregister compaction group for cleaned state tables.
                self.env
                    .notification_manager()
                    .notify_local_subscribers(LocalNotification::UnregisterTablesFromHummock(
                        state_table_ids
                            .into_iter()
                            .map(|id| id as StateTableId)
                            .collect_vec(),
                    ))
                    .await;

                // unregister cleaned sources.
                self.source_manager
                    .unregister_sources(source_ids.into_iter().map(|id| id as u32).collect())
                    .await;
            }
        }

        Ok(())
    }

    async fn recover_background_mv_progress(&self) -> MetaResult<()> {
        match &self.metadata_manager {
            MetadataManager::V1(_) => self.recover_background_mv_progress_v1().await,
            MetadataManager::V2(_) => self.recover_background_mv_progress_v2().await,
        }
    }

    async fn recover_background_mv_progress_v1(&self) -> MetaResult<()> {
        let mgr = self.metadata_manager.as_v1_ref();
        let mviews = mgr.catalog_manager.list_creating_background_mvs().await;

        let mut mview_definitions = HashMap::new();
        let mut table_map = HashMap::new();
        let mut table_fragment_map = HashMap::new();
        let mut upstream_mv_counts = HashMap::new();
        let mut senders = HashMap::new();
        let mut receivers = Vec::new();
        for mview in mviews {
            let table_id = TableId::new(mview.id);
            let fragments = mgr
                .fragment_manager
                .select_table_fragments_by_table_id(&table_id)
                .await?;
            let internal_table_ids = fragments.internal_table_ids();
            let internal_tables = mgr.catalog_manager.get_tables(&internal_table_ids).await;
            if fragments.is_created() {
                // If the mview is already created, we don't need to recover it.
                mgr.catalog_manager
                    .finish_create_table_procedure(internal_tables, mview)
                    .await?;
                tracing::debug!("notified frontend for stream job {}", table_id.table_id);
            } else {
                table_map.insert(table_id, fragments.backfill_actor_ids());
                mview_definitions.insert(table_id, mview.definition.clone());
                upstream_mv_counts.insert(table_id, fragments.dependent_table_ids());
                table_fragment_map.insert(table_id, fragments);
                let (finished_tx, finished_rx) = oneshot::channel();
                senders.insert(
                    table_id,
                    Notifier {
                        finished: Some(finished_tx),
                        ..Default::default()
                    },
                );
                receivers.push((mview, internal_tables, finished_rx));
            }
        }

        let version_stats = self.hummock_manager.get_version_stats().await;
        // If failed, enter recovery mode.
        {
            let mut tracker = self.tracker.lock().await;
            *tracker = CreateMviewProgressTracker::recover(
                table_map.into(),
                upstream_mv_counts.into(),
                mview_definitions.into(),
                version_stats,
                senders.into(),
                table_fragment_map.into(),
                self.metadata_manager.clone(),
            );
        }
        for (table, internal_tables, finished) in receivers {
            let catalog_manager = mgr.catalog_manager.clone();
            tokio::spawn(async move {
                let res: MetaResult<()> = try {
                    tracing::debug!("recovering stream job {}", table.id);
                    finished.await.ok().context("failed to finish command")??;

                    tracing::debug!("finished stream job {}", table.id);
                    // Once notified that job is finished we need to notify frontend.
                    // and mark catalog as created and commit to meta.
                    // both of these are done by catalog manager.
                    catalog_manager
                        .finish_create_table_procedure(internal_tables, table.clone())
                        .await?;
                    tracing::debug!("notified frontend for stream job {}", table.id);
                };
                if let Err(e) = res.as_ref() {
                    tracing::error!(
                        id = table.id,
                        error = %e.as_report(),
                        "stream job interrupted, will retry after recovery",
                    );
                    // NOTE(kwannoel): We should not cleanup stream jobs,
                    // we don't know if it's just due to CN killed,
                    // or the job has actually failed.
                    // Users have to manually cancel the stream jobs,
                    // if they want to clean it.
                }
            });
        }
        Ok(())
    }

    async fn recover_background_mv_progress_v2(&self) -> MetaResult<()> {
        let mgr = self.metadata_manager.as_v2_ref();
        let mviews = mgr
            .catalog_controller
            .list_background_creating_mviews()
            .await?;

        let mut senders = HashMap::new();
        let mut receivers = Vec::new();
        let mut table_fragment_map = HashMap::new();
        let mut mview_definitions = HashMap::new();
        let mut table_map = HashMap::new();
        let mut upstream_mv_counts = HashMap::new();
        for mview in &mviews {
            let (finished_tx, finished_rx) = oneshot::channel();
            let table_id = TableId::new(mview.table_id as _);
            senders.insert(
                table_id,
                Notifier {
                    finished: Some(finished_tx),
                    ..Default::default()
                },
            );

            let table_fragments = mgr
                .catalog_controller
                .get_job_fragments_by_id(mview.table_id)
                .await?;
            let table_fragments = TableFragments::from_protobuf(table_fragments);
            upstream_mv_counts.insert(table_id, table_fragments.dependent_table_ids());
            table_map.insert(table_id, table_fragments.backfill_actor_ids());
            table_fragment_map.insert(table_id, table_fragments);
            mview_definitions.insert(table_id, mview.definition.clone());
            receivers.push((mview.table_id, finished_rx));
        }

        let version_stats = self.hummock_manager.get_version_stats().await;
        // If failed, enter recovery mode.
        {
            let mut tracker = self.tracker.lock().await;
            *tracker = CreateMviewProgressTracker::recover(
                table_map.into(),
                upstream_mv_counts.into(),
                mview_definitions.into(),
                version_stats,
                senders.into(),
                table_fragment_map.into(),
                self.metadata_manager.clone(),
            );
        }
        for (id, finished) in receivers {
            let catalog_controller = mgr.catalog_controller.clone();
            tokio::spawn(async move {
                let res: MetaResult<()> = try {
                    tracing::debug!("recovering stream job {}", id);
                    finished.await.ok().context("failed to finish command")??;
                    tracing::debug!(id, "finished stream job");
                    catalog_controller.finish_streaming_job(id).await?;
                };
                if let Err(e) = &res {
                    tracing::error!(
                        id,
                        error = %e.as_report(),
                        "stream job interrupted, will retry after recovery",
                    );
                    // NOTE(kwannoel): We should not cleanup stream jobs,
                    // we don't know if it's just due to CN killed,
                    // or the job has actually failed.
                    // Users have to manually cancel the stream jobs,
                    // if they want to clean it.
                }
            });
        }

        Ok(())
    }
}

impl GlobalBarrierManager {
    /// Pre buffered drop and cancel command, return true if any.
    async fn pre_apply_drop_cancel(&self) -> MetaResult<bool> {
        let (dropped_actors, cancelled) = self.scheduled_barriers.pre_apply_drop_cancel_scheduled();
        let applied = !dropped_actors.is_empty() || !cancelled.is_empty();
        if !cancelled.is_empty() {
            match &self.context.metadata_manager {
                MetadataManager::V1(mgr) => {
                    let unregister_table_ids = mgr
                        .fragment_manager
                        .drop_table_fragments_vec(&cancelled)
                        .await?;
                    self.env
                        .notification_manager()
                        .notify_local_subscribers(LocalNotification::UnregisterTablesFromHummock(
                            unregister_table_ids,
                        ))
                        .await;
                }
                MetadataManager::V2(mgr) => {
                    for job_id in cancelled {
                        mgr.catalog_controller
                            .try_abort_creating_streaming_job(job_id.table_id as _, true)
                            .await?;
                    }
                }
            }
        }
        Ok(applied)
    }

    /// Recovery the whole cluster from the latest epoch.
    ///
    /// If `paused_reason` is `Some`, all data sources (including connectors and DMLs) will be
    /// immediately paused after recovery, until the user manually resume them either by restarting
    /// the cluster or `risectl` command. Used for debugging purpose.
    ///
    /// Returns the new state of the barrier manager after recovery.
    pub async fn recovery(&mut self, paused_reason: Option<PausedReason>) {
        let prev_epoch = TracedEpoch::new(
            self.context
                .hummock_manager
                .latest_snapshot()
                .committed_epoch
                .into(),
        );
        // Mark blocked and abort buffered schedules, they might be dirty already.
        self.scheduled_barriers
            .abort_and_mark_blocked("cluster is under recovering");

        tracing::info!("recovery start!");
        self.context
            .clean_dirty_streaming_jobs()
            .await
            .expect("clean dirty streaming jobs");

        self.context.sink_manager.reset().await;
        let retry_strategy = Self::get_retry_strategy();

        // Mview progress needs to be recovered.
        tracing::info!("recovering mview progress");
        self.context
            .recover_background_mv_progress()
            .await
            .expect("recover mview progress should not fail");
        tracing::info!("recovered mview progress");

        // We take retry into consideration because this is the latency user sees for a cluster to
        // get recovered.
        let recovery_timer = self.context.metrics.recovery_latency.start_timer();

        let (state, active_streaming_nodes) = tokio_retry::Retry::spawn(retry_strategy, || {
            async {
                let recovery_result: MetaResult<_> = try {
                    // This is a quick path to accelerate the process of dropping and canceling streaming jobs.
                    let _ = self.pre_apply_drop_cancel().await?;

                    let active_streaming_nodes = ActiveStreamingWorkerNodes::new_snapshot(
                        self.context.metadata_manager.clone(),
                    )
                    .await?;

                    let all_nodes = active_streaming_nodes
                        .current()
                        .values()
                        .cloned()
                        .collect_vec();

                    // Resolve actor info for recovery. If there's no actor to recover, most of the
                    // following steps will be no-op, while the compute nodes will still be reset.
                    let mut info = if !self.env.opts.disable_automatic_parallelism_control {
                        self.context
                            .scale_actors(all_nodes.clone())
                            .await
                            .inspect_err(|err| {
                                warn!(error = %err.as_report(), "scale actors failed");
                            })?;

                        self.context
                            .resolve_actor_info(all_nodes.clone())
                            .await
                            .inspect_err(|err| {
                                warn!(error = %err.as_report(), "resolve actor info failed");
                            })?
                    } else {
                        // Migrate actors in expired CN to newly joined one.
                        self.context
                            .migrate_actors(all_nodes.clone())
                            .await
                            .inspect_err(|err| {
                                warn!(error = %err.as_report(), "migrate actors failed");
                            })?
                    };

                    // Reset all compute nodes, stop and drop existing actors.
                    self.reset_compute_nodes(&info, prev_epoch.value().0)
                        .await
                        .inspect_err(|err| {
                            warn!(error = %err.as_report(), "reset compute nodes failed");
                        })?;

                    if self.pre_apply_drop_cancel().await? {
                        info = self
                            .context
                            .resolve_actor_info(all_nodes.clone())
                            .await
                            .inspect_err(|err| {
                                warn!(error = %err.as_report(), "resolve actor info failed");
                            })?
                    }

                    // update and build all actors.
                    self.update_actors(&info).await.inspect_err(|err| {
                        warn!(error = %err.as_report(), "update actors failed");
                    })?;
                    self.build_actors(&info).await.inspect_err(|err| {
                        warn!(error = %err.as_report(), "build_actors failed");
                    })?;

                    // get split assignments for all actors
                    let source_split_assignments =
                        self.context.source_manager.list_assignments().await;
                    let command = Command::Plain(Some(Mutation::Add(AddMutation {
                        // Actors built during recovery is not treated as newly added actors.
                        actor_dispatchers: Default::default(),
                        added_actors: Default::default(),
                        actor_splits: build_actor_connector_splits(&source_split_assignments),
                        pause: paused_reason.is_some(),
                    })));

                    // Use a different `curr_epoch` for each recovery attempt.
                    let new_epoch = prev_epoch.next();

                    // Inject the `Initial` barrier to initialize all executors.
                    let command_ctx = Arc::new(CommandContext::new(
                        info.clone(),
                        prev_epoch.clone(),
                        new_epoch.clone(),
                        paused_reason,
                        command,
                        BarrierKind::Initial,
                        self.context.clone(),
                        tracing::Span::current(), // recovery span
                    ));

                    let res = match self
                        .context
                        .inject_barrier(command_ctx.clone(), None, None)
                        .await
                        .result
                    {
                        Ok(response) => {
                            if let Err(err) = command_ctx.post_collect().await {
                                warn!(error = %err.as_report(), "post_collect failed");
                                Err(err)
                            } else {
                                Ok((new_epoch.clone(), response))
                            }
                        }
                        Err(err) => {
                            warn!(error = %err.as_report(), "inject_barrier failed");
                            Err(err)
                        }
                    };
                    let (new_epoch, _) = res?;

                    (
                        BarrierManagerState::new(new_epoch, info, command_ctx.next_paused_reason()),
                        active_streaming_nodes,
                    )
                };
                if recovery_result.is_err() {
                    self.context.metrics.recovery_failure_cnt.inc();
                }
                recovery_result
            }
            .instrument(tracing::info_span!("recovery_attempt"))
        })
        .await
        .expect("Retry until recovery success.");

        recovery_timer.observe_duration();
        self.scheduled_barriers.mark_ready();

        tracing::info!(
            epoch = state.in_flight_prev_epoch().value().0,
            paused = ?state.paused_reason(),
            "recovery success"
        );

        self.state = state;
        self.active_streaming_nodes = active_streaming_nodes;
    }
}

impl GlobalBarrierManagerContext {
    /// Migrate actors in expired CNs to newly joined ones, return true if any actor is migrated.
    async fn migrate_actors(&self, all_nodes: Vec<WorkerNode>) -> MetaResult<InflightActorInfo> {
        match &self.metadata_manager {
            MetadataManager::V1(_) => self.migrate_actors_v1(all_nodes).await,
            MetadataManager::V2(_) => self.migrate_actors_v2(all_nodes).await,
        }
    }

    async fn migrate_actors_v2(&self, all_nodes: Vec<WorkerNode>) -> MetaResult<InflightActorInfo> {
        let mgr = self.metadata_manager.as_v2_ref();

        let all_inuse_parallel_units: HashSet<_> = mgr
            .catalog_controller
            .all_inuse_parallel_units()
            .await?
            .into_iter()
            .collect();

        let active_parallel_units: HashSet<_> = all_nodes
            .iter()
            .flat_map(|node| node.parallel_units.iter().map(|pu| pu.id as i32))
            .collect();

        let expired_parallel_units: BTreeSet<_> = all_inuse_parallel_units
            .difference(&active_parallel_units)
            .cloned()
            .collect();
        if expired_parallel_units.is_empty() {
            debug!("no expired parallel units, skipping.");
            return self.resolve_actor_info(all_nodes.clone()).await;
        }

        debug!("start migrate actors.");
        let mut to_migrate_parallel_units = expired_parallel_units.into_iter().rev().collect_vec();
        debug!(
            "got to migrate parallel units {:#?}",
            to_migrate_parallel_units
        );
        let mut inuse_parallel_units: HashSet<_> = all_inuse_parallel_units
            .intersection(&active_parallel_units)
            .cloned()
            .collect();

        let start = Instant::now();
        let mut plan = HashMap::new();
        'discovery: while !to_migrate_parallel_units.is_empty() {
            let new_parallel_units = all_nodes
                .iter()
                .flat_map(|node| {
                    node.parallel_units
                        .iter()
                        .filter(|pu| !inuse_parallel_units.contains(&(pu.id as _)))
                })
                .cloned()
                .collect_vec();
            if !new_parallel_units.is_empty() {
                debug!("new parallel units found: {:#?}", new_parallel_units);
                for target_parallel_unit in new_parallel_units {
                    if let Some(from) = to_migrate_parallel_units.pop() {
                        debug!(
                            "plan to migrate from parallel unit {} to {}",
                            from, target_parallel_unit.id
                        );
                        inuse_parallel_units.insert(target_parallel_unit.id as i32);
                        plan.insert(from, target_parallel_unit);
                    } else {
                        break 'discovery;
                    }
                }
            }
            warn!(
                "waiting for new workers to join, elapsed: {}s",
                start.elapsed().as_secs()
            );
            // wait to get newly joined CN
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        mgr.catalog_controller.migrate_actors(plan).await?;

        debug!("migrate actors succeed.");

        self.resolve_actor_info(all_nodes).await
    }

    /// Migrate actors in expired CNs to newly joined ones, return true if any actor is migrated.
    async fn migrate_actors_v1(&self, all_nodes: Vec<WorkerNode>) -> MetaResult<InflightActorInfo> {
        let mgr = self.metadata_manager.as_v1_ref();

        let info = self.resolve_actor_info(all_nodes.clone()).await?;

        // 1. get expired workers.
        let expired_workers: HashSet<WorkerId> = info
            .actor_map
            .iter()
            .filter(|(&worker, actors)| !actors.is_empty() && !info.node_map.contains_key(&worker))
            .map(|(&worker, _)| worker)
            .collect();
        if expired_workers.is_empty() {
            debug!("no expired workers, skipping.");
            return Ok(info);
        }

        debug!("start migrate actors.");
        let migration_plan = self
            .generate_migration_plan(expired_workers, &all_nodes)
            .await?;
        // 2. start to migrate fragment one-by-one.
        mgr.fragment_manager
            .migrate_fragment_actors(&migration_plan)
            .await?;
        // 3. remove the migration plan.
        migration_plan.delete(self.env.meta_store_checked()).await?;
        debug!("migrate actors succeed.");

        self.resolve_actor_info(all_nodes).await
    }

    async fn scale_actors(&self, all_nodes: Vec<WorkerNode>) -> MetaResult<()> {
        let _guard = self.scale_controller.reschedule_lock.write().await;
        match &self.metadata_manager {
            MetadataManager::V1(_) => self.scale_actors_v1(all_nodes).await,
            MetadataManager::V2(_) => self.scale_actors_v2(all_nodes).await,
        }
    }

    async fn scale_actors_v2(&self, workers: Vec<WorkerNode>) -> MetaResult<()> {
        let mgr = self.metadata_manager.as_v2_ref();
        debug!("start resetting actors distribution");

        let table_parallelisms: HashMap<_, _> = {
            let streaming_parallelisms = mgr
                .catalog_controller
                .get_all_created_streaming_parallelisms()
                .await?;

            streaming_parallelisms
                .into_iter()
                .map(|(table_id, parallelism)| {
                    // no custom for sql backend
                    let table_parallelism = match parallelism {
                        StreamingParallelism::Adaptive => TableParallelism::Adaptive,
                        StreamingParallelism::Fixed(n) => TableParallelism::Fixed(n),
                    };

                    (table_id as u32, table_parallelism)
                })
                .collect()
        };

        let schedulable_worker_ids = workers
            .iter()
            .filter(|worker| {
                !worker
                    .property
                    .as_ref()
                    .map(|p| p.is_unschedulable)
                    .unwrap_or(false)
            })
            .map(|worker| worker.id)
            .collect();

        let plan = self
            .scale_controller
            .generate_table_resize_plan(TableResizePolicy {
                worker_ids: schedulable_worker_ids,
                table_parallelisms: table_parallelisms.clone(),
            })
            .await?;

        let table_parallelisms: HashMap<_, _> = table_parallelisms
            .into_iter()
            .map(|(table_id, parallelism)| {
                debug_assert_ne!(parallelism, TableParallelism::Custom);
                (TableId::new(table_id), parallelism)
            })
            .collect();

        let mut compared_table_parallelisms = table_parallelisms.clone();

        // skip reschedule if no reschedule is generated.
        let reschedule_fragment = if plan.is_empty() {
            HashMap::new()
        } else {
            let (reschedule_fragment, _) = self
                .scale_controller
                .prepare_reschedule_command(
                    plan,
                    RescheduleOptions {
                        resolve_no_shuffle_upstream: true,
                        skip_create_new_actors: true,
                    },
                    Some(&mut compared_table_parallelisms),
                )
                .await?;

            reschedule_fragment
        };

        // Because custom parallelism doesn't exist, this function won't result in a no-shuffle rewrite for table parallelisms.
        debug_assert_eq!(compared_table_parallelisms, table_parallelisms);

        if let Err(e) = self
            .scale_controller
            .post_apply_reschedule(&reschedule_fragment, &table_parallelisms)
            .await
        {
            tracing::error!(
                error = %e.as_report(),
                "failed to apply reschedule for offline scaling in recovery",
            );

            return Err(e);
        }

        debug!("scaling actors succeed.");
        Ok(())
    }

    async fn scale_actors_v1(&self, workers: Vec<WorkerNode>) -> MetaResult<()> {
        let info = self.resolve_actor_info(workers.clone()).await?;

        let mgr = self.metadata_manager.as_v1_ref();
        debug!("start resetting actors distribution");

        if info.actor_location_map.is_empty() {
            debug!("empty cluster, skipping");
            return Ok(());
        }

        let current_parallelism = info
            .node_map
            .values()
            .flat_map(|worker_node| worker_node.parallel_units.iter())
            .count();

        if current_parallelism == 0 {
            return Err(anyhow!("no available parallel units for auto scaling").into());
        }

        /// We infer the new parallelism strategy based on the prior level of parallelism of the table.
        /// If the parallelism strategy is Fixed or Auto, we won't make any modifications.
        /// For Custom, we'll assess the parallelism of the core fragment;
        /// if the parallelism is higher than the currently available parallelism, we'll set it to Auto.
        /// If it's lower, we'll set it to Fixed.
        fn derive_target_parallelism_for_custom(
            current_parallelism: usize,
            table: &TableFragments,
        ) -> TableParallelism {
            let derive_from_fragment = table.mview_fragment().or_else(|| table.sink_fragment());

            if let TableParallelism::Custom = &table.assigned_parallelism {
                if let Some(fragment) = derive_from_fragment {
                    let fragment_parallelism = fragment.get_actors().len();
                    if fragment_parallelism >= current_parallelism {
                        TableParallelism::Adaptive
                    } else {
                        TableParallelism::Fixed(fragment_parallelism)
                    }
                } else {
                    TableParallelism::Adaptive
                }
            } else {
                table.assigned_parallelism
            }
        }

        let table_parallelisms: HashMap<_, _> = {
            let guard = mgr.fragment_manager.get_fragment_read_guard().await;

            guard
                .table_fragments()
                .iter()
                //.filter(|&(_, table)| matches!(table.state(), State::Created))
                .map(|(table_id, table)| {
                    let target_parallelism =
                        derive_target_parallelism_for_custom(current_parallelism, table);

                    (table_id.table_id, target_parallelism)
                })
                .collect()
        };

        let schedulable_worker_ids = workers
            .iter()
            .filter(|worker| {
                !worker
                    .property
                    .as_ref()
                    .map(|p| p.is_unschedulable)
                    .unwrap_or(false)
            })
            .map(|worker| worker.id)
            .collect();

        let plan = self
            .scale_controller
            .generate_table_resize_plan(TableResizePolicy {
                worker_ids: schedulable_worker_ids,
                table_parallelisms: table_parallelisms.clone(),
            })
            .await?;

        let table_parallelisms: HashMap<_, _> = table_parallelisms
            .into_iter()
            .map(|(table_id, parallelism)| {
                debug_assert_ne!(parallelism, TableParallelism::Custom);
                (TableId::new(table_id), parallelism)
            })
            .collect();

        let mut compared_table_parallelisms = table_parallelisms.clone();

        // skip reschedule if no reschedule is generated.
        let (reschedule_fragment, applied_reschedules) = if plan.is_empty() {
            (HashMap::new(), HashMap::new())
        } else {
            self.scale_controller
                .prepare_reschedule_command(
                    plan,
                    RescheduleOptions {
                        resolve_no_shuffle_upstream: true,
                        skip_create_new_actors: true,
                    },
                    Some(&mut compared_table_parallelisms),
                )
                .await?
        };

        // Because custom parallelism doesn't exist, this function won't result in a no-shuffle rewrite for table parallelisms.
        debug_assert_eq!(compared_table_parallelisms, table_parallelisms);

        if let Err(e) = self
            .scale_controller
            .post_apply_reschedule(&reschedule_fragment, &table_parallelisms)
            .await
        {
            tracing::error!(
                error = %e.as_report(),
                "failed to apply reschedule for offline scaling in recovery",
            );

            mgr.fragment_manager
                .cancel_apply_reschedules(applied_reschedules)
                .await;

            return Err(e);
        }

        debug!("scaling actors succeed.");
        Ok(())
    }

    /// This function will generate a migration plan, which includes the mapping for all expired and
    /// in-used parallel unit to a new one.
    async fn generate_migration_plan(
        &self,
        expired_workers: HashSet<WorkerId>,
        all_nodes: &Vec<WorkerNode>,
    ) -> MetaResult<MigrationPlan> {
        let mgr = self.metadata_manager.as_v1_ref();

        let mut cached_plan = MigrationPlan::get(self.env.meta_store_checked()).await?;

        let all_worker_parallel_units = mgr.fragment_manager.all_worker_parallel_units().await;

        let (expired_inuse_workers, inuse_workers): (Vec<_>, Vec<_>) = all_worker_parallel_units
            .into_iter()
            .partition(|(worker, _)| expired_workers.contains(worker));

        let mut to_migrate_parallel_units: BTreeSet<_> = expired_inuse_workers
            .into_iter()
            .flat_map(|(_, pu)| pu.into_iter())
            .collect();
        let mut inuse_parallel_units: HashSet<_> = inuse_workers
            .into_iter()
            .flat_map(|(_, pu)| pu.into_iter())
            .collect();

        cached_plan.parallel_unit_plan.retain(|from, to| {
            if to_migrate_parallel_units.contains(from) {
                if !to_migrate_parallel_units.contains(&to.id) {
                    // clean up target parallel units in migration plan that are expired and not
                    // used by any actors.
                    return !expired_workers.contains(&to.worker_node_id);
                }
                return true;
            }
            false
        });
        to_migrate_parallel_units.retain(|id| !cached_plan.parallel_unit_plan.contains_key(id));
        inuse_parallel_units.extend(cached_plan.parallel_unit_plan.values().map(|pu| pu.id));

        if to_migrate_parallel_units.is_empty() {
            // all expired parallel units are already in migration plan.
            debug!("all expired parallel units are already in migration plan.");
            return Ok(cached_plan);
        }
        let mut to_migrate_parallel_units =
            to_migrate_parallel_units.into_iter().rev().collect_vec();
        debug!(
            "got to migrate parallel units {:#?}",
            to_migrate_parallel_units
        );

        let start = Instant::now();
        // if in-used expire parallel units are not empty, should wait for newly joined worker.
        'discovery: while !to_migrate_parallel_units.is_empty() {
            let mut new_parallel_units = all_nodes
                .iter()
                .flat_map(|worker| worker.parallel_units.iter().cloned())
                .collect_vec();
            new_parallel_units.retain(|pu| !inuse_parallel_units.contains(&pu.id));

            if !new_parallel_units.is_empty() {
                debug!("new parallel units found: {:#?}", new_parallel_units);
                for target_parallel_unit in new_parallel_units {
                    if let Some(from) = to_migrate_parallel_units.pop() {
                        debug!(
                            "plan to migrate from parallel unit {} to {}",
                            from, target_parallel_unit.id
                        );
                        inuse_parallel_units.insert(target_parallel_unit.id);
                        cached_plan
                            .parallel_unit_plan
                            .insert(from, target_parallel_unit);
                    } else {
                        break 'discovery;
                    }
                }
            }
            warn!(
                "waiting for new workers to join, elapsed: {}s",
                start.elapsed().as_secs()
            );
            // wait to get newly joined CN
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        // update migration plan, if there is a chain in the plan, update it.
        let mut new_plan = MigrationPlan::default();
        for (from, to) in &cached_plan.parallel_unit_plan {
            let mut to = to.clone();
            while let Some(target) = cached_plan.parallel_unit_plan.get(&to.id) {
                to = target.clone();
            }
            new_plan.parallel_unit_plan.insert(*from, to);
        }

        assert!(
            new_plan
                .parallel_unit_plan
                .values()
                .map(|pu| pu.id)
                .all_unique(),
            "target parallel units must be unique: {:?}",
            new_plan.parallel_unit_plan
        );

        new_plan.insert(self.env.meta_store_checked()).await?;
        Ok(new_plan)
    }
}

impl GlobalBarrierManager {
    /// Update all actors in compute nodes.
    async fn update_actors(&self, info: &InflightActorInfo) -> MetaResult<()> {
        if info.actor_map.is_empty() {
            tracing::debug!("no actor to update, skipping.");
            return Ok(());
        }

        let actor_infos: Vec<_> = info
            .actor_map
            .iter()
            .map(|(node_id, actors)| {
                let host = info
                    .node_map
                    .get(node_id)
                    .ok_or_else(|| anyhow::anyhow!("worker evicted, wait for online."))?
                    .host
                    .clone();
                Ok(actors.iter().map(move |&actor_id| ActorInfo {
                    actor_id,
                    host: host.clone(),
                })) as MetaResult<_>
            })
            .flatten_ok()
            .try_collect()?;

        let mut all_node_actors = self.context.metadata_manager.all_node_actors(false).await?;

        // Check if any actors were dropped after info resolved.
        if all_node_actors.iter().any(|(node_id, node_actors)| {
            !info.node_map.contains_key(node_id)
                || info
                    .actor_map
                    .get(node_id)
                    .map(|actors| actors.len() != node_actors.len())
                    .unwrap_or(true)
        }) {
            return Err(anyhow!("actors dropped during update").into());
        }

        self.context
            .stream_rpc_manager
            .broadcast_update_actor_info(
                &info.node_map,
                info.actor_map.keys().cloned(),
                actor_infos.into_iter(),
                info.actor_map.keys().map(|node_id| {
                    (
                        *node_id,
                        all_node_actors.remove(node_id).unwrap_or_default(),
                    )
                }),
            )
            .await?;

        Ok(())
    }

    /// Build all actors in compute nodes.
    async fn build_actors(&self, info: &InflightActorInfo) -> MetaResult<()> {
        if info.actor_map.is_empty() {
            tracing::debug!("no actor to build, skipping.");
            return Ok(());
        }

        self.context
            .stream_rpc_manager
            .build_actors(
                &info.node_map,
                info.actor_map.iter().map(|(node_id, actors)| {
                    let actors = actors.iter().cloned().collect();
                    (*node_id, actors)
                }),
            )
            .await?;

        Ok(())
    }

    /// Reset all compute nodes by calling `force_stop_actors`.
    async fn reset_compute_nodes(
        &self,
        info: &InflightActorInfo,
        prev_epoch: u64,
    ) -> MetaResult<()> {
        debug!(prev_epoch, worker = ?info.node_map.keys().collect_vec(), "force stop actors");
        self.context
            .stream_rpc_manager
            .force_stop_actors(info.node_map.values(), prev_epoch)
            .await?;

        debug!(prev_epoch, "all compute nodes have been reset.");

        Ok(())
    }
}

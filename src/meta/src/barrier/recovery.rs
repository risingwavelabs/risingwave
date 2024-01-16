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

use anyhow::anyhow;
use futures::future::try_join_all;
use futures::stream::FuturesUnordered;
use futures::TryStreamExt;
use itertools::Itertools;
use risingwave_common::catalog::TableId;
use risingwave_hummock_sdk::compaction_group::StateTableId;
use risingwave_pb::common::ActorInfo;
use risingwave_pb::meta::PausedReason;
use risingwave_pb::stream_plan::barrier::BarrierKind;
use risingwave_pb::stream_plan::barrier_mutation::Mutation;
use risingwave_pb::stream_plan::AddMutation;
use risingwave_pb::stream_service::{
    BroadcastActorInfoTableRequest, BuildActorsRequest, ForceStopActorsRequest, UpdateActorsRequest,
};
use tokio::sync::oneshot;
use tokio_retry::strategy::{jitter, ExponentialBackoff};
use tracing::{debug, warn, Instrument};
use uuid::Uuid;

use super::TracedEpoch;
use crate::barrier::command::CommandContext;
use crate::barrier::info::{CommandActorChanges, InflightActorInfo};
use crate::barrier::notifier::Notifier;
use crate::barrier::progress::CreateMviewProgressTracker;
use crate::barrier::schedule::ScheduledBarriers;
use crate::barrier::state::BarrierManagerState;
use crate::barrier::{Command, GlobalBarrierManagerContext};
use crate::controller::catalog::ReleaseContext;
use crate::manager::{MetadataManager, WorkerId};
use crate::model::{MetadataModel, MigrationPlan, TableFragments};
use crate::stream::{build_actor_connector_splits, RescheduleOptions, TableResizePolicy};
use crate::MetaResult;

impl GlobalBarrierManagerContext {
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

                mgr.fragment_manager
                    .drop_table_fragments_vec(&to_drop_streaming_ids)
                    .await?;

                // unregister compaction group for dirty table fragments.
                self.hummock_manager
                    .unregister_table_fragments_vec(&to_drop_table_fragments)
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
                self.hummock_manager
                    .unregister_table_ids_fail_fast(
                        &state_table_ids
                            .into_iter()
                            .map(|id| id as StateTableId)
                            .collect_vec(),
                    )
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
        let MetadataManager::V1(mgr) = &self.metadata_manager else {
            unreachable!()
        };
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
                    finished
                        .await
                        .map_err(|e| anyhow!("failed to finish command: {}", e))?;

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
                        "stream job {} interrupted, will retry after recovery: {e:?}",
                        table.id
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
        let MetadataManager::V2(mgr) = &self.metadata_manager else {
            unreachable!()
        };
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
                    finished
                        .await
                        .map_err(|e| anyhow!("failed to finish command: {}", e))?;
                    tracing::debug!("finished stream job {}", id);
                    catalog_controller.finish_streaming_job(id).await?;
                };
                if let Err(e) = &res {
                    tracing::error!(
                        "stream job {} interrupted, will retry after recovery: {e:?}",
                        id
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

    /// Recovery the whole cluster from the latest epoch.
    ///
    /// If `paused_reason` is `Some`, all data sources (including connectors and DMLs) will be
    /// immediately paused after recovery, until the user manually resume them either by restarting
    /// the cluster or `risectl` command. Used for debugging purpose.
    ///
    /// Returns the new state of the barrier manager after recovery.
    pub async fn recovery(
        &self,
        prev_epoch: TracedEpoch,
        paused_reason: Option<PausedReason>,
        scheduled_barriers: &ScheduledBarriers,
    ) -> BarrierManagerState {
        // Mark blocked and abort buffered schedules, they might be dirty already.
        scheduled_barriers
            .abort_and_mark_blocked("cluster is under recovering")
            .await;

        tracing::info!("recovery start!");
        self.clean_dirty_streaming_jobs()
            .await
            .expect("clean dirty streaming jobs");

        self.sink_manager.reset().await;
        let retry_strategy = Self::get_retry_strategy();

        // Mview progress needs to be recovered.
        tracing::info!("recovering mview progress");
        self.recover_background_mv_progress()
            .await
            .expect("recover mview progress should not fail");
        tracing::info!("recovered mview progress");

        // We take retry into consideration because this is the latency user sees for a cluster to
        // get recovered.
        let recovery_timer = self.metrics.recovery_latency.start_timer();

        let state = tokio_retry::Retry::spawn(retry_strategy, || {
            async {
                let recovery_result: MetaResult<_> = try {
                    // This is a quick path to accelerate the process of dropping and canceling streaming jobs.
                    let _ = scheduled_barriers.pre_apply_drop_scheduled().await;

                    // Resolve actor info for recovery. If there's no actor to recover, most of the
                    // following steps will be no-op, while the compute nodes will still be reset.
                    let mut info = if self.env.opts.enable_scale_in_when_recovery {
                        let info = self.resolve_actor_info().await;
                        let scaled = self.scale_actors(&info).await.inspect_err(|err| {
                            warn!(err = ?err, "scale actors failed");
                        })?;
                        if scaled {
                            self.resolve_actor_info().await
                        } else {
                            info
                        }
                    } else {
                        // Migrate actors in expired CN to newly joined one.
                        self.migrate_actors().await.inspect_err(|err| {
                            warn!(err = ?err, "migrate actors failed");
                        })?
                    };

                    // Reset all compute nodes, stop and drop existing actors.
                    self.reset_compute_nodes(&info).await.inspect_err(|err| {
                        warn!(err = ?err, "reset compute nodes failed");
                    })?;

                    let to_remove_actors = scheduled_barriers.pre_apply_drop_scheduled().await;
                    info.post_apply_non_checked(Some(CommandActorChanges {
                        to_remove: to_remove_actors,
                        to_add: Default::default(),
                    }));

                    // update and build all actors.
                    self.update_actors(&info).await.inspect_err(|err| {
                        warn!(err = ?err, "update actors failed");
                    })?;
                    self.build_actors(&info).await.inspect_err(|err| {
                        warn!(err = ?err, "build_actors failed");
                    })?;

                    // get split assignments for all actors
                    let source_split_assignments = self.source_manager.list_assignments().await;
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
                        self.clone(),
                        tracing::Span::current(), // recovery span
                    ));

                    #[cfg(not(all(test, feature = "failpoints")))]
                    {
                        use risingwave_common::util::epoch::INVALID_EPOCH;

                        let mce = self.hummock_manager.get_current_max_committed_epoch().await;

                        if mce != INVALID_EPOCH {
                            command_ctx.wait_epoch_commit(mce).await?;
                        }
                    };
                    let await_barrier_complete = self.inject_barrier(command_ctx.clone()).await;
                    let res = match await_barrier_complete.await.result {
                        Ok(response) => {
                            if let Err(err) = command_ctx.post_collect().await {
                                warn!(err = ?err, "post_collect failed");
                                Err(err)
                            } else {
                                Ok((new_epoch.clone(), response))
                            }
                        }
                        Err(err) => {
                            warn!(err = ?err, "inject_barrier failed");
                            Err(err)
                        }
                    };
                    let (new_epoch, _) = res?;

                    BarrierManagerState::new(new_epoch, info, command_ctx.next_paused_reason())
                };
                if recovery_result.is_err() {
                    self.metrics.recovery_failure_cnt.inc();
                }
                recovery_result
            }
            .instrument(tracing::info_span!("recovery_attempt"))
        })
        .await
        .expect("Retry until recovery success.");

        recovery_timer.observe_duration();
        scheduled_barriers.mark_ready().await;

        tracing::info!(
            epoch = state.in_flight_prev_epoch().value().0,
            paused = ?state.paused_reason(),
            "recovery success"
        );

        state
    }

    /// Migrate actors in expired CNs to newly joined ones, return true if any actor is migrated.
    async fn migrate_actors(&self) -> MetaResult<InflightActorInfo> {
        match &self.metadata_manager {
            MetadataManager::V1(_) => self.migrate_actors_v1().await,
            MetadataManager::V2(_) => self.migrate_actors_v2().await,
        }
    }

    async fn migrate_actors_v2(&self) -> MetaResult<InflightActorInfo> {
        let MetadataManager::V2(mgr) = &self.metadata_manager else {
            unreachable!()
        };

        let all_inuse_parallel_units: HashSet<_> = mgr
            .catalog_controller
            .all_inuse_parallel_units()
            .await?
            .into_iter()
            .collect();
        let active_parallel_units: HashSet<_> = mgr
            .cluster_controller
            .list_active_parallel_units()
            .await?
            .into_iter()
            .map(|pu| pu.id as i32)
            .collect();

        let expired_parallel_units: BTreeSet<_> = all_inuse_parallel_units
            .difference(&active_parallel_units)
            .cloned()
            .collect();
        if expired_parallel_units.is_empty() {
            debug!("no expired parallel units, skipping.");
            let info = self.resolve_actor_info().await;
            return Ok(info);
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
            let new_parallel_units = mgr
                .cluster_controller
                .list_active_parallel_units()
                .await?
                .into_iter()
                .map(|pu| pu.id as i32)
                .filter(|pu| !inuse_parallel_units.contains(pu))
                .collect_vec();
            if !new_parallel_units.is_empty() {
                debug!("new parallel units found: {:#?}", new_parallel_units);
                for target_parallel_unit in new_parallel_units {
                    if let Some(from) = to_migrate_parallel_units.pop() {
                        debug!(
                            "plan to migrate from parallel unit {} to {}",
                            from, target_parallel_unit
                        );
                        inuse_parallel_units.insert(target_parallel_unit);
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
        let info = self.resolve_actor_info().await;
        Ok(info)
    }

    /// Migrate actors in expired CNs to newly joined ones, return true if any actor is migrated.
    async fn migrate_actors_v1(&self) -> MetaResult<InflightActorInfo> {
        let MetadataManager::V1(mgr) = &self.metadata_manager else {
            unreachable!()
        };

        let info = self.resolve_actor_info().await;

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
        let migration_plan = self.generate_migration_plan(expired_workers).await?;
        // 2. start to migrate fragment one-by-one.
        mgr.fragment_manager
            .migrate_fragment_actors(&migration_plan)
            .await?;
        // 3. remove the migration plan.
        migration_plan.delete(self.env.meta_store()).await?;
        debug!("migrate actors succeed.");

        let info = self.resolve_actor_info().await;
        Ok(info)
    }

    async fn scale_actors(&self, info: &InflightActorInfo) -> MetaResult<bool> {
        match &self.metadata_manager {
            MetadataManager::V1(_) => self.scale_actors_v1(info).await,
            MetadataManager::V2(_) => self.scale_actors_v2(info),
        }
    }

    fn scale_actors_v2(&self, _info: &InflightActorInfo) -> MetaResult<bool> {
        let MetadataManager::V2(_mgr) = &self.metadata_manager else {
            unreachable!()
        };
        unimplemented!("implement auto-scale funcs in sql backend")
    }

    async fn scale_actors_v1(&self, info: &InflightActorInfo) -> MetaResult<bool> {
        let MetadataManager::V1(mgr) = &self.metadata_manager else {
            unreachable!()
        };
        debug!("start scaling-in offline actors.");

        let expired_workers: HashSet<WorkerId> = info
            .actor_map
            .iter()
            .filter(|(&worker, actors)| !actors.is_empty() && !info.node_map.contains_key(&worker))
            .map(|(&worker, _)| worker)
            .collect();

        if expired_workers.is_empty() {
            debug!("no expired workers, skipping.");
            return Ok(false);
        }

        let table_parallelisms = {
            let guard = mgr.fragment_manager.get_fragment_read_guard().await;

            guard
                .table_fragments()
                .iter()
                .map(|(table_id, table)| (table_id.table_id, table.assigned_parallelism))
                .collect()
        };

        let workers = mgr
            .cluster_manager
            .list_active_streaming_compute_nodes()
            .await;

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
            .as_ref()
            .unwrap()
            .generate_table_resize_plan(TableResizePolicy {
                worker_ids: schedulable_worker_ids,
                table_parallelisms,
            })
            .await?;

        let (reschedule_fragment, applied_reschedules) = self
            .scale_controller
            .as_ref()
            .unwrap()
            .prepare_reschedule_command(
                plan,
                RescheduleOptions {
                    resolve_no_shuffle_upstream: true,
                },
                None,
            )
            .await?;

        if let Err(e) = self
            .scale_controller
            .as_ref()
            .unwrap()
            .post_apply_reschedule(&reschedule_fragment, &Default::default())
            .await
        {
            tracing::error!(
                "failed to apply reschedule for offline scaling in recovery: {}",
                e.to_string()
            );

            mgr.fragment_manager
                .cancel_apply_reschedules(applied_reschedules)
                .await;

            return Err(e);
        }

        debug!("scaling-in actors succeed.");
        Ok(true)
    }

    /// This function will generate a migration plan, which includes the mapping for all expired and
    /// in-used parallel unit to a new one.
    async fn generate_migration_plan(
        &self,
        expired_workers: HashSet<WorkerId>,
    ) -> MetaResult<MigrationPlan> {
        let MetadataManager::V1(mgr) = &self.metadata_manager else {
            unreachable!()
        };

        let mut cached_plan = MigrationPlan::get(self.env.meta_store()).await?;

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
            let mut new_parallel_units = mgr
                .cluster_manager
                .list_active_streaming_parallel_units()
                .await;
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

        new_plan.insert(self.env.meta_store()).await?;
        Ok(new_plan)
    }

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

        let mut all_node_actors = self.metadata_manager.all_node_actors(false).await?;

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

        info.actor_map.iter().map(|(node_id, actors)| {
            let node_actors = all_node_actors.remove(node_id).unwrap_or_default();
            let node = info.node_map.get(node_id).unwrap();
            let actor_infos = actor_infos.clone();

            async move {
                let client = self.env.stream_client_pool().get(node).await?;
                client
                    .broadcast_actor_info_table(BroadcastActorInfoTableRequest {
                        info: actor_infos,
                    })
                    .await?;

                let request_id = Uuid::new_v4().to_string();
                tracing::debug!(request_id = request_id.as_str(), actors = ?actors, "update actors");
                client
                    .update_actors(UpdateActorsRequest {
                        request_id,
                        actors: node_actors,
                    })
                    .await?;

                Ok(()) as MetaResult<()>
            }
        }).collect::<FuturesUnordered<_>>().try_collect::<()>().await?;

        Ok(())
    }

    /// Build all actors in compute nodes.
    async fn build_actors(&self, info: &InflightActorInfo) -> MetaResult<()> {
        if info.actor_map.is_empty() {
            tracing::debug!("no actor to build, skipping.");
            return Ok(());
        }

        info.actor_map
            .iter()
            .map(|(node_id, actors)| async move {
                let actors = actors.iter().cloned().collect();
                let node = info.node_map.get(node_id).unwrap();
                let client = self.env.stream_client_pool().get(node).await?;

                let request_id = Uuid::new_v4().to_string();
                tracing::debug!(request_id = request_id.as_str(), actors = ?actors, "build actors");
                client
                    .build_actors(BuildActorsRequest {
                        request_id,
                        actor_id: actors,
                    })
                    .await?;

                Ok(()) as MetaResult<_>
            })
            .collect::<FuturesUnordered<_>>()
            .try_collect::<()>()
            .await?;

        Ok(())
    }

    /// Reset all compute nodes by calling `force_stop_actors`.
    async fn reset_compute_nodes(&self, info: &InflightActorInfo) -> MetaResult<()> {
        let futures = info.node_map.values().map(|worker_node| async move {
            let client = self.env.stream_client_pool().get(worker_node).await?;
            debug!(worker = ?worker_node.id, "force stop actors");
            client
                .force_stop_actors(ForceStopActorsRequest {
                    request_id: Uuid::new_v4().to_string(),
                })
                .await
        });

        try_join_all(futures).await?;
        debug!("all compute nodes have been reset.");

        Ok(())
    }
}

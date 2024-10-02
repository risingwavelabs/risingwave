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
use std::time::Duration;

use anyhow::{anyhow, Context};
use itertools::Itertools;
use risingwave_common::catalog::TableId;
use risingwave_common::config::DefaultParallelism;
use risingwave_common::hash::WorkerSlotId;
use risingwave_common::util::epoch::Epoch;
use risingwave_meta_model_v2::StreamingParallelism;
use risingwave_pb::meta::subscribe_response::{Info, Operation};
use risingwave_pb::meta::table_fragments::State;
use risingwave_pb::meta::{PausedReason, Recovery};
use risingwave_pb::stream_plan::barrier_mutation::Mutation;
use risingwave_pb::stream_plan::{AddMutation, StreamActor};
use thiserror_ext::AsReport;
use tokio::time::Instant;
use tokio_retry::strategy::{jitter, ExponentialBackoff};
use tracing::{debug, info, warn, Instrument};

use super::{CheckpointControl, TracedEpoch};
use crate::barrier::info::{InflightGraphInfo, InflightSubscriptionInfo};
use crate::barrier::progress::CreateMviewProgressTracker;
use crate::barrier::rpc::ControlStreamManager;
use crate::barrier::schedule::ScheduledBarriers;
use crate::barrier::state::BarrierManagerState;
use crate::barrier::{BarrierKind, GlobalBarrierManager, GlobalBarrierManagerContext};
use crate::manager::{ActiveStreamingWorkerNodes, MetadataManager, WorkerId};
use crate::model::{MetadataModel, MigrationPlan, TableFragments, TableParallelism};
use crate::stream::{build_actor_connector_splits, RescheduleOptions, TableResizePolicy};
use crate::{model, MetaError, MetaResult};

impl GlobalBarrierManager {
    // Migration timeout.
    const RECOVERY_FORCE_MIGRATION_TIMEOUT: Duration = Duration::from_secs(300);
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
                mgr.catalog_manager.clean_dirty_subscription().await?;
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

                let _unregister_table_ids = mgr
                    .fragment_manager
                    .drop_table_fragments_vec(&to_drop_streaming_ids)
                    .await?;

                // clean up source connector dirty changes.
                self.source_manager
                    .drop_source_fragments(&to_drop_table_fragments)
                    .await;
            }
            MetadataManager::V2(mgr) => {
                mgr.catalog_controller.clean_dirty_subscription().await?;
                let source_ids = mgr.catalog_controller.clean_dirty_creating_jobs().await?;

                // unregister cleaned sources.
                self.source_manager
                    .unregister_sources(source_ids.into_iter().map(|id| id as u32).collect())
                    .await;
            }
        }

        Ok(())
    }

    async fn purge_state_table_from_hummock(
        &self,
        all_state_table_ids: &HashSet<TableId>,
    ) -> MetaResult<()> {
        self.hummock_manager.purge(all_state_table_ids).await?;
        Ok(())
    }

    async fn recover_background_mv_progress(&self) -> MetaResult<CreateMviewProgressTracker> {
        match &self.metadata_manager {
            MetadataManager::V1(_) => self.recover_background_mv_progress_v1().await,
            MetadataManager::V2(_) => self.recover_background_mv_progress_v2().await,
        }
    }

    async fn recover_background_mv_progress_v1(&self) -> MetaResult<CreateMviewProgressTracker> {
        let mgr = self.metadata_manager.as_v1_ref();
        let mviews = mgr.catalog_manager.list_creating_background_mvs().await;

        let mut table_mview_map = HashMap::new();
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
                    .finish_create_materialized_view_procedure(internal_tables, mview)
                    .await?;
                tracing::debug!("notified frontend for stream job {}", table_id.table_id);
            } else {
                table_mview_map.insert(table_id, (fragments, mview, internal_tables));
            }
        }

        let version_stats = self.hummock_manager.get_version_stats().await;
        // If failed, enter recovery mode.
        Ok(CreateMviewProgressTracker::recover_v1(
            version_stats,
            table_mview_map,
            mgr.clone(),
        ))
    }

    async fn recover_background_mv_progress_v2(&self) -> MetaResult<CreateMviewProgressTracker> {
        let mgr = self.metadata_manager.as_v2_ref();
        let mviews = mgr
            .catalog_controller
            .list_background_creating_mviews(false)
            .await?;

        let mut mview_map = HashMap::new();
        for mview in &mviews {
            let table_id = TableId::new(mview.table_id as _);
            let table_fragments = mgr
                .catalog_controller
                .get_job_fragments_by_id(mview.table_id)
                .await?;
            let table_fragments = TableFragments::from_protobuf(table_fragments);
            mview_map.insert(table_id, (mview.definition.clone(), table_fragments));
        }

        let version_stats = self.hummock_manager.get_version_stats().await;
        // If failed, enter recovery mode.

        Ok(CreateMviewProgressTracker::recover_v2(
            mview_map,
            version_stats,
            mgr.clone(),
        ))
    }

    /// Pre buffered drop and cancel command, return true if any.
    async fn pre_apply_drop_cancel(
        &self,
        scheduled_barriers: &ScheduledBarriers,
    ) -> MetaResult<bool> {
        let (dropped_actors, cancelled) = scheduled_barriers.pre_apply_drop_cancel_scheduled();
        let applied = !dropped_actors.is_empty() || !cancelled.is_empty();
        if !cancelled.is_empty() {
            match &self.metadata_manager {
                MetadataManager::V1(mgr) => {
                    mgr.fragment_manager
                        .drop_table_fragments_vec(&cancelled)
                        .await?;
                }
                MetadataManager::V2(mgr) => {
                    for job_id in cancelled {
                        mgr.catalog_controller
                            .try_abort_creating_streaming_job(job_id.table_id as _, true)
                            .await?;
                    }
                }
            };
            // no need to unregister state table id from hummock manager here, because it's expected that
            // we call `purge_state_table_from_hummock` anyway after the current method returns.
        }
        Ok(applied)
    }
}

impl GlobalBarrierManager {
    /// Recovery the whole cluster from the latest epoch.
    ///
    /// If `paused_reason` is `Some`, all data sources (including connectors and DMLs) will be
    /// immediately paused after recovery, until the user manually resume them either by restarting
    /// the cluster or `risectl` command. Used for debugging purpose.
    ///
    /// Returns the new state of the barrier manager after recovery.
    pub async fn recovery(&mut self, paused_reason: Option<PausedReason>, err: Option<MetaError>) {
        // Mark blocked and abort buffered schedules, they might be dirty already.
        self.scheduled_barriers
            .abort_and_mark_blocked("cluster is under recovering");
        // Clear all control streams to release resources (connections to compute nodes) first.
        self.control_stream_manager.clear();

        tracing::info!("recovery start!");
        let retry_strategy = Self::get_retry_strategy();

        // We take retry into consideration because this is the latency user sees for a cluster to
        // get recovered.
        let recovery_timer = self.context.metrics.recovery_latency.start_timer();

        let new_state = tokio_retry::Retry::spawn(retry_strategy, || {
            async {
                let recovery_result: MetaResult<_> = try {
                    if let Some(err) = &err {
                        self.context
                            .metadata_manager
                            .notify_finish_failed(err)
                            .await;
                    }

                    self.context
                        .clean_dirty_streaming_jobs()
                        .await
                        .context("clean dirty streaming jobs")?;

                    // Mview progress needs to be recovered.
                    tracing::info!("recovering mview progress");
                    let tracker = self
                        .context
                        .recover_background_mv_progress()
                        .await
                        .context("recover mview progress should not fail")?;
                    tracing::info!("recovered mview progress");

                    // This is a quick path to accelerate the process of dropping and canceling streaming jobs.
                    let _ = self
                        .context
                        .pre_apply_drop_cancel(&self.scheduled_barriers)
                        .await?;

                    let mut active_streaming_nodes = ActiveStreamingWorkerNodes::new_snapshot(
                        self.context.metadata_manager.clone(),
                    )
                    .await?;

                    let background_streaming_jobs = self
                        .context
                        .metadata_manager
                        .list_background_creating_jobs()
                        .await?;

                    // Resolve actor info for recovery. If there's no actor to recover, most of the
                    // following steps will be no-op, while the compute nodes will still be reset.
                    // FIXME: Transactions should be used.
                    // TODO(error-handling): attach context to the errors and log them together, instead of inspecting everywhere.
                    let mut info = if !self.env.opts.disable_automatic_parallelism_control
                        && background_streaming_jobs.is_empty()
                    {
                        self.context
                            .scale_actors(&active_streaming_nodes)
                            .await
                            .inspect_err(|err| {
                                warn!(error = %err.as_report(), "scale actors failed");
                            })?;

                        self.context.resolve_graph_info().await.inspect_err(|err| {
                            warn!(error = %err.as_report(), "resolve actor info failed");
                        })?
                    } else {
                        // Migrate actors in expired CN to newly joined one.
                        self.context
                            .migrate_actors(&mut active_streaming_nodes)
                            .await
                            .inspect_err(|err| {
                                warn!(error = %err.as_report(), "migrate actors failed");
                            })?
                    };

                    if self
                        .context
                        .pre_apply_drop_cancel(&self.scheduled_barriers)
                        .await?
                    {
                        info = self.context.resolve_graph_info().await.inspect_err(|err| {
                            warn!(error = %err.as_report(), "resolve actor info failed");
                        })?
                    }

                    let info = info;

                    self.context
                        .purge_state_table_from_hummock(&info.existing_table_ids().collect())
                        .await
                        .context("purge state table from hummock")?;

                    let (prev_epoch, version_id) = self
                        .context
                        .hummock_manager
                        .on_current_version(|version| {
                            let state_table_info = version.state_table_info.info();
                            let committed_epoch = state_table_info
                                .values()
                                .map(|info| info.committed_epoch)
                                .next();
                            let existing_table_ids = info.existing_table_ids();
                            for table_id in existing_table_ids {
                                assert!(
                                    state_table_info.contains_key(&table_id),
                                    "table id {table_id} not registered to hummock but in recovered job {:?}. hummock table info{:?}",
                                    info.existing_table_ids().collect_vec(),
                                    state_table_info
                                );
                            }
                            if let Some(committed_epoch) = committed_epoch {
                                for (table_id, info) in version.state_table_info.info() {
                                    assert_eq!(
                                        info.committed_epoch, committed_epoch,
                                        "table {} with invisible epoch is not purged",
                                        table_id
                                    );
                                }
                            }
                            (
                                committed_epoch.map(|committed_epoch| {
                                    TracedEpoch::new(Epoch::from(committed_epoch))
                                }),
                                version.id,
                            )
                        })
                        .await;

                    let mut control_stream_manager =
                        ControlStreamManager::new(self.context.clone());

                    let subscription_info = InflightSubscriptionInfo {
                        mv_depended_subscriptions: self
                            .context
                            .metadata_manager
                            .get_mv_depended_subscriptions()
                            .await?,
                    };

                    let reset_start_time = Instant::now();
                    control_stream_manager
                        .reset(
                            version_id,
                            &subscription_info,
                            active_streaming_nodes.current(),
                        )
                        .await
                        .inspect_err(|err| {
                            warn!(error = %err.as_report(), "reset compute nodes failed");
                        })?;
                    info!(elapsed=?reset_start_time.elapsed(), "control stream reset");

                    self.context.sink_manager.reset().await;

                    // update and build all actors.
                    let node_actors = self
                        .context
                        .load_all_actors(&info, &active_streaming_nodes)
                        .await
                        .inspect_err(|err| {
                            warn!(error = %err.as_report(), "update actors failed");
                        })?;

                    // get split assignments for all actors
                    let source_split_assignments =
                        self.context.source_manager.list_assignments().await;
                    let mutation = Mutation::Add(AddMutation {
                        // Actors built during recovery is not treated as newly added actors.
                        actor_dispatchers: Default::default(),
                        added_actors: Default::default(),
                        actor_splits: build_actor_connector_splits(&source_split_assignments),
                        pause: paused_reason.is_some(),
                        subscriptions_to_add: Default::default(),
                    });

                    let new_epoch = if let Some(prev_epoch) = &prev_epoch {
                        // Use a different `curr_epoch` for each recovery attempt.
                        let new_epoch = prev_epoch.next();

                        let mut node_to_collect = control_stream_manager.inject_barrier(
                            None,
                            Some(mutation),
                            (&new_epoch, prev_epoch),
                            &BarrierKind::Initial,
                            &info,
                            Some(&info),
                            Some(node_actors),
                            vec![],
                            vec![],
                        )?;
                        debug!(?node_to_collect, "inject initial barrier");
                        while !node_to_collect.is_empty() {
                            let (worker_id, result) = control_stream_manager
                                .next_complete_barrier_response()
                                .await;
                            let resp = result?;
                            assert_eq!(resp.epoch, prev_epoch.value().0);
                            assert!(node_to_collect.remove(&worker_id));
                        }
                        debug!("collected initial barrier");
                        Some(new_epoch)
                    } else {
                        assert!(info.is_empty());
                        None
                    };

                    (
                        BarrierManagerState::new(new_epoch, info, subscription_info, paused_reason),
                        active_streaming_nodes,
                        control_stream_manager,
                        tracker,
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

        let create_mview_tracker: CreateMviewProgressTracker;

        (
            self.state,
            self.active_streaming_nodes,
            self.control_stream_manager,
            create_mview_tracker,
        ) = new_state;

        self.checkpoint_control =
            CheckpointControl::new(self.context.clone(), create_mview_tracker).await;

        tracing::info!(
            epoch = self.state.in_flight_prev_epoch().map(|epoch| epoch.value().0),
            paused = ?self.state.paused_reason(),
            "recovery success"
        );

        self.env
            .notification_manager()
            .notify_frontend_without_version(Operation::Update, Info::Recovery(Recovery {}));
    }
}

impl GlobalBarrierManagerContext {
    /// Migrate actors in expired CNs to newly joined ones, return true if any actor is migrated.
    async fn migrate_actors(
        &self,
        active_nodes: &mut ActiveStreamingWorkerNodes,
    ) -> MetaResult<InflightGraphInfo> {
        match &self.metadata_manager {
            MetadataManager::V1(_) => self.migrate_actors_v1(active_nodes).await,
            MetadataManager::V2(_) => self.migrate_actors_v2(active_nodes).await,
        }
    }

    async fn migrate_actors_v2(
        &self,
        active_nodes: &mut ActiveStreamingWorkerNodes,
    ) -> MetaResult<InflightGraphInfo> {
        let mgr = self.metadata_manager.as_v2_ref();

        // all worker slots used by actors
        let all_inuse_worker_slots: HashSet<_> = mgr
            .catalog_controller
            .all_inuse_worker_slots()
            .await?
            .into_iter()
            .collect();

        let active_worker_slots: HashSet<_> = active_nodes
            .current()
            .values()
            .flat_map(|node| {
                (0..node.parallelism).map(|idx| WorkerSlotId::new(node.id, idx as usize))
            })
            .collect();

        let expired_worker_slots: BTreeSet<_> = all_inuse_worker_slots
            .difference(&active_worker_slots)
            .cloned()
            .collect();

        if expired_worker_slots.is_empty() {
            debug!("no expired worker slots, skipping.");
            return self.resolve_graph_info().await;
        }

        debug!("start migrate actors.");
        let mut to_migrate_worker_slots = expired_worker_slots.into_iter().rev().collect_vec();
        debug!("got to migrate worker slots {:#?}", to_migrate_worker_slots);

        let mut inuse_worker_slots: HashSet<_> = all_inuse_worker_slots
            .intersection(&active_worker_slots)
            .cloned()
            .collect();

        let start = Instant::now();
        let mut plan = HashMap::new();
        'discovery: while !to_migrate_worker_slots.is_empty() {
            let mut new_worker_slots = active_nodes
                .current()
                .values()
                .flat_map(|worker| {
                    (0..worker.parallelism).map(move |i| WorkerSlotId::new(worker.id, i as _))
                })
                .collect_vec();

            new_worker_slots.retain(|worker_slot| !inuse_worker_slots.contains(worker_slot));
            let to_migration_size = to_migrate_worker_slots.len();
            let mut available_size = new_worker_slots.len();

            if available_size < to_migration_size
                && start.elapsed() > GlobalBarrierManager::RECOVERY_FORCE_MIGRATION_TIMEOUT
            {
                let mut factor = 2;

                while available_size < to_migration_size {
                    let mut extended_worker_slots = active_nodes
                        .current()
                        .values()
                        .flat_map(|worker| {
                            (0..worker.parallelism * factor)
                                .map(move |i| WorkerSlotId::new(worker.id, i as _))
                        })
                        .collect_vec();

                    extended_worker_slots
                        .retain(|worker_slot| !inuse_worker_slots.contains(worker_slot));

                    extended_worker_slots.sort_by(|a, b| {
                        a.slot_idx()
                            .cmp(&b.slot_idx())
                            .then(a.worker_id().cmp(&b.worker_id()))
                    });

                    available_size = extended_worker_slots.len();
                    new_worker_slots = extended_worker_slots;

                    factor *= 2;
                }

                tracing::info!(
                    "migration timed out, extending worker slots to {:?} by factor {}",
                    new_worker_slots,
                    factor,
                );
            }

            if !new_worker_slots.is_empty() {
                debug!("new worker slots found: {:#?}", new_worker_slots);
                for target_worker_slot in new_worker_slots {
                    if let Some(from) = to_migrate_worker_slots.pop() {
                        debug!(
                            "plan to migrate from worker slot {} to {}",
                            from, target_worker_slot
                        );
                        inuse_worker_slots.insert(target_worker_slot);
                        plan.insert(from, target_worker_slot);
                    } else {
                        break 'discovery;
                    }
                }
            }

            if to_migrate_worker_slots.is_empty() {
                break;
            }

            // wait to get newly joined CN
            let changed = active_nodes
                .wait_changed(
                    Duration::from_millis(5000),
                    GlobalBarrierManager::RECOVERY_FORCE_MIGRATION_TIMEOUT,
                    |active_nodes| {
                        let current_nodes = active_nodes
                            .current()
                            .values()
                            .map(|node| (node.id, &node.host, node.parallelism))
                            .collect_vec();
                        warn!(
                            current_nodes = ?current_nodes,
                            "waiting for new workers to join, elapsed: {}s",
                            start.elapsed().as_secs()
                        );
                    },
                )
                .await;
            warn!(?changed, "get worker changed or timed out. Retry migrate");
        }

        mgr.catalog_controller.migrate_actors(plan).await?;

        debug!("migrate actors succeed.");

        self.resolve_graph_info().await
    }

    /// Migrate actors in expired CNs to newly joined ones, return true if any actor is migrated.
    async fn migrate_actors_v1(
        &self,
        active_nodes: &mut ActiveStreamingWorkerNodes,
    ) -> MetaResult<InflightGraphInfo> {
        let mgr = self.metadata_manager.as_v1_ref();

        let info = self.resolve_graph_info().await?;

        // 1. get expired workers.
        let expired_workers: HashSet<WorkerId> = info
            .actor_map
            .iter()
            .filter(|(&worker, actors)| {
                !actors.is_empty() && !active_nodes.current().contains_key(&worker)
            })
            .map(|(&worker, _)| worker)
            .collect();
        if expired_workers.is_empty() {
            debug!("no expired workers, skipping.");
            return Ok(info);
        }

        debug!("start migrate actors.");
        let migration_plan = self
            .generate_migration_plan(expired_workers, active_nodes)
            .await?;
        // 2. start to migrate fragment one-by-one.
        mgr.fragment_manager
            .migrate_fragment_actors(&migration_plan)
            .await?;
        // 3. remove the migration plan.
        migration_plan.delete(self.env.meta_store().as_kv()).await?;
        debug!("migrate actors succeed.");

        self.resolve_graph_info().await
    }

    async fn scale_actors(&self, active_nodes: &ActiveStreamingWorkerNodes) -> MetaResult<()> {
        let Ok(_guard) = self.scale_controller.reschedule_lock.try_write() else {
            return Err(anyhow!("scale_actors failed to acquire reschedule_lock").into());
        };
        match &self.metadata_manager {
            MetadataManager::V1(_) => self.scale_actors_v1(active_nodes).await,
            MetadataManager::V2(_) => self.scale_actors_v2(active_nodes).await,
        }
    }

    async fn scale_actors_v2(&self, active_nodes: &ActiveStreamingWorkerNodes) -> MetaResult<()> {
        let mgr = self.metadata_manager.as_v2_ref();
        debug!("start resetting actors distribution");

        let available_parallelism = active_nodes
            .current()
            .values()
            .map(|worker_node| worker_node.parallelism as usize)
            .sum();

        let table_parallelisms: HashMap<_, _> = {
            let streaming_parallelisms = mgr
                .catalog_controller
                .get_all_created_streaming_parallelisms()
                .await?;

            let mut result = HashMap::new();

            for (object_id, streaming_parallelism) in streaming_parallelisms {
                let actual_fragment_parallelism = mgr
                    .catalog_controller
                    .get_actual_job_fragment_parallelism(object_id)
                    .await?;

                let table_parallelism = match streaming_parallelism {
                    StreamingParallelism::Adaptive => model::TableParallelism::Adaptive,
                    StreamingParallelism::Custom => model::TableParallelism::Custom,
                    StreamingParallelism::Fixed(n) => model::TableParallelism::Fixed(n as _),
                };

                let target_parallelism = Self::derive_target_parallelism(
                    available_parallelism,
                    table_parallelism,
                    actual_fragment_parallelism,
                    self.env.opts.default_parallelism,
                );

                if target_parallelism != table_parallelism {
                    tracing::info!(
                        "resetting table {} parallelism from {:?} to {:?}",
                        object_id,
                        table_parallelism,
                        target_parallelism
                    );
                }

                result.insert(object_id as u32, target_parallelism);
            }

            result
        };

        let schedulable_worker_ids = active_nodes
            .current()
            .values()
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
                .analyze_reschedule_plan(
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

    // We infer the new parallelism strategy based on the prior level of parallelism of the table.
    // If the parallelism strategy is Fixed or Auto, we won't make any modifications.
    // For Custom, we'll assess the parallelism of the core fragment;
    // if the parallelism is higher than the currently available parallelism, we'll set it to Adaptive.
    // If it's lower, we'll set it to Fixed.
    // If it was previously set to Adaptive, but the default_parallelism in the configuration isn’t Full,
    // and it matches the actual fragment parallelism, in this case, it will be handled by downgrading to Fixed.
    fn derive_target_parallelism(
        available_parallelism: usize,
        assigned_parallelism: TableParallelism,
        actual_fragment_parallelism: Option<usize>,
        default_parallelism: DefaultParallelism,
    ) -> TableParallelism {
        match assigned_parallelism {
            TableParallelism::Custom => {
                if let Some(fragment_parallelism) = actual_fragment_parallelism {
                    if fragment_parallelism >= available_parallelism {
                        TableParallelism::Adaptive
                    } else {
                        TableParallelism::Fixed(fragment_parallelism)
                    }
                } else {
                    TableParallelism::Adaptive
                }
            }
            TableParallelism::Adaptive => {
                match (default_parallelism, actual_fragment_parallelism) {
                    (DefaultParallelism::Default(n), Some(fragment_parallelism))
                        if fragment_parallelism == n.get() =>
                    {
                        TableParallelism::Fixed(fragment_parallelism)
                    }
                    _ => TableParallelism::Adaptive,
                }
            }
            _ => assigned_parallelism,
        }
    }

    async fn scale_actors_v1(&self, active_nodes: &ActiveStreamingWorkerNodes) -> MetaResult<()> {
        let info = self.resolve_graph_info().await?;

        let mgr = self.metadata_manager.as_v1_ref();
        debug!("start resetting actors distribution");

        if info.actor_location_map.is_empty() {
            debug!("empty cluster, skipping");
            return Ok(());
        }

        let available_parallelism = active_nodes
            .current()
            .values()
            .map(|worker_node| worker_node.parallelism as usize)
            .sum();

        if available_parallelism == 0 {
            return Err(anyhow!("no available worker slots for auto scaling").into());
        }

        let all_table_parallelisms: HashMap<_, _> = {
            let guard = mgr.fragment_manager.get_fragment_read_guard().await;

            guard
                .table_fragments()
                .iter()
                .filter(|&(_, table)| matches!(table.state(), State::Created))
                .map(|(table_id, table)| {
                    let actual_fragment_parallelism = table
                        .mview_fragment()
                        .or_else(|| table.sink_fragment())
                        .map(|fragment| fragment.get_actors().len());
                    (
                        *table_id,
                        Self::derive_target_parallelism(
                            available_parallelism,
                            table.assigned_parallelism,
                            actual_fragment_parallelism,
                            self.env.opts.default_parallelism,
                        ),
                    )
                })
                .collect()
        };

        let schedulable_worker_ids: BTreeSet<_> = active_nodes
            .current()
            .values()
            .filter(|worker| {
                !worker
                    .property
                    .as_ref()
                    .map(|p| p.is_unschedulable)
                    .unwrap_or(false)
            })
            .map(|worker| worker.id)
            .collect();

        for (table_id, table_parallelism) in all_table_parallelisms {
            let plan = self
                .scale_controller
                .generate_table_resize_plan(TableResizePolicy {
                    worker_ids: schedulable_worker_ids.clone(),
                    table_parallelisms: HashMap::from([(table_id.table_id, table_parallelism)]),
                })
                .await?;

            if plan.is_empty() {
                continue;
            }

            let table_parallelisms = HashMap::from([(table_id, table_parallelism)]);

            let mut compared_table_parallelisms = table_parallelisms.clone();

            let (reschedule_fragment, applied_reschedules) = self
                .scale_controller
                .analyze_reschedule_plan(
                    plan,
                    RescheduleOptions {
                        resolve_no_shuffle_upstream: true,
                        skip_create_new_actors: true,
                    },
                    Some(&mut compared_table_parallelisms),
                )
                .await?;

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

            tracing::info!(
                "offline rescheduling for job {} in recovery is done",
                table_id.table_id
            );
        }

        debug!("scaling actors succeed.");
        Ok(())
    }

    /// This function will generate a migration plan, which includes the mapping for all expired and
    /// in-used worker slot to a new one.
    async fn generate_migration_plan(
        &self,
        expired_workers: HashSet<WorkerId>,
        active_nodes: &mut ActiveStreamingWorkerNodes,
    ) -> MetaResult<MigrationPlan> {
        let mgr = self.metadata_manager.as_v1_ref();
        let mut cached_plan = MigrationPlan::get(self.env.meta_store().as_kv()).await?;

        let all_worker_slots = mgr.fragment_manager.all_worker_slots().await;

        let (expired_inuse_workers, inuse_workers): (Vec<_>, Vec<_>) = all_worker_slots
            .into_iter()
            .partition(|(worker, _)| expired_workers.contains(worker));

        let mut to_migrate_worker_slots: BTreeSet<_> = expired_inuse_workers
            .into_iter()
            .flat_map(|(_, worker_slots)| worker_slots.into_iter())
            .collect();
        let mut inuse_worker_slots: HashSet<_> = inuse_workers
            .into_iter()
            .flat_map(|(_, worker_slots)| worker_slots.into_iter())
            .collect();

        cached_plan.worker_slot_plan.retain(|from, to| {
            if to_migrate_worker_slots.contains(from) {
                if !to_migrate_worker_slots.contains(to) {
                    // clean up target worker slots in migration plan that are expired and not
                    // used by any actors.
                    return !expired_workers.contains(&to.worker_id());
                }
                return true;
            }
            false
        });
        to_migrate_worker_slots.retain(|id| !cached_plan.worker_slot_plan.contains_key(id));
        inuse_worker_slots.extend(cached_plan.worker_slot_plan.values());

        if to_migrate_worker_slots.is_empty() {
            // all expired worker slots are already in migration plan.
            debug!("all expired worker slots are already in migration plan.");
            return Ok(cached_plan);
        }
        let mut to_migrate_worker_slots = to_migrate_worker_slots.into_iter().rev().collect_vec();
        debug!("got to migrate worker slots {:#?}", to_migrate_worker_slots);

        let start = Instant::now();
        // if in-used expire worker slots are not empty, should wait for newly joined worker.
        'discovery: while !to_migrate_worker_slots.is_empty() {
            let mut new_worker_slots = active_nodes
                .current()
                .values()
                .flat_map(|worker| {
                    (0..worker.parallelism).map(move |i| WorkerSlotId::new(worker.id, i as _))
                })
                .collect_vec();

            new_worker_slots.retain(|worker_slot| !inuse_worker_slots.contains(worker_slot));

            let to_migration_size = to_migrate_worker_slots.len();
            let mut available_size = new_worker_slots.len();

            if available_size < to_migration_size
                && start.elapsed() > GlobalBarrierManager::RECOVERY_FORCE_MIGRATION_TIMEOUT
            {
                let mut factor = 2;
                while available_size < to_migration_size {
                    let mut extended_worker_slots = active_nodes
                        .current()
                        .values()
                        .flat_map(|worker| {
                            (0..worker.parallelism * factor)
                                .map(move |i| WorkerSlotId::new(worker.id, i as _))
                        })
                        .collect_vec();

                    extended_worker_slots
                        .retain(|worker_slot| !inuse_worker_slots.contains(worker_slot));

                    extended_worker_slots.sort_by(|a, b| {
                        a.slot_idx()
                            .cmp(&b.slot_idx())
                            .then(a.worker_id().cmp(&b.worker_id()))
                    });

                    available_size = extended_worker_slots.len();
                    new_worker_slots = extended_worker_slots;
                    factor *= 2;
                }

                tracing::info!(
                    "migration timed out, extending worker slots to {:?} by factor {}",
                    new_worker_slots,
                    factor,
                );
            }

            if !new_worker_slots.is_empty() {
                debug!("new worker slots found: {:#?}", new_worker_slots);
                for target_worker_slot in new_worker_slots {
                    if let Some(from) = to_migrate_worker_slots.pop() {
                        debug!(
                            "plan to migrate from worker slot {} to {}",
                            from, target_worker_slot
                        );
                        inuse_worker_slots.insert(target_worker_slot);
                        cached_plan
                            .worker_slot_plan
                            .insert(from, target_worker_slot);
                    } else {
                        break 'discovery;
                    }
                }
            }

            if to_migrate_worker_slots.is_empty() {
                break;
            }

            // wait to get newly joined CN
            let changed = active_nodes
                .wait_changed(
                    Duration::from_millis(5000),
                    GlobalBarrierManager::RECOVERY_FORCE_MIGRATION_TIMEOUT,
                    |active_nodes| {
                        let current_nodes = active_nodes
                            .current()
                            .values()
                            .map(|node| (node.id, &node.host, &node.parallelism))
                            .collect_vec();
                        warn!(
                            current_nodes = ?current_nodes,
                            "waiting for new workers to join, elapsed: {}s",
                            start.elapsed().as_secs()
                        );
                    },
                )
                .await;
            warn!(?changed, "get worker changed or timed out. Retry migrate");
        }

        // update migration plan, if there is a chain in the plan, update it.
        let mut new_plan = MigrationPlan::default();
        for (from, to) in &cached_plan.worker_slot_plan {
            let mut to = *to;
            while let Some(target) = cached_plan.worker_slot_plan.get(&to) {
                to = *target;
            }
            new_plan.worker_slot_plan.insert(*from, to);
        }

        new_plan.insert(self.env.meta_store().as_kv()).await?;
        Ok(new_plan)
    }

    /// Update all actors in compute nodes.
    async fn load_all_actors(
        &self,
        info: &InflightGraphInfo,
        active_nodes: &ActiveStreamingWorkerNodes,
    ) -> MetaResult<HashMap<WorkerId, Vec<StreamActor>>> {
        if info.actor_map.is_empty() {
            tracing::debug!("no actor to update, skipping.");
            return Ok(HashMap::new());
        }

        let all_node_actors = self.metadata_manager.all_node_actors(false).await?;

        // Check if any actors were dropped after info resolved.
        if all_node_actors.iter().any(|(node_id, node_actors)| {
            !active_nodes.current().contains_key(node_id)
                || info
                    .actor_map
                    .get(node_id)
                    .map(|actors| actors.len() != node_actors.len())
                    .unwrap_or(true)
        }) {
            return Err(anyhow!("actors dropped during update").into());
        }

        {
            for (node_id, actors) in &info.actor_map {
                if !actors.is_empty() && !all_node_actors.contains_key(node_id) {
                    return Err(anyhow!("streaming job dropped during update").into());
                }
            }
        }

        Ok(all_node_actors)
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use super::*;
    #[test]
    fn test_derive_target_parallelism() {
        // total 10, assigned custom, actual 5, default full -> fixed(5)
        assert_eq!(
            TableParallelism::Fixed(5),
            GlobalBarrierManagerContext::derive_target_parallelism(
                10,
                TableParallelism::Custom,
                Some(5),
                DefaultParallelism::Full,
            )
        );

        // total 10, assigned custom, actual 10, default full -> adaptive
        assert_eq!(
            TableParallelism::Adaptive,
            GlobalBarrierManagerContext::derive_target_parallelism(
                10,
                TableParallelism::Custom,
                Some(10),
                DefaultParallelism::Full,
            )
        );

        // total 10, assigned custom, actual 11, default full -> adaptive
        assert_eq!(
            TableParallelism::Adaptive,
            GlobalBarrierManagerContext::derive_target_parallelism(
                10,
                TableParallelism::Custom,
                Some(11),
                DefaultParallelism::Full,
            )
        );

        // total 10, assigned fixed(5), actual _, default full -> fixed(5)
        assert_eq!(
            TableParallelism::Adaptive,
            GlobalBarrierManagerContext::derive_target_parallelism(
                10,
                TableParallelism::Custom,
                None,
                DefaultParallelism::Full,
            )
        );

        // total 10, assigned adaptive, actual _, default full -> adaptive
        assert_eq!(
            TableParallelism::Adaptive,
            GlobalBarrierManagerContext::derive_target_parallelism(
                10,
                TableParallelism::Adaptive,
                None,
                DefaultParallelism::Full,
            )
        );

        // total 10, assigned adaptive, actual 5, default 5 -> fixed(5)
        assert_eq!(
            TableParallelism::Fixed(5),
            GlobalBarrierManagerContext::derive_target_parallelism(
                10,
                TableParallelism::Adaptive,
                Some(5),
                DefaultParallelism::Default(NonZeroUsize::new(5).unwrap()),
            )
        );

        // total 10, assigned adaptive, actual 6, default 5 -> adaptive
        assert_eq!(
            TableParallelism::Adaptive,
            GlobalBarrierManagerContext::derive_target_parallelism(
                10,
                TableParallelism::Adaptive,
                Some(6),
                DefaultParallelism::Default(NonZeroUsize::new(5).unwrap()),
            )
        );
    }
}

// Copyright 2023 RisingWave Labs
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

use std::collections::{BTreeSet, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::future::try_join_all;
use itertools::Itertools;
use risingwave_pb::common::ActorInfo;
use risingwave_pb::stream_plan::barrier::Mutation;
use risingwave_pb::stream_plan::AddMutation;
use risingwave_pb::stream_service::{
    BarrierCompleteResponse, BroadcastActorInfoTableRequest, BuildActorsRequest,
    ForceStopActorsRequest, UpdateActorsRequest,
};
use tokio_retry::strategy::{jitter, ExponentialBackoff};
use tracing::{debug, warn};
use uuid::Uuid;

use super::TracedEpoch;
use crate::barrier::command::CommandContext;
use crate::barrier::info::BarrierActorInfo;
use crate::barrier::{CheckpointControl, Command, GlobalBarrierManager};
use crate::manager::WorkerId;
use crate::model::MigrationPlan;
use crate::storage::MetaStore;
use crate::stream::build_actor_connector_splits;
use crate::MetaResult;

impl<S> GlobalBarrierManager<S>
where
    S: MetaStore,
{
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

    async fn resolve_actor_info_for_recovery(&self) -> BarrierActorInfo {
        self.resolve_actor_info(
            &mut CheckpointControl::new(self.metrics.clone()),
            &Command::barrier(),
        )
        .await
    }

    /// Clean up all dirty streaming jobs.
    async fn clean_dirty_fragments(&self) -> MetaResult<()> {
        let stream_job_ids = self.catalog_manager.list_stream_job_ids().await?;
        let table_fragments = self.fragment_manager.list_table_fragments().await;
        let to_drop_table_fragments = table_fragments
            .into_iter()
            .filter(|table_fragment| {
                !stream_job_ids.contains(&table_fragment.table_id().table_id)
                    || !table_fragment.is_created()
            })
            .collect_vec();

        let to_drop_streaming_ids = to_drop_table_fragments
            .iter()
            .map(|t| t.table_id())
            .collect();

        debug!("clean dirty table fragments: {:?}", to_drop_streaming_ids);
        self.fragment_manager
            .drop_table_fragments_vec(&to_drop_streaming_ids)
            .await?;

        // unregister compaction group for dirty table fragments.
        let _ = self.hummock_manager
            .unregister_table_fragments_vec(
                &to_drop_table_fragments
            )
            .await.inspect_err(|e|
            tracing::warn!(
            "Failed to unregister compaction group for {:#?}. They will be cleaned up on node restart. {:#?}",
            to_drop_table_fragments,
            e)
        );

        // clean up source connector dirty changes.
        self.source_manager
            .drop_source_change(&to_drop_table_fragments)
            .await;

        Ok(())
    }

    /// Recovery the whole cluster from the latest epoch.
    pub(crate) async fn recovery(&self, prev_epoch: TracedEpoch) -> TracedEpoch {
        // pause discovery of all connector split changes and trigger config change.
        let _source_pause_guard = self.source_manager.paused.lock().await;

        // Abort buffered schedules, they might be dirty already.
        self.scheduled_barriers.abort().await;

        tracing::info!("recovery start!");
        self.clean_dirty_fragments()
            .await
            .expect("clean dirty fragments");
        let retry_strategy = Self::get_retry_strategy();

        // We take retry into consideration because this is the latency user sees for a cluster to
        // get recovered.
        let recovery_timer = self.metrics.recovery_latency.start_timer();
        let (new_epoch, _responses) = tokio_retry::Retry::spawn(retry_strategy, || async {
            let recovery_result: MetaResult<(TracedEpoch, Vec<BarrierCompleteResponse>)> = try {
                let mut info = self.resolve_actor_info_for_recovery().await;
                let mut new_epoch = prev_epoch.next();

                // Migrate actors in expired CN to newly joined one.
                let migrated = self.migrate_actors(&info).await.inspect_err(|err| {
                    warn!(err = ?err, "migrate actors failed");
                })?;
                if migrated {
                    info = self.resolve_actor_info_for_recovery().await;
                }

                // Reset all compute nodes, stop and drop existing actors.
                self.reset_compute_nodes(&info).await.inspect_err(|err| {
                    warn!(err = ?err, "reset compute nodes failed");
                })?;

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
                })));

                let prev_epoch = new_epoch;
                new_epoch = prev_epoch.next();
                // checkpoint, used as init barrier to initialize all executors.
                let command_ctx = Arc::new(CommandContext::new(
                    self.fragment_manager.clone(),
                    self.env.stream_client_pool_ref(),
                    info,
                    prev_epoch,
                    new_epoch.clone(),
                    command,
                    true,
                    self.source_manager.clone(),
                ));

                #[cfg(not(all(test, feature = "failpoints")))]
                {
                    use risingwave_common::util::epoch::INVALID_EPOCH;

                    let mce = self
                        .hummock_manager
                        .get_current_version()
                        .await
                        .max_committed_epoch;

                    if mce != INVALID_EPOCH {
                        command_ctx.wait_epoch_commit(mce).await?;
                    }
                }

                let (barrier_complete_tx, mut barrier_complete_rx) =
                    tokio::sync::mpsc::unbounded_channel();
                self.inject_barrier(command_ctx.clone(), &barrier_complete_tx)
                    .await;
                let res = match barrier_complete_rx.recv().await.unwrap().result {
                    Ok(response) => {
                        if let Err(err) = command_ctx.post_collect().await {
                            warn!(err = ?err, "post_collect failed");
                            Err(err)
                        } else {
                            Ok((new_epoch, response))
                        }
                    }
                    Err(err) => {
                        warn!(err = ?err, "inject_barrier failed");
                        Err(err)
                    }
                };
                res?
            };
            if recovery_result.is_err() {
                self.metrics.recovery_failure_cnt.inc();
            }
            recovery_result
        })
        .await
        .expect("Retry until recovery success.");
        recovery_timer.observe_duration();
        tracing::info!("recovery success");

        new_epoch
    }

    /// Migrate actors in expired CNs to newly joined ones, return true if any actor is migrated.
    async fn migrate_actors(&self, info: &BarrierActorInfo) -> MetaResult<bool> {
        debug!("start migrate actors.");

        // 1. get expired workers.
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
        let migration_plan = self.generate_migration_plan(info, expired_workers).await?;
        // 2. start to migrate fragment one-by-one.
        self.fragment_manager
            .migrate_fragment_actors(&migration_plan)
            .await?;
        // 3. remove the migration plan.
        migration_plan.delete(self.env.meta_store()).await?;

        debug!("migrate actors succeed.");
        Ok(true)
    }

    /// This function will generate a migration plan, which includes the mapping for all expired and
    /// in-used parallel unit to a new one.
    async fn generate_migration_plan(
        &self,
        info: &BarrierActorInfo,
        expired_workers: HashSet<WorkerId>,
    ) -> MetaResult<MigrationPlan> {
        let mut cached_plan = MigrationPlan::get(self.env.meta_store()).await?;

        let all_worker_parallel_units = self.fragment_manager.all_worker_parallel_units().await;

        let mut to_migrate_parallel_units: BTreeSet<_> = all_worker_parallel_units
            .iter()
            .filter(|(worker, _)| expired_workers.contains(worker))
            .flat_map(|(_, pu)| pu.iter().cloned())
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
            let current_nodes = self
                .cluster_manager
                .list_active_streaming_compute_nodes()
                .await;
            let new_nodes = current_nodes
                .into_iter()
                .filter(|node| {
                    !info.actor_map.contains_key(&node.id)
                        && !cached_plan
                            .parallel_unit_plan
                            .values()
                            .map(|pu| pu.worker_node_id)
                            .contains(&node.id)
                })
                .collect_vec();
            if !new_nodes.is_empty() {
                debug!("new workers joined: {:#?}", new_nodes);
                let target_parallel_units = new_nodes
                    .into_iter()
                    .flat_map(|node| node.parallel_units)
                    .collect_vec();
                for target_parallel_unit in target_parallel_units {
                    if let Some(from) = to_migrate_parallel_units.pop() {
                        debug!(
                            "plan to migrate from parallel unit {} to {}",
                            from, target_parallel_unit.id
                        );
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
    async fn update_actors(&self, info: &BarrierActorInfo) -> MetaResult<()> {
        let mut actor_infos = vec![];
        for (node_id, actors) in &info.actor_map {
            let host = info
                .node_map
                .get(node_id)
                .ok_or_else(|| anyhow::anyhow!("worker evicted, wait for online."))?
                .host
                .clone();
            actor_infos.extend(actors.iter().map(|&actor_id| ActorInfo {
                actor_id,
                host: host.clone(),
            }));
        }

        let node_actors = self.fragment_manager.all_node_actors(false).await;
        for (node_id, actors) in &info.actor_map {
            let node = info.node_map.get(node_id).unwrap();
            let client = self.env.stream_client_pool().get(node).await?;

            client
                .broadcast_actor_info_table(BroadcastActorInfoTableRequest {
                    info: actor_infos.clone(),
                })
                .await?;

            let request_id = Uuid::new_v4().to_string();
            tracing::debug!(request_id = request_id.as_str(), actors = ?actors, "update actors");
            client
                .update_actors(UpdateActorsRequest {
                    request_id,
                    actors: node_actors.get(node_id).cloned().unwrap_or_default(),
                })
                .await?;
        }

        Ok(())
    }

    /// Build all actors in compute nodes.
    async fn build_actors(&self, info: &BarrierActorInfo) -> MetaResult<()> {
        for (node_id, actors) in &info.actor_map {
            let node = info.node_map.get(node_id).unwrap();
            let client = self.env.stream_client_pool().get(node).await?;

            let request_id = Uuid::new_v4().to_string();
            tracing::debug!(request_id = request_id.as_str(), actors = ?actors, "build actors");
            client
                .build_actors(BuildActorsRequest {
                    request_id,
                    actor_id: actors.to_owned(),
                })
                .await?;
        }

        Ok(())
    }

    /// Reset all compute nodes by calling `force_stop_actors`.
    async fn reset_compute_nodes(&self, info: &BarrierActorInfo) -> MetaResult<()> {
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

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

use std::cmp::{Ordering, max, min};
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};

use anyhow::{Context, anyhow};
use itertools::Itertools;
use risingwave_common::bail;
use risingwave_common::catalog::{DatabaseId, TableId};
use risingwave_common::config::DefaultParallelism;
use risingwave_common::hash::WorkerSlotId;
use risingwave_common::util::stream_graph_visitor::visit_stream_node_cont;
use risingwave_connector::source::cdc::CdcTableSnapshotSplitAssignmentWithGeneration;
use risingwave_hummock_sdk::version::HummockVersion;
use risingwave_meta_model::{ObjectId, StreamingParallelism};
use risingwave_pb::catalog::table::PbTableType;
use risingwave_pb::plan_common::ExprContext;
use risingwave_pb::stream_plan::stream_node::PbNodeBody;
use thiserror_ext::AsReport;
use tracing::{info, warn};

use super::BarrierWorkerRuntimeInfoSnapshot;
use crate::MetaResult;
use crate::barrier::context::GlobalBarrierWorkerContextImpl;
use crate::barrier::info::InflightStreamingJobInfo;
use crate::barrier::{DatabaseRuntimeInfoSnapshot, InflightSubscriptionInfo};
use crate::controller::fragment::InflightActorInfo;
use crate::manager::ActiveStreamingWorkerNodes;
use crate::model::{ActorId, StreamActor, StreamJobFragments, TableParallelism};
use crate::rpc::ddl_controller::refill_upstream_sink_union_in_table;
use crate::stream::cdc::assign_cdc_table_snapshot_splits_pairs;
use crate::stream::{SourceChange, StreamFragmentGraph};

impl GlobalBarrierWorkerContextImpl {
    /// Clean catalogs for creating streaming jobs that are in foreground mode or table fragments not persisted.
    async fn clean_dirty_streaming_jobs(&self, database_id: Option<DatabaseId>) -> MetaResult<()> {
        let database_id = database_id.map(|database_id| database_id.database_id as _);
        self.metadata_manager
            .catalog_controller
            .clean_dirty_subscription(database_id)
            .await?;
        let dirty_associated_source_ids = self
            .metadata_manager
            .catalog_controller
            .clean_dirty_creating_jobs(database_id)
            .await?;

        // unregister cleaned sources.
        self.source_manager
            .apply_source_change(SourceChange::DropSource {
                dropped_source_ids: dirty_associated_source_ids,
            })
            .await;

        Ok(())
    }

    async fn purge_state_table_from_hummock(
        &self,
        all_state_table_ids: &HashSet<TableId>,
    ) -> MetaResult<()> {
        self.hummock_manager.purge(all_state_table_ids).await?;
        Ok(())
    }

    // async fn list_background_job_progress(&self) -> MetaResult<Vec<(String, StreamJobFragments)>> {
    //     let mgr = &self.metadata_manager;
    //     let job_info = mgr
    //         .catalog_controller
    //         .list_background_creating_jobs(false)
    //         .await?;
    //
    //     try_join_all(
    //         job_info
    //             .into_iter()
    //             .map(|(id, definition, _init_at)| async move {
    //                 let table_id = TableId::new(id as _);
    //                 let stream_job_fragments = mgr.get_job_fragments_by_id(&table_id).await?;
    //                 assert_eq!(stream_job_fragments.stream_job_id(), table_id);
    //                 Ok((definition, stream_job_fragments))
    //             }),
    //     )
    //     .await
    //     // If failed, enter recovery mode.
    // }

    async fn list_background_jobs(&self) -> MetaResult<HashSet<ObjectId>> {
        let mgr = &self.metadata_manager;
        let job_info = mgr
            .catalog_controller
            .list_background_creating_jobs(false)
            .await?;

        let job_ids = job_info.into_iter().map(|(id, _, _)| id).collect();

        Ok(job_ids)
    }

    /// Resolve actor information from cluster, fragment manager and `ChangedTableId`.
    /// We use `changed_table_id` to modify the actors to be sent or collected. Because these actor
    /// will create or drop before this barrier flow through them.
    async fn resolve_graph_info(
        &self,
        database_id: Option<DatabaseId>,
        worker_nodes: &ActiveStreamingWorkerNodes,
    ) -> MetaResult<HashMap<DatabaseId, HashMap<TableId, InflightStreamingJobInfo>>> {
        println!("worker nodes {:#?}", worker_nodes);
        let database_id = database_id.map(|database_id| database_id.database_id as _);
        let all_actor_infos = self
            .metadata_manager
            .catalog_controller
            .load_all_actors_dynamic(database_id, worker_nodes)
            .await?;

        Ok(all_actor_infos
            .into_iter()
            .map(|(loaded_database_id, job_fragment_infos)| {
                if let Some(database_id) = database_id {
                    assert_eq!(database_id, loaded_database_id);
                }
                (
                    DatabaseId::new(loaded_database_id as _),
                    job_fragment_infos
                        .into_iter()
                        .map(|(job_id, fragment_infos)| {
                            let job_id = TableId::new(job_id as _);
                            (
                                job_id,
                                InflightStreamingJobInfo {
                                    job_id,
                                    fragment_infos: fragment_infos
                                        .into_iter()
                                        .map(|(fragment_id, info)| (fragment_id as _, info))
                                        .collect(),
                                },
                            )
                        })
                        .collect(),
                )
            })
            .collect())
    }

    #[expect(clippy::type_complexity)]
    fn resolve_hummock_version_epochs(
        background_jobs: &HashMap<TableId, (String, StreamJobFragments)>,
        version: &HummockVersion,
    ) -> MetaResult<(
        HashMap<TableId, u64>,
        HashMap<TableId, Vec<(Vec<u64>, u64)>>,
    )> {
        let table_committed_epoch: HashMap<_, _> = version
            .state_table_info
            .info()
            .iter()
            .map(|(table_id, info)| (*table_id, info.committed_epoch))
            .collect();
        let get_table_committed_epoch = |table_id| -> anyhow::Result<u64> {
            Ok(*table_committed_epoch
                .get(&table_id)
                .ok_or_else(|| anyhow!("cannot get committed epoch on table {}.", table_id))?)
        };
        let mut min_downstream_committed_epochs = HashMap::new();
        for (_, job) in background_jobs.values() {
            let Ok(job_committed_epoch) = get_table_committed_epoch(job.stream_job_id) else {
                // Question: should we get the committed epoch from any state tables in the job?
                warn!(
                    "background job {} has no committed epoch, skip resolving epochs",
                    job.stream_job_id
                );
                continue;
            };
            if let (Some(snapshot_backfill_info), _) =
                StreamFragmentGraph::collect_snapshot_backfill_info_impl(
                    job.fragments()
                        .map(|fragment| (&fragment.nodes, fragment.fragment_type_mask)),
                )?
            {
                for (upstream_table, snapshot_epoch) in
                    snapshot_backfill_info.upstream_mv_table_id_to_backfill_epoch
                {
                    let snapshot_epoch = snapshot_epoch.ok_or_else(|| {
                        anyhow!(
                            "recovered snapshot backfill job has not filled snapshot epoch: {:?}",
                            job
                        )
                    })?;
                    let pinned_epoch = max(snapshot_epoch, job_committed_epoch);
                    match min_downstream_committed_epochs.entry(upstream_table) {
                        Entry::Occupied(entry) => {
                            let prev_min_epoch = entry.into_mut();
                            *prev_min_epoch = min(*prev_min_epoch, pinned_epoch);
                        }
                        Entry::Vacant(entry) => {
                            entry.insert(pinned_epoch);
                        }
                    }
                }
            }
        }
        let mut log_epochs = HashMap::new();
        for (upstream_table_id, downstream_committed_epoch) in min_downstream_committed_epochs {
            let upstream_committed_epoch = get_table_committed_epoch(upstream_table_id)?;
            match upstream_committed_epoch.cmp(&downstream_committed_epoch) {
                Ordering::Less => {
                    return Err(anyhow!(
                        "downstream epoch {} later than upstream epoch {} of table {}",
                        downstream_committed_epoch,
                        upstream_committed_epoch,
                        upstream_table_id
                    )
                    .into());
                }
                Ordering::Equal => {
                    continue;
                }
                Ordering::Greater => {
                    if let Some(table_change_log) = version.table_change_log.get(&upstream_table_id)
                    {
                        let epochs = table_change_log
                            .filter_epoch((downstream_committed_epoch, upstream_committed_epoch))
                            .map(|epoch_log| {
                                (
                                    epoch_log.non_checkpoint_epochs.clone(),
                                    epoch_log.checkpoint_epoch,
                                )
                            })
                            .collect_vec();
                        let first_epochs = epochs.first();
                        if let Some((_, first_checkpoint_epoch)) = &first_epochs
                            && *first_checkpoint_epoch == downstream_committed_epoch
                        {
                        } else {
                            return Err(anyhow!(
                                "resolved first log epoch {:?} on table {} not matched with downstream committed epoch {}",
                                epochs, upstream_table_id, downstream_committed_epoch).into()
                            );
                        }
                        log_epochs
                            .try_insert(upstream_table_id, epochs)
                            .expect("non-duplicated");
                    } else {
                        return Err(anyhow!(
                            "upstream table {} on epoch {} has lagged downstream on epoch {} but no table change log",
                            upstream_table_id, upstream_committed_epoch, downstream_committed_epoch).into()
                        );
                    }
                }
            }
        }
        Ok((table_committed_epoch, log_epochs))
    }

    /// For normal DDL operations, the `UpstreamSinkUnion` operator is modified dynamically, and does not persist the
    /// newly added or deleted upstreams in meta-store. Therefore, when restoring jobs, we need to restore the
    /// information required by the operator based on the current state of the upstream (sink) and downstream (table) of
    /// the operator.
    async fn recovery_table_with_upstream_sinks(
        &self,
        inflight_jobs: &mut HashMap<DatabaseId, HashMap<TableId, InflightStreamingJobInfo>>,
    ) -> MetaResult<()> {
        let mut jobs = inflight_jobs.values_mut().try_fold(
            HashMap::new(),
            |mut acc, table_map| -> MetaResult<_> {
                for (tid, job) in table_map {
                    if acc.insert(tid.table_id, job).is_some() {
                        return Err(anyhow::anyhow!("Duplicate table id found: {:?}", tid).into());
                    }
                }
                Ok(acc)
            },
        )?;
        let job_ids = jobs.keys().cloned().collect_vec();
        // Only `Table` will be returned here, ignoring other catalog objects.
        let tables = self
            .metadata_manager
            .catalog_controller
            .get_user_created_table_by_ids(job_ids.into_iter().map(|id| id as _).collect())
            .await?;
        for table in tables {
            assert_eq!(table.table_type(), PbTableType::Table);
            let fragments = jobs.get_mut(&table.id).unwrap();
            let mut target_fragment_id = None;
            for fragment in fragments.fragment_infos.values() {
                let mut is_target_fragment = false;
                visit_stream_node_cont(&fragment.nodes, |node| {
                    if let Some(PbNodeBody::UpstreamSinkUnion(_)) = node.node_body {
                        is_target_fragment = true;
                        false
                    } else {
                        true
                    }
                });
                if is_target_fragment {
                    target_fragment_id = Some(fragment.fragment_id);
                    break;
                }
            }
            let Some(target_fragment_id) = target_fragment_id else {
                tracing::debug!(
                    "The table {} created by old versions has not yet been migrated, so sinks cannot be created or dropped on this table.",
                    table.id
                );
                continue;
            };
            let target_fragment = fragments
                .fragment_infos
                .get_mut(&target_fragment_id)
                .unwrap();
            let upstream_infos = self
                .metadata_manager
                .catalog_controller
                .get_all_upstream_sink_infos(&table, target_fragment_id as _)
                .await?;
            refill_upstream_sink_union_in_table(&mut target_fragment.nodes, &upstream_infos);
        }

        Ok(())
    }

    pub(super) async fn reload_runtime_info_impl(
        &self,
    ) -> MetaResult<BarrierWorkerRuntimeInfoSnapshot> {
        {
            {
                {
                    self.clean_dirty_streaming_jobs(None)
                        .await
                        .context("clean dirty streaming jobs")?;

                    // This is a quick path to accelerate the process of dropping and canceling streaming jobs.
                    let _ = self.scheduled_barriers.pre_apply_drop_cancel(None);

                    let active_streaming_nodes =
                        ActiveStreamingWorkerNodes::new_snapshot(self.metadata_manager.clone())
                            .await?;

                    let background_streaming_jobs = self.list_background_jobs().await?;

                    println!(
                        "background streaming jobs: {:?} total {}",
                        background_streaming_jobs,
                        background_streaming_jobs.len()
                    );

                    let unreschedulable_jobs = {
                        let mut unreschedulable_jobs = HashSet::new();

                        for job_id in &background_streaming_jobs {
                            let scan_types = self
                                .metadata_manager
                                .get_job_backfill_scan_types(*job_id)
                                .await?;

                            if scan_types
                                .values()
                                .any(|scan_type| !scan_type.is_reschedulable())
                            {
                                unreschedulable_jobs.insert(*job_id);
                            }
                        }

                        unreschedulable_jobs
                    };

                    if !unreschedulable_jobs.is_empty() {
                        tracing::info!(
                            "unreschedulable background jobs: {:?}",
                            unreschedulable_jobs
                        );
                    }

                    // Resolve actor info for recovery. If there's no actor to recover, most of the
                    // following steps will be no-op, while the compute nodes will still be reset.
                    // FIXME: Transactions should be used.
                    // TODO(error-handling): attach context to the errors and log them together, instead of inspecting everywhere.
                    let mut info = if unreschedulable_jobs.is_empty() {
                        info!("trigger offline scaling");
                        self.resolve_graph_info(None, &active_streaming_nodes)
                            .await
                            .inspect_err(|err| {
                                warn!(error = %err.as_report(), "resolve actor info failed");
                            })?
                    } else {
                        bail!("unimpl");
                    };

                    if self.scheduled_barriers.pre_apply_drop_cancel(None) {
                        info = self
                            .resolve_graph_info(None, &active_streaming_nodes)
                            .await
                            .inspect_err(|err| {
                                warn!(error = %err.as_report(), "resolve actor info failed");
                            })?
                    }

                    self.recovery_table_with_upstream_sinks(&mut info).await?;

                    let info = info;

                    self.purge_state_table_from_hummock(
                        &info
                            .values()
                            .flatten()
                            .flat_map(|(_, job)| job.existing_table_ids())
                            .collect(),
                    )
                    .await
                    .context("purge state table from hummock")?;

                    println!("generated info {:?}", info);

                    let background_jobs = {
                        let mut background_jobs = HashMap::new();

                        let job_infos: HashMap<_, _> = info
                            .values()
                            .flatten()
                            .filter(|(job_id, _)| {
                                background_streaming_jobs.contains(&(job_id.table_id as _))
                            })
                            .map(|(table_id, job_info)| {
                                (
                                    table_id.table_id() as ObjectId,
                                    job_info
                                        .fragment_infos
                                        .iter()
                                        .map(|(fragment_id, fragment_info)| {
                                            (*fragment_id as _, fragment_info.clone())
                                        })
                                        .collect(),
                                )
                            })
                            .collect();
                        let jobs = self
                            .metadata_manager
                            .catalog_controller
                            .restore_background_streaming_job(job_infos)
                            .await?;

                        for (job_id, info) in jobs {
                            background_jobs
                                .try_insert(job_id, info)
                                .expect("non-duplicate");
                        }
                        background_jobs
                    };

                    let (state_table_committed_epochs, state_table_log_epochs) = self
                        .hummock_manager
                        .on_current_version(|version| {
                            Self::resolve_hummock_version_epochs(&background_jobs, version)
                        })
                        .await?;

                    let subscription_infos = self
                        .metadata_manager
                        .get_mv_depended_subscriptions(None)
                        .await?
                        .into_iter()
                        .map(|(database_id, mv_depended_subscriptions)| {
                            (
                                database_id,
                                InflightSubscriptionInfo {
                                    mv_depended_subscriptions,
                                },
                            )
                        })
                        .collect();

                    // update and build all actors.
                    // let stream_actors = self.load_all_actors().await.inspect_err(|err| {
                    //     warn!(error = %err.as_report(), "update actors failed");
                    // })?;

                    let stream_actors = Self::fake_stream_actors(info.clone());

                    let fragment_relations = self
                        .metadata_manager
                        .catalog_controller
                        .get_fragment_downstream_relations(
                            info.values()
                                .flatten()
                                .flat_map(|(_, job)| job.fragment_infos())
                                .map(|fragment| fragment.fragment_id as _)
                                .collect(),
                        )
                        .await?;

                    let database_infos = self
                        .metadata_manager
                        .catalog_controller
                        .list_databases()
                        .await?;

                    // get split assignments for all actors
                    let source_splits = self.source_manager.list_assignments().await;
                    let cdc_table_backfill_actors = self
                        .metadata_manager
                        .catalog_controller
                        .cdc_table_backfill_actor_ids()?;

                    let cdc_table_ids = cdc_table_backfill_actors
                        .keys()
                        .cloned()
                        .collect::<Vec<_>>();
                    let cdc_table_snapshot_split_assignment =
                        assign_cdc_table_snapshot_splits_pairs(
                            cdc_table_backfill_actors,
                            self.env.meta_store_ref(),
                            self.env.cdc_table_backfill_tracker.completed_job_ids(),
                        )
                        .await?;
                    let cdc_table_snapshot_split_assignment =
                        if cdc_table_snapshot_split_assignment.is_empty() {
                            CdcTableSnapshotSplitAssignmentWithGeneration::empty()
                        } else {
                            let generation = self
                                .env
                                .cdc_table_backfill_tracker
                                .next_generation(cdc_table_ids.into_iter());
                            CdcTableSnapshotSplitAssignmentWithGeneration::new(
                                cdc_table_snapshot_split_assignment,
                                generation,
                            )
                        };
                    Ok(BarrierWorkerRuntimeInfoSnapshot {
                        active_streaming_nodes,
                        database_job_infos: info,
                        state_table_committed_epochs,
                        state_table_log_epochs,
                        subscription_infos,
                        stream_actors,
                        fragment_relations,
                        source_splits,
                        background_jobs,
                        hummock_version_stats: self.hummock_manager.get_version_stats().await,
                        database_infos,
                        cdc_table_snapshot_split_assignment,
                    })
                }
            }
        }
    }

    pub(super) async fn reload_database_runtime_info_impl(
        &self,
        database_id: DatabaseId,
    ) -> MetaResult<Option<DatabaseRuntimeInfoSnapshot>> {
        self.clean_dirty_streaming_jobs(Some(database_id))
            .await
            .context("clean dirty streaming jobs")?;

        // Background job progress needs to be recovered.
        tracing::info!(
            ?database_id,
            "recovering background job progress of database"
        );
        tracing::info!(?database_id, "recovered background job progress");

        // This is a quick path to accelerate the process of dropping and canceling streaming jobs.
        let _ = self
            .scheduled_barriers
            .pre_apply_drop_cancel(Some(database_id));

        let active_streaming_nodes =
            ActiveStreamingWorkerNodes::new_snapshot(self.metadata_manager.clone()).await?;

        let all_info = self
            .resolve_graph_info(None, &active_streaming_nodes)
            .await
            .inspect_err(|err| {
                warn!(error = %err.as_report(), "resolve actor info failed");
            })?;

        let mut info = HashMap::from([(database_id, all_info[&database_id].clone())]);

        self.recovery_table_with_upstream_sinks(&mut info).await?;

        let Some(info) = info.into_iter().next().map(|(loaded_database_id, info)| {
            assert_eq!(loaded_database_id, database_id);
            info
        }) else {
            return Ok(None);
        };

        let background_streaming_jobs = self.list_background_jobs().await?;

        let background_jobs = {
            let job_infos: HashMap<_, _> = info
                .iter()
                .filter(|(job_id, _)| background_streaming_jobs.contains(&(job_id.table_id as _)))
                .map(|(table_id, job_info)| {
                    (
                        table_id.table_id() as ObjectId,
                        job_info
                            .fragment_infos
                            .iter()
                            .map(|(fragment_id, fragment_info)| {
                                (*fragment_id as _, fragment_info.clone())
                            })
                            .collect(),
                    )
                })
                .collect();
            let jobs = self
                .metadata_manager
                .catalog_controller
                .restore_background_streaming_job(job_infos)
                .await?;

            jobs.into_values().collect_vec()
        };

        let background_jobs = {
            let jobs = background_jobs;
            let mut background_jobs = HashMap::new();
            for (definition, stream_job_fragments) in jobs {
                if !info.contains_key(&stream_job_fragments.stream_job_id()) {
                    continue;
                }
                if stream_job_fragments
                    .tracking_progress_actor_ids()
                    .is_empty()
                {
                    // If there's no tracking actor in the job, we can finish the job directly.
                    self.metadata_manager
                        .catalog_controller
                        .finish_streaming_job(stream_job_fragments.stream_job_id().table_id as _)
                        .await?;
                } else {
                    background_jobs
                        .try_insert(
                            stream_job_fragments.stream_job_id(),
                            (definition, stream_job_fragments),
                        )
                        .expect("non-duplicate");
                }
            }
            background_jobs
        };

        let (state_table_committed_epochs, state_table_log_epochs) = self
            .hummock_manager
            .on_current_version(|version| {
                Self::resolve_hummock_version_epochs(&background_jobs, version)
            })
            .await?;

        let subscription_infos = self
            .metadata_manager
            .get_mv_depended_subscriptions(Some(database_id))
            .await?;
        assert!(subscription_infos.len() <= 1);
        let mv_depended_subscriptions = subscription_infos
            .into_iter()
            .next()
            .map(|(loaded_database_id, subscriptions)| {
                assert_eq!(loaded_database_id, database_id);
                subscriptions
            })
            .unwrap_or_default();
        let subscription_info = InflightSubscriptionInfo {
            mv_depended_subscriptions,
        };

        let fragment_relations = self
            .metadata_manager
            .catalog_controller
            .get_fragment_downstream_relations(
                info.values()
                    .flatten()
                    .map(|fragment| fragment.fragment_id as _)
                    .collect(),
            )
            .await?;

        // // update and build all actors.
        // let stream_actors = self.load_all_actors().await.inspect_err(|err| {
        //     warn!(error = %err.as_report(), "update actors failed");
        // })?;

        let stream_actors = Self::fake_stream_actors(all_info);

        // get split assignments for all actors
        let source_splits = self.source_manager.list_assignments().await;

        let cdc_table_backfill_actors = self
            .metadata_manager
            .catalog_controller
            .cdc_table_backfill_actor_ids()?;

        let cdc_table_ids = cdc_table_backfill_actors
            .keys()
            .cloned()
            .collect::<Vec<_>>();

        let cdc_table_snapshot_split_assignment = assign_cdc_table_snapshot_splits_pairs(
            cdc_table_backfill_actors,
            self.env.meta_store_ref(),
            self.env.cdc_table_backfill_tracker.completed_job_ids(),
        )
        .await?;
        let cdc_table_snapshot_split_assignment = if cdc_table_snapshot_split_assignment.is_empty()
        {
            CdcTableSnapshotSplitAssignmentWithGeneration::empty()
        } else {
            CdcTableSnapshotSplitAssignmentWithGeneration::new(
                cdc_table_snapshot_split_assignment,
                self.env
                    .cdc_table_backfill_tracker
                    .next_generation(cdc_table_ids.into_iter()),
            )
        };
        Ok(Some(DatabaseRuntimeInfoSnapshot {
            job_infos: info,
            state_table_committed_epochs,
            state_table_log_epochs,
            subscription_info,
            stream_actors,
            fragment_relations,
            source_splits,
            background_jobs,
            cdc_table_snapshot_split_assignment,
        }))
    }

    fn fake_stream_actors(
        all_info: HashMap<DatabaseId, HashMap<TableId, InflightStreamingJobInfo>>,
    ) -> HashMap<ActorId, StreamActor> {
        let mut stream_actors = HashMap::new();

        for (_, streaming_info) in all_info.values().flatten() {
            for (fragment_id, fragment_info) in &streaming_info.fragment_infos {
                for (actor_id, InflightActorInfo { vnode_bitmap, .. }) in &fragment_info.actors {
                    stream_actors.insert(
                        *actor_id,
                        StreamActor {
                            actor_id: *actor_id as _,
                            fragment_id: *fragment_id as _,
                            vnode_bitmap: vnode_bitmap.clone(),
                            mview_definition: "".to_owned(),
                            expr_context: Some(ExprContext::default()),
                        },
                    );
                }
            }
        }
        stream_actors
    }
}

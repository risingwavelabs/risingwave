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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt::Debug;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use anyhow::anyhow;
use futures::future;
use itertools::Itertools;
use risingwave_common::bail;
use risingwave_common::bitmap::Bitmap;
use risingwave_common::catalog::{DatabaseId, FragmentTypeMask};
use risingwave_common::hash::ActorMapping;
use risingwave_meta_model::{
    ObjectId, StreamingParallelism, WorkerId, fragment, fragment_relation,
};
use risingwave_pb::common::{PbWorkerNode, WorkerNode, WorkerType};
use risingwave_pb::meta::FragmentWorkerSlotMappings;
use risingwave_pb::meta::subscribe_response::{Info, Operation};
use risingwave_pb::meta::table_fragments::fragment::PbFragmentDistributionType;
use risingwave_pb::stream_plan::{Dispatcher, PbDispatchOutputMapping, PbDispatcher, StreamNode};
use sea_orm::{ActiveModelTrait, ConnectionTrait, QuerySelect};
use thiserror_ext::AsReport;
use tokio::sync::oneshot::Receiver;
use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard, oneshot};
use tokio::task::JoinHandle;
use tokio::time::{Instant, MissedTickBehavior};

use crate::barrier::{Command, Reschedule, SharedFragmentInfo};
use crate::controller::scale::{
    RenderedGraph, WorkerInfo, find_fragment_no_shuffle_dags_detailed, render_jobs,
};
use crate::manager::{LocalNotification, MetaSrvEnv, MetadataManager};
use crate::model::{
    ActorId, DispatcherId, FragmentId, StreamActor, StreamActorWithDispatchers, StreamContext,
};
use crate::serving::{
    ServingVnodeMapping, to_deleted_fragment_worker_slot_mapping, to_fragment_worker_slot_mapping,
};
use crate::stream::{GlobalStreamManager, SourceManagerRef};
use crate::{MetaError, MetaResult};

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct WorkerReschedule {
    pub worker_actor_diff: BTreeMap<WorkerId, isize>,
}

pub struct CustomFragmentInfo {
    pub job_id: u32,
    pub fragment_id: u32,
    pub fragment_type_mask: FragmentTypeMask,
    pub distribution_type: PbFragmentDistributionType,
    pub state_table_ids: Vec<u32>,
    pub node: StreamNode,
    pub actor_template: StreamActorWithDispatchers,
    pub actors: Vec<CustomActorInfo>,
}

#[derive(Default, Clone)]
pub struct CustomActorInfo {
    pub actor_id: u32,
    pub fragment_id: u32,
    pub dispatcher: Vec<Dispatcher>,
    /// `None` if singleton.
    pub vnode_bitmap: Option<Bitmap>,
}

use risingwave_common::system_param::AdaptiveParallelismStrategy;
use risingwave_common::system_param::reader::SystemParamsRead;
use risingwave_meta_model::DispatcherType;
use risingwave_meta_model::fragment::DistributionType;
use risingwave_meta_model::prelude::{Fragment, FragmentRelation, StreamingJob};
use sea_orm::ActiveValue::Set;
use sea_orm::{ColumnTrait, EntityTrait, IntoActiveModel, QueryFilter, TransactionTrait};

use crate::controller::fragment::{InflightActorInfo, InflightFragmentInfo};
use crate::controller::utils::{compose_dispatchers, get_streaming_job_runtime_info};

pub type ScaleControllerRef = Arc<ScaleController>;

pub struct ScaleController {
    pub metadata_manager: MetadataManager,

    pub source_manager: SourceManagerRef,

    pub env: MetaSrvEnv,

    /// We will acquire lock during DDL to prevent scaling operations on jobs that are in the creating state.
    /// e.g., a MV cannot be rescheduled during foreground backfill.
    pub reschedule_lock: RwLock<()>,
}

impl ScaleController {
    pub fn new(
        metadata_manager: &MetadataManager,
        source_manager: SourceManagerRef,
        env: MetaSrvEnv,
    ) -> Self {
        Self {
            metadata_manager: metadata_manager.clone(),
            source_manager,
            env,
            reschedule_lock: RwLock::new(()),
        }
    }

    pub async fn resolve_related_no_shuffle_jobs(
        &self,
        jobs: &[ObjectId],
    ) -> MetaResult<HashSet<ObjectId>> {
        let inner = self.metadata_manager.catalog_controller.inner.read().await;
        let txn = inner.db.begin().await?;

        let fragment_ids: Vec<_> = Fragment::find()
            .select_only()
            .column(fragment::Column::FragmentId)
            .filter(fragment::Column::JobId.is_in(jobs.to_vec()))
            .into_tuple()
            .all(&txn)
            .await?;
        let ensembles = find_fragment_no_shuffle_dags_detailed(&txn, &fragment_ids).await?;
        let related_fragments = ensembles
            .iter()
            .flat_map(|ensemble| ensemble.fragments())
            .collect_vec();

        let job_ids: Vec<_> = Fragment::find()
            .select_only()
            .column(fragment::Column::JobId)
            .filter(fragment::Column::FragmentId.is_in(related_fragments))
            .into_tuple()
            .all(&txn)
            .await?;

        let job_ids = job_ids.into_iter().collect();

        Ok(job_ids)
    }

    pub fn diff_fragment(
        &self,
        prev_fragment_info: &SharedFragmentInfo,
        curr_actors: &HashMap<ActorId, InflightActorInfo>,
        upstream_fragments: HashMap<FragmentId, DispatcherType>,
        downstream_fragments: HashMap<FragmentId, DispatcherType>,
        all_actor_dispatchers: HashMap<ActorId, Vec<PbDispatcher>>,
        job_runtime_info: Option<&(Option<String>, String)>,
    ) -> MetaResult<Reschedule> {
        let prev_actors: HashMap<_, _> = prev_fragment_info
            .actors
            .iter()
            .map(|(actor_id, actor)| (*actor_id, actor))
            .collect();

        let prev_ids: HashSet<_> = prev_actors.keys().cloned().collect();
        let curr_ids: HashSet<_> = curr_actors.keys().cloned().collect();

        let removed_actors: HashSet<_> = &prev_ids - &curr_ids;
        let added_actor_ids: HashSet<_> = &curr_ids - &prev_ids;
        let kept_ids: HashSet<_> = prev_ids.intersection(&curr_ids).cloned().collect();

        let mut added_actors = HashMap::new();
        for &actor_id in &added_actor_ids {
            let InflightActorInfo { worker_id, .. } = curr_actors
                .get(&actor_id)
                .ok_or_else(|| anyhow!("BUG: Worker not found for new actor {}", actor_id))?;

            added_actors
                .entry(*worker_id)
                .or_insert_with(Vec::new)
                .push(actor_id);
        }

        let mut vnode_bitmap_updates = HashMap::new();
        for actor_id in kept_ids {
            let prev_actor = prev_actors[&actor_id];
            let curr_actor = &curr_actors[&actor_id];

            // Check if the vnode distribution has changed.
            if prev_actor.vnode_bitmap != curr_actor.vnode_bitmap
                && let Some(bitmap) = curr_actor.vnode_bitmap.clone()
            {
                vnode_bitmap_updates.insert(actor_id, bitmap);
            }
        }

        let upstream_dispatcher_mapping =
            if let DistributionType::Hash = prev_fragment_info.distribution_type {
                let actor_mapping = curr_actors
                    .iter()
                    .map(
                        |(
                            actor_id,
                            InflightActorInfo {
                                worker_id: _,
                                vnode_bitmap,
                                ..
                            },
                        )| { (*actor_id, vnode_bitmap.clone().unwrap()) },
                    )
                    .collect();
                Some(ActorMapping::from_bitmaps(&actor_mapping))
            } else {
                None
            };

        let upstream_fragment_dispatcher_ids = upstream_fragments
            .iter()
            .filter(|&(_, dispatcher_type)| *dispatcher_type != DispatcherType::NoShuffle)
            .map(|(upstream_fragment, _)| {
                (
                    *upstream_fragment,
                    prev_fragment_info.fragment_id as DispatcherId,
                )
            })
            .collect();

        let downstream_fragment_ids = downstream_fragments
            .iter()
            .filter(|&(_, dispatcher_type)| *dispatcher_type != DispatcherType::NoShuffle)
            .map(|(fragment_id, _)| *fragment_id)
            .collect();

        let (timezone, job_definition) = job_runtime_info.cloned().unwrap_or_default();

        let newly_created_actors: HashMap<ActorId, (StreamActorWithDispatchers, WorkerId)> =
            added_actor_ids
                .iter()
                .map(|actor_id| {
                    let actor = StreamActor {
                        actor_id: *actor_id,
                        fragment_id: prev_fragment_info.fragment_id,
                        vnode_bitmap: curr_actors[actor_id].vnode_bitmap.clone(),
                        mview_definition: job_definition.clone(),
                        expr_context: Some(
                            StreamContext {
                                timezone: timezone.clone(),
                            }
                            .to_expr_context(),
                        ),
                    };
                    (
                        *actor_id,
                        (
                            (
                                actor,
                                all_actor_dispatchers
                                    .get(actor_id)
                                    .cloned()
                                    .unwrap_or_default(),
                            ),
                            curr_actors[actor_id].worker_id,
                        ),
                    )
                })
                .collect();

        let actor_splits = curr_actors
            .iter()
            .map(|(&actor_id, info)| (actor_id, info.splits.clone()))
            .collect();

        let reschedule = Reschedule {
            added_actors,
            removed_actors,
            vnode_bitmap_updates,
            upstream_fragment_dispatcher_ids,
            upstream_dispatcher_mapping,
            downstream_fragment_ids,
            newly_created_actors,
            actor_splits,
            cdc_table_snapshot_split_assignment: Default::default(),
            cdc_table_id: None,
        };

        Ok(reschedule)
    }

    #[await_tree::instrument]
    pub async fn post_apply_reschedule(
        &self,
        reschedules: &HashMap<FragmentId, Reschedule>,
    ) -> MetaResult<()> {
        // Update serving fragment info after rescheduling in meta store.
        if !reschedules.is_empty() {
            let workers = self
                .metadata_manager
                .list_active_serving_compute_nodes()
                .await?;
            let streaming_parallelisms = self
                .metadata_manager
                .running_fragment_parallelisms(Some(reschedules.keys().cloned().collect()))?;
            let serving_worker_slot_mapping = Arc::new(ServingVnodeMapping::default());
            let max_serving_parallelism = self
                .env
                .session_params_manager_impl_ref()
                .get_params()
                .await
                .batch_parallelism()
                .map(|p| p.get());
            let (upserted, failed) = serving_worker_slot_mapping.upsert(
                streaming_parallelisms,
                &workers,
                max_serving_parallelism,
            );
            if !upserted.is_empty() {
                tracing::debug!(
                    "Update serving vnode mapping for fragments {:?}.",
                    upserted.keys()
                );
                self.env
                    .notification_manager()
                    .notify_frontend_without_version(
                        Operation::Update,
                        Info::ServingWorkerSlotMappings(FragmentWorkerSlotMappings {
                            mappings: to_fragment_worker_slot_mapping(&upserted),
                        }),
                    );
            }
            if !failed.is_empty() {
                tracing::debug!(
                    "Fail to update serving vnode mapping for fragments {:?}.",
                    failed
                );
                self.env
                    .notification_manager()
                    .notify_frontend_without_version(
                        Operation::Delete,
                        Info::ServingWorkerSlotMappings(FragmentWorkerSlotMappings {
                            mappings: to_deleted_fragment_worker_slot_mapping(&failed),
                        }),
                    );
            }
        }

        Ok(())
    }

    pub async fn reschedule_inplace(
        &self,
        policy: HashMap<ObjectId, ReschedulePolicy>,
        workers: HashMap<WorkerId, PbWorkerNode>,
    ) -> MetaResult<HashMap<DatabaseId, Command>> {
        // jobs: HashMap<ObjectId, TargetResourcePolicy>,
        // workers: BTreeMap<WorkerId, Worker>,
        // ) -> MetaResult<HashMap<DatabaseId, HashMap<TableId, HashMap<FragmentId, crate::controller::fragment::InflightFragmentInfo >>>>

        let inner = self.metadata_manager.catalog_controller.inner.read().await;
        let txn = inner.db.begin().await?;

        for (table_id, target) in &policy {
            let streaming_job = StreamingJob::find_by_id(*table_id)
                .one(&txn)
                .await?
                .ok_or_else(|| MetaError::catalog_id_not_found("table", table_id))?;

            let max_parallelism = streaming_job.max_parallelism;

            let mut streaming_job = streaming_job.into_active_model();

            match &target {
                ReschedulePolicy::Parallelism(p) | ReschedulePolicy::Both(p, _) => {
                    if let StreamingParallelism::Fixed(n) = p.parallelism
                        && n > max_parallelism as usize
                    {
                        bail!(format!(
                            "specified parallelism {n} should not exceed max parallelism {max_parallelism}"
                        ));
                    }

                    streaming_job.parallelism = Set(p.parallelism.clone());
                }
                _ => {}
            }

            match &target {
                ReschedulePolicy::ResourceGroup(r) | ReschedulePolicy::Both(_, r) => {
                    streaming_job.specific_resource_group = Set(r.resource_group.clone());
                }
                _ => {}
            }

            streaming_job.update(&txn).await?;
        }

        // update
        let jobs = policy.keys().copied().collect();

        let workers = workers
            .into_iter()
            .map(|(id, worker)| {
                (
                    id,
                    WorkerInfo {
                        weight: NonZeroUsize::new(worker.compute_node_parallelism()).unwrap(),
                        resource_group: worker.resource_group(),
                    },
                )
            })
            .collect();

        let command = self.rerender_inner(&txn, jobs, workers).await?;

        txn.commit().await?;

        Ok(command)
    }

    async fn rerender(
        &self,
        jobs: HashSet<ObjectId>,
        workers: BTreeMap<WorkerId, WorkerInfo>,
    ) -> MetaResult<HashMap<DatabaseId, Command>> {
        let inner = self.metadata_manager.catalog_controller.inner.read().await;
        self.rerender_inner(&inner.db, jobs, workers).await
    }

    async fn rerender_inner(
        &self,
        txn: &impl ConnectionTrait,
        jobs: HashSet<ObjectId>,
        workers: BTreeMap<WorkerId, WorkerInfo>,
    ) -> MetaResult<HashMap<DatabaseId, Command>> {
        let adaptive_parallelism_strategy = {
            let system_params_reader = self.env.system_params_reader().await;
            system_params_reader.adaptive_parallelism_strategy()
        };

        let id_gen = self.env.id_gen_manager();

        let RenderedGraph {
            fragments: render_result,
            ..
        } = render_jobs(txn, id_gen, jobs, workers, adaptive_parallelism_strategy).await?;

        // for (db, jobs) in &render_result {
        //     println!("\tdb: {db}");
        //     for (job, fragments) in jobs {
        //         println!("\t\tjob: {job}");
        //         for (fragment, fragment_info) in fragments {
        //             println!("\t\t\tfragment: {fragment:?}");
        //             for (actor, actor_info) in &fragment_info.actors {
        //                 println!("\t\t\t\tactor: {actor:?}, {actor_info:?}");
        //             }
        //         }
        //     }
        // }

        let job_ids = render_result
            .values()
            .flat_map(|jobs| jobs.keys().copied())
            .collect_vec();

        let job_infos = get_streaming_job_runtime_info(txn, job_ids).await?;

        let fragment_ids = render_result
            .values()
            .flat_map(|jobs| jobs.values())
            .flatten()
            .map(|(fragment_id, _)| *fragment_id)
            .collect_vec();

        let upstreams: Vec<(
            risingwave_meta_model::FragmentId,
            risingwave_meta_model::FragmentId,
            DispatcherType,
        )> = FragmentRelation::find()
            .select_only()
            .columns([
                fragment_relation::Column::TargetFragmentId,
                fragment_relation::Column::SourceFragmentId,
                fragment_relation::Column::DispatcherType,
            ])
            .filter(fragment_relation::Column::TargetFragmentId.is_in(fragment_ids.clone()))
            .into_tuple()
            .all(txn)
            .await
            .unwrap();

        let downstreams = FragmentRelation::find()
            .filter(fragment_relation::Column::SourceFragmentId.is_in(fragment_ids.clone()))
            .all(txn)
            .await?;

        let mut all_upstream_fragments = HashMap::new();

        for (fragment, upstream, dispatcher) in upstreams {
            let fragment_id = fragment as FragmentId;
            let upstream_id = upstream as FragmentId;
            all_upstream_fragments
                .entry(fragment_id)
                .or_insert(HashMap::new())
                .insert(upstream_id, dispatcher);
        }

        let mut all_downstream_fragments = HashMap::new();

        let mut downstream_relations = HashMap::new();
        for relation in downstreams {
            let source_fragment_id = relation.source_fragment_id as FragmentId;
            let target_fragment_id = relation.target_fragment_id as FragmentId;
            all_downstream_fragments
                .entry(source_fragment_id)
                .or_insert(HashMap::new())
                .insert(target_fragment_id, relation.dispatcher_type);

            downstream_relations.insert((source_fragment_id, target_fragment_id), relation);
        }

        let all_related_fragment_ids: HashSet<_> = fragment_ids
            .iter()
            .copied()
            .chain(
                all_upstream_fragments
                    .values()
                    .flatten()
                    .map(|(id, _)| *id as i32),
            )
            .chain(
                all_downstream_fragments
                    .values()
                    .flatten()
                    .map(|(id, _)| *id as i32),
            )
            .collect();

        let all_related_fragment_ids = all_related_fragment_ids.into_iter().collect_vec();

        // let all_fragments_from_db: HashMap<_, _> = Fragment::find()
        //     .filter(fragment::Column::FragmentId.is_in(all_related_fragment_ids.clone()))
        //     .all(&txn)
        //     .await?
        //     .into_iter()
        //     .map(|f| (f.fragment_id, f))
        //     .collect();

        let all_prev_fragments: HashMap<_, _> = {
            let read_guard = self.env.shared_actor_infos().read_guard();
            all_related_fragment_ids
                .iter()
                .map(|&fragment_id| {
                    (
                        fragment_id,
                        read_guard
                            .get_fragment(fragment_id as FragmentId)
                            .cloned()
                            .unwrap(),
                    )
                })
                .collect()
        };

        let all_rendered_fragments: HashMap<_, _> = render_result
            .values()
            .flat_map(|jobs| jobs.values())
            .flatten()
            .map(|(fragment_id, info)| (*fragment_id, info))
            .collect();

        let mut commands = HashMap::new();

        for (database_id, jobs) in &render_result {
            let mut all_fragment_actors = HashMap::new();
            let mut reschedules = HashMap::new();

            for (job_id, fragment_id, fragment_info) in
                jobs.iter().flat_map(|(job_id, fragments)| {
                    fragments
                        .iter()
                        .map(move |(fragment_id, info)| (job_id, fragment_id, info))
                })
            {
                let InflightFragmentInfo {
                    distribution_type,
                    actors,
                    ..
                } = fragment_info;

                let upstream_fragments = all_upstream_fragments
                    .remove(&(*fragment_id as FragmentId))
                    .unwrap_or_default();
                let downstream_fragments = all_downstream_fragments
                    .remove(&(*fragment_id as FragmentId))
                    .unwrap_or_default();

                let fragment_actors: HashMap<_, _> = upstream_fragments
                    .keys()
                    .copied()
                    .chain(downstream_fragments.keys().copied())
                    .map(|fragment_id| {
                        let fragment = all_prev_fragments.get(&(fragment_id as i32)).unwrap();
                        (
                            fragment_id,
                            fragment.actors.keys().copied().collect::<HashSet<_>>(),
                        )
                    })
                    .collect();

                all_fragment_actors.extend(fragment_actors);

                let source_fragment_actors = actors
                    .iter()
                    .map(|(actor_id, info)| (*actor_id, info.vnode_bitmap.clone()))
                    .collect();

                let mut all_actor_dispatchers: HashMap<_, Vec<_>> = HashMap::new();

                for downstream_fragment_id in downstream_fragments.keys() {
                    let target_fragment_actors =
                        match all_rendered_fragments.get(&(*downstream_fragment_id as i32)) {
                            None => {
                                let external_fragment = all_prev_fragments
                                    .get(&(*downstream_fragment_id as i32))
                                    .unwrap();

                                external_fragment
                                    .actors
                                    .iter()
                                    .map(|(actor_id, info)| (*actor_id, info.vnode_bitmap.clone()))
                                    .collect()
                            }
                            Some(downstream_rendered) => downstream_rendered
                                .actors
                                .iter()
                                .map(|(actor_id, info)| (*actor_id, info.vnode_bitmap.clone()))
                                .collect(),
                        };

                    let target_fragment_distribution = *distribution_type;

                    let fragment_relation::Model {
                        source_fragment_id: _,
                        target_fragment_id: _,
                        dispatcher_type,
                        dist_key_indices,
                        output_indices,
                        output_type_mapping,
                    } = downstream_relations
                        .remove(&(
                            *fragment_id as FragmentId,
                            *downstream_fragment_id as FragmentId,
                        ))
                        .expect("downstream relation should exist");

                    let pb_mapping = PbDispatchOutputMapping {
                        indices: output_indices.into_u32_array(),
                        types: output_type_mapping.unwrap_or_default().to_protobuf(),
                    };

                    let dispatchers = compose_dispatchers(
                        *distribution_type,
                        &source_fragment_actors,
                        *downstream_fragment_id,
                        target_fragment_distribution,
                        &target_fragment_actors,
                        dispatcher_type,
                        dist_key_indices.into_u32_array(),
                        pb_mapping,
                    );

                    for (actor_id, dispatcher) in dispatchers {
                        all_actor_dispatchers
                            .entry(actor_id)
                            .or_default()
                            .push(dispatcher);
                    }
                }

                let prev_fragment = all_prev_fragments.get(&{ *fragment_id }).unwrap();

                let reschedule = self.diff_fragment(
                    prev_fragment,
                    actors,
                    upstream_fragments,
                    downstream_fragments,
                    all_actor_dispatchers,
                    job_infos.get(job_id).clone(),
                )?;

                reschedules.insert(*fragment_id as FragmentId, reschedule);
            }

            let command = Command::RescheduleFragment {
                reschedules,
                fragment_actors: all_fragment_actors,
            };

            commands.insert(DatabaseId::new(*database_id as u32), command);
        }

        Ok(commands)
    }
}

#[derive(Clone, Debug)]
pub struct ParallelismPolicy {
    pub parallelism: StreamingParallelism,
}

#[derive(Clone, Debug)]
pub struct ResourceGroupPolicy {
    pub resource_group: Option<String>,
}

#[derive(Clone, Debug)]
pub enum ReschedulePolicy {
    Parallelism(ParallelismPolicy),
    ResourceGroup(ResourceGroupPolicy),
    Both(ParallelismPolicy, ResourceGroupPolicy),
}

impl GlobalStreamManager {
    #[await_tree::instrument("acquire_reschedule_read_guard")]
    pub async fn reschedule_lock_read_guard(&self) -> RwLockReadGuard<'_, ()> {
        self.scale_controller.reschedule_lock.read().await
    }

    #[await_tree::instrument("acquire_reschedule_write_guard")]
    pub async fn reschedule_lock_write_guard(&self) -> RwLockWriteGuard<'_, ()> {
        self.scale_controller.reschedule_lock.write().await
    }

    /// When new worker nodes joined, or the parallelism of existing worker nodes changed,
    /// examines if there are any jobs can be scaled, and scales them if found.
    ///
    /// This method will iterate over all `CREATED` jobs, and can be repeatedly called.
    ///
    /// Returns
    /// - `Ok(false)` if no jobs can be scaled;
    /// - `Ok(true)` if some jobs are scaled, and it is possible that there are more jobs can be scaled.
    async fn trigger_parallelism_control(&self) -> MetaResult<bool> {
        tracing::info!("trigger parallelism control");

        let _reschedule_job_lock = self.reschedule_lock_write_guard().await;

        let background_streaming_jobs = self
            .metadata_manager
            .list_background_creating_jobs()
            .await?;

        let skipped_jobs = if !background_streaming_jobs.is_empty() {
            let jobs = self
                .scale_controller
                .resolve_related_no_shuffle_jobs(&background_streaming_jobs)
                .await?;

            tracing::info!(
                "skipping parallelism control of background jobs {:?} and associated jobs {:?}",
                background_streaming_jobs,
                jobs
            );

            jobs
        } else {
            HashSet::new()
        };

        let database_objects: HashMap<risingwave_meta_model::DatabaseId, Vec<ObjectId>> = self
            .metadata_manager
            .catalog_controller
            .list_streaming_job_with_database()
            .await?;

        let job_ids = database_objects
            .iter()
            .flat_map(|(database_id, job_ids)| {
                job_ids
                    .iter()
                    .enumerate()
                    .map(move |(idx, job_id)| (idx, database_id, job_id))
            })
            .sorted_by(|(idx_a, database_a, _), (idx_b, database_b, _)| {
                idx_a.cmp(idx_b).then(database_a.cmp(database_b))
            })
            .map(|(_, database_id, job_id)| (*database_id, *job_id))
            .filter(|(_, job_id)| !skipped_jobs.contains(job_id))
            .collect_vec();

        if job_ids.is_empty() {
            tracing::info!("no streaming jobs for scaling, maybe an empty cluster");
            return Ok(false);
        }

        let workers = self
            .metadata_manager
            .cluster_controller
            .list_active_streaming_workers()
            .await?;

        let schedulable_workers: BTreeMap<_, _> = workers
            .iter()
            .filter(|worker| {
                !worker
                    .property
                    .as_ref()
                    .map(|p| p.is_unschedulable)
                    .unwrap_or(false)
            })
            .map(|worker| {
                (
                    worker.id as i32,
                    WorkerInfo {
                        weight: NonZeroUsize::new(worker.compute_node_parallelism()).unwrap(),
                        resource_group: worker.resource_group(),
                    },
                )
            })
            .collect();

        if job_ids.is_empty() {
            tracing::info!("no streaming jobs for scaling, maybe an empty cluster");
            return Ok(false);
        }

        tracing::info!(
            "trigger parallelism control for jobs: {:#?}, workers {:#?}",
            job_ids,
            schedulable_workers
        );

        let batch_size = match self.env.opts.parallelism_control_batch_size {
            0 => job_ids.len(),
            n => n,
        };

        tracing::info!(
            "total {} streaming jobs, batch size {}, schedulable worker ids: {:?}",
            job_ids.len(),
            batch_size,
            schedulable_workers
        );

        let batches: Vec<_> = job_ids
            .into_iter()
            .chunks(batch_size)
            .into_iter()
            .map(|chunk| chunk.collect_vec())
            .collect();

        for batch in batches {
            let jobs = batch.iter().map(|(_, job_id)| *job_id).collect();

            let commands = self
                .scale_controller
                .rerender(jobs, schedulable_workers.clone())
                .await?;

            let futures = commands.into_iter().map(|(database_id, command)| {
                let barrier_scheduler = self.barrier_scheduler.clone();
                async move { barrier_scheduler.run_command(database_id, command).await }
            });

            let _results = future::try_join_all(futures).await?;
        }

        Ok(false)
    }

    /// Handles notification of worker node activation and deletion, and triggers parallelism control.
    async fn run(&self, mut shutdown_rx: Receiver<()>) {
        tracing::info!("starting automatic parallelism control monitor");

        let check_period =
            Duration::from_secs(self.env.opts.parallelism_control_trigger_period_sec);

        let mut ticker = tokio::time::interval_at(
            Instant::now()
                + Duration::from_secs(self.env.opts.parallelism_control_trigger_first_delay_sec),
            check_period,
        );
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

        // waiting for the first tick
        ticker.tick().await;

        let (local_notification_tx, mut local_notification_rx) =
            tokio::sync::mpsc::unbounded_channel();

        self.env
            .notification_manager()
            .insert_local_sender(local_notification_tx);

        let worker_nodes = self
            .metadata_manager
            .list_active_streaming_compute_nodes()
            .await
            .expect("list active streaming compute nodes");

        let mut worker_cache: BTreeMap<_, _> = worker_nodes
            .into_iter()
            .map(|worker| (worker.id, worker))
            .collect();

        let mut previous_adaptive_parallelism_strategy = AdaptiveParallelismStrategy::default();

        let mut should_trigger = false;

        loop {
            tokio::select! {
                biased;

                _ = &mut shutdown_rx => {
                    tracing::info!("Stream manager is stopped");
                    break;
                }

                _ = ticker.tick(), if should_trigger => {
                    let include_workers = worker_cache.keys().copied().collect_vec();

                    if include_workers.is_empty() {
                        tracing::debug!("no available worker nodes");
                        should_trigger = false;
                        continue;
                    }

                    match self.trigger_parallelism_control().await {
                        Ok(cont) => {
                            should_trigger = cont;
                        }
                        Err(e) => {
                            tracing::warn!(error = %e.as_report(), "Failed to trigger scale out, waiting for next tick to retry after {}s", ticker.period().as_secs());
                            ticker.reset();
                        }
                    }
                }

                notification = local_notification_rx.recv() => {
                    let notification = notification.expect("local notification channel closed in loop of stream manager");

                    // Only maintain the cache for streaming compute nodes.
                    let worker_is_streaming_compute = |worker: &WorkerNode| {
                        worker.get_type() == Ok(WorkerType::ComputeNode)
                            && worker.property.as_ref().unwrap().is_streaming
                    };

                    match notification {
                        LocalNotification::SystemParamsChange(reader) => {
                            let new_strategy = reader.adaptive_parallelism_strategy();
                            if new_strategy != previous_adaptive_parallelism_strategy {
                                tracing::info!("adaptive parallelism strategy changed from {:?} to {:?}", previous_adaptive_parallelism_strategy, new_strategy);
                                should_trigger = true;
                                previous_adaptive_parallelism_strategy = new_strategy;
                            }
                        }
                        LocalNotification::WorkerNodeActivated(worker) => {
                            if !worker_is_streaming_compute(&worker) {
                                continue;
                            }

                            tracing::info!(worker = worker.id, "worker activated notification received");

                            let prev_worker = worker_cache.insert(worker.id, worker.clone());

                            match prev_worker {
                                Some(prev_worker) if prev_worker.compute_node_parallelism() != worker.compute_node_parallelism()  => {
                                    tracing::info!(worker = worker.id, "worker parallelism changed");
                                    should_trigger = true;
                                }
                                Some(prev_worker) if  prev_worker.resource_group() != worker.resource_group()  => {
                                    tracing::info!(worker = worker.id, "worker label changed");
                                    should_trigger = true;
                                }
                                None => {
                                    tracing::info!(worker = worker.id, "new worker joined");
                                    should_trigger = true;
                                }
                                _ => {}
                            }
                        }

                        // Since our logic for handling passive scale-in is within the barrier manager,
                        // there’s not much we can do here. All we can do is proactively remove the entries from our cache.
                        LocalNotification::WorkerNodeDeleted(worker) => {
                            if !worker_is_streaming_compute(&worker) {
                                continue;
                            }

                            match worker_cache.remove(&worker.id) {
                                Some(prev_worker) => {
                                    tracing::info!(worker = prev_worker.id, "worker removed from stream manager cache");
                                }
                                None => {
                                    tracing::warn!(worker = worker.id, "worker not found in stream manager cache, but it was removed");
                                }
                            }
                        }

                        _ => {}
                    }
                }
            }
        }
    }

    pub fn start_auto_parallelism_monitor(
        self: Arc<Self>,
    ) -> (JoinHandle<()>, oneshot::Sender<()>) {
        tracing::info!("Automatic parallelism scale-out is enabled for streaming jobs");
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let join_handle = tokio::spawn(async move {
            self.run(shutdown_rx).await;
        });

        (join_handle, shutdown_tx)
    }
}

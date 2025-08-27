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
use risingwave_common::bitmap::Bitmap;
use risingwave_common::catalog::{DatabaseId, FragmentTypeMask};
use risingwave_common::hash;
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
use crate::controller::scale::{WorkerInfo, find_fragment_no_shuffle_dags_detailed, render_jobs};
use crate::manager::{LocalNotification, MetaSrvEnv, MetadataManager};
use crate::model::{ActorId, DispatcherId, FragmentId, StreamActor, StreamActorWithDispatchers};
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
use risingwave_pb::plan_common::PbExprContext;
use sea_orm::ActiveValue::Set;
use sea_orm::{ColumnTrait, EntityTrait, IntoActiveModel, QueryFilter, TransactionTrait};

use super::SourceChange;
use crate::controller::fragment::{InflightActorInfo, InflightFragmentInfo};
use crate::controller::utils::compose_dispatchers;

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

    // pub async fn render_actors(
    //     &self,
    //     salt: ObjectId,
    //     parallelism: NonZeroUsize,
    //     vnode_count: usize,
    //     fragment_id: FragmentId,
    //     fragment_distribution_type: DistributionType,
    //     workers: BTreeMap<WorkerId, NonZeroUsize>,
    // ) -> MetaResult<HashMap<ActorId, InflightActorInfo>> {
    //     let fact_parallelism = parallelism.get().min(vnode_count);
    //     let assigner = AssignerBuilder::new(salt).build();
    //
    //     let actors = (0..fact_parallelism).collect_vec();
    //     let vnodes = (0..vnode_count).collect_vec();
    //
    //     let assignment = assigner.assign_hierarchical(&workers, &actors, &vnodes)?;
    //
    //     let actors = assignment
    //         .iter()
    //         .flat_map(|(worker_id, actors)| {
    //             actors
    //                 .iter()
    //                 .map(move |(actor_id, vnodes)| (worker_id, actor_id, vnodes))
    //         })
    //         .map(|(&worker_id, &actor_idx, vnodes)| {
    //             let vnode_bitmap = match fragment_distribution_type {
    //                 DistributionType::Single => None,
    //                 DistributionType::Hash => Some(Bitmap::from_indices(vnode_count, vnodes)),
    //             };
    //
    //             let actor_id = fragment_id << 16 as ActorId | actor_idx as ActorId;
    //             (
    //                 actor_id,
    //                 InflightActorInfo {
    //                     worker_id,
    //                     vnode_bitmap,
    //                 },
    //             )
    //         })
    //         .collect();
    //
    //     Ok(actors)
    // }

    pub fn diff_fragment(
        &self,
        prev_fragment_info: &SharedFragmentInfo,
        curr_actors: &HashMap<ActorId, InflightActorInfo>,
        upstream_fragments: HashMap<FragmentId, DispatcherType>,
        downstream_fragments: HashMap<FragmentId, DispatcherType>,
        all_actor_dispatchers: HashMap<ActorId, Vec<PbDispatcher>>,
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
                            },
                        )| {
                            (*actor_id as hash::ActorId, vnode_bitmap.clone().unwrap())
                        },
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
                    *upstream_fragment as FragmentId,
                    prev_fragment_info.fragment_id as DispatcherId,
                )
            })
            .collect();

        let downstream_fragment_ids = downstream_fragments
            .iter()
            .filter(|&(_, dispatcher_type)| *dispatcher_type != DispatcherType::NoShuffle)
            .map(|(fragment_id, _)| *fragment_id)
            .collect();

        let newly_created_actors: HashMap<ActorId, (StreamActorWithDispatchers, WorkerId)> =
            added_actor_ids
                .iter()
                .map(|actor_id| {
                    let actor = StreamActor {
                        actor_id: *actor_id,
                        fragment_id: prev_fragment_info.fragment_id as _,
                        vnode_bitmap: curr_actors[actor_id].vnode_bitmap.clone(),
                        mview_definition: "wtf".to_owned(), // TODO: handle mview definition
                        expr_context: Some(PbExprContext::default()),
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
        let reschedule = Reschedule {
            added_actors,
            removed_actors,
            vnode_bitmap_updates,
            upstream_fragment_dispatcher_ids,
            upstream_dispatcher_mapping,
            downstream_fragment_ids,
            actor_splits: Default::default(),
            newly_created_actors,
            cdc_table_snapshot_split_assignment: Default::default(),
        };

        Ok(reschedule)
    }

    #[await_tree::instrument]
    pub async fn post_apply_reschedule(
        &self,
        reschedules: &HashMap<FragmentId, Reschedule>,
    ) -> MetaResult<()> {
        // Update fragment info after rescheduling in meta store.
        // self.metadata_manager
        //     .post_apply_reschedules(reschedules.clone(), post_updates)
        //     .await?;

        println!("post apply");

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

        let mut stream_source_actor_splits = HashMap::new();
        let mut stream_source_dropped_actors = HashSet::new();

        // todo: handle adaptive splits
        for (fragment_id, reschedule) in reschedules {
            if !reschedule.actor_splits.is_empty() {
                stream_source_actor_splits
                    .insert(*fragment_id as FragmentId, reschedule.actor_splits.clone());
                stream_source_dropped_actors.extend(reschedule.removed_actors.clone());
            }
        }

        if !stream_source_actor_splits.is_empty() {
            self.source_manager
                .apply_source_change(SourceChange::Reschedule {
                    split_assignment: stream_source_actor_splits,
                    dropped_actors: stream_source_dropped_actors,
                })
                .await;
        }

        Ok(())
    }

    // pub async fn generate_job_reschedule_plan(
    //     &self,
    //     policy: JobReschedulePolicy,
    //     generate_plan_for_cdc_table_backfill: bool,
    // ) -> MetaResult<JobReschedulePlan> {
    //     type VnodeCount = usize;
    //
    //     let JobReschedulePolicy { targets } = policy;
    //
    //     let workers = self
    //         .metadata_manager
    //         .list_active_streaming_compute_nodes()
    //         .await?;
    //
    //     // The `schedulable` field should eventually be replaced by resource groups like `unschedulable`
    //     let workers: HashMap<_, _> = workers
    //         .into_iter()
    //         .filter(|worker| worker.is_streaming_schedulable())
    //         .map(|worker| (worker.id, worker))
    //         .collect();
    //
    //     #[derive(Debug)]
    //     struct JobUpdate {
    //         filtered_worker_ids: BTreeSet<WorkerId>,
    //         parallelism: TableParallelism,
    //     }
    //
    //     let mut job_parallelism_updates = HashMap::new();
    //
    //     let mut job_reschedule_post_updates = JobReschedulePostUpdates {
    //         parallelism_updates: Default::default(),
    //         resource_group_updates: Default::default(),
    //     };
    //
    //     for (
    //         job_id,
    //         JobRescheduleTarget {
    //             parallelism: parallelism_update,
    //             resource_group: resource_group_update,
    //         },
    //     ) in &targets
    //     {
    //         let parallelism = match parallelism_update {
    //             JobParallelismTarget::Update(parallelism) => *parallelism,
    //             JobParallelismTarget::Refresh => {
    //                 let parallelism = self
    //                     .metadata_manager
    //                     .catalog_controller
    //                     .get_job_streaming_parallelisms(*job_id as _)
    //                     .await?;
    //
    //                 parallelism.into()
    //             }
    //         };
    //
    //         job_reschedule_post_updates
    //             .parallelism_updates
    //             .insert(TableId::from(*job_id), parallelism);
    //
    //         let current_resource_group = match resource_group_update {
    //             JobResourceGroupTarget::Update(Some(specific_resource_group)) => {
    //                 job_reschedule_post_updates.resource_group_updates.insert(
    //                     *job_id as ObjectId,
    //                     Some(specific_resource_group.to_owned()),
    //                 );
    //
    //                 specific_resource_group.to_owned()
    //             }
    //             JobResourceGroupTarget::Update(None) => {
    //                 let database_resource_group = self
    //                     .metadata_manager
    //                     .catalog_controller
    //                     .get_existing_job_database_resource_group(*job_id as _)
    //                     .await?;
    //
    //                 job_reschedule_post_updates
    //                     .resource_group_updates
    //                     .insert(*job_id as ObjectId, None);
    //                 database_resource_group
    //             }
    //             JobResourceGroupTarget::Keep => {
    //                 self.metadata_manager
    //                     .catalog_controller
    //                     .get_existing_job_resource_group(*job_id as _)
    //                     .await?
    //             }
    //         };
    //
    //         let filtered_worker_ids =
    //             filter_workers_by_resource_group(&workers, current_resource_group.as_str());
    //
    //         if filtered_worker_ids.is_empty() {
    //             bail!("Cannot resize streaming_job {job_id} to empty worker set")
    //         }
    //
    //         job_parallelism_updates.insert(
    //             *job_id,
    //             JobUpdate {
    //                 filtered_worker_ids,
    //                 parallelism,
    //             },
    //         );
    //     }
    //
    //     // index for no shuffle relation
    //     let mut no_shuffle_source_fragment_ids = HashSet::new();
    //     let mut no_shuffle_target_fragment_ids = HashSet::new();
    //
    //     // index for fragment_id -> (distribution_type, vnode_count)
    //     let mut fragment_distribution_map = HashMap::new();
    //     // index for actor -> worker id
    //     let mut actor_location = HashMap::new();
    //     // index for table_id -> [fragment_id]
    //     let mut table_fragment_id_map = HashMap::new();
    //     // index for fragment_id -> [actor_id]
    //     let mut fragment_actor_id_map = HashMap::new();
    //
    //     async fn build_index(
    //         no_shuffle_source_fragment_ids: &mut HashSet<FragmentId>,
    //         no_shuffle_target_fragment_ids: &mut HashSet<FragmentId>,
    //         fragment_distribution_map: &mut HashMap<
    //             FragmentId,
    //             (FragmentDistributionType, VnodeCount, bool),
    //         >,
    //         actor_location: &mut HashMap<ActorId, WorkerId>,
    //         table_fragment_id_map: &mut HashMap<u32, HashSet<FragmentId>>,
    //         fragment_actor_id_map: &mut HashMap<FragmentId, HashSet<u32>>,
    //         mgr: &MetadataManager,
    //         table_ids: Vec<ObjectId>,
    //         generate_plan_only_for_cdc_table_backfill: bool,
    //     ) -> Result<(), MetaError> {
    //         let RescheduleWorkingSet {
    //             fragments,
    //             actors,
    //             actor_dispatchers: _actor_dispatchers,
    //             fragment_downstreams,
    //             fragment_upstreams: _fragment_upstreams,
    //             related_jobs: _related_jobs,
    //             job_resource_groups: _job_resource_groups,
    //         } = mgr
    //             .catalog_controller
    //             .resolve_working_set_for_reschedule_tables(table_ids)
    //             .await?;
    //
    //         for (fragment_id, downstreams) in fragment_downstreams {
    //             for (downstream_fragment_id, dispatcher_type) in downstreams {
    //                 if let risingwave_meta_model::DispatcherType::NoShuffle = dispatcher_type {
    //                     no_shuffle_source_fragment_ids.insert(fragment_id as FragmentId);
    //                     no_shuffle_target_fragment_ids.insert(downstream_fragment_id as FragmentId);
    //                 }
    //             }
    //         }
    //
    //         for (fragment_id, fragment) in fragments {
    //             let is_cdc_backfill_v2_fragment =
    //                 FragmentTypeMask::from(fragment.fragment_type_mask)
    //                     .contains(FragmentTypeFlag::StreamCdcScan);
    //             if generate_plan_only_for_cdc_table_backfill && !is_cdc_backfill_v2_fragment {
    //                 continue;
    //             }
    //             fragment_distribution_map.insert(
    //                 fragment_id as FragmentId,
    //                 (
    //                     FragmentDistributionType::from(fragment.distribution_type),
    //                     fragment.vnode_count as _,
    //                     is_cdc_backfill_v2_fragment,
    //                 ),
    //             );
    //
    //             table_fragment_id_map
    //                 .entry(fragment.job_id as u32)
    //                 .or_default()
    //                 .insert(fragment_id as FragmentId);
    //         }
    //
    //         for (actor_id, actor) in actors {
    //             actor_location.insert(actor_id as ActorId, actor.worker_id as WorkerId);
    //             fragment_actor_id_map
    //                 .entry(actor.fragment_id as FragmentId)
    //                 .or_default()
    //                 .insert(actor_id as ActorId);
    //         }
    //
    //         Ok(())
    //     }
    //
    //     let table_ids = targets.keys().map(|id| *id as ObjectId).collect();
    //
    //     build_index(
    //         &mut no_shuffle_source_fragment_ids,
    //         &mut no_shuffle_target_fragment_ids,
    //         &mut fragment_distribution_map,
    //         &mut actor_location,
    //         &mut table_fragment_id_map,
    //         &mut fragment_actor_id_map,
    //         &self.metadata_manager,
    //         table_ids,
    //         generate_plan_for_cdc_table_backfill,
    //     )
    //     .await?;
    //     tracing::debug!(
    //         ?job_reschedule_post_updates,
    //         ?job_parallelism_updates,
    //         ?no_shuffle_source_fragment_ids,
    //         ?no_shuffle_target_fragment_ids,
    //         ?fragment_distribution_map,
    //         ?actor_location,
    //         ?table_fragment_id_map,
    //         ?fragment_actor_id_map,
    //         "generate_table_resize_plan, after build_index"
    //     );
    //
    //     let adaptive_parallelism_strategy = self
    //         .env
    //         .system_params_reader()
    //         .await
    //         .adaptive_parallelism_strategy();
    //
    //     let mut target_plan = HashMap::new();
    //
    //     for (
    //         table_id,
    //         JobUpdate {
    //             filtered_worker_ids,
    //             parallelism,
    //         },
    //     ) in job_parallelism_updates
    //     {
    //         let assigner = AssignerBuilder::new(table_id).build();
    //
    //         let fragment_map = table_fragment_id_map.remove(&table_id).unwrap();
    //
    //         let available_worker_slots = workers
    //             .iter()
    //             .filter(|(id, _)| filtered_worker_ids.contains(&(**id as WorkerId)))
    //             .map(|(_, worker)| {
    //                 (
    //                     worker.id as WorkerId,
    //                     NonZeroUsize::new(worker.compute_node_parallelism()).unwrap(),
    //                 )
    //             })
    //             .collect::<BTreeMap<_, _>>();
    //
    //         for fragment_id in fragment_map {
    //             // Currently, all of our NO_SHUFFLE relation propagations are only transmitted from upstream to downstream.
    //             if no_shuffle_target_fragment_ids.contains(&fragment_id) {
    //                 continue;
    //             }
    //
    //             let mut fragment_slots: BTreeMap<WorkerId, usize> = BTreeMap::new();
    //
    //             for actor_id in &fragment_actor_id_map[&fragment_id] {
    //                 let worker_id = actor_location[actor_id];
    //                 *fragment_slots.entry(worker_id).or_default() += 1;
    //             }
    //
    //             let available_slot_count: usize = available_worker_slots
    //                 .values()
    //                 .cloned()
    //                 .map(NonZeroUsize::get)
    //                 .sum();
    //
    //             if available_slot_count == 0 {
    //                 bail!(
    //                     "No schedulable slots available for fragment {}",
    //                     fragment_id
    //                 );
    //             }
    //
    //             let (dist, vnode_count, is_cdc_backfill_v2_fragment) =
    //                 fragment_distribution_map[&fragment_id];
    //             let max_parallelism = vnode_count;
    //             let fragment_parallelism_strategy = if generate_plan_for_cdc_table_backfill {
    //                 assert!(is_cdc_backfill_v2_fragment);
    //                 let TableParallelism::Fixed(new_parallelism) = parallelism else {
    //                     return Err(anyhow::anyhow!(
    //                         "invalid new parallelism {:?}, expect fixed parallelism",
    //                         parallelism
    //                     )
    //                     .into());
    //                 };
    //                 if new_parallelism > max_parallelism || new_parallelism == 0 {
    //                     return Err(anyhow::anyhow!(
    //                         "invalid new parallelism {}, max parallelism {}",
    //                         new_parallelism,
    //                         max_parallelism
    //                     )
    //                     .into());
    //                 }
    //                 TableParallelism::Fixed(new_parallelism)
    //             } else if is_cdc_backfill_v2_fragment {
    //                 TableParallelism::Fixed(fragment_actor_id_map[&fragment_id].len())
    //             } else {
    //                 parallelism
    //             };
    //             match dist {
    //                 FragmentDistributionType::Unspecified => unreachable!(),
    //                 FragmentDistributionType::Single => {
    //                     let (single_worker_id, should_be_one) = fragment_slots
    //                         .iter()
    //                         .exactly_one()
    //                         .expect("single fragment should have only one worker slot");
    //
    //                     assert_eq!(*should_be_one, 1);
    //
    //                     let assignment =
    //                         assigner.count_actors_per_worker(&available_worker_slots, 1);
    //
    //                     let (chosen_target_worker_id, should_be_one) =
    //                         assignment.iter().exactly_one().ok().with_context(|| {
    //                             format!(
    //                                 "Cannot find a single target worker for fragment {fragment_id}"
    //                             )
    //                         })?;
    //
    //                     assert_eq!(*should_be_one, 1);
    //
    //                     if *chosen_target_worker_id == *single_worker_id {
    //                         tracing::debug!(
    //                             "single fragment {fragment_id} already on target worker {chosen_target_worker_id}"
    //                         );
    //                         continue;
    //                     }
    //
    //                     target_plan.insert(
    //                         fragment_id,
    //                         WorkerReschedule {
    //                             worker_actor_diff: BTreeMap::from_iter(vec![
    //                                 (*chosen_target_worker_id, 1),
    //                                 (*single_worker_id, -1),
    //                             ]),
    //                         },
    //                     );
    //                 }
    //                 FragmentDistributionType::Hash => match fragment_parallelism_strategy {
    //                     TableParallelism::Adaptive => {
    //                         let target_slot_count = adaptive_parallelism_strategy
    //                             .compute_target_parallelism(available_slot_count);
    //
    //                         if target_slot_count > max_parallelism {
    //                             tracing::warn!(
    //                                 "available parallelism for table {table_id} is larger than max parallelism, force limit to {max_parallelism}"
    //                             );
    //
    //                             let target_worker_slots = assigner.count_actors_per_worker(
    //                                 &available_worker_slots,
    //                                 max_parallelism,
    //                             );
    //
    //                             target_plan.insert(
    //                                 fragment_id,
    //                                 Self::diff_worker_slot_changes(
    //                                     &fragment_slots,
    //                                     &target_worker_slots,
    //                                 ),
    //                             );
    //                         } else if available_slot_count != target_slot_count {
    //                             tracing::info!(
    //                                 "available parallelism for table {table_id} is limit by adaptive strategy {adaptive_parallelism_strategy}, resetting to {target_slot_count}"
    //                             );
    //
    //                             let target_worker_slots = assigner.count_actors_per_worker(
    //                                 &available_worker_slots,
    //                                 target_slot_count,
    //                             );
    //
    //                             target_plan.insert(
    //                                 fragment_id,
    //                                 Self::diff_worker_slot_changes(
    //                                     &fragment_slots,
    //                                     &target_worker_slots,
    //                                 ),
    //                             );
    //                         } else {
    //                             let available_worker_slots = available_worker_slots
    //                                 .iter()
    //                                 .map(|(worker_id, v)| (*worker_id, v.get()))
    //                                 .collect();
    //
    //                             target_plan.insert(
    //                                 fragment_id,
    //                                 Self::diff_worker_slot_changes(
    //                                     &fragment_slots,
    //                                     &available_worker_slots,
    //                                 ),
    //                             );
    //                         }
    //                     }
    //                     TableParallelism::Fixed(mut n) => {
    //                         if n > max_parallelism {
    //                             tracing::warn!(
    //                                 "specified parallelism {n} for table {table_id} is larger than max parallelism, force limit to {max_parallelism}"
    //                             );
    //                             n = max_parallelism
    //                         }
    //
    //                         let target_worker_slots =
    //                             assigner.count_actors_per_worker(&available_worker_slots, n);
    //
    //                         target_plan.insert(
    //                             fragment_id,
    //                             Self::diff_worker_slot_changes(
    //                                 &fragment_slots,
    //                                 &target_worker_slots,
    //                             ),
    //                         );
    //                     }
    //                     TableParallelism::Custom => {
    //                         // skipping for custom
    //                     }
    //                 },
    //             }
    //         }
    //     }
    //
    //     target_plan.retain(|_, plan| !plan.worker_actor_diff.is_empty());
    //     tracing::debug!(
    //         ?target_plan,
    //         "generate_table_resize_plan finished target_plan"
    //     );
    //     if generate_plan_for_cdc_table_backfill {
    //         job_reschedule_post_updates.resource_group_updates = HashMap::default();
    //         job_reschedule_post_updates.parallelism_updates = HashMap::default();
    //     }
    //     Ok(JobReschedulePlan {
    //         reschedules: target_plan,
    //         post_updates: job_reschedule_post_updates,
    //     })
    // }

    pub async fn reschedule_update(
        &self,
        policy: HashMap<ObjectId, RescheduleTarget>,
        workers: HashMap<WorkerId, PbWorkerNode>,
    ) -> MetaResult<HashMap<DatabaseId, Command>> {
        println!("aaaaaaaa");
        // jobs: HashMap<ObjectId, TargetResourcePolicy>,
        // workers: BTreeMap<WorkerId, Worker>,
        // ) -> MetaResult<HashMap<DatabaseId, HashMap<TableId, HashMap<FragmentId, crate::controller::fragment::InflightFragmentInfo >>>>

        let inner = self.metadata_manager.catalog_controller.inner.read().await;
        let txn = inner.db.begin().await?;

        for (table_id, target) in &policy {
            let mut streaming_job = StreamingJob::find_by_id(*table_id)
                .one(&txn)
                .await?
                .ok_or_else(|| MetaError::catalog_id_not_found("table", table_id))?
                .into_active_model();

            match &target {
                RescheduleTarget::Parallelism(p) | RescheduleTarget::Both(p, _) => {
                    streaming_job.parallelism = Set(p.parallelism.clone());
                }
                _ => {}
            }

            match &target {
                RescheduleTarget::ResourceGroup(r) | RescheduleTarget::Both(_, r) => {
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

        println!("command {:#?}", command);

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

        let result = render_jobs(txn, jobs, workers, adaptive_parallelism_strategy).await?;

        let all_rendered_fragments: HashMap<_, _> = result
            .values()
            .flat_map(|jobs| jobs.values())
            .flatten()
            .map(|(fragment_id, info)| (*fragment_id, info))
            .collect();

        let fragment_ids = all_rendered_fragments.keys().copied().collect_vec();

        let upstreams: Vec<(FragmentId, FragmentId, DispatcherType)> = FragmentRelation::find()
            .select_only()
            .columns([
                fragment_relation::Column::TargetFragmentId,
                fragment_relation::Column::SourceFragmentId,
                fragment_relation::Column::DispatcherType,
            ])
            .filter(fragment_relation::Column::TargetFragmentId.is_in(fragment_ids.clone()))
            .into_tuple()
            .all(txn)
            .await?;

        let downstreams = FragmentRelation::find()
            .filter(fragment_relation::Column::SourceFragmentId.is_in(fragment_ids.clone()))
            .all(txn)
            .await?;

        let mut all_upstream_fragments = HashMap::new();

        for (fragment, upstream, dispatcher) in upstreams {
            all_upstream_fragments
                .entry(fragment)
                .or_insert(HashMap::new())
                .insert(upstream, dispatcher);
        }

        let mut all_downstream_fragments = HashMap::new();

        let mut downstream_relations = HashMap::new();
        for relation in downstreams {
            all_downstream_fragments
                .entry(relation.source_fragment_id as u32)
                .or_insert(HashMap::new())
                .insert(relation.target_fragment_id as u32, relation.dispatcher_type);

            downstream_relations.insert(
                (
                    relation.source_fragment_id as u32,
                    relation.target_fragment_id as u32,
                ),
                relation,
            );
        }

        let all_prev_fragments: HashMap<_, _> = {
            let all_fragment_ids: HashSet<_> = fragment_ids
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

            let read_guard = self.env.shared_actor_infos().read_guard();

            all_fragment_ids
                .into_iter()
                .map(|fragment_id| {
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

        let mut commands = HashMap::new();

        for (database_id, jobs) in &result {
            let mut all_fragment_actors = HashMap::new();
            let mut reschedules = HashMap::new();
            for (fragment_id, fragment_info) in jobs.values().flatten() {
                let InflightFragmentInfo {
                    distribution_type,
                    actors,
                    ..
                } = fragment_info;

                let upstream_fragments = all_upstream_fragments
                    .remove(&(*fragment_id as u32))
                    .unwrap_or_default();
                let downstream_fragments = all_downstream_fragments
                    .remove(&(*fragment_id as u32))
                    .unwrap_or_default();

                println!("fragment {} down {:?}", fragment_id, downstream_fragments);
                println!("fragment {} up {:?}", fragment_id, upstream_fragments);

                let fragment_actors: HashMap<_, _> = upstream_fragments
                    .keys()
                    .chain(downstream_fragments.keys())
                    .map(|fragment_id| {
                        let fragment = all_prev_fragments.get(&(*fragment_id as i32)).unwrap();
                        (
                            *fragment_id,
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
                    println!("for down {}", downstream_fragment_id);
                    let target_fragment_actors =
                        match all_rendered_fragments.get(&(*downstream_fragment_id as _)) {
                            None => {
                                println!(
                                    "no more downstream_fragments for {}",
                                    downstream_fragment_id
                                );
                                let external_fragment = all_prev_fragments
                                    .get(&(*downstream_fragment_id as i32))
                                    .unwrap();

                                external_fragment
                                    .actors
                                    .iter()
                                    .map(|(actor_id, info)| (*actor_id, info.vnode_bitmap.clone()))
                                    .collect()
                            }
                            Some(downstream_rendered) => {
                                println!("down frag actors found for {}", downstream_fragment_id);
                                downstream_rendered
                                    .actors
                                    .iter()
                                    .map(|(actor_id, info)| (*actor_id, info.vnode_bitmap.clone()))
                                    .collect()
                            }
                        };

                    println!("down frag actors {:#?}", target_fragment_actors);

                    let target_fragment_distribution = *distribution_type;

                    let fragment_relation::Model {
                        source_fragment_id: _,
                        target_fragment_id: _,
                        dispatcher_type,
                        dist_key_indices,
                        output_indices,
                        output_type_mapping,
                    } = downstream_relations
                        .remove(&(*fragment_id as _, *downstream_fragment_id as _))
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
                println!("aap {:#?}", all_actor_dispatchers);

                let prev_fragment = all_prev_fragments.get(&{ *fragment_id }).unwrap();

                let reschedule = self.diff_fragment(
                    prev_fragment,
                    actors,
                    upstream_fragments,
                    downstream_fragments,
                    all_actor_dispatchers,
                )?;

                reschedules.insert(*fragment_id as _, reschedule);
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
pub struct ParallelismTarget {
    pub parallelism: StreamingParallelism,
}

#[derive(Clone, Debug)]
pub struct ResourceGroupTarget {
    pub resource_group: Option<String>,
}

#[derive(Clone, Debug)]
pub enum RescheduleTarget {
    Parallelism(ParallelismTarget),
    ResourceGroup(ResourceGroupTarget),
    Both(ParallelismTarget, ResourceGroupTarget),
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
                    worker.id as _,
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
                println!("running command for db {}", database_id);
                async move { barrier_scheduler.run_command(database_id, command).await }
            });

            let _results = future::try_join_all(futures).await?;
        }

        Ok(true)
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

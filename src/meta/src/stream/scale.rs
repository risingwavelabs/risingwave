// Copyright 2022 RisingWave Labs
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
use std::sync::Arc;
use std::time::Duration;

use anyhow::anyhow;
use futures::future;
use itertools::Itertools;
use risingwave_common::bail;
use risingwave_common::catalog::DatabaseId;
use risingwave_common::hash::ActorMapping;
use risingwave_meta_model::{StreamingParallelism, WorkerId, fragment, fragment_relation};
use risingwave_pb::common::{PbWorkerNode, WorkerNode, WorkerType};
use risingwave_pb::stream_plan::{PbDispatchOutputMapping, PbDispatcher};
use sea_orm::{ConnectionTrait, QuerySelect};
use thiserror_ext::AsReport;
use tokio::sync::oneshot::Receiver;
use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard, oneshot};
use tokio::task::JoinHandle;
use tokio::time::{Instant, MissedTickBehavior};
use tokio_retry::strategy::{ExponentialBackoff, jitter};

use crate::barrier::{Command, Reschedule, SharedFragmentInfo};
use crate::controller::scale::{
    FragmentRenderMap, NoShuffleEnsemble, RenderedGraph, find_fragment_no_shuffle_dags_detailed,
    render_fragments, render_jobs,
};
use crate::error::{MetaErrorInner, bail_invalid_parameter};
use crate::manager::{ActiveStreamingWorkerNodes, LocalNotification, MetaSrvEnv, MetadataManager};
use crate::model::{ActorId, FragmentId, StreamActor, StreamActorWithDispatchers};
use crate::stream::{GlobalStreamManager, SourceManagerRef};
use crate::{MetaError, MetaResult};

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct WorkerReschedule {
    pub worker_actor_diff: BTreeMap<WorkerId, isize>,
}

use risingwave_common::id::JobId;
use risingwave_common::system_param::AdaptiveParallelismStrategy;
use risingwave_common::system_param::reader::SystemParamsRead;
use risingwave_meta_model::DispatcherType;
use risingwave_meta_model::fragment::DistributionType;
use risingwave_meta_model::prelude::{Fragment, FragmentRelation, StreamingJob};
use sea_orm::ActiveValue::Set;
use sea_orm::{ColumnTrait, EntityTrait, IntoActiveModel, QueryFilter, TransactionTrait};

use crate::controller::fragment::{InflightActorInfo, InflightFragmentInfo};
use crate::controller::utils::{
    StreamingJobExtraInfo, compose_dispatchers, get_streaming_job_extra_info,
};

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
        jobs: &[JobId],
    ) -> MetaResult<HashSet<JobId>> {
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
        job_extra_info: Option<&StreamingJobExtraInfo>,
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
        debug_assert!(
            kept_ids.is_empty(),
            "kept actors found in scale; expected full rebuild, prev={prev_ids:?}, curr={curr_ids:?}, kept={kept_ids:?}"
        );

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
            .map(|(upstream_fragment, _)| (*upstream_fragment, prev_fragment_info.fragment_id))
            .collect();

        let downstream_fragment_ids = downstream_fragments
            .iter()
            .filter(|&(_, dispatcher_type)| *dispatcher_type != DispatcherType::NoShuffle)
            .map(|(fragment_id, _)| *fragment_id)
            .collect();

        let extra_info = job_extra_info.cloned().unwrap_or_default();
        let expr_context = extra_info.stream_context().to_expr_context();
        let job_definition = extra_info.job_definition;
        let config_override = extra_info.config_override;

        let newly_created_actors: HashMap<ActorId, (StreamActorWithDispatchers, WorkerId)> =
            added_actor_ids
                .iter()
                .map(|actor_id| {
                    let actor = StreamActor {
                        actor_id: *actor_id,
                        fragment_id: prev_fragment_info.fragment_id,
                        vnode_bitmap: curr_actors[actor_id].vnode_bitmap.clone(),
                        mview_definition: job_definition.clone(),
                        expr_context: Some(expr_context.clone()),
                        config_override: config_override.clone(),
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
            actor_splits,
            newly_created_actors,
        };

        Ok(reschedule)
    }

    pub async fn reschedule_inplace(
        &self,
        policy: HashMap<JobId, ReschedulePolicy>,
        workers: &HashMap<WorkerId, WorkerNode>,
    ) -> MetaResult<HashMap<DatabaseId, Command>> {
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

            StreamingJob::update(streaming_job).exec(&txn).await?;
        }

        let jobs = policy.keys().copied().collect();

        let command = self.rerender_inner(&txn, jobs, workers).await?;

        txn.commit().await?;

        Ok(command)
    }

    pub async fn reschedule_fragment_inplace(
        &self,
        policy: HashMap<risingwave_meta_model::FragmentId, Option<StreamingParallelism>>,
        workers: &HashMap<WorkerId, PbWorkerNode>,
    ) -> MetaResult<HashMap<DatabaseId, Command>> {
        if policy.is_empty() {
            return Ok(HashMap::new());
        }

        let inner = self.metadata_manager.catalog_controller.inner.read().await;
        let txn = inner.db.begin().await?;

        let fragment_id_list = policy.keys().copied().collect_vec();

        let existing_fragment_ids: HashSet<_> = Fragment::find()
            .select_only()
            .column(fragment::Column::FragmentId)
            .filter(fragment::Column::FragmentId.is_in(fragment_id_list.clone()))
            .into_tuple::<risingwave_meta_model::FragmentId>()
            .all(&txn)
            .await?
            .into_iter()
            .collect();

        if let Some(missing_fragment_id) = fragment_id_list
            .iter()
            .find(|fragment_id| !existing_fragment_ids.contains(fragment_id))
        {
            return Err(MetaError::catalog_id_not_found(
                "fragment",
                *missing_fragment_id,
            ));
        }

        let mut target_ensembles = vec![];

        for ensemble in find_fragment_no_shuffle_dags_detailed(&txn, &fragment_id_list).await? {
            let entry_fragment_ids = ensemble.entry_fragments().collect_vec();

            let desired_parallelism = match entry_fragment_ids
                .iter()
                .filter_map(|fragment_id| policy.get(fragment_id).cloned())
                .dedup()
                .collect_vec()
                .as_slice()
            {
                [] => {
                    bail_invalid_parameter!(
                        "none of the entry fragments {:?} were included in the reschedule request; \
                         provide at least one entry fragment id",
                        entry_fragment_ids
                    );
                }
                [parallelism] => parallelism.clone(),
                parallelisms => {
                    bail!(
                        "conflicting reschedule policies for fragments in the same no-shuffle ensemble: {:?}",
                        parallelisms
                    );
                }
            };

            let fragments = Fragment::find()
                .filter(fragment::Column::FragmentId.is_in(entry_fragment_ids))
                .all(&txn)
                .await?;

            debug_assert!(
                fragments
                    .iter()
                    .map(|fragment| fragment.parallelism.as_ref())
                    .all_equal(),
                "entry fragments in the same ensemble should share the same parallelism"
            );

            let current_parallelism = fragments
                .first()
                .and_then(|fragment| fragment.parallelism.clone());

            if current_parallelism == desired_parallelism {
                continue;
            }

            for fragment in fragments {
                let mut fragment = fragment.into_active_model();
                fragment.parallelism = Set(desired_parallelism.clone());
                Fragment::update(fragment).exec(&txn).await?;
            }

            target_ensembles.push(ensemble);
        }

        let command = self
            .rerender_fragment_inner(&txn, target_ensembles, workers)
            .await?;

        txn.commit().await?;

        Ok(command)
    }

    async fn rerender(
        &self,
        jobs: HashSet<JobId>,
        workers: &HashMap<WorkerId, WorkerNode>,
    ) -> MetaResult<HashMap<DatabaseId, Command>> {
        let inner = self.metadata_manager.catalog_controller.inner.read().await;
        self.rerender_inner(&inner.db, jobs, workers).await
    }

    async fn rerender_fragment_inner(
        &self,
        txn: &impl ConnectionTrait,
        ensembles: Vec<NoShuffleEnsemble>,
        workers: &HashMap<WorkerId, WorkerNode>,
    ) -> MetaResult<HashMap<DatabaseId, Command>> {
        if ensembles.is_empty() {
            return Ok(HashMap::new());
        }

        let adaptive_parallelism_strategy = {
            let system_params_reader = self.env.system_params_reader().await;
            system_params_reader.adaptive_parallelism_strategy()
        };

        let RenderedGraph { fragments, .. } = render_fragments(
            txn,
            self.env.actor_id_generator(),
            ensembles,
            workers,
            adaptive_parallelism_strategy,
        )
        .await?;

        self.build_reschedule_commands(txn, fragments).await
    }

    async fn rerender_inner(
        &self,
        txn: &impl ConnectionTrait,
        jobs: HashSet<JobId>,
        workers: &HashMap<WorkerId, WorkerNode>,
    ) -> MetaResult<HashMap<DatabaseId, Command>> {
        let adaptive_parallelism_strategy = {
            let system_params_reader = self.env.system_params_reader().await;
            system_params_reader.adaptive_parallelism_strategy()
        };

        let RenderedGraph { fragments, .. } = render_jobs(
            txn,
            self.env.actor_id_generator(),
            jobs,
            workers,
            adaptive_parallelism_strategy,
        )
        .await?;

        self.build_reschedule_commands(txn, fragments).await
    }

    async fn build_reschedule_commands(
        &self,
        txn: &impl ConnectionTrait,
        render_result: FragmentRenderMap,
    ) -> MetaResult<HashMap<DatabaseId, Command>> {
        if render_result.is_empty() {
            return Ok(HashMap::new());
        }

        let job_ids = render_result
            .values()
            .flat_map(|jobs| jobs.keys().copied())
            .collect_vec();

        let job_extra_info = get_streaming_job_extra_info(txn, job_ids).await?;

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
            .await?;

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
            .chain(all_upstream_fragments.values().flatten().map(|(id, _)| *id))
            .chain(
                all_downstream_fragments
                    .values()
                    .flatten()
                    .map(|(id, _)| *id),
            )
            .collect();

        let all_related_fragment_ids = all_related_fragment_ids.into_iter().collect_vec();

        let all_prev_fragments: HashMap<_, _> = {
            let read_guard = self.env.shared_actor_infos().read_guard();
            all_related_fragment_ids
                .iter()
                .map(|&fragment_id| {
                    read_guard
                        .get_fragment(fragment_id as FragmentId)
                        .cloned()
                        .map(|fragment| (fragment_id, fragment))
                        .ok_or_else(|| {
                            MetaError::from(anyhow!(
                                "previous fragment info for {fragment_id} not found"
                            ))
                        })
                })
                .collect::<MetaResult<_>>()?
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
                        all_prev_fragments
                            .get(&fragment_id)
                            .map(|fragment| {
                                (
                                    fragment_id,
                                    fragment.actors.keys().copied().collect::<HashSet<_>>(),
                                )
                            })
                            .ok_or_else(|| {
                                MetaError::from(anyhow!(
                                    "fragment {} not found in previous state",
                                    fragment_id
                                ))
                            })
                    })
                    .collect::<MetaResult<_>>()?;

                all_fragment_actors.extend(fragment_actors);

                let source_fragment_actors = actors
                    .iter()
                    .map(|(actor_id, info)| (*actor_id, info.vnode_bitmap.clone()))
                    .collect();

                let mut all_actor_dispatchers: HashMap<_, Vec<_>> = HashMap::new();

                for downstream_fragment_id in downstream_fragments.keys() {
                    let target_fragment_actors =
                        match all_rendered_fragments.get(downstream_fragment_id) {
                            None => {
                                let external_fragment = all_prev_fragments
                                    .get(downstream_fragment_id)
                                    .ok_or_else(|| {
                                        MetaError::from(anyhow!(
                                            "fragment {} not found in previous state",
                                            downstream_fragment_id
                                        ))
                                    })?;

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
                        .ok_or_else(|| {
                            MetaError::from(anyhow!(
                                "downstream relation missing for {} -> {}",
                                fragment_id,
                                downstream_fragment_id
                            ))
                        })?;

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

                let prev_fragment = all_prev_fragments.get(&{ *fragment_id }).ok_or_else(|| {
                    MetaError::from(anyhow!(
                        "fragment {} not found in previous state",
                        fragment_id
                    ))
                })?;

                let reschedule = self.diff_fragment(
                    prev_fragment,
                    actors,
                    upstream_fragments,
                    downstream_fragments,
                    all_actor_dispatchers,
                    job_extra_info.get(job_id),
                )?;

                reschedules.insert(*fragment_id as FragmentId, reschedule);
            }

            let command = Command::RescheduleFragment {
                reschedules,
                fragment_actors: all_fragment_actors,
            };

            if let Command::RescheduleFragment { reschedules, .. } = &command {
                debug_assert!(
                    reschedules
                        .values()
                        .all(|reschedule| reschedule.vnode_bitmap_updates.is_empty()),
                    "RescheduleFragment command carries vnode_bitmap_updates, expected full rebuild"
                );
            }

            commands.insert(*database_id, command);
        }

        Ok(commands)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
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

const STREAM_TASK_RETRY_BASE_DELAY: Duration = Duration::from_millis(200);
const STREAM_TASK_RETRY_MAX_DELAY: Duration = Duration::from_secs(5);
const STREAM_TASK_RETRY_MAX_ATTEMPTS: usize = 60;
const STREAM_TASK_RETRY_TICK_INTERVAL: Duration = Duration::from_millis(200);

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
enum RetryTask {
    PostBackfillReschedule(JobId),
}

impl RetryTask {
    async fn execute(&self, manager: &GlobalStreamManager) -> MetaResult<()> {
        match self {
            RetryTask::PostBackfillReschedule(job_id) => {
                manager.apply_post_backfill_parallelism(*job_id).await
            }
        }
    }

    fn is_retryable(&self, err: &MetaError) -> bool {
        match self {
            RetryTask::PostBackfillReschedule(_) => matches!(
                err.inner(),
                MetaErrorInner::RescheduleBlockedBySnapshotBackfill
            ),
        }
    }
}

struct RetryState {
    attempts: usize,
    backoff: ExponentialBackoff,
    next_attempt_at: Instant,
    warned_after_max: bool,
}

impl RetryState {
    fn new(now: Instant) -> Self {
        let mut backoff =
            ExponentialBackoff::from_millis(STREAM_TASK_RETRY_BASE_DELAY.as_millis() as u64)
                .factor(2)
                .max_delay(STREAM_TASK_RETRY_MAX_DELAY);
        let delay = backoff.next().unwrap_or(STREAM_TASK_RETRY_MAX_DELAY);
        let next_attempt_at = now + jitter(delay);
        Self {
            attempts: 0,
            backoff,
            next_attempt_at,
            warned_after_max: false,
        }
    }

    fn schedule_next(&mut self, now: Instant) {
        let delay = self.backoff.next().unwrap_or(STREAM_TASK_RETRY_MAX_DELAY);
        self.next_attempt_at = now + jitter(delay);
    }
}

struct RetryQueue {
    pending: HashMap<RetryTask, RetryState>,
    ticker: tokio::time::Interval,
}

impl RetryQueue {
    fn new() -> Self {
        let mut ticker = tokio::time::interval(STREAM_TASK_RETRY_TICK_INTERVAL);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        Self {
            pending: HashMap::new(),
            ticker,
        }
    }

    fn enqueue(&mut self, task: RetryTask) {
        if self.pending.contains_key(&task) {
            return;
        }
        let now = Instant::now();
        self.pending.insert(task, RetryState::new(now));
    }

    fn has_pending(&self) -> bool {
        !self.pending.is_empty()
    }

    async fn execute_or_enqueue(&mut self, manager: &GlobalStreamManager, task: RetryTask) {
        if let Err(e) = task.execute(manager).await {
            if task.is_retryable(&e) {
                self.enqueue(task.clone());
                tracing::warn!(
                    task = ?task,
                    error = %e.as_report(),
                    "stream task failed; will retry"
                );
            } else {
                tracing::warn!(
                    task = ?task,
                    error = %e.as_report(),
                    "stream task failed with non-retryable error"
                );
            }
        }
    }

    async fn tick(&mut self, manager: &GlobalStreamManager) {
        if self.pending.is_empty() {
            return;
        }

        let now = Instant::now();
        let due_tasks = self
            .pending
            .iter()
            .filter(|(_, state)| state.next_attempt_at <= now)
            .map(|(task, _)| task.clone())
            .collect_vec();

        for task in due_tasks {
            let mut state = match self.pending.remove(&task) {
                Some(state) => state,
                None => continue,
            };

            state.attempts += 1;

            match task.execute(manager).await {
                Ok(()) => {
                    tracing::info!(
                        task = ?task,
                        attempts = state.attempts,
                        "stream task succeeded after retry"
                    );
                }
                Err(e) => {
                    if !task.is_retryable(&e) {
                        tracing::warn!(
                            task = ?task,
                            error = %e.as_report(),
                            "stream task failed with non-retryable error"
                        );
                        continue;
                    }

                    if state.attempts >= STREAM_TASK_RETRY_MAX_ATTEMPTS {
                        if !state.warned_after_max {
                            tracing::warn!(
                                task = ?task,
                                attempts = state.attempts,
                                error = %e.as_report(),
                                "stream task retry limit reached; continue with max delay"
                            );
                            state.warned_after_max = true;
                        }
                        state.schedule_next(Instant::now());
                        self.pending.insert(task, state);
                        continue;
                    }

                    state.schedule_next(Instant::now());
                    tracing::debug!(
                        task = ?task,
                        attempts = state.attempts,
                        next_retry_at = ?state.next_attempt_at,
                        "stream task retry scheduled"
                    );
                    self.pending.insert(task, state);
                }
            }
        }
    }
}

struct StreamManagerRunState {
    worker_cache: BTreeMap<WorkerId, WorkerNode>,
    previous_adaptive_parallelism_strategy: AdaptiveParallelismStrategy,
    should_trigger: bool,
}

impl StreamManagerRunState {
    fn new(worker_nodes: Vec<WorkerNode>) -> Self {
        let worker_cache = worker_nodes
            .into_iter()
            .map(|worker| (worker.id, worker))
            .collect();
        Self {
            worker_cache,
            previous_adaptive_parallelism_strategy: AdaptiveParallelismStrategy::default(),
            should_trigger: false,
        }
    }

    fn is_streaming_compute(worker: &WorkerNode) -> bool {
        worker.get_type() == Ok(WorkerType::ComputeNode)
            && worker.property.as_ref().unwrap().is_streaming
    }

    async fn handle_scale_tick(
        &mut self,
        manager: &GlobalStreamManager,
        ticker: &mut tokio::time::Interval,
    ) {
        if self.worker_cache.is_empty() {
            tracing::debug!("no available worker nodes");
            self.should_trigger = false;
            return;
        }

        match manager.trigger_parallelism_control().await {
            Ok(cont) => {
                self.should_trigger = cont;
            }
            Err(e) => {
                tracing::warn!(
                    error = %e.as_report(),
                    "Failed to trigger scale out, waiting for next tick to retry after {}s",
                    ticker.period().as_secs()
                );
                ticker.reset();
            }
        }
    }

    fn handle_system_params_change(&mut self, reader: &impl SystemParamsRead) {
        let new_strategy = reader.adaptive_parallelism_strategy();
        if new_strategy != self.previous_adaptive_parallelism_strategy {
            tracing::info!(
                "adaptive parallelism strategy changed from {:?} to {:?}",
                self.previous_adaptive_parallelism_strategy,
                new_strategy
            );
            self.should_trigger = true;
            self.previous_adaptive_parallelism_strategy = new_strategy;
        }
    }

    fn handle_worker_activated(&mut self, worker: WorkerNode) {
        if !Self::is_streaming_compute(&worker) {
            return;
        }

        tracing::info!(worker = %worker.id, "worker activated notification received");

        let prev_worker = self.worker_cache.insert(worker.id, worker.clone());

        match prev_worker {
            Some(prev_worker)
                if prev_worker.compute_node_parallelism() != worker.compute_node_parallelism() =>
            {
                tracing::info!(worker = %worker.id, "worker parallelism changed");
                self.should_trigger = true;
            }
            Some(prev_worker) if prev_worker.resource_group() != worker.resource_group() => {
                tracing::info!(worker = %worker.id, "worker label changed");
                self.should_trigger = true;
            }
            None => {
                tracing::info!(worker = %worker.id, "new worker joined");
                self.should_trigger = true;
            }
            _ => {}
        }
    }

    fn handle_worker_deleted(&mut self, worker: WorkerNode) {
        if !Self::is_streaming_compute(&worker) {
            return;
        }

        match self.worker_cache.remove(&worker.id) {
            Some(prev_worker) => {
                tracing::info!(
                    worker = %prev_worker.id,
                    "worker removed from stream manager cache"
                );
            }
            None => {
                tracing::warn!(
                    worker = %worker.id,
                    "worker not found in stream manager cache, but it was removed"
                );
            }
        }
    }

    async fn handle_notification(
        &mut self,
        manager: &GlobalStreamManager,
        retry_queue: &mut RetryQueue,
        notification: LocalNotification,
    ) {
        match notification {
            LocalNotification::SystemParamsChange(reader) => {
                self.handle_system_params_change(&reader);
            }
            LocalNotification::WorkerNodeActivated(worker) => {
                self.handle_worker_activated(worker);
            }
            LocalNotification::WorkerNodeDeleted(worker) => {
                self.handle_worker_deleted(worker);
            }
            LocalNotification::StreamingJobBackfillFinished(job_id) => {
                tracing::debug!(
                    job_id = %job_id,
                    "received backfill finished notification"
                );
                retry_queue
                    .execute_or_enqueue(manager, RetryTask::PostBackfillReschedule(job_id))
                    .await;
            }
            _ => {}
        }
    }
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

        let unreschedulable_jobs = self
            .metadata_manager
            .collect_unreschedulable_backfill_jobs(&background_streaming_jobs)
            .await?;

        let database_objects: HashMap<risingwave_meta_model::DatabaseId, Vec<JobId>> = self
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
            .filter(|(_, job_id)| !unreschedulable_jobs.contains(job_id))
            .collect_vec();

        if job_ids.is_empty() {
            tracing::info!("no streaming jobs for scaling, maybe an empty cluster");
            return Ok(false);
        }

        let active_workers =
            ActiveStreamingWorkerNodes::new_snapshot(self.metadata_manager.clone()).await?;

        tracing::info!(
            "trigger parallelism control for jobs: {:#?}, workers {:#?}",
            job_ids,
            active_workers.current()
        );

        let batch_size = match self.env.opts.parallelism_control_batch_size {
            0 => job_ids.len(),
            n => n,
        };

        tracing::info!(
            "total {} streaming jobs, batch size {}, schedulable worker ids: {:?}",
            job_ids.len(),
            batch_size,
            active_workers.current()
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
                .rerender(jobs, active_workers.current())
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

        let (local_notification_tx, mut local_notification_rx) =
            tokio::sync::mpsc::unbounded_channel();

        self.env
            .notification_manager()
            .insert_local_sender(local_notification_tx);

        // waiting for the first tick
        ticker.tick().await;

        let worker_nodes = self
            .metadata_manager
            .list_active_streaming_compute_nodes()
            .await
            .expect("list active streaming compute nodes");

        let mut run_state = StreamManagerRunState::new(worker_nodes);
        let mut retry_queue = RetryQueue::new();

        loop {
            let has_pending_retry = retry_queue.has_pending();

            tokio::select! {
                biased;

                _ = &mut shutdown_rx => {
                    tracing::info!("Stream manager is stopped");
                    break;
                }

                _ = ticker.tick(), if run_state.should_trigger => {
                    run_state.handle_scale_tick(self, &mut ticker).await;
                }

                _ = retry_queue.ticker.tick(), if has_pending_retry => {
                    retry_queue.tick(self).await;
                }

                notification = local_notification_rx.recv() => {
                    let notification = notification.expect("local notification channel closed in loop of stream manager");

                    run_state
                        .handle_notification(self, &mut retry_queue, notification)
                        .await;
                }
            }
        }
    }

    /// Restores a streaming job's parallelism to its target value after backfill completes.
    async fn apply_post_backfill_parallelism(&self, job_id: JobId) -> MetaResult<()> {
        // Fetch both the target parallelism (final desired state) and the backfill parallelism
        // (temporary parallelism used during backfill phase) from the catalog.
        let (target, backfill_parallelism) = self
            .metadata_manager
            .catalog_controller
            .get_job_parallelisms(job_id)
            .await?;

        // Determine if we need to reschedule based on the backfill configuration.
        match backfill_parallelism {
            Some(backfill_parallelism) if backfill_parallelism == target => {
                // Backfill parallelism matches target - no reschedule needed since the job
                // is already running at the desired parallelism.
                tracing::debug!(
                    job_id = %job_id,
                    ?backfill_parallelism,
                    ?target,
                    "backfill parallelism equals job parallelism, skip reschedule"
                );
                return Ok(());
            }
            Some(_) => {
                // Backfill parallelism differs from target - proceed to restore target parallelism.
            }
            None => {
                // No backfill parallelism was configured, meaning the job was created without
                // a special backfill override. No reschedule is necessary.
                tracing::debug!(
                    job_id = %job_id,
                    ?target,
                    "no backfill parallelism configured, skip post-backfill reschedule"
                );
                return Ok(());
            }
        }

        // Reschedule the job to restore its target parallelism.
        tracing::info!(
            job_id = %job_id,
            ?target,
            ?backfill_parallelism,
            "restoring parallelism after backfill via reschedule"
        );
        let policy = ReschedulePolicy::Parallelism(ParallelismPolicy {
            parallelism: target,
        });
        self.reschedule_streaming_job(job_id, policy, false).await?;

        tracing::info!(job_id = %job_id, "parallelism reschedule after backfill submitted");
        Ok(())
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

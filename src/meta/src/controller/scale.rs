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

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU32, Ordering};

use anyhow::anyhow;
use itertools::Itertools;
use risingwave_common::bitmap::Bitmap;
use risingwave_common::catalog::{FragmentTypeFlag, FragmentTypeMask};
use risingwave_common::id::JobId;
use risingwave_common::system_param::AdaptiveParallelismStrategy;
use risingwave_common::util::worker_util::DEFAULT_RESOURCE_GROUP;
use risingwave_connector::source::{SplitImpl, SplitMetaData};
use risingwave_meta_model::fragment::DistributionType;
use risingwave_meta_model::prelude::{
    Database, Fragment, FragmentRelation, FragmentSplits, Sink, Source, StreamingJob, Table,
};
use risingwave_meta_model::{
    DatabaseId, DispatcherType, FragmentId, SourceId, StreamingParallelism, WorkerId, database,
    fragment, fragment_relation, fragment_splits, object, sink, source, streaming_job, table,
};
use risingwave_meta_model_migration::Condition;
use sea_orm::{
    ColumnTrait, ConnectionTrait, EntityTrait, JoinType, QueryFilter, QuerySelect, QueryTrait,
    RelationTrait,
};

use crate::MetaResult;
use crate::controller::fragment::{InflightActorInfo, InflightFragmentInfo};
use crate::manager::ActiveStreamingWorkerNodes;
use crate::model::{ActorId, StreamActor};
use crate::stream::{AssignerBuilder, SplitDiffOptions};

pub(crate) async fn resolve_streaming_job_definition<C>(
    txn: &C,
    job_ids: &HashSet<JobId>,
) -> MetaResult<HashMap<JobId, String>>
where
    C: ConnectionTrait,
{
    let job_ids = job_ids.iter().cloned().collect_vec();

    // including table, materialized view, index
    let common_job_definitions: Vec<(JobId, String)> = Table::find()
        .select_only()
        .columns([
            table::Column::TableId,
            #[cfg(not(debug_assertions))]
            table::Column::Name,
            #[cfg(debug_assertions)]
            table::Column::Definition,
        ])
        .filter(table::Column::TableId.is_in(job_ids.clone()))
        .into_tuple()
        .all(txn)
        .await?;

    let sink_definitions: Vec<(JobId, String)> = Sink::find()
        .select_only()
        .columns([
            sink::Column::SinkId,
            #[cfg(not(debug_assertions))]
            sink::Column::Name,
            #[cfg(debug_assertions)]
            sink::Column::Definition,
        ])
        .filter(sink::Column::SinkId.is_in(job_ids.clone()))
        .into_tuple()
        .all(txn)
        .await?;

    let source_definitions: Vec<(JobId, String)> = Source::find()
        .select_only()
        .columns([
            source::Column::SourceId,
            #[cfg(not(debug_assertions))]
            source::Column::Name,
            #[cfg(debug_assertions)]
            source::Column::Definition,
        ])
        .filter(source::Column::SourceId.is_in(job_ids.clone()))
        .into_tuple()
        .all(txn)
        .await?;

    let definitions: HashMap<JobId, String> = common_job_definitions
        .into_iter()
        .chain(sink_definitions.into_iter())
        .chain(source_definitions.into_iter())
        .collect();

    Ok(definitions)
}

pub async fn load_fragment_info<C>(
    txn: &C,
    actor_id_counter: &AtomicU32,
    database_id: Option<DatabaseId>,
    worker_nodes: &ActiveStreamingWorkerNodes,
    adaptive_parallelism_strategy: AdaptiveParallelismStrategy,
) -> MetaResult<FragmentRenderMap>
where
    C: ConnectionTrait,
{
    let mut query = StreamingJob::find()
        .select_only()
        .column(streaming_job::Column::JobId);

    if let Some(database_id) = database_id {
        query = query
            .join(JoinType::InnerJoin, streaming_job::Relation::Object.def())
            .filter(object::Column::DatabaseId.eq(database_id));
    }

    let jobs: Vec<JobId> = query.into_tuple().all(txn).await?;

    if jobs.is_empty() {
        return Ok(HashMap::new());
    }

    let jobs: HashSet<JobId> = jobs.into_iter().collect();

    let available_workers: BTreeMap<_, _> = worker_nodes
        .current()
        .values()
        .filter(|worker| worker.is_streaming_schedulable())
        .map(|worker| {
            (
                worker.id as i32,
                WorkerInfo {
                    parallelism: NonZeroUsize::new(worker.compute_node_parallelism()).unwrap(),
                    resource_group: worker.resource_group(),
                },
            )
        })
        .collect();

    let RenderedGraph { fragments, .. } = render_jobs(
        txn,
        actor_id_counter,
        jobs,
        available_workers,
        adaptive_parallelism_strategy,
    )
    .await?;

    Ok(fragments)
}

#[derive(Debug)]
pub struct TargetResourcePolicy {
    pub resource_group: Option<String>,
    pub parallelism: StreamingParallelism,
}

#[derive(Debug, Clone)]
pub struct WorkerInfo {
    pub parallelism: NonZeroUsize,
    pub resource_group: Option<String>,
}

pub type FragmentRenderMap =
    HashMap<DatabaseId, HashMap<JobId, HashMap<FragmentId, InflightFragmentInfo>>>;

#[derive(Default)]
pub struct RenderedGraph {
    pub fragments: FragmentRenderMap,
    pub ensembles: Vec<NoShuffleEnsemble>,
}

impl RenderedGraph {
    pub fn empty() -> Self {
        Self::default()
    }
}

/// Fragment-scoped rendering entry point used by operational tooling.
/// It validates that the requested fragments are roots of their no-shuffle ensembles,
/// resolves only the metadata required for those components, and then reuses the shared
/// rendering pipeline to materialize actor assignments.
pub async fn render_fragments<C>(
    txn: &C,
    actor_id_counter: &AtomicU32,
    ensembles: Vec<NoShuffleEnsemble>,
    workers: BTreeMap<WorkerId, WorkerInfo>,
    adaptive_parallelism_strategy: AdaptiveParallelismStrategy,
) -> MetaResult<RenderedGraph>
where
    C: ConnectionTrait,
{
    if ensembles.is_empty() {
        return Ok(RenderedGraph::empty());
    }

    let required_fragment_ids: HashSet<_> = ensembles
        .iter()
        .flat_map(|ensemble| ensemble.components.iter().copied())
        .collect();

    let fragment_models = Fragment::find()
        .filter(fragment::Column::FragmentId.is_in(required_fragment_ids.iter().copied()))
        .all(txn)
        .await?;

    let found_fragment_ids: HashSet<_> = fragment_models
        .iter()
        .map(|fragment| fragment.fragment_id)
        .collect();

    if found_fragment_ids.len() != required_fragment_ids.len() {
        let missing = required_fragment_ids
            .difference(&found_fragment_ids)
            .copied()
            .collect_vec();
        return Err(anyhow!("fragments {:?} not found", missing).into());
    }

    let fragment_map: HashMap<_, _> = fragment_models
        .into_iter()
        .map(|fragment| (fragment.fragment_id, fragment))
        .collect();

    let job_ids: HashSet<_> = fragment_map
        .values()
        .map(|fragment| fragment.job_id)
        .collect();

    if job_ids.is_empty() {
        return Ok(RenderedGraph::empty());
    }

    let jobs: HashMap<_, _> = StreamingJob::find()
        .filter(streaming_job::Column::JobId.is_in(job_ids.iter().copied().collect_vec()))
        .all(txn)
        .await?
        .into_iter()
        .map(|job| (job.job_id, job))
        .collect();

    let found_job_ids: HashSet<_> = jobs.keys().copied().collect();
    if found_job_ids.len() != job_ids.len() {
        let missing = job_ids.difference(&found_job_ids).copied().collect_vec();
        return Err(anyhow!("streaming jobs {:?} not found", missing).into());
    }

    let fragments = render_no_shuffle_ensembles(
        txn,
        actor_id_counter,
        &ensembles,
        &fragment_map,
        &jobs,
        &workers,
        adaptive_parallelism_strategy,
    )
    .await?;

    Ok(RenderedGraph {
        fragments,
        ensembles,
    })
}

/// Job-scoped rendering entry point that walks every no-shuffle root belonging to the
/// provided streaming jobs before delegating to the shared rendering backend.
pub async fn render_jobs<C>(
    txn: &C,
    actor_id_counter: &AtomicU32,
    job_ids: HashSet<JobId>,
    workers: BTreeMap<WorkerId, WorkerInfo>,
    adaptive_parallelism_strategy: AdaptiveParallelismStrategy,
) -> MetaResult<RenderedGraph>
where
    C: ConnectionTrait,
{
    let excluded_fragments_query = FragmentRelation::find()
        .select_only()
        .column(fragment_relation::Column::TargetFragmentId)
        .filter(fragment_relation::Column::DispatcherType.eq(DispatcherType::NoShuffle))
        .into_query();

    let condition = Condition::all()
        .add(fragment::Column::JobId.is_in(job_ids.clone()))
        .add(fragment::Column::FragmentId.not_in_subquery(excluded_fragments_query));

    let fragments: Vec<FragmentId> = Fragment::find()
        .select_only()
        .column(fragment::Column::FragmentId)
        .filter(condition)
        .into_tuple()
        .all(txn)
        .await?;

    let ensembles = find_fragment_no_shuffle_dags_detailed(txn, &fragments).await?;

    let fragments = Fragment::find()
        .filter(
            fragment::Column::FragmentId.is_in(
                ensembles
                    .iter()
                    .flat_map(|graph| graph.components.iter())
                    .cloned()
                    .collect_vec(),
            ),
        )
        .all(txn)
        .await?;

    let fragment_map: HashMap<_, _> = fragments
        .into_iter()
        .map(|fragment| (fragment.fragment_id, fragment))
        .collect();

    let job_ids = fragment_map
        .values()
        .map(|fragment| fragment.job_id)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect_vec();

    let jobs: HashMap<_, _> = StreamingJob::find()
        .filter(streaming_job::Column::JobId.is_in(job_ids))
        .all(txn)
        .await?
        .into_iter()
        .map(|job| (job.job_id, job))
        .collect();

    let fragments = render_no_shuffle_ensembles(
        txn,
        actor_id_counter,
        &ensembles,
        &fragment_map,
        &jobs,
        &workers,
        adaptive_parallelism_strategy,
    )
    .await?;

    Ok(RenderedGraph {
        fragments,
        ensembles,
    })
}

/// Core rendering routine that consumes no-shuffle ensembles and produces
/// `InflightFragmentInfo`s by:
///   * determining the eligible worker pools and effective parallelism,
///   * generating actor→worker assignments plus vnode bitmaps and source splits,
///   * grouping the rendered fragments by database and streaming job.
async fn render_no_shuffle_ensembles<C>(
    txn: &C,
    actor_id_counter: &AtomicU32,
    ensembles: &[NoShuffleEnsemble],
    fragment_map: &HashMap<FragmentId, fragment::Model>,
    job_map: &HashMap<JobId, streaming_job::Model>,
    worker_map: &BTreeMap<WorkerId, WorkerInfo>,
    adaptive_parallelism_strategy: AdaptiveParallelismStrategy,
) -> MetaResult<FragmentRenderMap>
where
    C: ConnectionTrait,
{
    if ensembles.is_empty() {
        return Ok(HashMap::new());
    }

    #[cfg(debug_assertions)]
    {
        debug_sanity_check(ensembles, fragment_map, job_map);
    }

    let (fragment_source_ids, fragment_splits) =
        resolve_source_fragments(txn, fragment_map).await?;

    let job_ids = job_map.keys().copied().collect_vec();

    let streaming_job_databases: HashMap<JobId, _> = StreamingJob::find()
        .select_only()
        .column(streaming_job::Column::JobId)
        .column(object::Column::DatabaseId)
        .join(JoinType::LeftJoin, streaming_job::Relation::Object.def())
        .filter(streaming_job::Column::JobId.is_in(job_ids))
        .into_tuple()
        .all(txn)
        .await?
        .into_iter()
        .collect();

    let database_map: HashMap<_, _> = Database::find()
        .filter(
            database::Column::DatabaseId
                .is_in(streaming_job_databases.values().copied().collect_vec()),
        )
        .all(txn)
        .await?
        .into_iter()
        .map(|db| (db.database_id, db))
        .collect();

    let context = RenderActorsContext {
        fragment_source_ids: &fragment_source_ids,
        fragment_splits: &fragment_splits,
        streaming_job_databases: &streaming_job_databases,
        database_map: &database_map,
    };

    render_actors(
        actor_id_counter,
        ensembles,
        fragment_map,
        job_map,
        worker_map,
        adaptive_parallelism_strategy,
        context,
    )
}

// Only metadata resolved asynchronously lives here so the renderer stays synchronous
// and the call site keeps the runtime dependencies (maps, strategy, actor counter, etc.) explicit.
struct RenderActorsContext<'a> {
    fragment_source_ids: &'a HashMap<FragmentId, SourceId>,
    fragment_splits: &'a HashMap<FragmentId, Vec<SplitImpl>>,
    streaming_job_databases: &'a HashMap<JobId, DatabaseId>,
    database_map: &'a HashMap<DatabaseId, database::Model>,
}

fn render_actors(
    actor_id_counter: &AtomicU32,
    ensembles: &[NoShuffleEnsemble],
    fragment_map: &HashMap<FragmentId, fragment::Model>,
    job_map: &HashMap<JobId, streaming_job::Model>,
    worker_map: &BTreeMap<WorkerId, WorkerInfo>,
    adaptive_parallelism_strategy: AdaptiveParallelismStrategy,
    context: RenderActorsContext<'_>,
) -> MetaResult<FragmentRenderMap> {
    let RenderActorsContext {
        fragment_source_ids,
        fragment_splits: fragment_splits_map,
        streaming_job_databases,
        database_map,
    } = context;

    let mut all_fragments: FragmentRenderMap = HashMap::new();

    for NoShuffleEnsemble {
        entries,
        components,
    } in ensembles
    {
        tracing::debug!("rendering ensemble entries {:?}", entries);

        let entry_fragments = entries
            .iter()
            .map(|fragment_id| fragment_map.get(fragment_id).unwrap())
            .collect_vec();

        let entry_fragment_parallelism = entry_fragments
            .iter()
            .map(|fragment| fragment.parallelism.clone())
            .dedup()
            .exactly_one()
            .map_err(|_| {
                anyhow!(
                    "entry fragments {:?} have inconsistent parallelism settings",
                    entries.iter().copied().collect_vec()
                )
            })?;

        let (job_id, vnode_count) = entry_fragments
            .iter()
            .map(|f| (f.job_id, f.vnode_count as usize))
            .dedup()
            .exactly_one()
            .map_err(|_| anyhow!("Multiple jobs found in no-shuffle ensemble"))?;

        let job = job_map
            .get(&job_id)
            .ok_or_else(|| anyhow!("streaming job {job_id} not found"))?;

        let resource_group = match &job.specific_resource_group {
            None => {
                let database = streaming_job_databases
                    .get(&job_id)
                    .and_then(|database_id| database_map.get(database_id))
                    .unwrap();
                database.resource_group.clone()
            }
            Some(resource_group) => resource_group.clone(),
        };

        let available_workers: BTreeMap<WorkerId, NonZeroUsize> = worker_map
            .iter()
            .filter_map(|(worker_id, worker)| {
                if worker
                    .resource_group
                    .as_deref()
                    .unwrap_or(DEFAULT_RESOURCE_GROUP)
                    == resource_group.as_str()
                {
                    Some((*worker_id, worker.parallelism))
                } else {
                    None
                }
            })
            .collect();

        let total_parallelism = available_workers.values().map(|w| w.get()).sum::<usize>();

        let actual_parallelism = match entry_fragment_parallelism
            .as_ref()
            .unwrap_or(&job.parallelism)
        {
            StreamingParallelism::Adaptive | StreamingParallelism::Custom => {
                adaptive_parallelism_strategy.compute_target_parallelism(total_parallelism)
            }
            StreamingParallelism::Fixed(n) => *n,
        }
        .min(vnode_count)
        .min(job.max_parallelism as usize);

        tracing::debug!(
            "job {}, final {} parallelism {:?} total_parallelism {} job_max {} vnode count {} fragment_override {:?}",
            job_id,
            actual_parallelism,
            job.parallelism,
            total_parallelism,
            job.max_parallelism,
            vnode_count,
            entry_fragment_parallelism
        );

        let assigner = AssignerBuilder::new(job_id).build();

        let actors = (0..actual_parallelism).collect_vec();
        let vnodes = (0..vnode_count).collect_vec();

        let assignment = assigner.assign_hierarchical(&available_workers, &actors, &vnodes)?;

        let source_entry_fragment = entry_fragments.iter().find(|f| {
            let mask = FragmentTypeMask::from(f.fragment_type_mask);
            if mask.contains(FragmentTypeFlag::Source) {
                assert!(!mask.contains(FragmentTypeFlag::SourceScan))
            }
            mask.contains(FragmentTypeFlag::Source) && !mask.contains(FragmentTypeFlag::Dml)
        });

        let (fragment_splits, shared_source_id) = match source_entry_fragment {
            Some(entry_fragment) => {
                let source_id = fragment_source_ids
                    .get(&entry_fragment.fragment_id)
                    .ok_or_else(|| {
                        anyhow!(
                            "missing source id in source fragment {}",
                            entry_fragment.fragment_id
                        )
                    })?;

                let entry_fragment_id = entry_fragment.fragment_id;

                let empty_actor_splits: HashMap<_, _> = actors
                    .iter()
                    .map(|actor_id| (*actor_id as u32, vec![]))
                    .collect();

                let splits = fragment_splits_map
                    .get(&entry_fragment_id)
                    .cloned()
                    .unwrap_or_default();

                let splits: BTreeMap<_, _> = splits.into_iter().map(|s| (s.id(), s)).collect();

                let fragment_splits = crate::stream::source_manager::reassign_splits(
                    entry_fragment_id as u32,
                    empty_actor_splits,
                    &splits,
                    SplitDiffOptions::default(),
                )
                .unwrap_or_default();
                (fragment_splits, Some(*source_id))
            }
            None => (HashMap::new(), None),
        };

        for component_fragment_id in components {
            let &fragment::Model {
                fragment_id,
                job_id,
                fragment_type_mask,
                distribution_type,
                ref stream_node,
                ref state_table_ids,
                ..
            } = fragment_map.get(component_fragment_id).unwrap();

            let actor_count =
                u32::try_from(actors.len()).expect("actor parallelism exceeds u32::MAX");
            let actor_id_base = actor_id_counter.fetch_add(actor_count, Ordering::Relaxed);

            let actors: HashMap<ActorId, InflightActorInfo> = assignment
                .iter()
                .flat_map(|(worker_id, actors)| {
                    actors
                        .iter()
                        .map(move |(actor_id, vnodes)| (worker_id, actor_id, vnodes))
                })
                .map(|(&worker_id, &actor_idx, vnodes)| {
                    let vnode_bitmap = match distribution_type {
                        DistributionType::Single => None,
                        DistributionType::Hash => Some(Bitmap::from_indices(vnode_count, vnodes)),
                    };

                    let actor_id = actor_id_base + actor_idx as u32;

                    let splits = if let Some(source_id) = fragment_source_ids.get(&fragment_id) {
                        assert_eq!(shared_source_id, Some(*source_id));

                        fragment_splits
                            .get(&(actor_idx as u32))
                            .cloned()
                            .unwrap_or_default()
                    } else {
                        vec![]
                    };

                    (
                        actor_id,
                        InflightActorInfo {
                            worker_id,
                            vnode_bitmap,
                            splits,
                        },
                    )
                })
                .collect();

            let fragment = InflightFragmentInfo {
                fragment_id: fragment_id as u32,
                distribution_type,
                fragment_type_mask: fragment_type_mask.into(),
                vnode_count,
                nodes: stream_node.to_protobuf(),
                actors,
                state_table_ids: state_table_ids.inner_ref().iter().copied().collect(),
            };

            let &database_id = streaming_job_databases.get(&job_id).ok_or_else(|| {
                anyhow!("streaming job {job_id} not found in streaming_job_databases")
            })?;

            all_fragments
                .entry(database_id)
                .or_default()
                .entry(job_id)
                .or_default()
                .insert(fragment_id, fragment);
        }
    }

    Ok(all_fragments)
}

#[cfg(debug_assertions)]
fn debug_sanity_check(
    ensembles: &[NoShuffleEnsemble],
    fragment_map: &HashMap<FragmentId, fragment::Model>,
    jobs: &HashMap<JobId, streaming_job::Model>,
) {
    // Debug-only assertions to catch inconsistent ensemble metadata early.
    debug_assert!(
        ensembles
            .iter()
            .all(|ensemble| ensemble.entries.is_subset(&ensemble.components)),
        "entries must be subset of components"
    );

    let mut missing_fragments = BTreeSet::new();
    let mut missing_jobs = BTreeSet::new();

    for fragment_id in ensembles
        .iter()
        .flat_map(|ensemble| ensemble.components.iter())
    {
        match fragment_map.get(fragment_id) {
            Some(fragment) => {
                if !jobs.contains_key(&fragment.job_id) {
                    missing_jobs.insert(fragment.job_id);
                }
            }
            None => {
                missing_fragments.insert(*fragment_id);
            }
        }
    }

    debug_assert!(
        missing_fragments.is_empty(),
        "missing fragments in fragment_map: {:?}",
        missing_fragments
    );

    debug_assert!(
        missing_jobs.is_empty(),
        "missing jobs for fragments' job_id: {:?}",
        missing_jobs
    );

    for ensemble in ensembles {
        let unique_vnode_counts: Vec<_> = ensemble
            .components
            .iter()
            .flat_map(|fragment_id| {
                fragment_map
                    .get(fragment_id)
                    .map(|fragment| fragment.vnode_count)
            })
            .unique()
            .collect();

        debug_assert!(
            unique_vnode_counts.len() <= 1,
            "components in ensemble must share same vnode_count: ensemble={:?}, vnode_counts={:?}",
            ensemble.components,
            unique_vnode_counts
        );
    }
}

async fn resolve_source_fragments<C>(
    txn: &C,
    fragment_map: &HashMap<FragmentId, fragment::Model>,
) -> MetaResult<(
    HashMap<FragmentId, SourceId>,
    HashMap<FragmentId, Vec<SplitImpl>>,
)>
where
    C: ConnectionTrait,
{
    let mut source_fragment_ids = HashMap::new();
    for (fragment_id, fragment) in fragment_map {
        let mask = FragmentTypeMask::from(fragment.fragment_type_mask);
        if mask.contains(FragmentTypeFlag::Source)
            && let Some(source_id) = fragment.stream_node.to_protobuf().find_stream_source()
        {
            source_fragment_ids
                .entry(source_id)
                .or_insert_with(BTreeSet::new)
                .insert(fragment_id);
        }

        if mask.contains(FragmentTypeFlag::SourceScan)
            && let Some((source_id, _)) = fragment.stream_node.to_protobuf().find_source_backfill()
        {
            source_fragment_ids
                .entry(source_id)
                .or_insert_with(BTreeSet::new)
                .insert(fragment_id);
        }
    }

    let fragment_source_ids: HashMap<_, _> = source_fragment_ids
        .iter()
        .flat_map(|(source_id, fragment_ids)| {
            fragment_ids
                .iter()
                .map(|fragment_id| (**fragment_id, *source_id as SourceId))
        })
        .collect();

    let fragment_ids = fragment_source_ids.keys().copied().collect_vec();

    let fragment_splits: Vec<_> = FragmentSplits::find()
        .filter(fragment_splits::Column::FragmentId.is_in(fragment_ids))
        .all(txn)
        .await?;

    let fragment_splits: HashMap<_, _> = fragment_splits
        .into_iter()
        .flat_map(|model| {
            model.splits.map(|splits| {
                (
                    model.fragment_id,
                    splits
                        .to_protobuf()
                        .splits
                        .iter()
                        .flat_map(SplitImpl::try_from)
                        .collect_vec(),
                )
            })
        })
        .collect();

    Ok((fragment_source_ids, fragment_splits))
}

// Helper struct to make the function signature cleaner and to properly bundle the required data.
#[derive(Debug)]
pub struct ActorGraph<'a> {
    pub fragments: &'a HashMap<FragmentId, (Fragment, Vec<StreamActor>)>,
    pub locations: &'a HashMap<ActorId, WorkerId>,
}

#[derive(Debug, Clone)]
pub struct NoShuffleEnsemble {
    entries: HashSet<FragmentId>,
    components: HashSet<FragmentId>,
}

impl NoShuffleEnsemble {
    pub fn fragments(&self) -> impl Iterator<Item = FragmentId> + '_ {
        self.components.iter().cloned()
    }

    pub fn entry_fragments(&self) -> impl Iterator<Item = FragmentId> + '_ {
        self.entries.iter().copied()
    }

    pub fn component_fragments(&self) -> impl Iterator<Item = FragmentId> + '_ {
        self.components.iter().copied()
    }

    pub fn contains_entry(&self, fragment_id: &FragmentId) -> bool {
        self.entries.contains(fragment_id)
    }
}

pub async fn find_fragment_no_shuffle_dags_detailed(
    db: &impl ConnectionTrait,
    initial_fragment_ids: &[FragmentId],
) -> MetaResult<Vec<NoShuffleEnsemble>> {
    let all_no_shuffle_relations: Vec<(_, _)> = FragmentRelation::find()
        .columns([
            fragment_relation::Column::SourceFragmentId,
            fragment_relation::Column::TargetFragmentId,
        ])
        .filter(fragment_relation::Column::DispatcherType.eq(DispatcherType::NoShuffle))
        .into_tuple()
        .all(db)
        .await?;

    let mut forward_edges: HashMap<FragmentId, Vec<FragmentId>> = HashMap::new();
    let mut backward_edges: HashMap<FragmentId, Vec<FragmentId>> = HashMap::new();

    for (src, dst) in all_no_shuffle_relations {
        forward_edges.entry(src).or_default().push(dst);
        backward_edges.entry(dst).or_default().push(src);
    }

    find_no_shuffle_graphs(initial_fragment_ids, &forward_edges, &backward_edges)
}

fn find_no_shuffle_graphs(
    initial_fragment_ids: &[FragmentId],
    forward_edges: &HashMap<FragmentId, Vec<FragmentId>>,
    backward_edges: &HashMap<FragmentId, Vec<FragmentId>>,
) -> MetaResult<Vec<NoShuffleEnsemble>> {
    let mut graphs: Vec<NoShuffleEnsemble> = Vec::new();
    let mut globally_visited: HashSet<FragmentId> = HashSet::new();

    for &init_id in initial_fragment_ids {
        if globally_visited.contains(&init_id) {
            continue;
        }

        // Found a new component. Traverse it to find all its nodes.
        let mut components = HashSet::new();
        let mut queue: VecDeque<FragmentId> = VecDeque::new();

        queue.push_back(init_id);
        globally_visited.insert(init_id);

        while let Some(current_id) = queue.pop_front() {
            components.insert(current_id);
            let neighbors = forward_edges
                .get(&current_id)
                .into_iter()
                .flatten()
                .chain(backward_edges.get(&current_id).into_iter().flatten());

            for &neighbor_id in neighbors {
                if globally_visited.insert(neighbor_id) {
                    queue.push_back(neighbor_id);
                }
            }
        }

        // For the newly found component, identify its roots.
        let mut entries = HashSet::new();
        for &node_id in &components {
            let is_root = match backward_edges.get(&node_id) {
                Some(parents) => parents.iter().all(|p| !components.contains(p)),
                None => true,
            };
            if is_root {
                entries.insert(node_id);
            }
        }

        // Store the detailed DAG structure (roots, all nodes in this DAG).
        if !entries.is_empty() {
            graphs.push(NoShuffleEnsemble {
                entries,
                components,
            });
        }
    }

    Ok(graphs)
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeSet, HashMap, HashSet};
    use std::sync::Arc;

    use risingwave_connector::source::SplitImpl;
    use risingwave_connector::source::test_source::TestSourceSplit;
    use risingwave_meta_model::{CreateType, I32Array, JobStatus, StreamNode, TableIdArray};
    use risingwave_pb::stream_plan::StreamNode as PbStreamNode;

    use super::*;

    // Helper type aliases for cleaner test code
    // Using the actual FragmentId type from the module
    type Edges = (
        HashMap<FragmentId, Vec<FragmentId>>,
        HashMap<FragmentId, Vec<FragmentId>>,
    );

    /// A helper function to build forward and backward edge maps from a simple list of tuples.
    /// This reduces boilerplate in each test.
    fn build_edges(relations: &[(FragmentId, FragmentId)]) -> Edges {
        let mut forward_edges: HashMap<FragmentId, Vec<FragmentId>> = HashMap::new();
        let mut backward_edges: HashMap<FragmentId, Vec<FragmentId>> = HashMap::new();
        for &(src, dst) in relations {
            forward_edges.entry(src).or_default().push(dst);
            backward_edges.entry(dst).or_default().push(src);
        }
        (forward_edges, backward_edges)
    }

    /// Helper function to create a `HashSet` from a slice easily.
    fn to_hashset(ids: &[FragmentId]) -> HashSet<FragmentId> {
        ids.iter().cloned().collect()
    }

    #[allow(deprecated)]
    fn build_fragment(
        fragment_id: FragmentId,
        job_id: JobId,
        fragment_type_mask: i32,
        distribution_type: DistributionType,
        vnode_count: i32,
        parallelism: StreamingParallelism,
    ) -> fragment::Model {
        fragment::Model {
            fragment_id,
            job_id,
            fragment_type_mask,
            distribution_type,
            stream_node: StreamNode::from(&PbStreamNode::default()),
            state_table_ids: TableIdArray::default(),
            upstream_fragment_id: I32Array::default(),
            vnode_count,
            parallelism: Some(parallelism),
        }
    }

    type ActorState = (u32, WorkerId, Option<Vec<usize>>, Vec<String>);

    fn collect_actor_state(fragment: &InflightFragmentInfo) -> Vec<ActorState> {
        let base = fragment.actors.keys().copied().min().unwrap_or_default();

        let mut entries: Vec<_> = fragment
            .actors
            .iter()
            .map(|(&actor_id, info)| {
                let idx = actor_id - base;
                let vnode_indices = info.vnode_bitmap.as_ref().map(|bitmap| {
                    bitmap
                        .iter()
                        .enumerate()
                        .filter_map(|(pos, is_set)| is_set.then_some(pos))
                        .collect::<Vec<_>>()
                });
                let splits = info
                    .splits
                    .iter()
                    .map(|split| split.id().to_string())
                    .collect::<Vec<_>>();
                (idx, info.worker_id, vnode_indices, splits)
            })
            .collect();

        entries.sort_by_key(|(idx, _, _, _)| *idx);
        entries
    }

    #[test]
    fn test_single_linear_chain() {
        // Scenario: A simple linear graph 1 -> 2 -> 3.
        // We start from the middle node (2).
        let (forward, backward) = build_edges(&[(1, 2), (2, 3)]);
        let initial_ids = &[2];

        // Act
        let result = find_no_shuffle_graphs(initial_ids, &forward, &backward);

        // Assert
        assert!(result.is_ok());
        let graphs = result.unwrap();

        assert_eq!(graphs.len(), 1);
        let graph = &graphs[0];
        assert_eq!(graph.entries, to_hashset(&[1]));
        assert_eq!(graph.components, to_hashset(&[1, 2, 3]));
    }

    #[test]
    fn test_two_disconnected_graphs() {
        // Scenario: Two separate graphs: 1->2 and 10->11.
        // We start with one node from each graph.
        let (forward, backward) = build_edges(&[(1, 2), (10, 11)]);
        let initial_ids = &[2, 10];

        // Act
        let mut graphs = find_no_shuffle_graphs(initial_ids, &forward, &backward).unwrap();

        // Assert
        assert_eq!(graphs.len(), 2);

        // Sort results to make the test deterministic, as HashMap iteration order is not guaranteed.
        graphs.sort_by_key(|g| *g.components.iter().min().unwrap_or(&0));

        // Graph 1
        assert_eq!(graphs[0].entries, to_hashset(&[1]));
        assert_eq!(graphs[0].components, to_hashset(&[1, 2]));

        // Graph 2
        assert_eq!(graphs[1].entries, to_hashset(&[10]));
        assert_eq!(graphs[1].components, to_hashset(&[10, 11]));
    }

    #[test]
    fn test_multiple_entries_in_one_graph() {
        // Scenario: A graph with two roots feeding into one node: 1->3, 2->3.
        let (forward, backward) = build_edges(&[(1, 3), (2, 3)]);
        let initial_ids = &[3];

        // Act
        let graphs = find_no_shuffle_graphs(initial_ids, &forward, &backward).unwrap();

        // Assert
        assert_eq!(graphs.len(), 1);
        let graph = &graphs[0];
        assert_eq!(graph.entries, to_hashset(&[1, 2]));
        assert_eq!(graph.components, to_hashset(&[1, 2, 3]));
    }

    #[test]
    fn test_diamond_shape_graph() {
        // Scenario: A diamond shape: 1->2, 1->3, 2->4, 3->4
        let (forward, backward) = build_edges(&[(1, 2), (1, 3), (2, 4), (3, 4)]);
        let initial_ids = &[4];

        // Act
        let graphs = find_no_shuffle_graphs(initial_ids, &forward, &backward).unwrap();

        // Assert
        assert_eq!(graphs.len(), 1);
        let graph = &graphs[0];
        assert_eq!(graph.entries, to_hashset(&[1]));
        assert_eq!(graph.components, to_hashset(&[1, 2, 3, 4]));
    }

    #[test]
    fn test_starting_with_multiple_nodes_in_same_graph() {
        // Scenario: Start with two different nodes (2 and 4) from the same component.
        // Should only identify one graph, not two.
        let (forward, backward) = build_edges(&[(1, 2), (2, 3), (3, 4)]);
        let initial_ids = &[2, 4];

        // Act
        let graphs = find_no_shuffle_graphs(initial_ids, &forward, &backward).unwrap();

        // Assert
        assert_eq!(graphs.len(), 1);
        let graph = &graphs[0];
        assert_eq!(graph.entries, to_hashset(&[1]));
        assert_eq!(graph.components, to_hashset(&[1, 2, 3, 4]));
    }

    #[test]
    fn test_empty_initial_ids() {
        // Scenario: The initial ID list is empty.
        let (forward, backward) = build_edges(&[(1, 2)]);
        let initial_ids = &[];

        // Act
        let graphs = find_no_shuffle_graphs(initial_ids, &forward, &backward).unwrap();

        // Assert
        assert!(graphs.is_empty());
    }

    #[test]
    fn test_isolated_node_as_input() {
        // Scenario: Start with an ID that has no relations.
        let (forward, backward) = build_edges(&[(1, 2)]);
        let initial_ids = &[100];

        // Act
        let graphs = find_no_shuffle_graphs(initial_ids, &forward, &backward).unwrap();

        // Assert
        assert_eq!(graphs.len(), 1);
        let graph = &graphs[0];
        assert_eq!(graph.entries, to_hashset(&[100]));
        assert_eq!(graph.components, to_hashset(&[100]));
    }

    #[test]
    fn test_graph_with_a_cycle() {
        // Scenario: A graph with a cycle: 1 -> 2 -> 3 -> 1.
        // The algorithm should correctly identify all nodes in the component.
        // Crucially, NO node is a root because every node has a parent *within the component*.
        // Therefore, the `entries` set should be empty, and the graph should not be included in the results.
        let (forward, backward) = build_edges(&[(1, 2), (2, 3), (3, 1)]);
        let initial_ids = &[2];

        // Act
        let graphs = find_no_shuffle_graphs(initial_ids, &forward, &backward).unwrap();

        // Assert
        assert!(
            graphs.is_empty(),
            "A graph with no entries should not be returned"
        );
    }
    #[test]
    fn test_custom_complex() {
        let (forward, backward) = build_edges(&[(1, 3), (1, 8), (2, 3), (4, 3), (3, 5), (6, 7)]);
        let initial_ids = &[1, 2, 4, 6];

        // Act
        let mut graphs = find_no_shuffle_graphs(initial_ids, &forward, &backward).unwrap();

        // Assert
        assert_eq!(graphs.len(), 2);
        // Sort results to make the test deterministic, as HashMap iteration order is not guaranteed.
        graphs.sort_by_key(|g| *g.components.iter().min().unwrap_or(&0));

        // Graph 1
        assert_eq!(graphs[0].entries, to_hashset(&[1, 2, 4]));
        assert_eq!(graphs[0].components, to_hashset(&[1, 2, 3, 4, 5, 8]));

        // Graph 2
        assert_eq!(graphs[1].entries, to_hashset(&[6]));
        assert_eq!(graphs[1].components, to_hashset(&[6, 7]));
    }

    #[test]
    fn render_actors_increments_actor_counter() {
        let actor_id_counter = AtomicU32::new(100);
        let fragment_id: FragmentId = 1;
        let job_id: JobId = 10.into();
        let database_id: DatabaseId = DatabaseId::new(3);

        let fragment_model = build_fragment(
            fragment_id,
            job_id,
            0,
            DistributionType::Single,
            1,
            StreamingParallelism::Fixed(1),
        );

        let job_model = streaming_job::Model {
            job_id,
            job_status: JobStatus::Created,
            create_type: CreateType::Foreground,
            timezone: None,
            parallelism: StreamingParallelism::Fixed(1),
            max_parallelism: 1,
            specific_resource_group: None,
        };

        let database_model = database::Model {
            database_id,
            name: "test_db".into(),
            resource_group: "rg-a".into(),
            barrier_interval_ms: None,
            checkpoint_frequency: None,
        };

        let ensembles = vec![NoShuffleEnsemble {
            entries: HashSet::from([fragment_id]),
            components: HashSet::from([fragment_id]),
        }];

        let fragment_map = HashMap::from([(fragment_id, fragment_model)]);
        let job_map = HashMap::from([(job_id, job_model)]);

        let worker_map = BTreeMap::from([(
            1,
            WorkerInfo {
                parallelism: NonZeroUsize::new(1).unwrap(),
                resource_group: Some("rg-a".into()),
            },
        )]);

        let fragment_source_ids: HashMap<FragmentId, SourceId> = HashMap::new();
        let fragment_splits: HashMap<FragmentId, Vec<SplitImpl>> = HashMap::new();
        let streaming_job_databases = HashMap::from([(job_id, database_id)]);
        let database_map = HashMap::from([(database_id, database_model)]);

        let context = RenderActorsContext {
            fragment_source_ids: &fragment_source_ids,
            fragment_splits: &fragment_splits,
            streaming_job_databases: &streaming_job_databases,
            database_map: &database_map,
        };

        let result = render_actors(
            &actor_id_counter,
            &ensembles,
            &fragment_map,
            &job_map,
            &worker_map,
            AdaptiveParallelismStrategy::Auto,
            context,
        )
        .expect("actor rendering succeeds");

        let state = collect_actor_state(&result[&database_id][&job_id][&fragment_id]);
        assert_eq!(state.len(), 1);
        assert!(
            state[0].2.is_none(),
            "single distribution should not assign vnode bitmaps"
        );
        assert_eq!(actor_id_counter.load(Ordering::Relaxed), 101);
    }

    #[test]
    fn render_actors_aligns_hash_vnode_bitmaps() {
        let actor_id_counter = AtomicU32::new(0);
        let entry_fragment_id: FragmentId = 1;
        let downstream_fragment_id: FragmentId = 2;
        let job_id: JobId = 20.into();
        let database_id: DatabaseId = DatabaseId::new(5);

        let entry_fragment = build_fragment(
            entry_fragment_id,
            job_id,
            0,
            DistributionType::Hash,
            4,
            StreamingParallelism::Fixed(2),
        );

        let downstream_fragment = build_fragment(
            downstream_fragment_id,
            job_id,
            0,
            DistributionType::Hash,
            4,
            StreamingParallelism::Fixed(2),
        );

        let job_model = streaming_job::Model {
            job_id,
            job_status: JobStatus::Created,
            create_type: CreateType::Background,
            timezone: None,
            parallelism: StreamingParallelism::Fixed(2),
            max_parallelism: 2,
            specific_resource_group: None,
        };

        let database_model = database::Model {
            database_id,
            name: "test_db_hash".into(),
            resource_group: "rg-hash".into(),
            barrier_interval_ms: None,
            checkpoint_frequency: None,
        };

        let ensembles = vec![NoShuffleEnsemble {
            entries: HashSet::from([entry_fragment_id]),
            components: HashSet::from([entry_fragment_id, downstream_fragment_id]),
        }];

        let fragment_map = HashMap::from([
            (entry_fragment_id, entry_fragment),
            (downstream_fragment_id, downstream_fragment),
        ]);
        let job_map = HashMap::from([(job_id, job_model)]);

        let worker_map = BTreeMap::from([
            (
                1,
                WorkerInfo {
                    parallelism: NonZeroUsize::new(1).unwrap(),
                    resource_group: Some("rg-hash".into()),
                },
            ),
            (
                2,
                WorkerInfo {
                    parallelism: NonZeroUsize::new(1).unwrap(),
                    resource_group: Some("rg-hash".into()),
                },
            ),
        ]);

        let fragment_source_ids: HashMap<FragmentId, SourceId> = HashMap::new();
        let fragment_splits: HashMap<FragmentId, Vec<SplitImpl>> = HashMap::new();
        let streaming_job_databases = HashMap::from([(job_id, database_id)]);
        let database_map = HashMap::from([(database_id, database_model)]);

        let context = RenderActorsContext {
            fragment_source_ids: &fragment_source_ids,
            fragment_splits: &fragment_splits,
            streaming_job_databases: &streaming_job_databases,
            database_map: &database_map,
        };

        let result = render_actors(
            &actor_id_counter,
            &ensembles,
            &fragment_map,
            &job_map,
            &worker_map,
            AdaptiveParallelismStrategy::Auto,
            context,
        )
        .expect("actor rendering succeeds");

        let entry_state = collect_actor_state(&result[&database_id][&job_id][&entry_fragment_id]);
        let downstream_state =
            collect_actor_state(&result[&database_id][&job_id][&downstream_fragment_id]);

        assert_eq!(entry_state.len(), 2);
        assert_eq!(entry_state, downstream_state);

        let assigned_vnodes: BTreeSet<_> = entry_state
            .iter()
            .flat_map(|(_, _, vnodes, _)| {
                vnodes
                    .as_ref()
                    .expect("hash distribution should populate vnode bitmap")
                    .iter()
                    .copied()
            })
            .collect();
        assert_eq!(assigned_vnodes, BTreeSet::from([0, 1, 2, 3]));
        assert_eq!(actor_id_counter.load(Ordering::Relaxed), 4);
    }

    #[test]
    fn render_actors_propagates_source_splits() {
        let actor_id_counter = AtomicU32::new(0);
        let entry_fragment_id: FragmentId = 11;
        let downstream_fragment_id: FragmentId = 12;
        let job_id: JobId = 30.into();
        let database_id: DatabaseId = DatabaseId::new(7);
        let source_id: SourceId = 99;

        let source_mask = FragmentTypeFlag::raw_flag([FragmentTypeFlag::Source]) as i32;
        let source_scan_mask = FragmentTypeFlag::raw_flag([FragmentTypeFlag::SourceScan]) as i32;

        let entry_fragment = build_fragment(
            entry_fragment_id,
            job_id,
            source_mask,
            DistributionType::Hash,
            4,
            StreamingParallelism::Fixed(2),
        );

        let downstream_fragment = build_fragment(
            downstream_fragment_id,
            job_id,
            source_scan_mask,
            DistributionType::Hash,
            4,
            StreamingParallelism::Fixed(2),
        );

        let job_model = streaming_job::Model {
            job_id,
            job_status: JobStatus::Created,
            create_type: CreateType::Background,
            timezone: None,
            parallelism: StreamingParallelism::Fixed(2),
            max_parallelism: 2,
            specific_resource_group: None,
        };

        let database_model = database::Model {
            database_id,
            name: "split_db".into(),
            resource_group: "rg-source".into(),
            barrier_interval_ms: None,
            checkpoint_frequency: None,
        };

        let ensembles = vec![NoShuffleEnsemble {
            entries: HashSet::from([entry_fragment_id]),
            components: HashSet::from([entry_fragment_id, downstream_fragment_id]),
        }];

        let fragment_map = HashMap::from([
            (entry_fragment_id, entry_fragment),
            (downstream_fragment_id, downstream_fragment),
        ]);
        let job_map = HashMap::from([(job_id, job_model)]);

        let worker_map = BTreeMap::from([
            (
                1,
                WorkerInfo {
                    parallelism: NonZeroUsize::new(1).unwrap(),
                    resource_group: Some("rg-source".into()),
                },
            ),
            (
                2,
                WorkerInfo {
                    parallelism: NonZeroUsize::new(1).unwrap(),
                    resource_group: Some("rg-source".into()),
                },
            ),
        ]);

        let split_a = SplitImpl::Test(TestSourceSplit {
            id: Arc::<str>::from("split-a"),
            properties: HashMap::new(),
            offset: "0".into(),
        });
        let split_b = SplitImpl::Test(TestSourceSplit {
            id: Arc::<str>::from("split-b"),
            properties: HashMap::new(),
            offset: "0".into(),
        });

        let fragment_source_ids = HashMap::from([
            (entry_fragment_id, source_id),
            (downstream_fragment_id, source_id),
        ]);
        let fragment_splits =
            HashMap::from([(entry_fragment_id, vec![split_a.clone(), split_b.clone()])]);
        let streaming_job_databases = HashMap::from([(job_id, database_id)]);
        let database_map = HashMap::from([(database_id, database_model)]);

        let context = RenderActorsContext {
            fragment_source_ids: &fragment_source_ids,
            fragment_splits: &fragment_splits,
            streaming_job_databases: &streaming_job_databases,
            database_map: &database_map,
        };

        let result = render_actors(
            &actor_id_counter,
            &ensembles,
            &fragment_map,
            &job_map,
            &worker_map,
            AdaptiveParallelismStrategy::Auto,
            context,
        )
        .expect("actor rendering succeeds");

        let entry_state = collect_actor_state(&result[&database_id][&job_id][&entry_fragment_id]);
        let downstream_state =
            collect_actor_state(&result[&database_id][&job_id][&downstream_fragment_id]);

        assert_eq!(entry_state, downstream_state);

        let split_ids: BTreeSet<_> = entry_state
            .iter()
            .flat_map(|(_, _, _, splits)| splits.iter().cloned())
            .collect();
        assert_eq!(
            split_ids,
            BTreeSet::from([split_a.id().to_string(), split_b.id().to_string()])
        );
        assert_eq!(actor_id_counter.load(Ordering::Relaxed), 4);
    }
}

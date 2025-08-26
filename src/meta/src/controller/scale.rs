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

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::num::NonZeroUsize;

use anyhow::anyhow;
use itertools::Itertools;
use risingwave_common::bitmap::Bitmap;
use risingwave_common::catalog;
use risingwave_common::system_param::AdaptiveParallelismStrategy;
use risingwave_common::util::worker_util::DEFAULT_RESOURCE_GROUP;
use risingwave_meta_model::fragment::DistributionType;
use risingwave_meta_model::prelude::{
    Database, Fragment, FragmentRelation, Sink, Source, StreamingJob, Table,
};
use risingwave_meta_model::{
    DatabaseId, DispatcherType, FragmentId, ObjectId, StreamingParallelism, TableId, WorkerId,
    database, fragment, fragment_relation, object, sink, source, streaming_job, table,
};
use risingwave_meta_model_migration::Condition;
use sea_orm::{
    ColumnTrait, ConnectionTrait, DbBackend, EntityTrait, JoinType, QueryFilter, QuerySelect,
    QueryTrait, RelationTrait,
};

use crate::MetaResult;
use crate::controller::fragment::{InflightActorInfo, InflightFragmentInfo};
use crate::manager::ActiveStreamingWorkerNodes;
use crate::model::{ActorId, StreamActor};
use crate::stream::AssignerBuilder;

pub(crate) async fn resolve_streaming_job_definition<C>(
    txn: &C,
    job_ids: &HashSet<ObjectId>,
) -> MetaResult<HashMap<ObjectId, String>>
where
    C: ConnectionTrait,
{
    let job_ids = job_ids.iter().cloned().collect_vec();

    // including table, materialized view, index
    let common_job_definitions: Vec<(ObjectId, String)> = Table::find()
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

    let sink_definitions: Vec<(ObjectId, String)> = Sink::find()
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

    let source_definitions: Vec<(ObjectId, String)> = Source::find()
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

    let definitions: HashMap<ObjectId, String> = common_job_definitions
        .into_iter()
        .chain(sink_definitions.into_iter())
        .chain(source_definitions.into_iter())
        .collect();

    Ok(definitions)
}

pub async fn load_fragment_info<C>(
    txn: &C,
    database_id: Option<DatabaseId>,
    worker_nodes: &ActiveStreamingWorkerNodes,
    adaptive_parallelism_strategy: AdaptiveParallelismStrategy,
) -> MetaResult<HashMap<DatabaseId, HashMap<TableId, HashMap<FragmentId, InflightFragmentInfo>>>>
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

    let jobs: Vec<ObjectId> = query.into_tuple().all(txn).await?;

    if jobs.is_empty() {
        return Ok(HashMap::new());
    }

    let jobs: HashSet<ObjectId> = jobs.into_iter().collect();

    let available_workers: BTreeMap<_, _> = worker_nodes
        .current()
        .values()
        .filter(|worker| worker.is_streaming_schedulable())
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

    println!("before render");

    render_jobs(txn, jobs, available_workers, adaptive_parallelism_strategy).await
}

#[derive(Debug)]
pub struct TargetResourcePolicy {
    pub resource_group: Option<String>,
    pub parallelism: StreamingParallelism,
}

#[derive(Debug, Clone)]
pub struct WorkerInfo {
    pub weight: NonZeroUsize,
    pub resource_group: Option<String>,
}

pub async fn render_jobs<C>(
    txn: &C,
    job_ids: HashSet<ObjectId>,
    workers: BTreeMap<WorkerId, WorkerInfo>,
    adaptive_parallelism_strategy: AdaptiveParallelismStrategy,
) -> MetaResult<HashMap<DatabaseId, HashMap<TableId, HashMap<FragmentId, InflightFragmentInfo>>>>
where
    C: ConnectionTrait,
{
    println!("render jobs");

    println!("jobs {:?}", job_ids);
    println!("workers {:?}", workers);

    let jobs = StreamingJob::find()
        .filter(streaming_job::Column::JobId.is_in(job_ids.clone()))
        .all(txn)
        .await?;

    let jobs = jobs
        .into_iter()
        .map(|job| (job.job_id, job))
        .collect::<HashMap<_, _>>();

    let excluded_fragments_query = FragmentRelation::find()
        .select_only()
        .column(fragment_relation::Column::TargetFragmentId)
        .filter(fragment_relation::Column::DispatcherType.eq(DispatcherType::NoShuffle))
        .into_query();

    let condition = Condition::all()
        .add(fragment::Column::JobId.is_in(job_ids.clone()))
        .add(fragment::Column::FragmentId.not_in_subquery(excluded_fragments_query));

    let select = Fragment::find()
        .select_only()
        .column(fragment::Column::FragmentId)
        .filter(condition);

    let statement = select.build(DbBackend::Postgres);
    println!("sql {}", statement);
    let fragments: Vec<FragmentId> = select.into_tuple().all(txn).await?;

    println!("11111111");

    let ensembles = find_fragment_no_shuffle_dags_detailed(txn, &fragments).await?;

    let related_fragment_ids: HashSet<_> = ensembles
        .iter()
        .flat_map(|graph| graph.components.iter().cloned())
        .collect();

    let fragments = Fragment::find()
        .filter(fragment::Column::FragmentId.is_in(related_fragment_ids.clone()))
        .all(txn)
        .await?;

    println!("2222222");

    let mut fragment_map: HashMap<_, _> =
        fragments.into_iter().map(|f| (f.fragment_id, f)).collect();

    println!("333333");

    let object_databases: Vec<(ObjectId, DatabaseId)> = StreamingJob::find()
        .select_only()
        .column(streaming_job::Column::JobId)
        .column(object::Column::DatabaseId)
        .join(JoinType::LeftJoin, streaming_job::Relation::Object.def())
        //.filter(streaming_job::Column::JobId.is_in(job_ids))
        .into_tuple()
        .all(txn)
        .await?;

    println!("obj db {:#?}", object_databases);

    let streaming_job_databases = object_databases.into_iter().collect::<HashMap<_, _>>();

    let database_ids = streaming_job_databases.values().copied().collect_vec();

    println!("444444");

    let databases = Database::find()
        .filter(database::Column::DatabaseId.is_in(database_ids))
        .all(txn)
        .await?;

    let database_map = databases
        .into_iter()
        .map(|db| (db.database_id, db))
        .collect::<HashMap<_, _>>();

    let mut result: HashMap<
        DatabaseId,
        HashMap<TableId, HashMap<FragmentId, InflightFragmentInfo>>,
    > = HashMap::new();

    for NoShuffleEnsemble {
        entries,
        components,
    } in ensembles
    {
        let entry_fragments = entries
            .iter()
            .map(|fragment_id| fragment_map.get(fragment_id).unwrap())
            .collect_vec();

        let (job_id, vnode_count) = entry_fragments
            .iter()
            .map(|f| (f.job_id, f.vnode_count as usize))
            .dedup()
            .exactly_one()
            .map_err(|_| anyhow!("Multiple jobs found in no-shuffle ensemble"))?;

        let job = jobs.get(&job_id).unwrap();

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

        let workers: BTreeMap<WorkerId, NonZeroUsize> = workers
            .iter()
            .filter_map(|(worker_id, worker)| {
                if worker
                    .resource_group
                    .as_deref()
                    .unwrap_or(DEFAULT_RESOURCE_GROUP)
                    == resource_group.as_str()
                {
                    Some((*worker_id, worker.weight))
                } else {
                    None
                }
            })
            .collect();

        let total_parallelism = workers.values().map(|w| w.get()).sum::<usize>();

        let fact_parallelism = match job.parallelism {
            StreamingParallelism::Adaptive => {
                adaptive_parallelism_strategy.compute_target_parallelism(total_parallelism)
            }
            StreamingParallelism::Fixed(n) => n,
            StreamingParallelism::Custom => unreachable!(),
        }
        .min(total_parallelism) // limit fixed
        .min(job.max_parallelism as usize) // limit max parallelism
        .min(vnode_count);

        let assigner = AssignerBuilder::new(job_id).build();

        let actors = (0..fact_parallelism).collect_vec();
        let vnodes = (0..vnode_count).collect_vec();

        let assignment = assigner.assign_hierarchical(&workers, &actors, &vnodes)?;

        for fragment_id in components {
            let fragment::Model {
                fragment_id,
                job_id,
                fragment_type_mask,
                distribution_type,
                stream_node,
                state_table_ids,
                ..
            } = fragment_map.remove(&fragment_id).unwrap();

            let actors: HashMap<crate::model::ActorId, InflightActorInfo> = assignment
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

                    let actor_id = (fragment_id << 16) as u32 | actor_idx as u32;
                    (
                        actor_id,
                        InflightActorInfo {
                            worker_id,
                            vnode_bitmap,
                        },
                    )
                })
                .collect();

            let fragment = InflightFragmentInfo {
                fragment_id: fragment_id as u32,
                job_id,
                distribution_type,
                fragment_type_mask: fragment_type_mask.into(),
                vnode_count,
                nodes: stream_node.to_protobuf(),
                actors,
                state_table_ids: state_table_ids
                    .into_inner()
                    .into_iter()
                    .map(|id| catalog::TableId::new(id as _))
                    .collect(),
            };

            println!("all {:#?} job id {}", streaming_job_databases, job_id);

            let database_id = streaming_job_databases[&job_id];

            result
                .entry(database_id)
                .or_default()
                .entry(job_id)
                .or_default()
                .insert(fragment_id, fragment);
        }
    }
    Ok(result)
}

async fn render_inner<C>(
    txn: &C,
    ensembles: Vec<NoShuffleEnsemble>,
    workers: &BTreeMap<WorkerId, NonZeroUsize>,
) -> MetaResult<HashMap<DatabaseId, HashMap<TableId, HashMap<FragmentId, InflightFragmentInfo>>>>
where
    C: ConnectionTrait,
{
    let total_parallelism = workers.values().map(|w| w.get()).sum::<usize>();

    let all_fragment_ids: HashSet<_> = ensembles
        .iter()
        .flat_map(|graph| graph.components.iter().cloned())
        .collect();

    let fragments = Fragment::find()
        .filter(fragment::Column::FragmentId.is_in(all_fragment_ids.clone()))
        .all(txn)
        .await?;

    let mut fragment_map: HashMap<_, _> =
        fragments.into_iter().map(|f| (f.fragment_id, f)).collect();

    let streaming_jobs = StreamingJob::find()
        .join(JoinType::InnerJoin, streaming_job::Relation::Object.def())
        .join(JoinType::InnerJoin, object::Relation::Fragment.def())
        .filter(fragment::Column::FragmentId.is_in(all_fragment_ids))
        .all(txn)
        .await?;

    println!("xxxx");

    let streaming_jobs_map: HashMap<_, _> = streaming_jobs
        .into_iter()
        .map(|job| (job.job_id, job))
        .collect();

    // let job_definitions =
    //     resolve_streaming_job_definition(txn, &streaming_jobs_map.keys().cloned().collect())
    //         .await?;

    // let object_databases: Vec<(ObjectId, DatabaseId)> = Object::find()
    //     .columns([object::Column::Oid, object::Column::DatabaseId])
    //     .filter(object::Column::DatabaseId.is_not_null())
    //     .into_tuple()
    //     .all(txn)
    //     .await?;

    let object_databases: Vec<(ObjectId, DatabaseId)> = StreamingJob::find()
        .select_only()
        .column(streaming_job::Column::JobId)
        .column(object::Column::DatabaseId)
        .join(JoinType::LeftJoin, streaming_job::Relation::Object.def())
        .into_tuple()
        .all(txn)
        .await?;

    let streaming_job_database: HashMap<_, _> = object_databases.into_iter().collect();

    let mut result: HashMap<
        DatabaseId,
        HashMap<TableId, HashMap<FragmentId, InflightFragmentInfo>>,
    > = HashMap::new();

    println!("yyyyy");

    for NoShuffleEnsemble {
        entries,
        components,
    } in ensembles
    {
        let entry_fragments = entries
            .iter()
            .map(|fragment_id| fragment_map.get(fragment_id).unwrap())
            .collect_vec();

        let (job_id, vnode_count) = entry_fragments
            .iter()
            .map(|f| (f.job_id, f.vnode_count as usize))
            .dedup()
            .exactly_one()
            .map_err(|_| anyhow!("Multiple jobs found in no-shuffle ensemble"))?;

        let job = streaming_jobs_map.get(&job_id).unwrap();

        let parallelism = &job.parallelism;
        let max_parallelism = job.max_parallelism;

        let fact_parallelism = match parallelism {
            StreamingParallelism::Fixed(parallelism) => *parallelism,
            _ => total_parallelism,
        }
        .min(max_parallelism as usize)
        .min(vnode_count);

        let assigner = AssignerBuilder::new(job_id).build();

        let actors = (0..fact_parallelism).collect_vec();
        let vnodes = (0..vnode_count).collect_vec();

        let assignment = assigner.assign_hierarchical(workers, &actors, &vnodes)?;

        for fragment_id in components {
            let fragment::Model {
                fragment_id,
                job_id,
                fragment_type_mask,
                distribution_type,
                stream_node,
                state_table_ids,
                ..
            } = fragment_map.remove(&fragment_id).unwrap();

            let actors: HashMap<crate::model::ActorId, InflightActorInfo> = assignment
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

                    let actor_id = (fragment_id << 16) as u32 | actor_idx as u32;
                    (
                        actor_id,
                        InflightActorInfo {
                            worker_id,
                            vnode_bitmap,
                        },
                    )
                })
                .collect();

            let fragment = InflightFragmentInfo {
                fragment_id: fragment_id as u32,
                job_id,
                distribution_type,
                fragment_type_mask: fragment_type_mask.into(),
                vnode_count,
                nodes: stream_node.to_protobuf(),
                actors,
                state_table_ids: state_table_ids
                    .into_inner()
                    .into_iter()
                    .map(|id| catalog::TableId::new(id as _))
                    .collect(),
            };

            result
                .entry(streaming_job_database[&job_id])
                .or_default()
                .entry(job_id)
                .or_default()
                .insert(fragment_id, fragment);
        }
    }
    Ok(result)
}

pub async fn render_fragments<C>(
    txn: &C,
    fragment_ids: &[FragmentId],
    workers: BTreeMap<WorkerId, NonZeroUsize>,
) -> MetaResult<HashMap<DatabaseId, HashMap<TableId, HashMap<FragmentId, InflightFragmentInfo>>>>
where
    C: ConnectionTrait,
{
    let graphs = find_fragment_no_shuffle_dags_detailed(txn, fragment_ids).await?;

    let result = render_inner(txn, graphs, &workers).await?;

    Ok(result)
}

// Helper struct to make the function signature cleaner and to properly bundle the required data.
#[derive(Debug)]
pub struct ActorGraph<'a> {
    pub fragments: &'a HashMap<FragmentId, (Fragment, Vec<StreamActor>)>,
    pub locations: &'a HashMap<ActorId, WorkerId>,
}

struct NoShuffleEnsemble {
    entries: HashSet<FragmentId>,
    components: HashSet<FragmentId>,
}

async fn find_fragment_no_shuffle_dags_detailed(
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
    use std::collections::{HashMap, HashSet};

    use super::*;

    // Helper type aliases for cleaner test code
    type FragmentId = i32; // Assuming i32 based on previous context
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
}
// #[cfg(test)]
// mod tests {
//     use super::*;
//
//     #[test]
//     fn test_dag() {
//         let x = find_no_shuffle_graphs(
//             &[1, 2, 4, 6],
//             &HashMap::from([
//                 (1, vec![3, 8]),
//                 (2, vec![3]),
//                 (4, vec![3]),
//                 (3, vec![5]),
//                 (6, vec![7]),
//             ]),
//             &HashMap::from([(7, vec![6]), (8, vec![1]), (3, vec![1, 2, 4]), (5, vec![3])]),
//         )
//         .unwrap();
//
//         println!("{:?}", x);
//     }
// }

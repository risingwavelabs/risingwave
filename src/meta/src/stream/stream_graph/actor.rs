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

use std::collections::{BTreeMap, HashMap};
use std::num::NonZeroUsize;

use assert_matches::assert_matches;
use itertools::Itertools;
use risingwave_common::bail;
use risingwave_common::bitmap::Bitmap;
use risingwave_common::hash::{ActorAlignmentId, IsSingleton, VnodeCount, VnodeCountCompat};
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_common::util::stream_graph_visitor::visit_tables;
use risingwave_meta_model::WorkerId;
use risingwave_pb::plan_common::ExprContext;
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_plan::{
    DispatchStrategy, DispatcherType, MergeNode, StreamNode, StreamScanType,
};

use super::Locations;
use crate::MetaResult;
use crate::controller::cluster::StreamingClusterInfo;
use crate::manager::{MetaSrvEnv, StreamingJob};
use crate::model::{
    Fragment, FragmentDownstreamRelation, FragmentId, FragmentNewNoShuffle,
    FragmentReplaceUpstream, StreamActor,
};
use crate::stream::stream_graph::fragment::{
    CompleteStreamFragmentGraph, DownstreamExternalEdgeId, EdgeId, EitherFragment,
    StreamFragmentEdge,
};
use crate::stream::stream_graph::id::{GlobalActorId, GlobalActorIdGen, GlobalFragmentId};
use crate::stream::stream_graph::schedule;
use crate::stream::stream_graph::schedule::Distribution;

/// [`ActorBuilder`] builds a stream actor in a stream DAG.
#[derive(Debug)]
struct ActorBuilder {
    /// The ID of this actor.
    actor_id: GlobalActorId,

    /// The fragment ID of this actor.
    fragment_id: GlobalFragmentId,

    /// The virtual node bitmap, if this fragment is hash distributed.
    vnode_bitmap: Option<Bitmap>,
}

impl ActorBuilder {
    fn new(
        actor_id: GlobalActorId,
        fragment_id: GlobalFragmentId,
        vnode_bitmap: Option<Bitmap>,
    ) -> Self {
        Self {
            actor_id,
            fragment_id,
            vnode_bitmap,
        }
    }
}

impl FragmentActorBuilder {
    /// Rewrite the actor body.
    ///
    /// During this process, the following things will be done:
    /// 1. Replace the logical `Exchange` in node's input with `Merge`, which can be executed on the
    ///    compute nodes.
    fn rewrite(&self) -> MetaResult<StreamNode> {
        self.rewrite_inner(&self.node, 0)
    }

    fn rewrite_inner(&self, stream_node: &StreamNode, depth: usize) -> MetaResult<StreamNode> {
        match stream_node.get_node_body()? {
            // Leaf node `Exchange`.
            NodeBody::Exchange(exchange) => {
                // The exchange node should always be the bottom of the plan node. If we find one
                // when the depth is 0, it means that the plan node is not well-formed.
                if depth == 0 {
                    bail!(
                        "there should be no ExchangeNode on the top of the plan node: {:#?}",
                        stream_node
                    )
                }
                assert!(!stream_node.get_fields().is_empty());
                assert!(stream_node.input.is_empty());

                // Index the upstreams by the an internal edge ID.
                let (upstream_fragment_id, _) = &self.upstreams[&EdgeId::Internal {
                    link_id: stream_node.get_operator_id(),
                }];

                let upstream_fragment_id = upstream_fragment_id.as_global_id();

                Ok(StreamNode {
                    node_body: Some(NodeBody::Merge(Box::new({
                        #[expect(deprecated)]
                        MergeNode {
                            upstream_actor_id: vec![],
                            upstream_fragment_id,
                            upstream_dispatcher_type: exchange.get_strategy()?.r#type,
                            fields: stream_node.get_fields().clone(),
                        }
                    }))),
                    identity: "MergeExecutor".to_owned(),
                    ..stream_node.clone()
                })
            }

            // "Leaf" node `StreamScan`.
            NodeBody::StreamScan(stream_scan) => {
                let input = stream_node.get_input();
                if stream_scan.stream_scan_type() == StreamScanType::CrossDbSnapshotBackfill {
                    // CrossDbSnapshotBackfill is a special case, which doesn't have any upstream actor
                    // and always reads from log store.
                    assert!(input.is_empty());
                    return Ok(stream_node.clone());
                }
                assert_eq!(input.len(), 2);

                let merge_node = &input[0];
                assert_matches!(merge_node.node_body, Some(NodeBody::Merge(_)));
                let batch_plan_node = &input[1];
                assert_matches!(batch_plan_node.node_body, Some(NodeBody::BatchPlan(_)));

                // Index the upstreams by the an external edge ID.
                let (upstream_fragment_id, upstream_no_shuffle_actor) = &self.upstreams
                    [&EdgeId::UpstreamExternal {
                        upstream_table_id: stream_scan.table_id.into(),
                        downstream_fragment_id: self.fragment_id,
                    }];

                let is_shuffled_backfill = stream_scan.stream_scan_type
                    == StreamScanType::ArrangementBackfill as i32
                    || stream_scan.stream_scan_type == StreamScanType::SnapshotBackfill as i32;
                if !is_shuffled_backfill {
                    assert!(upstream_no_shuffle_actor.is_some());
                }

                let upstream_dispatcher_type = if is_shuffled_backfill {
                    // FIXME(kwannoel): Should the upstream dispatcher type depends on the upstream distribution?
                    // If singleton, use `Simple` dispatcher, otherwise use `Hash` dispatcher.
                    DispatcherType::Hash as _
                } else {
                    DispatcherType::NoShuffle as _
                };

                let upstream_fragment_id = upstream_fragment_id.as_global_id();

                let input = vec![
                    // Fill the merge node body with correct upstream info.
                    StreamNode {
                        node_body: Some(NodeBody::Merge(Box::new({
                            #[expect(deprecated)]
                            MergeNode {
                                upstream_actor_id: vec![],
                                upstream_fragment_id,
                                upstream_dispatcher_type,
                                fields: merge_node.fields.clone(),
                            }
                        }))),
                        ..merge_node.clone()
                    },
                    batch_plan_node.clone(),
                ];

                Ok(StreamNode {
                    input,
                    ..stream_node.clone()
                })
            }

            // "Leaf" node `CdcFilter` and `SourceBackfill`. They both `Merge` an upstream `Source`
            // cdc_filter -> backfill -> mview
            // source_backfill -> mview
            NodeBody::CdcFilter(_) | NodeBody::SourceBackfill(_) => {
                let input = stream_node.get_input();
                assert_eq!(input.len(), 1);

                let merge_node = &input[0];
                assert_matches!(merge_node.node_body, Some(NodeBody::Merge(_)));

                let upstream_source_id = match stream_node.get_node_body()? {
                    NodeBody::CdcFilter(node) => node.upstream_source_id,
                    NodeBody::SourceBackfill(node) => node.upstream_source_id,
                    _ => unreachable!(),
                };

                // Index the upstreams by the an external edge ID.
                let (upstream_fragment_id, upstream_actors) = &self.upstreams
                    [&EdgeId::UpstreamExternal {
                        upstream_table_id: upstream_source_id.into(),
                        downstream_fragment_id: self.fragment_id,
                    }];

                assert!(
                    upstream_actors.is_some(),
                    "Upstream Cdc Source should be singleton. \
                    SourceBackfill is NoShuffle 1-1 correspondence. \
                    So they both should have only one upstream actor."
                );

                let upstream_fragment_id = upstream_fragment_id.as_global_id();

                // rewrite the input
                let input = vec![
                    // Fill the merge node body with correct upstream info.
                    StreamNode {
                        node_body: Some(NodeBody::Merge(Box::new({
                            #[expect(deprecated)]
                            MergeNode {
                                upstream_actor_id: vec![],
                                upstream_fragment_id,
                                upstream_dispatcher_type: DispatcherType::NoShuffle as _,
                                fields: merge_node.fields.clone(),
                            }
                        }))),
                        ..merge_node.clone()
                    },
                ];
                Ok(StreamNode {
                    input,
                    ..stream_node.clone()
                })
            }

            // For other nodes, visit the children recursively.
            _ => {
                let mut new_stream_node = stream_node.clone();
                for (input, new_input) in stream_node
                    .input
                    .iter()
                    .zip_eq_fast(&mut new_stream_node.input)
                {
                    *new_input = self.rewrite_inner(input, depth + 1)?;
                }
                Ok(new_stream_node)
            }
        }
    }
}

impl ActorBuilder {
    /// Build an actor after all the upstreams and downstreams are processed.
    fn build(self, job: &StreamingJob, expr_context: ExprContext) -> MetaResult<StreamActor> {
        // Only fill the definition when debug assertions enabled, otherwise use name instead.
        #[cfg(not(debug_assertions))]
        let mview_definition = job.name();
        #[cfg(debug_assertions)]
        let mview_definition = job.definition();

        Ok(StreamActor {
            actor_id: self.actor_id.as_global_id(),
            fragment_id: self.fragment_id.as_global_id(),
            vnode_bitmap: self.vnode_bitmap,
            mview_definition,
            expr_context: Some(expr_context),
        })
    }
}

/// The required changes to an existing external actor to build the graph of a streaming job.
///
/// For example, when we're creating an mview on an existing mview, we need to add new downstreams
/// to the upstream actors, by adding new dispatchers.
#[derive(Default)]
struct UpstreamFragmentChange {
    /// The new downstreams to be added.
    new_downstreams: HashMap<GlobalFragmentId, DispatchStrategy>,
}

#[derive(Default)]
struct DownstreamFragmentChange {
    /// The new upstreams to be added (replaced), indexed by the edge id to upstream fragment.
    /// `edge_id` -> new upstream fragment id
    new_upstreams:
        HashMap<DownstreamExternalEdgeId, (GlobalFragmentId, Option<NewExternalNoShuffle>)>,
}

impl UpstreamFragmentChange {
    /// Add a dispatcher to the external actor.
    fn add_dispatcher(
        &mut self,
        downstream_fragment_id: GlobalFragmentId,
        dispatch: DispatchStrategy,
    ) {
        self.new_downstreams
            .try_insert(downstream_fragment_id, dispatch)
            .unwrap();
    }
}

impl DownstreamFragmentChange {
    /// Add an upstream to the external actor.
    fn add_upstream(
        &mut self,
        edge_id: DownstreamExternalEdgeId,
        new_upstream_fragment_id: GlobalFragmentId,
        no_shuffle_actor_mapping: Option<HashMap<GlobalActorId, GlobalActorId>>,
    ) {
        self.new_upstreams
            .try_insert(
                edge_id,
                (new_upstream_fragment_id, no_shuffle_actor_mapping),
            )
            .unwrap();
    }
}

/// The location of actors.
type ActorLocations = BTreeMap<GlobalActorId, ActorAlignmentId>;
// no_shuffle upstream actor_id -> actor_id
type NewExternalNoShuffle = HashMap<GlobalActorId, GlobalActorId>;

#[derive(Debug)]
struct FragmentActorBuilder {
    fragment_id: GlobalFragmentId,
    node: StreamNode,
    actor_builders: BTreeMap<GlobalActorId, ActorBuilder>,
    downstreams: HashMap<GlobalFragmentId, DispatchStrategy>,
    // edge_id -> (upstream fragment_id, no shuffle actor pairs if it's no shuffle dispatched)
    upstreams: HashMap<EdgeId, (GlobalFragmentId, Option<NewExternalNoShuffle>)>,
}

impl FragmentActorBuilder {
    fn new(fragment_id: GlobalFragmentId, node: StreamNode) -> Self {
        Self {
            fragment_id,
            node,
            actor_builders: Default::default(),
            downstreams: Default::default(),
            upstreams: Default::default(),
        }
    }
}

/// The actual mutable state of building an actor graph.
///
/// When the fragments are visited in a topological order, actor builders will be added to this
/// state and the scheduled locations will be added. As the building process is run on the
/// **complete graph** which also contains the info of the existing (external) fragments, the info
/// of them will be also recorded.
#[derive(Default)]
struct ActorGraphBuildStateInner {
    /// The builders of the actors to be built.
    fragment_actor_builders: BTreeMap<GlobalFragmentId, FragmentActorBuilder>,

    /// The scheduled locations of the actors to be built.
    building_locations: ActorLocations,

    /// The required changes to the external downstream fragment. See [`DownstreamFragmentChange`].
    /// Indexed by the `fragment_id` of fragments that have updates on its downstream.
    downstream_fragment_changes: BTreeMap<GlobalFragmentId, DownstreamFragmentChange>,

    /// The required changes to the external upstream fragment. See [`UpstreamFragmentChange`].
    /// /// Indexed by the `fragment_id` of fragments that have updates on its upstream.
    upstream_fragment_changes: BTreeMap<GlobalFragmentId, UpstreamFragmentChange>,

    /// The actual locations of the external actors.
    external_locations: ActorLocations,
}

/// The information of a fragment, used for parameter passing for `Inner::add_link`.
struct FragmentLinkNode<'a> {
    fragment_id: GlobalFragmentId,
    actor_ids: &'a [GlobalActorId],
}

impl ActorGraphBuildStateInner {
    /// Insert new generated actor and record its location.
    ///
    /// The `vnode_bitmap` should be `Some` for the actors of hash-distributed fragments.
    fn add_actor(
        &mut self,
        (fragment_id, actor_id): (GlobalFragmentId, GlobalActorId),
        actor_alignment_id: ActorAlignmentId,
        vnode_bitmap: Option<Bitmap>,
    ) {
        self.fragment_actor_builders
            .get_mut(&fragment_id)
            .expect("should added previously")
            .actor_builders
            .try_insert(
                actor_id,
                ActorBuilder::new(actor_id, fragment_id, vnode_bitmap),
            )
            .unwrap();

        self.building_locations
            .try_insert(actor_id, actor_alignment_id)
            .unwrap();
    }

    /// Record the location of an external actor.
    fn record_external_location(
        &mut self,
        actor_id: GlobalActorId,
        actor_alignment_id: ActorAlignmentId,
    ) {
        self.external_locations
            .try_insert(actor_id, actor_alignment_id)
            .unwrap();
    }

    /// Add the new downstream fragment relation to a fragment.
    ///
    /// - If the fragment is to be built, the fragment relation will be added to the fragment actor builder.
    /// - If the fragment is an external existing fragment, the fragment relation will be added to the external changes.
    fn add_dispatcher(
        &mut self,
        fragment_id: GlobalFragmentId,
        downstream_fragment_id: GlobalFragmentId,
        dispatch: DispatchStrategy,
    ) {
        if let Some(builder) = self.fragment_actor_builders.get_mut(&fragment_id) {
            builder
                .downstreams
                .try_insert(downstream_fragment_id, dispatch)
                .unwrap();
        } else {
            self.upstream_fragment_changes
                .entry(fragment_id)
                .or_default()
                .add_dispatcher(downstream_fragment_id, dispatch);
        }
    }

    /// Add the new upstream for an actor.
    ///
    /// - If the actor is to be built, the upstream will be added to the actor builder.
    /// - If the actor is an external actor, the upstream will be added to the external changes.
    fn add_upstream(
        &mut self,
        fragment_id: GlobalFragmentId,
        edge_id: EdgeId,
        upstream_fragment_id: GlobalFragmentId,
        no_shuffle_actor_mapping: Option<HashMap<GlobalActorId, GlobalActorId>>,
    ) {
        if let Some(builder) = self.fragment_actor_builders.get_mut(&fragment_id) {
            builder
                .upstreams
                .try_insert(edge_id, (upstream_fragment_id, no_shuffle_actor_mapping))
                .unwrap();
        } else {
            let EdgeId::DownstreamExternal(edge_id) = edge_id else {
                unreachable!("edge from internal to external must be `DownstreamExternal`")
            };
            self.downstream_fragment_changes
                .entry(fragment_id)
                .or_default()
                .add_upstream(edge_id, upstream_fragment_id, no_shuffle_actor_mapping);
        }
    }

    /// Get the location of an actor. Will look up the location map of both the actors to be built
    /// and the external actors.
    fn get_location(&self, actor_id: GlobalActorId) -> ActorAlignmentId {
        self.building_locations
            .get(&actor_id)
            .copied()
            .or_else(|| self.external_locations.get(&actor_id).copied())
            .unwrap()
    }

    /// Add a "link" between two fragments in the graph.
    ///
    /// The `edge` will be transformed into the fragment relation (downstream - upstream) pair between two fragments,
    /// based on the distribution and the dispatch strategy. They will be
    /// finally transformed to `Dispatcher` and `Merge` nodes when building the actors.
    ///
    /// If there're existing (external) fragments, the info will be recorded in `upstream_fragment_changes` and `downstream_fragment_changes`,
    /// instead of the actor builders.
    fn add_link<'a>(
        &mut self,
        upstream: FragmentLinkNode<'a>,
        downstream: FragmentLinkNode<'a>,
        edge: &'a StreamFragmentEdge,
    ) {
        let dt = edge.dispatch_strategy.r#type();

        match dt {
            // For `NoShuffle`, make n "1-1" links between the actors.
            DispatcherType::NoShuffle => {
                assert_eq!(upstream.actor_ids.len(), downstream.actor_ids.len());
                let upstream_locations: HashMap<_, _> = upstream
                    .actor_ids
                    .iter()
                    .map(|id| (self.get_location(*id), *id))
                    .collect();
                let downstream_locations: HashMap<_, _> = downstream
                    .actor_ids
                    .iter()
                    .map(|id| (self.get_location(*id), *id))
                    .collect();

                // Create a new dispatcher just between these two actors.
                self.add_dispatcher(
                    upstream.fragment_id,
                    downstream.fragment_id,
                    edge.dispatch_strategy.clone(),
                );

                // Also record the upstream for the downstream actor.
                self.add_upstream(
                    downstream.fragment_id,
                    edge.id,
                    upstream.fragment_id,
                    Some(
                        downstream_locations
                            .iter()
                            .map(|(location, downstream_actor_id)| {
                                let upstream_actor_id = upstream_locations.get(location).unwrap();
                                (*upstream_actor_id, *downstream_actor_id)
                            })
                            .collect(),
                    ),
                );
            }

            // Otherwise, make m * n links between the actors.
            DispatcherType::Hash | DispatcherType::Broadcast | DispatcherType::Simple => {
                self.add_dispatcher(
                    upstream.fragment_id,
                    downstream.fragment_id,
                    edge.dispatch_strategy.clone(),
                );
                self.add_upstream(downstream.fragment_id, edge.id, upstream.fragment_id, None);
            }

            DispatcherType::Unspecified => unreachable!(),
        }
    }
}

/// The mutable state of building an actor graph. See [`ActorGraphBuildStateInner`].
struct ActorGraphBuildState {
    /// The actual state.
    inner: ActorGraphBuildStateInner,

    /// The actor IDs of each fragment.
    fragment_actors: HashMap<GlobalFragmentId, Vec<GlobalActorId>>,

    /// The next local actor id to use.
    next_local_id: u32,

    /// The global actor id generator.
    actor_id_gen: GlobalActorIdGen,
}

impl ActorGraphBuildState {
    /// Create an empty state with the given id generator.
    fn new(actor_id_gen: GlobalActorIdGen) -> Self {
        Self {
            inner: Default::default(),
            fragment_actors: Default::default(),
            next_local_id: 0,
            actor_id_gen,
        }
    }

    /// Get the next global actor id.
    fn next_actor_id(&mut self) -> GlobalActorId {
        let local_id = self.next_local_id;
        self.next_local_id += 1;

        self.actor_id_gen.to_global_id(local_id)
    }

    /// Finish the build and return the inner state.
    fn finish(self) -> ActorGraphBuildStateInner {
        // Assert that all the actors are built.
        assert_eq!(self.actor_id_gen.len(), self.next_local_id);

        self.inner
    }
}

/// The result of a built actor graph. Will be further embedded into the `Context` for building
/// actors on the compute nodes.
pub struct ActorGraphBuildResult {
    /// The graph of sealed fragments, including all actors.
    pub graph: BTreeMap<FragmentId, Fragment>,
    /// The downstream fragments of the fragments from the new graph to be created.
    pub downstream_fragment_relations: FragmentDownstreamRelation,

    /// The scheduled locations of the actors to be built.
    pub building_locations: Locations,

    /// The actual locations of the external actors.
    pub existing_locations: Locations,

    /// The new dispatchers to be added to the upstream mview actors. Used for MV on MV.
    pub upstream_fragment_downstreams: FragmentDownstreamRelation,

    /// The updates to be applied to the downstream fragment merge node. Used for schema change (replace
    /// table plan).
    pub replace_upstream: FragmentReplaceUpstream,

    /// The new no shuffle added to create the new streaming job, including the no shuffle from existing fragments to
    /// the newly created fragments, between two newly created fragments, and from newly created fragments to existing
    /// downstream fragments (for create sink into table and replace table).
    pub new_no_shuffle: FragmentNewNoShuffle,
}

/// [`ActorGraphBuilder`] builds the actor graph for the given complete fragment graph, based on the
/// current cluster info and the required parallelism.
#[derive(Debug)]
pub struct ActorGraphBuilder {
    /// The pre-scheduled distribution for each building fragment.
    distributions: HashMap<GlobalFragmentId, Distribution>,

    /// The actual distribution for each existing fragment.
    existing_distributions: HashMap<GlobalFragmentId, Distribution>,

    /// The complete fragment graph.
    fragment_graph: CompleteStreamFragmentGraph,

    /// The cluster info for creating a streaming job.
    cluster_info: StreamingClusterInfo,
}

impl ActorGraphBuilder {
    /// Create a new actor graph builder with the given "complete" graph. Returns an error if the
    /// graph is failed to be scheduled.
    pub fn new(
        streaming_job_id: u32,
        resource_group: String,
        fragment_graph: CompleteStreamFragmentGraph,
        cluster_info: StreamingClusterInfo,
        default_parallelism: NonZeroUsize,
    ) -> MetaResult<Self> {
        let expected_vnode_count = fragment_graph.max_parallelism();
        let existing_distributions = fragment_graph.existing_distribution();

        let schedulable_workers =
            cluster_info.filter_schedulable_workers_by_resource_group(&resource_group);

        let scheduler = schedule::Scheduler::new(
            streaming_job_id,
            &schedulable_workers,
            default_parallelism,
            expected_vnode_count,
        )?;

        let distributions = scheduler.schedule(&fragment_graph)?;

        // Fill the vnode count for each internal table, based on schedule result.
        let mut fragment_graph = fragment_graph;
        for (id, fragment) in fragment_graph.building_fragments_mut() {
            let mut error = None;
            let fragment_vnode_count = distributions[id].vnode_count();
            visit_tables(fragment, |table, _| {
                if error.is_some() {
                    return;
                }
                // There are special cases where a hash-distributed fragment contains singleton
                // internal tables, e.g., the state table of `Source` executors.
                let vnode_count = if table.is_singleton() {
                    if fragment_vnode_count > 1 {
                        tracing::info!(
                            table.name,
                            "found singleton table in hash-distributed fragment"
                        );
                    }
                    1
                } else {
                    fragment_vnode_count
                };
                match table.vnode_count_inner().value_opt() {
                    // Vnode count of this table is not set to placeholder, meaning that we are replacing
                    // a streaming job, and the existing state table requires a specific vnode count.
                    // Check if it's the same with what we derived from the schedule result.
                    //
                    // Typically, inconsistency should not happen as we force to align the vnode count
                    // when planning the new streaming job in the frontend.
                    Some(required_vnode_count) if required_vnode_count != vnode_count => {
                        error = Some(format!(
                            "failed to align vnode count for table {}({}): required {}, but got {}",
                            table.id, table.name, required_vnode_count, vnode_count
                        ));
                    }
                    // Normal cases.
                    _ => table.maybe_vnode_count = VnodeCount::set(vnode_count).to_protobuf(),
                }
            });
            if let Some(error) = error {
                bail!(error);
            }
        }

        Ok(Self {
            distributions,
            existing_distributions,
            fragment_graph,
            cluster_info,
        })
    }

    /// Get the distribution of the given fragment. Will look up the distribution map of both the
    /// building and existing fragments.
    fn get_distribution(&self, fragment_id: GlobalFragmentId) -> &Distribution {
        self.distributions
            .get(&fragment_id)
            .or_else(|| self.existing_distributions.get(&fragment_id))
            .unwrap()
    }

    /// Convert the actor location map to the [`Locations`] struct.
    fn build_locations(&self, actor_locations: ActorLocations) -> Locations {
        let actor_locations = actor_locations
            .into_iter()
            .map(|(id, alignment_id)| (id.as_global_id(), alignment_id))
            .collect();

        let worker_locations = self
            .cluster_info
            .worker_nodes
            .iter()
            .map(|(id, node)| (*id as WorkerId, node.clone()))
            .collect();

        Locations {
            actor_locations,
            worker_locations,
        }
    }

    /// Build a stream graph by duplicating each fragment as parallel actors. Returns
    /// [`ActorGraphBuildResult`] that will be further used to build actors on the compute nodes.
    pub fn generate_graph(
        self,
        env: &MetaSrvEnv,
        job: &StreamingJob,
        expr_context: ExprContext,
    ) -> MetaResult<ActorGraphBuildResult> {
        // Pre-generate IDs for all actors.
        let actor_len = self
            .distributions
            .values()
            .map(|d| d.parallelism())
            .sum::<usize>() as u64;
        let id_gen = GlobalActorIdGen::new(env.id_gen_manager(), actor_len);

        // Build the actor graph and get the final state.
        let ActorGraphBuildStateInner {
            fragment_actor_builders,
            building_locations,
            downstream_fragment_changes,
            upstream_fragment_changes,
            external_locations,
        } = self.build_actor_graph(id_gen)?;

        for alignment_id in external_locations.values() {
            if self
                .cluster_info
                .unschedulable_workers
                .contains(&alignment_id.worker_id())
            {
                bail!(
                    "The worker {} where the associated upstream is located is unschedulable",
                    alignment_id.worker_id(),
                );
            }
        }

        let mut downstream_fragment_relations: FragmentDownstreamRelation = HashMap::new();
        let mut new_no_shuffle: FragmentNewNoShuffle = HashMap::new();
        // Serialize the graph into a map of sealed fragments.
        let graph = {
            let mut fragment_actors: HashMap<GlobalFragmentId, (StreamNode, Vec<StreamActor>)> =
                HashMap::new();

            // As all fragments are processed, we can now `build` the actors where the `Exchange`
            // and `Chain` are rewritten.
            for (fragment_id, builder) in fragment_actor_builders {
                let global_fragment_id = fragment_id.as_global_id();
                let node = builder.rewrite()?;
                for (upstream_fragment_id, no_shuffle_upstream) in builder.upstreams.into_values() {
                    if let Some(no_shuffle_upstream) = no_shuffle_upstream {
                        new_no_shuffle
                            .entry(upstream_fragment_id.as_global_id())
                            .or_default()
                            .try_insert(
                                global_fragment_id,
                                no_shuffle_upstream
                                    .iter()
                                    .map(|(upstream_actor_id, actor_id)| {
                                        (upstream_actor_id.as_global_id(), actor_id.as_global_id())
                                    })
                                    .collect(),
                            )
                            .expect("non-duplicate");
                    }
                }
                downstream_fragment_relations
                    .try_insert(
                        global_fragment_id,
                        builder
                            .downstreams
                            .into_iter()
                            .map(|(id, dispatch)| (id.as_global_id(), dispatch).into())
                            .collect(),
                    )
                    .expect("non-duplicate");
                fragment_actors
                    .try_insert(
                        fragment_id,
                        (
                            node,
                            builder
                                .actor_builders
                                .into_values()
                                .map(|builder| builder.build(job, expr_context.clone()))
                                .try_collect()?,
                        ),
                    )
                    .expect("non-duplicate");
            }

            {
                fragment_actors
                    .into_iter()
                    .map(|(fragment_id, (stream_node, actors))| {
                        let distribution = self.distributions[&fragment_id].clone();
                        let fragment = self.fragment_graph.seal_fragment(
                            fragment_id,
                            actors,
                            distribution,
                            stream_node,
                        );
                        let fragment_id = fragment_id.as_global_id();
                        (fragment_id, fragment)
                    })
                    .collect()
            }
        };

        // Convert the actor location map to the `Locations` struct.
        let building_locations = self.build_locations(building_locations);
        let existing_locations = self.build_locations(external_locations);

        // Extract the new fragment relation from the external changes.
        let upstream_fragment_downstreams = upstream_fragment_changes
            .into_iter()
            .map(|(fragment_id, changes)| {
                (
                    fragment_id.as_global_id(),
                    changes
                        .new_downstreams
                        .into_iter()
                        .map(|(downstream_fragment_id, new_dispatch)| {
                            (downstream_fragment_id.as_global_id(), new_dispatch).into()
                        })
                        .collect(),
                )
            })
            .collect();

        // Extract the updates for merge executors from the external changes.
        let replace_upstream = downstream_fragment_changes
            .into_iter()
            .map(|(fragment_id, changes)| {
                let fragment_id = fragment_id.as_global_id();
                let new_no_shuffle = &mut new_no_shuffle;
                (
                    fragment_id,
                    changes
                        .new_upstreams
                        .into_iter()
                        .map(
                            move |(edge_id, (upstream_fragment_id, upstream_new_no_shuffle))| {
                                let upstream_fragment_id = upstream_fragment_id.as_global_id();
                                if let Some(upstream_new_no_shuffle) = upstream_new_no_shuffle
                                    && !upstream_new_no_shuffle.is_empty()
                                {
                                    let no_shuffle_actors = new_no_shuffle
                                        .entry(upstream_fragment_id)
                                        .or_default()
                                        .entry(fragment_id)
                                        .or_default();
                                    no_shuffle_actors.extend(
                                        upstream_new_no_shuffle.into_iter().map(
                                            |(upstream_actor_id, actor_id)| {
                                                (
                                                    upstream_actor_id.as_global_id(),
                                                    actor_id.as_global_id(),
                                                )
                                            },
                                        ),
                                    );
                                }
                                let DownstreamExternalEdgeId {
                                    original_upstream_fragment_id,
                                    ..
                                } = edge_id;
                                (
                                    original_upstream_fragment_id.as_global_id(),
                                    upstream_fragment_id,
                                )
                            },
                        )
                        .collect(),
                )
            })
            .filter(|(_, fragment_changes): &(_, HashMap<_, _>)| !fragment_changes.is_empty())
            .collect();

        Ok(ActorGraphBuildResult {
            graph,
            downstream_fragment_relations,
            building_locations,
            existing_locations,
            upstream_fragment_downstreams,
            replace_upstream,
            new_no_shuffle,
        })
    }

    /// Build actor graph for each fragment, using topological order.
    fn build_actor_graph(&self, id_gen: GlobalActorIdGen) -> MetaResult<ActorGraphBuildStateInner> {
        let mut state = ActorGraphBuildState::new(id_gen);

        // Use topological sort to build the graph from downstream to upstream. (The first fragment
        // popped out from the heap will be the top-most node in plan, or the sink in stream graph.)
        for fragment_id in self.fragment_graph.topo_order()? {
            self.build_actor_graph_fragment(fragment_id, &mut state)?;
        }

        Ok(state.finish())
    }

    /// Build actor graph for a specific fragment.
    fn build_actor_graph_fragment(
        &self,
        fragment_id: GlobalFragmentId,
        state: &mut ActorGraphBuildState,
    ) -> MetaResult<()> {
        let current_fragment = self.fragment_graph.get_fragment(fragment_id);
        let distribution = self.get_distribution(fragment_id);

        // First, add or record the actors for the current fragment into the state.
        let actor_ids = match current_fragment {
            // For building fragments, we need to generate the actor builders.
            EitherFragment::Building(current_fragment) => {
                let node = current_fragment.node.clone().unwrap();
                state
                    .inner
                    .fragment_actor_builders
                    .try_insert(fragment_id, FragmentActorBuilder::new(fragment_id, node))
                    .expect("non-duplicate");
                let bitmaps = distribution.as_hash().map(|m| m.to_bitmaps());

                distribution
                    .actors()
                    .map(|alignment_id| {
                        let actor_id = state.next_actor_id();
                        let vnode_bitmap = bitmaps
                            .as_ref()
                            .map(|m: &HashMap<ActorAlignmentId, Bitmap>| &m[&alignment_id])
                            .cloned();

                        state
                            .inner
                            .add_actor((fragment_id, actor_id), alignment_id, vnode_bitmap);

                        actor_id
                    })
                    .collect_vec()
            }

            // For existing fragments, we only need to record the actor locations.
            EitherFragment::Existing(existing_fragment) => existing_fragment
                .actors
                .iter()
                .map(|a| {
                    let actor_id = GlobalActorId::new(a.actor_id);
                    let alignment_id = match &distribution {
                        Distribution::Singleton(worker_id) => {
                            ActorAlignmentId::new_single(*worker_id as u32)
                        }
                        Distribution::Hash(mapping) => mapping
                            .get_matched(a.vnode_bitmap.as_ref().unwrap())
                            .unwrap(),
                    };

                    state.inner.record_external_location(actor_id, alignment_id);

                    actor_id
                })
                .collect_vec(),
        };

        // Then, add links between the current fragment and its downstream fragments.
        for (downstream_fragment_id, edge) in self.fragment_graph.get_downstreams(fragment_id) {
            let downstream_actors = state
                .fragment_actors
                .get(&downstream_fragment_id)
                .expect("downstream fragment not processed yet");

            state.inner.add_link(
                FragmentLinkNode {
                    fragment_id,
                    actor_ids: &actor_ids,
                },
                FragmentLinkNode {
                    fragment_id: downstream_fragment_id,
                    actor_ids: downstream_actors,
                },
                edge,
            );
        }

        // Finally, record the actor IDs for the current fragment.
        state
            .fragment_actors
            .try_insert(fragment_id, actor_ids)
            .unwrap_or_else(|_| panic!("fragment {:?} is already processed", fragment_id));

        Ok(())
    }
}

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

use std::collections::{BTreeMap, HashMap};
use std::num::NonZeroUsize;

use assert_matches::assert_matches;
use risingwave_common::bail;
use risingwave_common::hash::{IsSingleton, VnodeCount, VnodeCountCompat};
use risingwave_common::id::JobId;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_common::util::stream_graph_visitor::visit_tables;
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_plan::{
    DispatchStrategy, DispatcherType, MergeNode, StreamNode, StreamScanType,
};

use crate::MetaResult;
use crate::model::{
    Fragment, FragmentDownstreamRelation, FragmentId, FragmentNewNoShuffle, FragmentReplaceUpstream,
};
use crate::stream::stream_graph::fragment::{
    CompleteStreamFragmentGraph, DownstreamExternalEdgeId, EdgeId, EitherFragment,
    StreamFragmentEdge,
};
use crate::stream::stream_graph::id::GlobalFragmentId;
use crate::stream::stream_graph::schedule;
use crate::stream::stream_graph::schedule::Distribution;

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
                    link_id: stream_node.get_operator_id().as_raw_id(),
                }];

                let upstream_fragment_id = upstream_fragment_id.as_global_id();

                Ok(StreamNode {
                    node_body: Some(NodeBody::Merge(Box::new({
                        MergeNode {
                            upstream_fragment_id,
                            upstream_dispatcher_type: exchange.get_strategy()?.r#type,
                            ..Default::default()
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
                        upstream_job_id: stream_scan.table_id.as_job_id(),
                        downstream_fragment_id: self.fragment_id,
                    }];

                let is_shuffled_backfill = stream_scan.stream_scan_type
                    == StreamScanType::ArrangementBackfill as i32
                    || stream_scan.stream_scan_type == StreamScanType::SnapshotBackfill as i32;
                if !is_shuffled_backfill {
                    assert!(*upstream_no_shuffle_actor);
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
                            MergeNode {
                                upstream_fragment_id,
                                upstream_dispatcher_type,
                                ..Default::default()
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
                let (upstream_fragment_id, upstream_is_no_shuffle) = &self.upstreams
                    [&EdgeId::UpstreamExternal {
                        upstream_job_id: upstream_source_id.as_share_source_job_id(),
                        downstream_fragment_id: self.fragment_id,
                    }];

                assert!(
                    *upstream_is_no_shuffle,
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
                            MergeNode {
                                upstream_fragment_id,
                                upstream_dispatcher_type: DispatcherType::NoShuffle as _,
                                ..Default::default()
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
    /// `edge_id` -> (new upstream fragment id, whether the edge is no shuffle)
    new_upstreams: HashMap<DownstreamExternalEdgeId, (GlobalFragmentId, bool)>,
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
        is_no_shuffle: bool,
    ) {
        self.new_upstreams
            .try_insert(edge_id, (new_upstream_fragment_id, is_no_shuffle))
            .unwrap();
    }
}

#[derive(Debug)]
struct FragmentActorBuilder {
    fragment_id: GlobalFragmentId,
    node: StreamNode,
    downstreams: HashMap<GlobalFragmentId, DispatchStrategy>,
    // edge_id -> (upstream fragment_id, whether the edge is no shuffle)
    upstreams: HashMap<EdgeId, (GlobalFragmentId, bool)>,
}

impl FragmentActorBuilder {
    fn new(fragment_id: GlobalFragmentId, node: StreamNode) -> Self {
        Self {
            fragment_id,
            node,
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

    /// The required changes to the external downstream fragment. See [`DownstreamFragmentChange`].
    /// Indexed by the `fragment_id` of fragments that have updates on its downstream.
    downstream_fragment_changes: BTreeMap<GlobalFragmentId, DownstreamFragmentChange>,

    /// The required changes to the external upstream fragment. See [`UpstreamFragmentChange`].
    /// /// Indexed by the `fragment_id` of fragments that have updates on its upstream.
    upstream_fragment_changes: BTreeMap<GlobalFragmentId, UpstreamFragmentChange>,
}

/// The information of a fragment, used for parameter passing for `Inner::add_link`.
struct FragmentLinkNode {
    fragment_id: GlobalFragmentId,
}

impl ActorGraphBuildStateInner {
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
        is_no_shuffle: bool,
    ) {
        if let Some(builder) = self.fragment_actor_builders.get_mut(&fragment_id) {
            builder
                .upstreams
                .try_insert(edge_id, (upstream_fragment_id, is_no_shuffle))
                .unwrap();
        } else {
            let EdgeId::DownstreamExternal(edge_id) = edge_id else {
                unreachable!("edge from internal to external must be `DownstreamExternal`")
            };
            self.downstream_fragment_changes
                .entry(fragment_id)
                .or_default()
                .add_upstream(edge_id, upstream_fragment_id, is_no_shuffle);
        }
    }

    /// Add a "link" between two fragments in the graph.
    ///
    /// The `edge` will be transformed into the fragment relation (downstream - upstream) pair between two fragments,
    /// based on the distribution and the dispatch strategy. They will be
    /// finally transformed to `Dispatcher` and `Merge` nodes when building the actors.
    ///
    /// If there're existing (external) fragments, the info will be recorded in `upstream_fragment_changes` and `downstream_fragment_changes`,
    /// instead of the actor builders.
    fn add_link(
        &mut self,
        upstream: FragmentLinkNode,
        downstream: FragmentLinkNode,
        edge: &StreamFragmentEdge,
    ) {
        let dt = edge.dispatch_strategy.r#type();

        match dt {
            // For `NoShuffle`, make n "1-1" links between the actors.
            DispatcherType::NoShuffle => {
                // Create a new dispatcher just between these two actors.
                self.add_dispatcher(
                    upstream.fragment_id,
                    downstream.fragment_id,
                    edge.dispatch_strategy.clone(),
                );

                // Also record the upstream for the downstream actor.
                self.add_upstream(downstream.fragment_id, edge.id, upstream.fragment_id, true);
            }

            // Otherwise, make m * n links between the actors.
            DispatcherType::Hash | DispatcherType::Broadcast | DispatcherType::Simple => {
                self.add_dispatcher(
                    upstream.fragment_id,
                    downstream.fragment_id,
                    edge.dispatch_strategy.clone(),
                );
                self.add_upstream(downstream.fragment_id, edge.id, upstream.fragment_id, false);
            }

            DispatcherType::Unspecified => unreachable!(),
        }
    }
}

/// The mutable state of building an actor graph. See [`ActorGraphBuildStateInner`].
struct ActorGraphBuildState {
    /// The actual state.
    inner: ActorGraphBuildStateInner,
}

impl ActorGraphBuildState {
    /// Create an empty state with the given id generator.
    fn new() -> Self {
        Self {
            inner: Default::default(),
        }
    }

    /// Finish the build and return the inner state.
    fn finish(self) -> ActorGraphBuildStateInner {
        self.inner
    }
}

/// The result of a built actor graph. Will be further embedded into the `Context` for building
/// actors on the compute nodes.
pub struct ActorGraphBuildResult {
    /// The graph of sealed fragments, including all actors.
    pub graph: BTreeMap<FragmentId, Fragment>,
    /// The downstream fragments of the fragments from the new graph to be created.
    /// Including the fragment relation to external downstream fragment.
    pub downstream_fragment_relations: FragmentDownstreamRelation,

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

    /// The complete fragment graph.
    fragment_graph: CompleteStreamFragmentGraph,
}

impl ActorGraphBuilder {
    /// Create a new actor graph builder with the given "complete" graph. Returns an error if the
    /// graph is failed to be scheduled.
    pub fn new(
        streaming_job_id: JobId,
        fragment_graph: CompleteStreamFragmentGraph,
        default_parallelism: NonZeroUsize,
    ) -> MetaResult<Self> {
        let expected_vnode_count = fragment_graph.max_parallelism();

        let scheduler =
            schedule::Scheduler::new(streaming_job_id, default_parallelism, expected_vnode_count)?;

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
            fragment_graph,
        })
    }

    /// Build a stream graph by duplicating each fragment as parallel actors. Returns
    /// [`ActorGraphBuildResult`] that will be further used to build actors on the compute nodes.
    pub fn generate_graph(self) -> MetaResult<ActorGraphBuildResult> {
        // Build the actor graph and get the final state.
        let ActorGraphBuildStateInner {
            fragment_actor_builders,
            downstream_fragment_changes,
            upstream_fragment_changes,
        } = self.build_actor_graph()?;

        let mut downstream_fragment_relations: FragmentDownstreamRelation = HashMap::new();
        let mut new_no_shuffle: FragmentNewNoShuffle = HashMap::new();
        // Serialize the graph into sealed fragments.
        let graph = {
            // As all fragments are processed, we can now `rewrite` the stream nodes where the
            // `Exchange` and `Chain` are rewritten.
            let mut fragment_nodes: HashMap<GlobalFragmentId, StreamNode> = HashMap::new();

            for (fragment_id, builder) in fragment_actor_builders {
                let global_fragment_id = fragment_id.as_global_id();
                let node = builder.rewrite()?;
                for (upstream_fragment_id, is_no_shuffle) in builder.upstreams.into_values() {
                    if is_no_shuffle {
                        new_no_shuffle
                            .entry(upstream_fragment_id.as_global_id())
                            .or_default()
                            .insert(global_fragment_id);
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
                fragment_nodes
                    .try_insert(fragment_id, node)
                    .expect("non-duplicate");
            }

            let mut graph = BTreeMap::new();
            for (fragment_id, stream_node) in fragment_nodes {
                let distribution = self.distributions[&fragment_id].clone();
                let fragment =
                    self.fragment_graph
                        .seal_fragment(fragment_id, distribution, stream_node);
                let fragment_id = fragment_id.as_global_id();
                graph.insert(fragment_id, fragment);
            }
            graph
        };

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
                        .map(move |(edge_id, (upstream_fragment_id, is_no_shuffle))| {
                            let upstream_fragment_id = upstream_fragment_id.as_global_id();
                            if is_no_shuffle {
                                new_no_shuffle
                                    .entry(upstream_fragment_id)
                                    .or_default()
                                    .insert(fragment_id);
                            }
                            let DownstreamExternalEdgeId {
                                original_upstream_fragment_id,
                                ..
                            } = edge_id;
                            (
                                original_upstream_fragment_id.as_global_id(),
                                upstream_fragment_id,
                            )
                        })
                        .collect(),
                )
            })
            .filter(|(_, fragment_changes): &(_, HashMap<_, _>)| !fragment_changes.is_empty())
            .collect();

        Ok(ActorGraphBuildResult {
            graph,
            downstream_fragment_relations,
            upstream_fragment_downstreams,
            replace_upstream,
            new_no_shuffle,
        })
    }

    /// Build actor graph for each fragment, using topological order.
    fn build_actor_graph(&self) -> MetaResult<ActorGraphBuildStateInner> {
        let mut state = ActorGraphBuildState::new();

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

        // First, add or record the actors for the current fragment into the state.
        match current_fragment {
            // For building fragments, we need to generate the actor builders.
            EitherFragment::Building(current_fragment) => {
                let node = current_fragment.node.clone().unwrap();
                state
                    .inner
                    .fragment_actor_builders
                    .try_insert(fragment_id, FragmentActorBuilder::new(fragment_id, node))
                    .expect("non-duplicate");
            }

            // For existing fragments, we only need to record the actor locations.
            EitherFragment::Existing => {}
        };

        // Then, add links between the current fragment and its downstream fragments.
        for (downstream_fragment_id, edge) in self.fragment_graph.get_downstreams(fragment_id) {
            state.inner.add_link(
                FragmentLinkNode { fragment_id },
                FragmentLinkNode {
                    fragment_id: downstream_fragment_id,
                },
                edge,
            );
        }

        Ok(())
    }
}

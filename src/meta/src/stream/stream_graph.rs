// Copyright 2023 Singularity Data
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

use std::collections::hash_map::HashMap;
use std::collections::{BTreeMap, HashSet, VecDeque};
use std::ops::{Deref, Range};
use std::sync::{Arc, LazyLock};

use assert_matches::assert_matches;
use itertools::Itertools;
use risingwave_common::bail;
use risingwave_common::catalog::{generate_internal_table_name_with_type, TableId};
use risingwave_pb::catalog::Table;
use risingwave_pb::meta::table_fragments::fragment::FragmentDistributionType;
use risingwave_pb::meta::table_fragments::Fragment;
use risingwave_pb::stream_plan::stream_fragment_graph::{StreamFragment, StreamFragmentEdge};
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_plan::{
    agg_call_state, DispatchStrategy, Dispatcher, DispatcherType, MergeNode, StreamActor,
    StreamFragmentGraph as StreamFragmentGraphProto, StreamNode,
};

use super::CreateStreamingJobContext;
use crate::manager::{IdCategory, IdGeneratorManagerRef, StreamingJob};
use crate::model::FragmentId;
use crate::storage::MetaStore;
use crate::MetaResult;

/// Id of an Actor, maybe local or global
#[derive(Clone, Copy, Debug, Hash, Eq, PartialEq, PartialOrd, Ord)]
enum LocalActorId {
    /// The global allocated id of a fragment.
    Global(u32),
    /// The local id of a fragment, need to be converted to global id if being used in the meta
    /// service.
    Local(u32),
}

/// Id of a fragment, maybe local or global
#[derive(Clone, Copy, Debug, Hash, Eq, PartialEq, PartialOrd, Ord)]
struct GlobalFragmentId(u32);

impl LocalActorId {
    pub fn as_global_id(&self) -> u32 {
        match self {
            Self::Global(id) => *id,
            _ => panic!("actor id is not global id"),
        }
    }

    pub fn as_local_id(&self) -> u32 {
        match self {
            Self::Local(id) => *id,
            _ => panic!("actor id is not local id"),
        }
    }

    #[expect(dead_code)]
    pub fn is_global(&self) -> bool {
        matches!(self, Self::Global(_))
    }

    #[expect(dead_code)]
    pub fn is_local(&self) -> bool {
        matches!(self, Self::Local(_))
    }

    /// Convert local id to global id. Panics if the actor id is not local, or actor id >=
    /// len.
    pub fn to_global_id(self, offset: u32, len: u32) -> Self {
        let id = self.as_local_id();
        assert!(id < len, "actor id {} is out of range (len: {})", id, len);
        Self::Global(id + offset)
    }
}

impl GlobalFragmentId {
    pub fn as_global_id(&self) -> u32 {
        self.0
    }

    /// Convert local id to global id. Panics if the fragment id is not local, or fragment id >=
    /// len.
    pub fn from_local_id(id: u32, offset: u32, len: u32) -> Self {
        assert!(
            id < len,
            "fragment id {} is out of range (len: {})",
            id,
            len
        );
        Self(id + offset)
    }
}

/// A list of actors with order.
#[derive(Debug, Clone)]
struct OrderedActorLink(pub Vec<LocalActorId>);

impl OrderedActorLink {
    pub fn to_global_ids(&self, actor_id_offset: u32, actor_id_len: u32) -> Self {
        Self(
            self.0
                .iter()
                .map(|x| x.to_global_id(actor_id_offset, actor_id_len))
                .collect(),
        )
    }

    pub fn as_global_ids(&self) -> Vec<u32> {
        Self::slice_as_global_ids(self.0.as_slice())
    }

    pub fn slice_as_global_ids(data: &[LocalActorId]) -> Vec<u32> {
        data.iter().map(|x| x.as_global_id()).collect()
    }
}

struct StreamActorDownstream {
    dispatch_strategy: DispatchStrategy,
    dispatcher_id: u64,

    /// Downstream actors.
    actors: OrderedActorLink,

    /// Whether to place the downstream actors on the same node
    same_worker_node: bool,
}

struct StreamActorUpstream {
    /// Upstream actors
    actors: OrderedActorLink,
    /// associate fragment id
    fragment_id: GlobalFragmentId,
    /// Whether to place the upstream actors on the same node
    same_worker_node: bool,
}

/// [`StreamActorBuilder`] builds a stream actor in a stream DAG.
struct StreamActorBuilder {
    /// actor id field
    actor_id: LocalActorId,

    /// associated fragment id
    fragment_id: GlobalFragmentId,

    /// associated stream node
    nodes: Arc<StreamNode>,

    /// downstream dispatchers (dispatcher, downstream actor, hash mapping)
    downstreams: Vec<StreamActorDownstream>,

    /// upstreams, exchange node operator_id -> upstream actor ids
    upstreams: HashMap<u64, StreamActorUpstream>,

    /// Whether to place this actors on the same node as chain's upstream MVs.
    chain_same_worker_node: bool,

    /// whether this actor builder has been sealed
    sealed: bool,
}

impl StreamActorBuilder {
    fn is_chain_same_worker_node(stream_node: &StreamNode) -> bool {
        fn visit(stream_node: &StreamNode) -> bool {
            if let Some(NodeBody::Chain(ref chain)) = stream_node.node_body {
                return chain.same_worker_node;
            }
            stream_node.input.iter().any(visit)
        }
        visit(stream_node)
    }

    pub fn new(
        actor_id: LocalActorId,
        fragment_id: GlobalFragmentId,
        node: Arc<StreamNode>,
    ) -> Self {
        Self {
            actor_id,
            fragment_id,
            chain_same_worker_node: Self::is_chain_same_worker_node(&node),
            nodes: node,
            downstreams: vec![],
            upstreams: HashMap::new(),
            sealed: false,
        }
    }

    pub fn get_fragment_id(&self) -> GlobalFragmentId {
        self.fragment_id
    }

    /// Add a dispatcher to this actor.
    pub fn add_dispatcher(
        &mut self,
        dispatch_strategy: DispatchStrategy,
        dispatcher_id: u64,
        downstream_actors: OrderedActorLink,
        same_worker_node: bool,
    ) {
        assert!(!self.sealed);

        self.downstreams.push(StreamActorDownstream {
            dispatch_strategy,
            dispatcher_id,
            actors: downstream_actors,
            same_worker_node,
        });
    }

    /// Build an actor from given information. At the same time, convert local actor id to global
    /// actor id.
    pub fn seal(&mut self, actor_id_offset: u32, actor_id_len: u32) {
        assert!(!self.sealed);

        self.actor_id = self.actor_id.to_global_id(actor_id_offset, actor_id_len);
        self.downstreams = std::mem::take(&mut self.downstreams)
            .into_iter()
            .map(
                |StreamActorDownstream {
                     dispatch_strategy,
                     dispatcher_id,
                     actors: downstreams,
                     same_worker_node,
                 }| {
                    let downstreams = downstreams.to_global_ids(actor_id_offset, actor_id_len);

                    if dispatch_strategy.r#type == DispatcherType::NoShuffle as i32 {
                        assert_eq!(
                            downstreams.0.len(),
                            1,
                            "no shuffle should only have one actor downstream"
                        );
                        assert!(
                            dispatch_strategy.column_indices.is_empty(),
                            "should leave `column_indices` empty"
                        );
                    }

                    StreamActorDownstream {
                        dispatch_strategy,
                        dispatcher_id,
                        actors: downstreams,
                        same_worker_node,
                    }
                },
            )
            .collect();

        self.upstreams = std::mem::take(&mut self.upstreams)
            .into_iter()
            .map(
                |(
                    exchange_id,
                    StreamActorUpstream {
                        actors,
                        same_worker_node,
                        fragment_id,
                    },
                )| {
                    (
                        exchange_id,
                        StreamActorUpstream {
                            actors: actors.to_global_ids(actor_id_offset, actor_id_len),
                            same_worker_node,
                            fragment_id,
                        },
                    )
                },
            )
            .collect();
        self.sealed = true;
    }

    /// Build an actor after seal.
    pub fn build(&self) -> StreamActor {
        assert!(self.sealed);

        let dispatcher = self
            .downstreams
            .iter()
            .map(
                |StreamActorDownstream {
                     dispatch_strategy,
                     dispatcher_id,
                     actors,
                     same_worker_node: _,
                 }| Dispatcher {
                    downstream_actor_id: actors.as_global_ids(),
                    r#type: dispatch_strategy.r#type,
                    column_indices: dispatch_strategy.column_indices.clone(),
                    // will be filled later by stream manager
                    hash_mapping: None,
                    dispatcher_id: *dispatcher_id,
                },
            )
            .collect_vec();

        StreamActor {
            actor_id: self.actor_id.as_global_id(),
            fragment_id: self.fragment_id.as_global_id(),
            nodes: Some(self.nodes.deref().clone()),
            dispatcher,
            upstream_actor_id: self
                .upstreams
                .iter()
                .flat_map(|(_, StreamActorUpstream { actors, .. })| actors.0.iter().copied())
                .map(|x| x.as_global_id())
                .collect(), // TODO: store each upstream separately
            same_worker_node_as_upstream: self.chain_same_worker_node
                || self.upstreams.values().any(|u| u.same_worker_node),
            vnode_bitmap: None,
            // To be filled by `StreamGraphBuilder::build`
            mview_definition: "".to_owned(),
        }
    }
}

/// [`StreamGraphBuilder`] build a stream graph. It injects some information to achieve
/// dependencies. See `build_inner` for more details.
#[derive(Default)]
struct StreamGraphBuilder {
    actor_builders: BTreeMap<LocalActorId, StreamActorBuilder>,
}

impl StreamGraphBuilder {
    /// Insert new generated actor.
    pub fn add_actor(
        &mut self,
        actor_id: LocalActorId,
        fragment_id: GlobalFragmentId,
        node: Arc<StreamNode>,
    ) {
        self.actor_builders.insert(
            actor_id,
            StreamActorBuilder::new(actor_id, fragment_id, node),
        );
    }

    /// Number of actors in the graph builder
    pub fn actor_len(&self) -> usize {
        self.actor_builders.len()
    }

    /// Add dependency between two connected node in the graph.
    pub fn add_link(
        &mut self,
        upstream_fragment_id: GlobalFragmentId,
        upstream_actor_ids: &[LocalActorId],
        downstream_actor_ids: &[LocalActorId],
        edge: StreamFragmentEdge,
    ) {
        let exchange_operator_id = edge.link_id;
        let same_worker_node = edge.same_worker_node;
        let dispatch_strategy = edge.dispatch_strategy.unwrap();
        // We can't use the exchange operator id directly as the dispatch id, because an exchange
        // could belong to more than one downstream in DAG.
        // We can use downstream fragment id as an unique id for dispatcher.
        // In this way we can ensure the dispatchers of `StreamActor` would have different id,
        // even though they come from the same exchange operator.
        let dispatch_id = edge.downstream_id as u64;

        if dispatch_strategy.get_type().unwrap() == DispatcherType::NoShuffle {
            assert_eq!(
                upstream_actor_ids.len(),
                downstream_actor_ids.len(),
                "mismatched length when processing no-shuffle exchange: {:?} -> {:?} on exchange {}",
                upstream_actor_ids,
                downstream_actor_ids,
                exchange_operator_id
            );

            // update 1v1 relationship
            upstream_actor_ids
                .iter()
                .zip_eq(downstream_actor_ids.iter())
                .for_each(|(upstream_id, downstream_id)| {
                    self.actor_builders
                        .get_mut(upstream_id)
                        .unwrap()
                        .add_dispatcher(
                            dispatch_strategy.clone(),
                            dispatch_id,
                            OrderedActorLink(vec![*downstream_id]),
                            same_worker_node,
                        );

                    let ret = self
                        .actor_builders
                        .get_mut(downstream_id)
                        .unwrap()
                        .upstreams
                        .insert(
                            exchange_operator_id,
                            StreamActorUpstream {
                                actors: OrderedActorLink(vec![*upstream_id]),
                                fragment_id: upstream_fragment_id,
                                same_worker_node,
                            },
                        );

                    assert!(
                        ret.is_none(),
                        "duplicated exchange input {} for no-shuffle actors {:?} -> {:?}",
                        exchange_operator_id,
                        upstream_id,
                        downstream_id
                    );
                });

            return;
        }

        // otherwise, make m * n links between actors.

        assert!(
            !same_worker_node,
            "same_worker_node only applies to 1v1 dispatchers."
        );

        // update actors to have dispatchers, link upstream -> downstream.
        upstream_actor_ids.iter().for_each(|upstream_id| {
            self.actor_builders
                .get_mut(upstream_id)
                .unwrap()
                .add_dispatcher(
                    dispatch_strategy.clone(),
                    dispatch_id,
                    OrderedActorLink(downstream_actor_ids.to_vec()),
                    same_worker_node,
                );
        });

        // update actors to have upstreams, link downstream <- upstream.
        downstream_actor_ids.iter().for_each(|downstream_id| {
            let ret = self
                .actor_builders
                .get_mut(downstream_id)
                .unwrap()
                .upstreams
                .insert(
                    exchange_operator_id,
                    StreamActorUpstream {
                        actors: OrderedActorLink(upstream_actor_ids.to_vec()),
                        fragment_id: upstream_fragment_id,
                        same_worker_node,
                    },
                );
            assert!(
                ret.is_none(),
                "duplicated exchange input {} for actors {:?} -> {:?}",
                exchange_operator_id,
                upstream_actor_ids,
                downstream_actor_ids
            );
        });
    }

    /// Build final stream DAG with dependencies with current actor builders.
    #[allow(clippy::type_complexity)]
    pub fn build(
        mut self,
        ctx: &mut CreateStreamingJobContext,
        actor_id_offset: u32,
        actor_id_len: u32,
    ) -> MetaResult<HashMap<GlobalFragmentId, Vec<StreamActor>>> {
        let mut graph: HashMap<GlobalFragmentId, Vec<StreamActor>> = HashMap::new();

        for builder in self.actor_builders.values_mut() {
            builder.seal(actor_id_offset, actor_id_len);
        }

        for builder in self.actor_builders.values() {
            let fragment_id = builder.get_fragment_id();
            let mut actor = builder.build();
            let mut upstream_actors = builder
                .upstreams
                .iter()
                .map(|(id, StreamActorUpstream { actors, .. })| (*id, actors.clone()))
                .collect();
            let mut upstream_fragments = builder
                .upstreams
                .iter()
                .map(|(id, StreamActorUpstream { fragment_id, .. })| (*id, *fragment_id))
                .collect();
            let stream_node = self.build_inner(
                ctx,
                actor.get_nodes()?,
                fragment_id,
                &mut upstream_actors,
                &mut upstream_fragments,
            )?;

            actor.nodes = Some(stream_node);
            actor.mview_definition = ctx.streaming_definition.clone();

            graph
                .entry(builder.get_fragment_id())
                .or_default()
                .push(actor);
        }
        Ok(graph)
    }

    /// Build stream actor inside, two works will be done:
    /// 1. replace node's input with [`MergeNode`] if it is `ExchangeNode`, and swallow
    /// mergeNode's input.
    /// 2. ignore root node when it's `ExchangeNode`.
    /// 3. replace node's `ExchangeNode` input with [`MergeNode`] and resolve its upstream actor
    /// ids if it is a `ChainNode`.
    fn build_inner(
        &self,
        ctx: &mut CreateStreamingJobContext,
        stream_node: &StreamNode,
        fragment_id: GlobalFragmentId,
        upstream_actor_id: &mut HashMap<u64, OrderedActorLink>,
        upstream_fragment_id: &mut HashMap<u64, GlobalFragmentId>,
    ) -> MetaResult<StreamNode> {
        match stream_node.get_node_body()? {
            NodeBody::Exchange(_) => {
                panic!("ExchangeNode should be eliminated from the top of the plan node when converting fragments to actors: {:#?}", stream_node)
            }
            NodeBody::Chain(_) => Ok(self.resolve_chain_node(stream_node)?),
            _ => {
                let mut new_stream_node = stream_node.clone();

                for (input, new_input) in stream_node
                    .input
                    .iter()
                    .zip_eq(new_stream_node.input.iter_mut())
                {
                    *new_input = match input.get_node_body()? {
                        NodeBody::Exchange(e) => {
                            assert!(!input.get_fields().is_empty());
                            StreamNode {
                                input: vec![],
                                stream_key: input.stream_key.clone(),
                                node_body: Some(NodeBody::Merge(MergeNode {
                                    upstream_actor_id: upstream_actor_id
                                        .remove(&input.get_operator_id())
                                        .expect("failed to find upstream actor id for given exchange node").as_global_ids(),
                                    upstream_fragment_id: upstream_fragment_id.get(&input.get_operator_id()).unwrap().as_global_id(),
                                    upstream_dispatcher_type: e.get_strategy()?.r#type,
                                    fields: input.get_fields().clone(),
                                })),
                                fields: input.get_fields().clone(),
                                operator_id: input.operator_id,
                                identity: "MergeExecutor".to_string(),
                                append_only: input.append_only,
                            }
                        }
                        NodeBody::Chain(_) => self.resolve_chain_node(input)?,
                        _ => self.build_inner(
                            ctx,
                            input,
                            fragment_id,
                            upstream_actor_id,
                            upstream_fragment_id,
                        )?,
                    }
                }
                Ok(new_stream_node)
            }
        }
    }

    /// Resolve the chain node, only rewrite the schema of input `MergeNode`.
    fn resolve_chain_node(&self, stream_node: &StreamNode) -> MetaResult<StreamNode> {
        let NodeBody::Chain(chain_node) = stream_node.get_node_body().unwrap() else {
            unreachable!()
        };
        let input = stream_node.get_input();
        assert_eq!(input.len(), 2);

        let merge_node = &input[0];
        assert_matches!(merge_node.node_body, Some(NodeBody::Merge(_)));
        let batch_plan_node = &input[1];
        assert_matches!(batch_plan_node.node_body, Some(NodeBody::BatchPlan(_)));

        let chain_input = vec![
            StreamNode {
                input: vec![],
                stream_key: merge_node.stream_key.clone(),
                node_body: Some(NodeBody::Merge(MergeNode {
                    upstream_actor_id: vec![],
                    upstream_fragment_id: 0,
                    upstream_dispatcher_type: DispatcherType::NoShuffle as _,
                    fields: chain_node.upstream_fields.clone(),
                })),
                fields: chain_node.upstream_fields.clone(),
                operator_id: merge_node.operator_id,
                identity: "MergeExecutor".to_string(),
                append_only: stream_node.append_only,
            },
            batch_plan_node.clone(),
        ];

        Ok(StreamNode {
            input: chain_input,
            stream_key: stream_node.stream_key.clone(),
            node_body: Some(NodeBody::Chain(chain_node.clone())),
            operator_id: stream_node.operator_id,
            identity: "ChainExecutor".to_string(),
            fields: chain_node.upstream_fields.clone(),
            append_only: stream_node.append_only,
        })
    }
}

/// The mutable state when building actor graph.
#[derive(Default)]
struct BuildActorGraphState {
    /// stream graph builder, to build streaming DAG.
    stream_graph_builder: StreamGraphBuilder,
    /// when converting fragment graph to actor graph, we need to know which actors belong to a
    /// fragment.
    fragment_actors: HashMap<GlobalFragmentId, Vec<LocalActorId>>,
    /// local actor id
    next_local_actor_id: u32,
}

impl BuildActorGraphState {
    fn gen_actor_ids(&mut self, parallel_degree: u32) -> Range<u32> {
        let start_actor_id = self.next_local_actor_id;
        self.next_local_actor_id += parallel_degree;
        start_actor_id..start_actor_id + parallel_degree
    }
}

/// [`ActorGraphBuilder`] generates the proto for interconnected actors for a streaming pipeline.
pub struct ActorGraphBuilder {
    /// Default parallelism.
    default_parallelism: u32,

    fragment_graph: StreamFragmentGraph,
}

impl ActorGraphBuilder {
    pub fn new(fragment_graph: StreamFragmentGraph, default_parallelism: u32) -> Self {
        Self {
            default_parallelism,
            fragment_graph,
        }
    }

    /// Build a stream graph by duplicating each fragment as parallel actors.
    pub async fn generate_graph<S>(
        &self,
        id_gen_manager: IdGeneratorManagerRef<S>,
        ctx: &mut CreateStreamingJobContext,
    ) -> MetaResult<BTreeMap<FragmentId, Fragment>>
    where
        S: MetaStore,
    {
        let stream_graph = {
            let BuildActorGraphState {
                stream_graph_builder,
                next_local_actor_id,
                ..
            } = {
                let mut state = BuildActorGraphState::default();

                // Generate actors of the streaming plan
                self.build_actor_graph(&mut state, &self.fragment_graph, ctx)?;
                state
            };

            // generates global ids
            let (actor_len, start_actor_id) = {
                let actor_len = stream_graph_builder.actor_len() as u32;
                assert_eq!(actor_len, next_local_actor_id);
                let start_actor_id = id_gen_manager
                    .generate_interval::<{ IdCategory::Actor }>(actor_len as u64)
                    .await? as _;

                (actor_len, start_actor_id)
            };

            stream_graph_builder.build(ctx, start_actor_id, actor_len)?
        };

        // Serialize the graph
        let stream_graph = stream_graph
            .into_iter()
            .map(|(fragment_id, actors)| {
                let fragment = self.fragment_graph.seal_fragment(fragment_id, actors);
                let fragment_id = fragment_id.as_global_id();
                (fragment_id, fragment)
            })
            .collect();

        Ok(stream_graph)
    }

    /// Build actor graph from fragment graph using topological sort. Setup dispatcher in actor and
    /// generate actors by their parallelism.
    fn build_actor_graph(
        &self,
        state: &mut BuildActorGraphState,
        fragment_graph: &StreamFragmentGraph,
        ctx: &mut CreateStreamingJobContext,
    ) -> MetaResult<()> {
        // Use topological sort to build the graph from downstream to upstream. (The first fragment
        // popped out from the heap will be the top-most node in plan, or the sink in stream graph.)
        let mut actionable_fragment_id = VecDeque::new();
        let mut downstream_cnts = HashMap::new();

        // Iterate all fragments
        for fragment_id in fragment_graph.fragments.keys() {
            // Count how many downstreams we have for a given fragment
            let downstream_cnt = fragment_graph.get_downstreams(*fragment_id).len();
            if downstream_cnt == 0 {
                actionable_fragment_id.push_back(*fragment_id);
            } else {
                downstream_cnts.insert(*fragment_id, downstream_cnt);
            }
        }

        while let Some(fragment_id) = actionable_fragment_id.pop_front() {
            // Build the actors corresponding to the fragment
            self.build_actor_graph_fragment(fragment_id, state, fragment_graph, ctx)?;

            // Find if we can process more fragments
            for upstream_id in fragment_graph.get_upstreams(fragment_id).keys() {
                let downstream_cnt = downstream_cnts
                    .get_mut(upstream_id)
                    .expect("the upstream should exist");
                *downstream_cnt -= 1;
                if *downstream_cnt == 0 {
                    downstream_cnts.remove(upstream_id);
                    actionable_fragment_id.push_back(*upstream_id);
                }
            }
        }

        if !downstream_cnts.is_empty() {
            // There are fragments that are not processed yet.
            bail!("graph is not a DAG");
        }

        Ok(())
    }

    fn build_actor_graph_fragment(
        &self,
        fragment_id: GlobalFragmentId,
        state: &mut BuildActorGraphState,
        fragment_graph: &StreamFragmentGraph,
        ctx: &mut CreateStreamingJobContext,
    ) -> MetaResult<()> {
        let current_fragment = fragment_graph.get_fragment(fragment_id).unwrap().clone();
        let upstream_table_id = current_fragment
            .upstream_table_ids
            .iter()
            .at_most_one()
            .unwrap()
            .map(TableId::from);
        if let Some(upstream_table_id) = upstream_table_id {
            ctx.chain_fragment_upstream_table_map
                .insert(fragment_id.as_global_id(), upstream_table_id);
        }

        let parallel_degree = if current_fragment.is_singleton {
            1
        } else if let Some(upstream_table_id) = upstream_table_id {
            // set fragment parallelism to the parallelism of its dependent table.
            let upstream_actors = ctx
                .table_mview_map
                .get(&upstream_table_id)
                .expect("upstream actor should exist");
            upstream_actors.len() as u32
        } else {
            self.default_parallelism
        };

        let node = Arc::new(current_fragment.node.unwrap());
        let actor_ids = state
            .gen_actor_ids(parallel_degree)
            .into_iter()
            .map(LocalActorId::Local)
            .collect_vec();

        for id in &actor_ids {
            state
                .stream_graph_builder
                .add_actor(*id, fragment_id, node.clone());
        }

        for (downstream_fragment_id, dispatch_edge) in fragment_graph.get_downstreams(fragment_id) {
            let downstream_actors = state
                .fragment_actors
                .get(downstream_fragment_id)
                .expect("downstream fragment not processed yet");

            let dispatch_strategy = dispatch_edge.dispatch_strategy.as_ref().unwrap();
            match dispatch_strategy.get_type()? {
                DispatcherType::Hash
                | DispatcherType::Simple
                | DispatcherType::Broadcast
                | DispatcherType::NoShuffle => {
                    state.stream_graph_builder.add_link(
                        fragment_id,
                        &actor_ids,
                        downstream_actors,
                        dispatch_edge.clone(),
                    );
                }
                DispatcherType::Unspecified => unreachable!(),
            }
        }

        let ret = state.fragment_actors.insert(fragment_id, actor_ids);
        assert!(
            ret.is_none(),
            "fragment {:?} already processed",
            fragment_id
        );

        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct BuildingFragment {
    inner: StreamFragment,

    internal_tables: Vec<Table>,
    table_id: Option<u32>,
}

impl BuildingFragment {
    fn new(id: GlobalFragmentId, fragment: StreamFragment) -> Self {
        Self {
            inner: StreamFragment {
                fragment_id: id.as_global_id(),
                ..fragment
            },
            internal_tables: vec![],
            table_id: None,
        }
    }
}

impl Deref for BuildingFragment {
    type Target = StreamFragment;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[derive(Default)]
pub struct StreamFragmentGraph {
    /// stores all the fragments in the graph.
    fragments: HashMap<GlobalFragmentId, BuildingFragment>,

    /// stores edges between fragments: upstream => downstream.
    downstreams: HashMap<GlobalFragmentId, HashMap<GlobalFragmentId, StreamFragmentEdge>>,

    /// stores edges between fragments: downstream -> upstream.
    upstreams: HashMap<GlobalFragmentId, HashMap<GlobalFragmentId, StreamFragmentEdge>>,

    /// Dependent relations of this job.
    dependent_relations: HashSet<TableId>,
}

impl StreamFragmentGraph {
    pub async fn create<S: MetaStore>(
        proto: StreamFragmentGraphProto,
        id_gen: IdGeneratorManagerRef<S>,
        job: &StreamingJob,
    ) -> MetaResult<Self> {
        let fragment_len = proto.fragments.len() as u64;
        let fragment_id_offset = id_gen
            .generate_interval::<{ IdCategory::Fragment }>(fragment_len)
            .await? as u32;
        let mut graph = Self::from_protobuf(proto.clone(), fragment_id_offset); // TODO: Remove this clone

        let table_len = proto.table_ids_cnt as u64;
        let table_id_offset = id_gen
            .generate_interval::<{ IdCategory::Table }>(table_len)
            .await? as u32;
        let internal_table_count = graph.fill_internal_tables(table_id_offset, job);
        assert_eq!(table_len, internal_table_count as u64);

        graph.fill_object_id(job);

        Ok(graph)
    }

    fn fill_object_id(&mut self, job: &StreamingJob) {
        let table_id = job.id();

        for fragment in self.fragments.values_mut() {
            let fragment_id = fragment.fragment_id;
            let fragment_table_id = &mut fragment.table_id;

            visit_fragment(&mut fragment.inner, |node_body| match node_body {
                NodeBody::Materialize(materialize_node) => {
                    materialize_node.table_id = table_id;

                    // Fill the ID of the `Table`.
                    let table = materialize_node.table.as_mut().unwrap();
                    table.id = table_id;
                    table.database_id = job.database_id();
                    table.schema_id = job.schema_id();
                    table.fragment_id = fragment_id;

                    // Record the table id of this fragment.
                    fragment_table_id.replace(table_id);
                }
                NodeBody::Sink(sink_node) => {
                    sink_node.table_id = table_id;

                    // Record the table id of this fragment.
                    fragment_table_id.replace(table_id);
                }
                NodeBody::Dml(dml_node) => {
                    dml_node.table_id = table_id;
                }
                _ => {}
            });
        }
    }

    fn fill_internal_tables(&mut self, id_offset: u32, job: &StreamingJob) -> usize {
        let mut count = 0;

        for fragment in self.fragments.values_mut() {
            let fragment_id = fragment.fragment_id;
            let internal_tables = &mut fragment.internal_tables;

            visit_internal_tables(&mut fragment.inner, |table, table_type_name| {
                table.id += id_offset;
                table.schema_id = job.schema_id();
                table.database_id = job.database_id();
                table.name = generate_internal_table_name_with_type(
                    &job.name(),
                    fragment_id,
                    table.id,
                    table_type_name,
                );
                table.fragment_id = fragment_id;

                // Record the internal table.
                internal_tables.push(table.clone());
                count += 1;
            })
        }

        count
    }

    fn seal_fragment(&self, id: GlobalFragmentId, actors: Vec<StreamActor>) -> Fragment {
        let BuildingFragment {
            inner,
            internal_tables,
            table_id,
        } = self.fragments.get(&id).unwrap().to_owned();

        let distribution_type = if inner.is_singleton {
            FragmentDistributionType::Single
        } else {
            FragmentDistributionType::Hash
        } as i32;

        let state_table_ids = internal_tables
            .iter()
            .map(|t| t.id)
            .chain(table_id)
            .collect();

        let upstream_fragment_ids = self
            .get_upstreams(id)
            .keys()
            .map(|id| id.as_global_id())
            .collect();

        Fragment {
            fragment_id: inner.fragment_id,
            fragment_type_mask: inner.fragment_type_mask,
            distribution_type,
            actors,
            // Will be filled in `Scheduler::schedule` later.
            vnode_mapping: None,
            state_table_ids,
            upstream_fragment_ids,
        }
    }

    /// Build the graph from the protobuf representation. All local IDs generated by the frontend
    /// will be converted to global IDs by `local_id + offset`.
    fn from_protobuf(proto: StreamFragmentGraphProto, offset: u32) -> Self {
        let len = proto.fragments.len() as u32;

        let fragments = proto
            .fragments
            .into_iter()
            .map(|(id, fragment)| {
                let id = GlobalFragmentId::from_local_id(id, offset, len);
                let fragment = BuildingFragment::new(id, fragment);
                (id, fragment)
            })
            .collect();

        let mut downstreams = HashMap::new();
        let mut upstreams = HashMap::new();

        for edge in proto.edges {
            let upstream_id = GlobalFragmentId::from_local_id(edge.upstream_id, offset, len);
            let downstream_id = GlobalFragmentId::from_local_id(edge.downstream_id, offset, len);

            upstreams
                .entry(downstream_id)
                .or_insert_with(HashMap::new)
                .try_insert(
                    upstream_id,
                    StreamFragmentEdge {
                        upstream_id: upstream_id.as_global_id(),
                        downstream_id: downstream_id.as_global_id(),
                        ..edge.clone()
                    },
                )
                .unwrap();
            downstreams
                .entry(upstream_id)
                .or_insert_with(HashMap::new)
                .try_insert(
                    downstream_id,
                    StreamFragmentEdge {
                        upstream_id: upstream_id.as_global_id(),
                        downstream_id: downstream_id.as_global_id(),
                        ..edge
                    },
                )
                .unwrap();
        }

        // TODO: note here
        let dependent_relations = proto
            .dependent_table_ids
            .iter()
            .map(TableId::from)
            .collect();

        Self {
            fragments,
            downstreams,
            upstreams,
            dependent_relations,
        }
    }

    pub fn internal_tables(&self) -> HashMap<u32, Table> {
        let mut tables = HashMap::new();
        for fragment in self.fragments.values() {
            for table in &fragment.internal_tables {
                tables
                    .try_insert(table.id, table.clone())
                    .unwrap_or_else(|_| panic!("duplicated table id `{}`", table.id));
            }
        }
        tables
    }

    pub fn table_fragment_id(&self) -> FragmentId {
        self.fragments
            .values()
            .filter(|b| b.table_id.is_some())
            .map(|b| b.fragment_id)
            .exactly_one()
            .expect("require exactly 1 materialize/sink node when creating the streaming job")
    }

    pub fn dependent_relations(&self) -> &HashSet<TableId> {
        &self.dependent_relations
    }

    fn get_fragment(&self, fragment_id: GlobalFragmentId) -> Option<&StreamFragment> {
        self.fragments.get(&fragment_id).map(|b| b.deref())
    }

    fn get_downstreams(
        &self,
        fragment_id: GlobalFragmentId,
    ) -> &HashMap<GlobalFragmentId, StreamFragmentEdge> {
        self.downstreams.get(&fragment_id).unwrap_or(&EMPTY_HASHMAP)
    }

    fn get_upstreams(
        &self,
        fragment_id: GlobalFragmentId,
    ) -> &HashMap<GlobalFragmentId, StreamFragmentEdge> {
        self.upstreams.get(&fragment_id).unwrap_or(&EMPTY_HASHMAP)
    }
}

static EMPTY_HASHMAP: LazyLock<HashMap<GlobalFragmentId, StreamFragmentEdge>> =
    LazyLock::new(HashMap::new);

/// A utility for visiting and mutating the [`NodeBody`] of the [`StreamNode`]s in a
/// [`StreamFragment`] recursively.
pub fn visit_fragment<F>(fragment: &mut StreamFragment, mut f: F)
where
    F: FnMut(&mut NodeBody),
{
    fn visit_inner<F>(stream_node: &mut StreamNode, f: &mut F)
    where
        F: FnMut(&mut NodeBody),
    {
        f(stream_node.node_body.as_mut().unwrap());
        for input in &mut stream_node.input {
            visit_inner(input, f);
        }
    }

    visit_inner(fragment.node.as_mut().unwrap(), &mut f)
}

/// Visit the internal tables of a [`StreamFragment`].
fn visit_internal_tables<F>(fragment: &mut StreamFragment, mut f: F)
where
    F: FnMut(&mut Table, &'static str),
{
    macro_rules! always {
        ($table:expr, $name:expr) => {{
            let table = $table
                .as_mut()
                .unwrap_or_else(|| panic!("internal table {} should always exist", $name));
            f(table, $name);
        }};
    }

    #[allow(unused_macros)]
    macro_rules! optional {
        ($table:expr, $name:expr) => {
            if let Some(table) = &mut $table {
                f(table, $name);
            }
        };
    }

    visit_fragment(fragment, |body| {
        match body {
            // Join
            NodeBody::HashJoin(node) => {
                // TODO: make the degree table optional
                always!(node.left_table, "HashJoinLeft");
                always!(node.left_degree_table, "HashJoinDegreeLeft");
                always!(node.right_table, "HashJoinRight");
                always!(node.right_degree_table, "HashJoinDegreeRight");
            }
            NodeBody::DynamicFilter(node) => {
                always!(node.left_table, "DynamicFilterLeft");
                always!(node.right_table, "DynamicFilterRight");
            }

            // Aggregation
            NodeBody::HashAgg(node) => {
                assert_eq!(node.agg_call_states.len(), node.agg_calls.len());
                always!(node.result_table, "HashAggResult");
                for state in &mut node.agg_call_states {
                    if let agg_call_state::Inner::MaterializedInputState(s) =
                        state.inner.as_mut().unwrap()
                    {
                        always!(s.table, "HashAgg");
                    }
                }
            }
            NodeBody::GlobalSimpleAgg(node) => {
                assert_eq!(node.agg_call_states.len(), node.agg_calls.len());
                always!(node.result_table, "GlobalSimpleAggResult");
                for state in &mut node.agg_call_states {
                    if let agg_call_state::Inner::MaterializedInputState(s) =
                        state.inner.as_mut().unwrap()
                    {
                        always!(s.table, "GlobalSimpleAgg");
                    }
                }
            }

            // Top-N
            NodeBody::AppendOnlyTopN(node) => {
                always!(node.table, "AppendOnlyTopN");
            }
            NodeBody::TopN(node) => {
                always!(node.table, "TopN");
            }
            NodeBody::GroupTopN(node) => {
                always!(node.table, "GroupTopN");
            }

            // Source
            NodeBody::Source(node) => {
                if let Some(source) = &mut node.source_inner {
                    always!(source.state_table, "Source");
                }
            }
            NodeBody::Now(node) => {
                always!(node.state_table, "Now");
            }

            // Shared arrangement
            NodeBody::Arrange(node) => {
                always!(node.table, "Arrange");
            }

            // Note: add internal tables for new nodes here.
            _ => {}
        }
    })
}

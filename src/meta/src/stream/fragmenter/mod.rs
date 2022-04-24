// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

mod rewrite;

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::ops::Range;
use std::sync::Arc;

use itertools::Itertools;
use risingwave_common::catalog::TableId;
use risingwave_common::error::{ErrorCode, Result, RwError};
use risingwave_pb::meta::table_fragments::fragment::{FragmentDistributionType, FragmentType};
use risingwave_pb::meta::table_fragments::Fragment;
use risingwave_pb::plan::JoinType;
use risingwave_pb::stream_plan::stream_node::Node;
use risingwave_pb::stream_plan::{
    DispatchStrategy, Dispatcher, DispatcherType, ExchangeNode, StreamNode,
};

use super::graph::StreamFragmentEdge;
use super::{CreateMaterializedViewContext, FragmentManagerRef};
use crate::manager::{IdCategory, IdGeneratorManagerRef};
use crate::model::{FragmentId, LocalActorId, LocalFragmentId};
use crate::storage::MetaStore;
use crate::stream::graph::{
    StreamActorBuilder, StreamFragment, StreamFragmentGraph, StreamGraphBuilder,
};

/// [`StreamFragmenter`] generates the proto for interconnected actors for a streaming pipeline.
pub struct StreamFragmenter<S> {
    /// fragment graph field, transformed from input streaming plan.
    pub(super) fragment_graph: StreamFragmentGraph,

    /// fragment manager, used to retrieve upstream table fragment infos (dist_keys, actor_ids).
    pub(super) fragment_manager: FragmentManagerRef<S>,

    /// stream graph builder, to build streaming DAG.
    stream_graph: StreamGraphBuilder,

    /// id generator, used to generate actor id.
    id_gen_manager: IdGeneratorManagerRef<S>,

    /// local fragment id
    next_local_fragment_id: u32,

    /// local actor id
    next_local_actor_id: u32,

    /// rewrite will produce new operators, and we need to track next operator id
    next_operator_id: u32,

    /// when converting fragment graph to actor graph, we need to know which actors belong to a
    /// fragment.
    fragment_actors: HashMap<LocalFragmentId, Vec<LocalActorId>>,

    /// degree of parallelism
    parallel_degree: u32,

    /// dependent table ids
    dependent_table_ids: HashSet<TableId>,

    /// upstream dispatch keys
    distribution_keys: Vec<i32>,

    // TODO: remove this when we deprecate Java frontend.
    is_legacy_frontend: bool,
}

impl<S> StreamFragmenter<S>
where
    S: MetaStore,
{
    pub fn new(
        id_gen_manager: IdGeneratorManagerRef<S>,
        fragment_manager: FragmentManagerRef<S>,
        parallel_degree: u32,
        is_legacy_frontend: bool,
    ) -> Self {
        Self {
            fragment_graph: StreamFragmentGraph::new(),
            fragment_manager,
            stream_graph: Default::default(),
            id_gen_manager,
            next_local_fragment_id: 0,
            next_local_actor_id: 0,
            next_operator_id: u32::MAX - 1,
            fragment_actors: HashMap::new(),
            parallel_degree,
            dependent_table_ids: HashSet::new(),
            distribution_keys: Vec::new(),
            is_legacy_frontend,
        }
    }

    /// Build a stream graph in two steps:
    /// (1) Break the streaming plan into fragments with their dependency.
    /// (2) Duplicate each fragment as parallel actors.
    ///
    /// Return a pair of (1) all stream actors, and (2) the actor id of sources to be forcefully
    /// round-robin scheduled.
    pub async fn generate_graph(
        mut self,
        stream_node: &StreamNode,
        ctx: &mut CreateMaterializedViewContext,
    ) -> Result<BTreeMap<FragmentId, Fragment>> {
        let stream_node = stream_node.clone();

        // Generate fragment graph and seal
        self.generate_fragment_graph(stream_node)?;
        // The stream node might be rewritten after this point. Don't use `stream_node` anymore.

        // save distribution key and dependent table ids in ctx
        ctx.distribution_keys = self.distribution_keys.clone();
        ctx.dependent_table_ids = self.dependent_table_ids.clone();

        // resolve upstream table infos first
        // TODO: this info is only used by `resolve_chain_node`. We can move that logic to
        // stream manager and remove dependency on fragment manager.
        let info = self
            .fragment_manager
            .get_build_graph_info(&self.dependent_table_ids)
            .await?;
        self.stream_graph.fill_info(info);

        let fragment_len = self.fragment_graph.fragment_len() as u32;
        assert_eq!(fragment_len, self.next_local_fragment_id);
        let offset = self
            .id_gen_manager
            .generate_interval::<{ IdCategory::Fragment }>(fragment_len as i32)
            .await? as _;
        self.fragment_graph.seal(offset, fragment_len);

        // Generate actors of the streaming plan
        self.build_actor_graph()?;

        let actor_len = self.stream_graph.actor_len() as u32;
        assert_eq!(actor_len, self.next_local_actor_id);
        let start_actor_id = self
            .id_gen_manager
            .generate_interval::<{ IdCategory::Actor }>(actor_len as i32)
            .await? as _;

        // Compute how many table ids should be allocated for all actors.
        let mut table_ids_cnt = 0;
        for (local_fragment_id, fragment) in self.fragment_graph.fragments() {
            let num_of_actors = self
                .fragment_actors
                .get(local_fragment_id)
                .expect("Fragment should have at least one actor")
                .len();
            // Total table ids number equals to table ids cnt in all actors.
            table_ids_cnt += num_of_actors * fragment.table_ids_cnt;
        }
        let start_table_id = self
            .id_gen_manager
            .generate_interval::<{ IdCategory::Table }>(table_ids_cnt as i32)
            .await? as _;

        let stream_graph = self.stream_graph.build(
            ctx,
            start_actor_id,
            actor_len,
            start_table_id,
            table_ids_cnt as u32,
        )?;

        // Serialize the graph
        stream_graph
            .iter()
            .map(|(fragment_id, actors)| {
                Ok::<_, RwError>((
                    fragment_id.as_global_id(),
                    Fragment {
                        fragment_id: fragment_id.as_global_id(),
                        fragment_type: self
                            .fragment_graph
                            .get_fragment(*fragment_id)
                            .unwrap()
                            .fragment_type as i32,
                        distribution_type: if self
                            .fragment_graph
                            .get_fragment(*fragment_id)
                            .unwrap()
                            .is_singleton
                        {
                            FragmentDistributionType::Single
                        } else {
                            FragmentDistributionType::Hash
                        } as i32,
                        actors: actors.clone(),
                    },
                ))
            })
            .collect::<Result<BTreeMap<_, _>>>()
    }

    /// Do some dirty rewrites on meta. Currently, it will split stateful operators into two
    /// fragments.
    fn rewrite_stream_node(&mut self, stream_node: StreamNode) -> Result<StreamNode> {
        self.rewrite_stream_node_inner(stream_node, false)
    }

    fn rewrite_stream_node_inner(
        &mut self,
        stream_node: StreamNode,
        insert_exchange_flag: bool,
    ) -> Result<StreamNode> {
        let mut inputs = vec![];

        for child_node in stream_node.input {
            let input = match child_node.get_node()? {
                // For stateful operators, set `exchange_flag = true`. If it's already true, force
                // add an exchange.
                Node::HashAggNode(_) | Node::HashJoinNode(_) => {
                    // We didn't make `fields` available on Java frontend yet, so we check if schema
                    // is available (by `child_node.fields.is_empty()`) before deciding to do the
                    // rewrite.
                    if insert_exchange_flag && !child_node.fields.is_empty() {
                        let child_node = self.rewrite_stream_node_inner(child_node, false)?;

                        let strategy = DispatchStrategy {
                            r#type: DispatcherType::NoShuffle.into(),
                            column_indices: vec![],
                        };
                        let append_only = child_node.append_only;
                        StreamNode {
                            pk_indices: child_node.pk_indices.clone(),
                            fields: child_node.fields.clone(),
                            node: Some(Node::ExchangeNode(ExchangeNode {
                                strategy: Some(strategy.clone()),
                            })),
                            operator_id: self.gen_operator_id() as u64,
                            input: vec![child_node],
                            identity: "Exchange (NoShuffle)".to_string(),
                            append_only,
                        }
                    } else {
                        self.rewrite_stream_node_inner(child_node, true)?
                    }
                }
                _ => child_node,
            };
            inputs.push(input);
        }

        Ok(StreamNode {
            input: inputs,
            ..stream_node
        })
    }

    /// Generate fragment DAG from input streaming plan by their dependency.
    fn generate_fragment_graph(&mut self, stream_node: StreamNode) -> Result<()> {
        let stream_node = self.rewrite_stream_node(stream_node)?;
        self.build_and_add_fragment(stream_node)?;
        Ok(())
    }

    /// Use the given `stream_node` to create a fragment and add it to graph.
    fn build_and_add_fragment(&mut self, stream_node: StreamNode) -> Result<StreamFragment> {
        let mut fragment = self.new_stream_fragment();
        let node = self.build_fragment(&mut fragment, stream_node)?;
        fragment.seal_node(node);
        self.fragment_graph.add_fragment(fragment.clone());
        Ok(fragment)
    }

    /// Build new fragment and link dependencies by visiting children recursively, update
    /// `is_singleton` and `fragment_type` properties for current fragment. While traversing the
    /// tree, count how many table ids should be allocated in this fragment.
    // TODO: Should we store the concurrency in StreamFragment directly?
    fn build_fragment(
        &mut self,
        current_fragment: &mut StreamFragment,
        mut stream_node: StreamNode,
    ) -> Result<StreamNode> {
        // Update current fragment based on the node we're visiting.
        match stream_node.get_node()? {
            Node::SourceNode(_) => current_fragment.fragment_type = FragmentType::Source,

            Node::MaterializeNode(ref node) => {
                current_fragment.fragment_type = FragmentType::Sink;
                // store distribution keys, later it will be persisted with `TableFragment`
                assert!(self.distribution_keys.is_empty()); // should have only one sink node.
                self.distribution_keys = node.distribution_keys.clone();
            }

            // TODO: Force singleton for TopN as a workaround. We should implement two phase TopN.
            Node::TopNNode(_) => current_fragment.is_singleton = true,

            Node::ChainNode(ref node) => {
                // TODO: Remove this when we deprecate Java frontend.
                current_fragment.is_singleton = self.is_legacy_frontend;

                // memorize table id for later use
                self.dependent_table_ids
                    .insert(TableId::from(&node.table_ref_id));
            }

            _ => {}
        };

        // For HashJoin nodes, attempting to rewrite to delta joins only on inner join
        // with only equal conditions
        if let Node::HashJoinNode(hash_join_node) = stream_node.get_node()? {
            current_fragment.table_ids_cnt += 2;
            if hash_join_node.is_delta_join {
                if hash_join_node.get_join_type()? == JoinType::Inner
                    && hash_join_node.condition.is_none()
                {
                    return self.build_delta_join(current_fragment, stream_node);
                } else {
                    panic!(
                        "only inner join without non-equal condition is supported for delta joins"
                    );
                }
            }
        }

        let inputs = std::mem::take(&mut stream_node.input);
        // Visit plan children.
        let inputs = inputs
            .into_iter()
            .map(|mut child_node| -> Result<StreamNode> {
                match child_node.get_node()? {
                    Node::ExchangeNode(_) if child_node.input.is_empty() => {
                        // When exchange node is generated when doing rewrites, it could be having
                        // zero input. In this case, we won't recursively
                        // visit its children.
                        Ok(child_node)
                    }
                    // Exchange node indicates a new child fragment.
                    Node::ExchangeNode(exchange_node) => {
                        let exchange_node = exchange_node.clone();

                        assert_eq!(child_node.input.len(), 1);
                        let child_fragment =
                            self.build_and_add_fragment(child_node.input.remove(0))?;
                        self.fragment_graph.add_edge(
                            child_fragment.fragment_id,
                            current_fragment.fragment_id,
                            StreamFragmentEdge {
                                dispatch_strategy: exchange_node.get_strategy()?.clone(),
                                same_worker_node: false,
                                link_id: child_node.operator_id,
                            },
                        );

                        let is_simple_dispatcher =
                            exchange_node.get_strategy()?.get_type()? == DispatcherType::Simple;
                        if is_simple_dispatcher {
                            current_fragment.is_singleton = true;
                        }
                        Ok(child_node)
                    }

                    // For other children, visit recursively.
                    _ => self.build_fragment(current_fragment, child_node),
                }
            })
            .try_collect()?;

        stream_node.input = inputs;

        Ok(stream_node)
    }

    /// Create a new stream fragment with given node with generating a fragment id.
    fn new_stream_fragment(&mut self) -> StreamFragment {
        let fragment = StreamFragment::new(LocalFragmentId::Local(self.next_local_fragment_id));

        self.next_local_fragment_id += 1;

        fragment
    }

    /// Generate actor id from id generator.
    fn gen_actor_ids(&mut self, parallel_degree: u32) -> Range<u32> {
        let start_actor_id = self.next_local_actor_id;
        self.next_local_actor_id += parallel_degree;

        start_actor_id..start_actor_id + parallel_degree
    }

    /// Generate an operator id
    fn gen_operator_id(&mut self) -> u32 {
        self.next_operator_id -= 1;
        self.next_operator_id
    }

    fn build_actor_graph_fragment(&mut self, fragment_id: LocalFragmentId) -> Result<()> {
        let current_fragment = self
            .fragment_graph
            .get_fragment(fragment_id)
            .unwrap()
            .clone();

        let parallel_degree = if current_fragment.is_singleton {
            1
        } else {
            self.parallel_degree
        };

        let node = Arc::new(current_fragment.get_node().clone());
        let actor_ids = self
            .gen_actor_ids(parallel_degree)
            .into_iter()
            .map(LocalActorId::Local)
            .collect_vec();

        for id in &actor_ids {
            let actor_builder = StreamActorBuilder::new(*id, fragment_id, node.clone());
            self.stream_graph.add_actor(actor_builder);
        }

        for (downstream_fragment_id, dispatch_edge) in
            self.fragment_graph.get_downstreams(fragment_id).iter()
        {
            let downstream_actors = self
                .fragment_actors
                .get(downstream_fragment_id)
                .expect("downstream fragment not processed yet")
                .clone();

            match dispatch_edge.dispatch_strategy.get_type()? {
                ty @ (DispatcherType::Hash
                | DispatcherType::Simple
                | DispatcherType::Broadcast
                | DispatcherType::NoShuffle) => {
                    self.stream_graph.add_link(
                        &actor_ids,
                        &downstream_actors,
                        dispatch_edge.link_id,
                        Dispatcher {
                            r#type: ty.into(),
                            column_indices: dispatch_edge.dispatch_strategy.column_indices.clone(),
                            hash_mapping: None,
                            downstream_actor_id: vec![],
                        },
                        dispatch_edge.same_worker_node,
                    );
                }
                DispatcherType::Invalid => unreachable!(),
            }
        }

        let ret = self.fragment_actors.insert(fragment_id, actor_ids);
        assert!(
            ret.is_none(),
            "fragment {:?} already processed",
            fragment_id
        );

        Ok(())
    }

    /// Build actor graph from fragment graph using topological sort. Setup dispatcher in actor and
    /// generate actors by their parallelism.
    fn build_actor_graph(&mut self) -> Result<()> {
        // Use topological sort to build the graph from downstream to upstream. (The first fragment
        // poped out from the heap will be the top-most node in plan, or the sink in stream graph.)
        let mut actionable_fragment_id = VecDeque::new();
        let mut downstream_cnts = HashMap::new();

        // Iterate all fragments
        for (fragment_id, _) in self.fragment_graph.fragments().iter() {
            // Count how many downstreams we have for a given fragment
            let downstream_cnt = self.fragment_graph.get_downstreams(*fragment_id).len();
            if downstream_cnt == 0 {
                actionable_fragment_id.push_back(*fragment_id);
            } else {
                downstream_cnts.insert(*fragment_id, downstream_cnt);
            }
        }

        while let Some(fragment_id) = actionable_fragment_id.pop_front() {
            // Build the actors corresponding to the fragment
            self.build_actor_graph_fragment(fragment_id)?;

            // Find if we can process more fragments
            for upstream_id in self.fragment_graph.get_upstreams(fragment_id).keys() {
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
            return Err(ErrorCode::InternalError("graph is not a DAG".into()).into());
        }

        Ok(())
    }
}

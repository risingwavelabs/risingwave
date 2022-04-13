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

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::ops::Range;
use std::sync::Arc;

use itertools::Itertools;
use risingwave_common::error::{ErrorCode, Result, RwError};
use risingwave_pb::meta::table_fragments::fragment::{FragmentDistributionType, FragmentType};
use risingwave_pb::meta::table_fragments::Fragment;
use risingwave_pb::stream_plan::stream_node::Node;
use risingwave_pb::stream_plan::{Dispatcher, DispatcherType, StreamNode};

use super::graph::StreamFragmentEdge;
use super::{CreateMaterializedViewContext, FragmentManagerRef};
use crate::cluster::ParallelUnitId;
use crate::manager::{IdCategory, IdGeneratorManagerRef};
use crate::model::{FragmentId, LocalActorId, LocalFragmentId};
use crate::storage::MetaStore;
use crate::stream::graph::{
    StreamActorBuilder, StreamFragment, StreamFragmentGraph, StreamGraphBuilder,
};

/// [`StreamFragmenter`] generates the proto for interconnected actors for a streaming pipeline.
pub struct StreamFragmenter<S> {
    /// fragment graph field, transformed from input streaming plan.
    fragment_graph: StreamFragmentGraph,

    /// stream graph builder, to build streaming DAG.
    stream_graph: StreamGraphBuilder<S>,

    /// id generator, used to generate actor id.
    id_gen_manager: IdGeneratorManagerRef<S>,

    /// hash mapping, used for hash dispatcher
    hash_mapping: Vec<ParallelUnitId>,

    /// local fragment id
    next_local_fragment_id: u32,

    /// local actor id
    next_local_actor_id: u32,

    /// when converting fragment graph to actor graph, we need to know which actors belong to a
    /// fragment.
    fragment_actors: HashMap<LocalFragmentId, Vec<LocalActorId>>,
}

impl<S> StreamFragmenter<S>
where
    S: MetaStore,
{
    pub fn new(
        id_gen_manager: IdGeneratorManagerRef<S>,
        fragment_manager: FragmentManagerRef<S>,
        hash_mapping: Vec<ParallelUnitId>,
    ) -> Self {
        Self {
            fragment_graph: StreamFragmentGraph::new(),
            stream_graph: StreamGraphBuilder::new(fragment_manager),
            id_gen_manager,
            hash_mapping,
            next_local_fragment_id: 0,
            next_local_actor_id: 0,
            fragment_actors: HashMap::new(),
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
        // Generate fragment graph and seal
        self.generate_fragment_graph(stream_node)?;
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

        let stream_graph = self.stream_graph.build(ctx, start_actor_id, actor_len)?;

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

    /// Generate fragment DAG from input streaming plan by their dependency.
    fn generate_fragment_graph(&mut self, stream_node: &StreamNode) -> Result<()> {
        self.build_and_add_fragment(stream_node)?;
        Ok(())
    }

    /// Use the given `stream_node` to create a fragment and add it to graph.
    fn build_and_add_fragment(&mut self, stream_node: &StreamNode) -> Result<StreamFragment> {
        let mut fragment = self.new_stream_fragment(stream_node.clone());
        self.build_fragment(&mut fragment, stream_node)?;
        self.fragment_graph.add_fragment(fragment.clone());
        Ok(fragment)
    }

    /// Build new fragment and link dependencies by visiting children recursively, update
    /// `is_singleton` and `fragment_type` properties for current fragment.
    // TODO: Should we store the concurrency in StreamFragment directly?
    fn build_fragment(
        &mut self,
        current_fragment: &mut StreamFragment,
        stream_node: &StreamNode,
    ) -> Result<()> {
        // Update current fragment based on the node we're visiting.
        match stream_node.get_node()? {
            Node::SourceNode(_) => current_fragment.fragment_type = FragmentType::Source,

            Node::MaterializeNode(_) => current_fragment.fragment_type = FragmentType::Sink,

            // TODO: Force singleton for TopN as a workaround. We should implement two phase TopN.
            Node::TopNNode(_) => current_fragment.is_singleton = true,

            // TODO: Force Chain to be singleton as a workaround. Remove this if parallel Chain is
            // supported
            Node::ChainNode(_) => current_fragment.is_singleton = true,

            _ => {}
        };

        // Visit plan children.
        for child_node in stream_node.get_input() {
            match child_node.get_node()? {
                // Exchange node indicates a new child fragment.
                Node::ExchangeNode(exchange_node) => {
                    let child_fragment = self.build_and_add_fragment(child_node)?;
                    self.fragment_graph.add_edge(
                        child_fragment.fragment_id,
                        current_fragment.fragment_id,
                        StreamFragmentEdge {
                            dispatch_strategy: exchange_node.get_strategy()?.clone(),
                            same_worker_node: false,
                        },
                    );

                    let is_simple_dispatcher =
                        exchange_node.get_strategy()?.get_type()? == DispatcherType::Simple;
                    if is_simple_dispatcher {
                        current_fragment.is_singleton = true;
                    }
                }

                // For other children, visit recursively.
                _ => {
                    self.build_fragment(current_fragment, child_node)?;
                }
            };
        }

        Ok(())
    }

    /// Create a new stream fragment with given node with generating a fragment id.
    fn new_stream_fragment(&mut self, node: StreamNode) -> StreamFragment {
        let fragment = StreamFragment::new(
            LocalFragmentId::Local(self.next_local_fragment_id),
            Arc::new(node),
        );

        self.next_local_fragment_id += 1;

        fragment
    }

    /// Generate actor id from id generator.
    fn gen_actor_ids(&mut self, parallel_degree: u32) -> Range<u32> {
        let start_actor_id = self.next_local_actor_id;
        self.next_local_actor_id += parallel_degree;

        start_actor_id..start_actor_id + parallel_degree
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
            self.hash_mapping.iter().unique().count() as u32
        };

        let node = &current_fragment.node;
        let actor_ids = self
            .gen_actor_ids(parallel_degree)
            .into_iter()
            .map(LocalActorId::Local)
            .collect_vec();

        // TODO: in the future, there might be multiple dispatchers and multiple set of downstream
        // actors.
        let downstream_fragments = self
            .fragment_graph
            .get_downstreams(fragment_id)
            .iter()
            .map(|(id, _)| *id)
            .collect_vec();

        let downstream_actors;
        let exchange_node_id;
        let dispatcher;

        match downstream_fragments.as_slice() {
            [] => {
                downstream_actors = vec![];
                exchange_node_id = None;
                dispatcher = Dispatcher {
                    r#type: DispatcherType::Broadcast as i32,
                    ..Default::default()
                };
            }
            [fragment] => {
                downstream_actors = self
                    .fragment_actors
                    .get(fragment)
                    .expect("downstream fragment not processed yet")
                    .clone();

                dispatcher = match node.get_node()? {
                    Node::ExchangeNode(exchange_node) => {
                        exchange_node_id = Some(node.operator_id);
                        let strategy = exchange_node.get_strategy()?;
                        Dispatcher {
                            r#type: strategy.r#type,
                            column_indices: strategy.column_indices.clone(),
                            ..Default::default()
                        }
                    }
                    _ => panic!("expect exchange node or sink node at the top of the plan"),
                };
            }
            _ => todo!("there should not be multiple downstreams in plan"),
        };

        for id in &actor_ids {
            let actor_builder = StreamActorBuilder::new(*id, fragment_id, node.clone());
            self.stream_graph.add_actor(actor_builder);
        }

        let streaming_hash_mapping;

        // Construct a consistent hash mapping of actors based on that of parallel units. Set
        // the new mapping into hash dispatchers.
        let dispatcher = if dispatcher.r#type == DispatcherType::Hash as i32 {
            // Theoretically, a hash dispatcher should have `self.hash_parallel_count` as the
            // number of its downstream actors. However, since the frontend optimizer is still
            // WIP, there exists some unoptimized situation where a hash dispatcher has ONLY
            // ONE downstream actor, which makes it behave like a simple dispatcher. As an
            // expedient, we specially compute the consistent hash mapping here. The `if`
            // branch could be removed after the optimizer has been fully implemented.

            streaming_hash_mapping = if downstream_actors.len() == 1 {
                Some(vec![downstream_actors[0]; self.hash_mapping.len()])
            } else {
                let hash_parallel_units = self.hash_mapping.iter().unique().collect_vec();
                assert_eq!(downstream_actors.len(), hash_parallel_units.len());
                let parallel_unit_actor_map: HashMap<_, _> = hash_parallel_units
                    .into_iter()
                    .zip_eq(downstream_actors.iter().copied())
                    .collect();
                Some(
                    self.hash_mapping
                        .iter()
                        .map(|parallel_unit_id| parallel_unit_actor_map[parallel_unit_id])
                        .collect_vec(),
                )
            };

            Dispatcher {
                r#type: DispatcherType::Hash.into(),
                column_indices: dispatcher.column_indices,
                hash_mapping: None,
                downstream_actor_id: vec![],
            }
        } else {
            streaming_hash_mapping = None;

            dispatcher
        };

        if !downstream_actors.is_empty() {
            self.stream_graph.add_link(
                &actor_ids,
                &downstream_actors,
                exchange_node_id.unwrap(),
                dispatcher,
                streaming_hash_mapping,
            );
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

        // Iterator all fragments
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

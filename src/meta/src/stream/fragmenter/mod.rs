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

mod graph;
use graph::*;
mod rewrite;

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::ops::Range;
use std::sync::Arc;

use derivative::Derivative;
use itertools::Itertools;
use risingwave_common::catalog::TableId;
use risingwave_common::error::{ErrorCode, Result};
use risingwave_pb::meta::table_fragments::fragment::{FragmentDistributionType, FragmentType};
use risingwave_pb::meta::table_fragments::Fragment;
use risingwave_pb::plan_common::JoinType;
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_plan::{
    DispatchStrategy, Dispatcher, DispatcherType, ExchangeNode, StreamNode,
};

use super::{CreateMaterializedViewContext, FragmentManagerRef};
use crate::manager::{IdCategory, IdGeneratorManagerRef};
use crate::model::{FragmentId, LocalActorId, LocalFragmentId};
use crate::storage::MetaStore;

/// [`StreamFragmenter`] generates the proto for interconnected actors for a streaming pipeline.
pub struct StreamFragmenter {
    /// degree of parallelism
    parallel_degree: u32,
    // TODO: remove this when we deprecate Java frontend.
    is_legacy_frontend: bool,
}

/// The mutable state when building fragment graph.
#[derive(Derivative)]
#[derivative(Default)]
struct BuildFragmentGraphState {
    /// fragment graph field, transformed from input streaming plan.
    fragment_graph: StreamFragmentGraph,
    /// local fragment id
    next_local_fragment_id: u32,

    /// Next local table id to be allocated. It equals to total table ids cnt when finish stream
    /// node traversing.
    next_table_id: u32,

    /// rewrite will produce new operators, and we need to track next operator id
    #[derivative(Default(value = "u32::MAX - 1"))]
    next_operator_id: u32,

    /// dependent table ids
    dependent_table_ids: HashSet<TableId>,
}

impl BuildFragmentGraphState {
    /// Create a new stream fragment with given node with generating a fragment id.
    fn new_stream_fragment(&mut self) -> StreamFragment {
        let fragment = StreamFragment::new(LocalFragmentId::Local(self.next_local_fragment_id));
        self.next_local_fragment_id += 1;
        fragment
    }

    /// Generate an operator id
    fn gen_operator_id(&mut self) -> u32 {
        self.next_operator_id -= 1;
        self.next_operator_id
    }

    /// Generate an table id
    fn gen_table_id(&mut self) -> u32 {
        let ret = self.next_table_id;
        self.next_table_id += 1;
        ret
    }
}

/// The mutable state when building actor graph.
#[derive(Default)]
struct BuildActorGraphState {
    /// stream graph builder, to build streaming DAG.
    stream_graph_builder: StreamGraphBuilder,
    /// when converting fragment graph to actor graph, we need to know which actors belong to a
    /// fragment.
    fragment_actors: HashMap<LocalFragmentId, Vec<LocalActorId>>,
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

impl StreamFragmenter {
    pub async fn generate_graph<S>(
        id_gen_manager: IdGeneratorManagerRef<S>,
        fragment_manager: FragmentManagerRef<S>,
        parallel_degree: u32,
        is_legacy_frontend: bool,
        stream_node: &StreamNode,
        ctx: &mut CreateMaterializedViewContext,
    ) -> Result<BTreeMap<FragmentId, Fragment>>
    where
        S: MetaStore,
    {
        Self {
            parallel_degree,
            is_legacy_frontend,
        }
        .generate_graph_inner(id_gen_manager, fragment_manager, stream_node, ctx)
        .await
    }

    /// Build a stream graph in two steps:
    ///
    /// 1. Break the streaming plan into fragments with their dependency.
    /// 2. Duplicate each fragment as parallel actors.
    async fn generate_graph_inner<S>(
        self,
        id_gen_manager: IdGeneratorManagerRef<S>,
        fragment_manager: FragmentManagerRef<S>,
        stream_node: &StreamNode,
        ctx: &mut CreateMaterializedViewContext,
    ) -> Result<BTreeMap<FragmentId, Fragment>>
    where
        S: MetaStore,
    {
        // The stream node might be rewritten in `generate_fragment_graph`.
        // So we `clone` and move it to prevent it from being used later.
        let stream_node = stream_node.clone();
        let fragment_graph = {
            let BuildFragmentGraphState {
                mut fragment_graph,
                next_local_fragment_id,
                next_operator_id: _,
                dependent_table_ids,
                next_table_id: next_local_table_id,
            } = {
                let mut state = BuildFragmentGraphState::default();
                self.generate_fragment_graph(&mut state, stream_node)?;
                state
            };

            // save dependent table ids in ctx
            ctx.dependent_table_ids = dependent_table_ids;

            let fragment_len = fragment_graph.fragment_len() as u32;
            assert_eq!(fragment_len, next_local_fragment_id);
            let offset = id_gen_manager
                .generate_interval::<{ IdCategory::Fragment }>(fragment_len as i32)
                .await? as _;

            // Compute how many table ids should be allocated for all actors.
            // Allocate all needed table ids for current MV.
            let table_ids_cnt = next_local_table_id;
            let start_table_id = id_gen_manager
                .generate_interval::<{ IdCategory::Table }>(table_ids_cnt as i32)
                .await? as _;
            ctx.table_id_offset = start_table_id;

            fragment_graph.seal(offset);
            fragment_graph
        };

        let stream_graph = {
            let BuildActorGraphState {
                stream_graph_builder,
                fragment_actors: _,
                next_local_actor_id,
            } = {
                let mut state = BuildActorGraphState::default();
                // resolve upstream table infos first
                // TODO: this info is only used by `resolve_chain_node`. We can move that logic to
                // stream manager and remove dependency on fragment manager.
                let info = fragment_manager
                    .get_build_graph_info(&ctx.dependent_table_ids)
                    .await?;
                state.stream_graph_builder.fill_info(info);

                // Generate actors of the streaming plan
                self.build_actor_graph(&mut state, &fragment_graph)?;
                state
            };

            // generates global ids
            let (actor_len, start_actor_id) = {
                let actor_len = stream_graph_builder.actor_len() as u32;
                assert_eq!(actor_len, next_local_actor_id);
                let start_actor_id = id_gen_manager
                    .generate_interval::<{ IdCategory::Actor }>(actor_len as i32)
                    .await? as _;

                (actor_len, start_actor_id)
            };

            stream_graph_builder.build(ctx, start_actor_id, actor_len)?
        };

        // Serialize the graph
        let stream_graph = stream_graph
            .into_iter()
            .map(|(fragment_id, actors)| {
                let fragment = fragment_graph.get_fragment(fragment_id).unwrap();
                let fragment_id = fragment_id.as_global_id();
                (
                    fragment_id,
                    Fragment {
                        fragment_id,
                        fragment_type: fragment.fragment_type as i32,
                        distribution_type: if fragment.is_singleton {
                            FragmentDistributionType::Single
                        } else {
                            FragmentDistributionType::Hash
                        } as i32,
                        actors,
                    },
                )
            })
            .collect();
        Ok(stream_graph)
    }

    /// Do some dirty rewrites on meta. Currently, it will split stateful operators into two
    /// fragments.
    fn rewrite_stream_node(
        &self,
        state: &mut BuildFragmentGraphState,
        stream_node: StreamNode,
    ) -> Result<StreamNode> {
        self.rewrite_stream_node_inner(state, stream_node, false)
    }

    fn rewrite_stream_node_inner(
        &self,
        state: &mut BuildFragmentGraphState,
        stream_node: StreamNode,
        insert_exchange_flag: bool,
    ) -> Result<StreamNode> {
        let mut inputs = vec![];

        for child_node in stream_node.input {
            let input = match child_node.get_node_body()? {
                // For stateful operators, set `exchange_flag = true`. If it's already true, force
                // add an exchange.
                NodeBody::HashAgg(_) | NodeBody::HashJoin(_) | NodeBody::DeltaIndexJoin(_) => {
                    // We didn't make `fields` available on Java frontend yet, so we check if schema
                    // is available (by `child_node.fields.is_empty()`) before deciding to do the
                    // rewrite.
                    if insert_exchange_flag && !child_node.fields.is_empty() {
                        let child_node =
                            self.rewrite_stream_node_inner(state, child_node, false)?;

                        let strategy = DispatchStrategy {
                            r#type: DispatcherType::NoShuffle.into(),
                            column_indices: vec![],
                        };
                        let append_only = child_node.append_only;
                        StreamNode {
                            pk_indices: child_node.pk_indices.clone(),
                            fields: child_node.fields.clone(),
                            node_body: Some(NodeBody::Exchange(ExchangeNode {
                                strategy: Some(strategy.clone()),
                            })),
                            operator_id: state.gen_operator_id() as u64,
                            input: vec![child_node],
                            identity: "Exchange (NoShuffle)".to_string(),
                            append_only,
                        }
                    } else {
                        self.rewrite_stream_node_inner(state, child_node, true)?
                    }
                }
                // For exchanges, reset the flag.
                NodeBody::Exchange(_) => {
                    self.rewrite_stream_node_inner(state, child_node, false)?
                }
                // Otherwise, recursively visit the children.
                _ => self.rewrite_stream_node_inner(state, child_node, insert_exchange_flag)?,
            };
            inputs.push(input);
        }

        Ok(StreamNode {
            input: inputs,
            ..stream_node
        })
    }

    /// Generate fragment DAG from input streaming plan by their dependency.
    fn generate_fragment_graph(
        &self,
        state: &mut BuildFragmentGraphState,
        stream_node: StreamNode,
    ) -> Result<()> {
        let stream_node = self.rewrite_stream_node(state, stream_node)?;
        self.build_and_add_fragment(state, stream_node)?;
        Ok(())
    }

    /// Use the given `stream_node` to create a fragment and add it to graph.
    fn build_and_add_fragment(
        &self,
        state: &mut BuildFragmentGraphState,
        stream_node: StreamNode,
    ) -> Result<StreamFragment> {
        let mut fragment = state.new_stream_fragment();
        let node = self.build_fragment(state, &mut fragment, stream_node)?;
        fragment.seal_node(node);
        state.fragment_graph.add_fragment(fragment.clone());
        Ok(fragment)
    }

    /// Build new fragment and link dependencies by visiting children recursively, update
    /// `is_singleton` and `fragment_type` properties for current fragment. While traversing the
    /// tree, count how many table ids should be allocated in this fragment.
    // TODO: Should we store the concurrency in StreamFragment directly?
    fn build_fragment(
        &self,
        state: &mut BuildFragmentGraphState,
        current_fragment: &mut StreamFragment,
        mut stream_node: StreamNode,
    ) -> Result<StreamNode> {
        // Update current fragment based on the node we're visiting.
        match stream_node.get_node_body()? {
            NodeBody::Source(_) => current_fragment.fragment_type = FragmentType::Source,

            NodeBody::Materialize(_) => current_fragment.fragment_type = FragmentType::Sink,

            // TODO: Force singleton for TopN as a workaround. We should implement two phase TopN.
            NodeBody::TopN(_) => current_fragment.is_singleton = true,

            NodeBody::Chain(ref node) => {
                // TODO: Remove this when we deprecate Java frontend.
                current_fragment.is_singleton = self.is_legacy_frontend;

                // memorize table id for later use
                state
                    .dependent_table_ids
                    .insert(TableId::from(&node.table_ref_id));
            }

            _ => {}
        };

        // For HashJoin nodes, attempting to rewrite to delta joins only on inner join
        // with only equal conditions
        if let NodeBody::HashJoin(hash_join_node) = stream_node.node_body.as_mut().unwrap() {
            // Allocate local table id. It will be rewrite to global table id after get table id
            // offset from id generator.
            hash_join_node.left_table_id = state.gen_table_id();
            hash_join_node.right_table_id = state.gen_table_id();
            if hash_join_node.is_delta_join {
                if hash_join_node.get_join_type()? == JoinType::Inner
                    && hash_join_node.condition.is_none()
                {
                    return self.build_delta_join(state, current_fragment, stream_node);
                } else {
                    panic!(
                        "only inner join without non-equal condition is supported for delta joins"
                    );
                }
            }
        }

        if let NodeBody::DeltaIndexJoin(delta_index_join) = stream_node.node_body.as_mut().unwrap()
        {
            if delta_index_join.get_join_type()? == JoinType::Inner
                && delta_index_join.condition.is_none()
            {
                return self.build_delta_join_without_arrange(state, current_fragment, stream_node);
            } else {
                panic!("only inner join without non-equal condition is supported for delta joins");
            }
        }

        // Rewrite hash agg. One agg call -> one table id.
        if let NodeBody::HashAgg(hash_agg_node) = stream_node.node_body.as_mut().unwrap() {
            for _ in &hash_agg_node.agg_calls {
                hash_agg_node.table_ids.push(state.gen_table_id());
            }
        }

        match stream_node.node_body.as_mut().unwrap() {
            NodeBody::GlobalSimpleAgg(node) | NodeBody::LocalSimpleAgg(node) => {
                for _ in &node.agg_calls {
                    node.table_ids.push(state.gen_table_id());
                }
            }
            _ => {}
        }

        let inputs = std::mem::take(&mut stream_node.input);
        // Visit plan children.
        let inputs = inputs
            .into_iter()
            .map(|mut child_node| -> Result<StreamNode> {
                match child_node.get_node_body()? {
                    NodeBody::Exchange(_) if child_node.input.is_empty() => {
                        // When exchange node is generated when doing rewrites, it could be having
                        // zero input. In this case, we won't recursively
                        // visit its children.
                        Ok(child_node)
                    }
                    // Exchange node indicates a new child fragment.
                    NodeBody::Exchange(exchange_node) => {
                        let exchange_node = exchange_node.clone();

                        assert_eq!(child_node.input.len(), 1);
                        let child_fragment =
                            self.build_and_add_fragment(state, child_node.input.remove(0))?;
                        state.fragment_graph.add_edge(
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
                    _ => self.build_fragment(state, current_fragment, child_node),
                }
            })
            .try_collect()?;

        stream_node.input = inputs;

        Ok(stream_node)
    }

    fn build_actor_graph_fragment(
        &self,
        fragment_id: LocalFragmentId,
        state: &mut BuildActorGraphState,
        fragment_graph: &StreamFragmentGraph,
    ) -> Result<()> {
        let current_fragment = fragment_graph.get_fragment(fragment_id).unwrap().clone();

        let parallel_degree = if current_fragment.is_singleton {
            1
        } else {
            self.parallel_degree
        };

        let node = Arc::new(current_fragment.get_node().clone());
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

            match dispatch_edge.dispatch_strategy.get_type()? {
                ty @ (DispatcherType::Hash
                | DispatcherType::Simple
                | DispatcherType::Broadcast
                | DispatcherType::NoShuffle) => {
                    state.stream_graph_builder.add_link(
                        &actor_ids,
                        downstream_actors,
                        dispatch_edge.link_id,
                        Dispatcher {
                            r#type: ty.into(),
                            column_indices: dispatch_edge.dispatch_strategy.column_indices.clone(),
                            hash_mapping: None,
                            dispatcher_id: dispatch_edge.link_id,
                            downstream_actor_id: vec![],
                        },
                        dispatch_edge.same_worker_node,
                    );
                }
                DispatcherType::Invalid => unreachable!(),
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

    /// Build actor graph from fragment graph using topological sort. Setup dispatcher in actor and
    /// generate actors by their parallelism.
    fn build_actor_graph(
        &self,
        state: &mut BuildActorGraphState,
        fragment_graph: &StreamFragmentGraph,
    ) -> Result<()> {
        // Use topological sort to build the graph from downstream to upstream. (The first fragment
        // poped out from the heap will be the top-most node in plan, or the sink in stream graph.)
        let mut actionable_fragment_id = VecDeque::new();
        let mut downstream_cnts = HashMap::new();

        // Iterate all fragments
        for (fragment_id, _) in fragment_graph.fragments().iter() {
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
            self.build_actor_graph_fragment(fragment_id, state, fragment_graph)?;

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
            return Err(ErrorCode::InternalError("graph is not a DAG".into()).into());
        }

        Ok(())
    }
}

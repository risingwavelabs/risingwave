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
use risingwave_pb::stream_plan::stream_node::NodeBody;
mod rewrite;

use std::collections::HashSet;

use derivative::Derivative;
use risingwave_common::catalog::TableId;
use risingwave_common::error::Result;
use risingwave_pb::plan_common::JoinType;
use risingwave_pb::stream_plan::{
    DispatchStrategy, DispatcherType, ExchangeNode, FragmentType,
    StreamFragmentGraph as StreamFragmentGraphProto, StreamNode,
};

use self::rewrite::build_delta_join_without_arrange;
use crate::optimizer::PlanRef;

/// The mutable state when building fragment graph.
#[derive(Derivative)]
#[derivative(Default)]
pub struct BuildFragmentGraphState {
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
        let fragment = StreamFragment::new(self.next_local_fragment_id);
        self.next_local_fragment_id += 1;
        fragment
    }

    /// Generate an operator id
    fn gen_operator_id(&mut self) -> u32 {
        self.next_operator_id -= 1;
        self.next_operator_id
    }

    /// Generate an table id
    pub fn gen_table_id(&mut self) -> u32 {
        let ret = self.next_table_id;
        self.next_table_id += 1;
        ret
    }

    /// Generate an table id
    pub fn gen_table_id_wrapped(&mut self) -> TableId {
        TableId::new(self.gen_table_id())
    }
}

pub fn build_graph(plan_node: PlanRef) -> StreamFragmentGraphProto {
    let mut state = BuildFragmentGraphState::default();
    let stream_node = plan_node.to_stream_prost(&mut state);
    generate_fragment_graph(&mut state, stream_node).unwrap();
    let mut fragment_graph = state.fragment_graph.to_protobuf();
    fragment_graph.dependent_table_ids = state
        .dependent_table_ids
        .into_iter()
        .map(|id| id.table_id)
        .collect();
    fragment_graph.table_ids_cnt = state.next_table_id;
    fragment_graph
}

fn is_stateful_executor(stream_node: &StreamNode) -> bool {
    matches!(
        stream_node.get_node_body().unwrap(),
        NodeBody::HashAgg(_)
            | NodeBody::HashJoin(_)
            | NodeBody::DeltaIndexJoin(_)
            | NodeBody::Chain(_)
            | NodeBody::DynamicFilter(_)
    )
}

/// Do some dirty rewrites on meta. Currently, it will split stateful operators into two
/// fragments.
fn rewrite_stream_node(
    state: &mut BuildFragmentGraphState,
    stream_node: StreamNode,
    insert_exchange_flag: bool,
) -> Result<StreamNode> {
    let f = |child| {
        // For stateful operators, set `exchange_flag = true`. If it's already true,
        // force add an exchange.
        if is_stateful_executor(&child) {
            if insert_exchange_flag {
                let child_node = rewrite_stream_node(state, child, true)?;

                let strategy = DispatchStrategy {
                    r#type: DispatcherType::NoShuffle.into(),
                    column_indices: vec![], // TODO: use distribution key
                };
                Ok(StreamNode {
                    stream_key: child_node.stream_key.clone(),
                    fields: child_node.fields.clone(),
                    node_body: Some(NodeBody::Exchange(ExchangeNode {
                        strategy: Some(strategy),
                    })),
                    operator_id: state.gen_operator_id() as u64,
                    append_only: child_node.append_only,
                    input: vec![child_node],
                    identity: "Exchange (NoShuffle)".to_string(),
                })
            } else {
                rewrite_stream_node(state, child, true)
            }
        } else {
            match child.get_node_body()? {
                // For exchanges, reset the flag.
                NodeBody::Exchange(_) => rewrite_stream_node(state, child, false),
                // Otherwise, recursively visit the children.
                _ => rewrite_stream_node(state, child, insert_exchange_flag),
            }
        }
    };
    Ok(StreamNode {
        input: stream_node
            .input
            .into_iter()
            .map(f)
            .collect::<Result<_>>()?,
        ..stream_node
    })
}

/// Generate fragment DAG from input streaming plan by their dependency.
fn generate_fragment_graph(
    state: &mut BuildFragmentGraphState,
    stream_node: StreamNode,
) -> Result<()> {
    let stateful = is_stateful_executor(&stream_node);
    let stream_node = rewrite_stream_node(state, stream_node, stateful)?;
    build_and_add_fragment(state, stream_node)?;
    Ok(())
}

/// Use the given `stream_node` to create a fragment and add it to graph.
pub(self) fn build_and_add_fragment(
    state: &mut BuildFragmentGraphState,
    stream_node: StreamNode,
) -> Result<StreamFragment> {
    let mut fragment = state.new_stream_fragment();
    let node = build_fragment(state, &mut fragment, stream_node)?;

    assert!(fragment.node.is_none());
    fragment.node = Some(Box::new(node));

    state.fragment_graph.add_fragment(fragment.clone());
    Ok(fragment)
}

/// Build new fragment and link dependencies by visiting children recursively, update
/// `is_singleton` and `fragment_type` properties for current fragment. While traversing the
/// tree, count how many table ids should be allocated in this fragment.
// TODO: Should we store the concurrency in StreamFragment directly?
fn build_fragment(
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

        // FIXME: workaround for single-fragment mview on singleton upstream mview.
        NodeBody::Chain(node) => {
            // memorize table id for later use
            state
                .dependent_table_ids
                .insert(TableId::new(node.table_id));
            current_fragment.upstream_table_ids.push(node.table_id);
            current_fragment.is_singleton = node.is_singleton;
        }

        _ => {}
    };

    // handle join logic
    if let NodeBody::DeltaIndexJoin(delta_index_join) = stream_node.node_body.as_mut().unwrap() {
        if delta_index_join.get_join_type()? == JoinType::Inner
            && delta_index_join.condition.is_none()
        {
            return build_delta_join_without_arrange(state, current_fragment, stream_node);
        } else {
            panic!("only inner join without non-equal condition is supported for delta joins");
        }
    }

    // Visit plan children.
    stream_node.input = stream_node
        .input
        .into_iter()
        .map(|mut child_node| {
            match child_node.get_node_body()? {
                // When exchange node is generated when doing rewrites, it could be having
                // zero input. In this case, we won't recursively visit its children.
                NodeBody::Exchange(_) if child_node.input.is_empty() => Ok(child_node),
                // Exchange node indicates a new child fragment.
                NodeBody::Exchange(exchange_node) => {
                    let exchange_node_strategy = exchange_node.get_strategy()?.clone();

                    let is_simple_dispatcher =
                        exchange_node_strategy.get_type()? == DispatcherType::Simple;

                    let [input]: [_; 1] = std::mem::take(&mut child_node.input).try_into().unwrap();
                    let child_fragment = build_and_add_fragment(state, input)?;
                    state.fragment_graph.add_edge(
                        child_fragment.fragment_id,
                        current_fragment.fragment_id,
                        StreamFragmentEdge {
                            dispatch_strategy: exchange_node_strategy,
                            same_worker_node: false,
                            link_id: child_node.operator_id,
                        },
                    );

                    if is_simple_dispatcher {
                        current_fragment.is_singleton = true;
                    }
                    Ok(child_node)
                }

                // For other children, visit recursively.
                _ => build_fragment(state, current_fragment, child_node),
            }
        })
        .collect::<Result<_>>()?;
    Ok(stream_node)
}

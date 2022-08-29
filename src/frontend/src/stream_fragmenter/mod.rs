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
use itertools::Itertools;
use risingwave_common::catalog::TableId;
use risingwave_common::error::Result;
use risingwave_pb::plan_common::JoinType;
use risingwave_pb::stream_plan::{
    DispatchStrategy, DispatcherType, ExchangeNode, FragmentType,
    StreamFragmentGraph as StreamFragmentGraphProto, StreamNode,
};

use crate::optimizer::PlanRef;

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
    fn gen_table_id(&mut self) -> u32 {
        let ret = self.next_table_id;
        self.next_table_id += 1;
        ret
    }
}

pub fn build_graph(plan_node: PlanRef) -> StreamFragmentGraphProto {
    let stream_node = plan_node.to_stream_prost();
    let BuildFragmentGraphState {
        fragment_graph,
        next_local_fragment_id: _,
        next_operator_id: _,
        dependent_table_ids,
        next_table_id,
    } = {
        let mut state = BuildFragmentGraphState::default();
        StreamFragmenter::generate_fragment_graph(&mut state, stream_node).unwrap();
        state
    };

    let mut fragment_graph = fragment_graph.to_protobuf();
    fragment_graph.dependent_table_ids = dependent_table_ids
        .into_iter()
        .map(|id| id.table_id)
        .collect();
    fragment_graph.table_ids_cnt = next_table_id;
    fragment_graph
}

pub struct StreamFragmenter {}

impl StreamFragmenter {
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
            if Self::is_stateful_executor(&child) {
                if insert_exchange_flag {
                    let child_node = Self::rewrite_stream_node(state, child, true)?;

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
                    Self::rewrite_stream_node(state, child, true)
                }
            } else {
                match child.get_node_body()? {
                    // For exchanges, reset the flag.
                    NodeBody::Exchange(_) => Self::rewrite_stream_node(state, child, false),
                    // Otherwise, recursively visit the children.
                    _ => Self::rewrite_stream_node(state, child, insert_exchange_flag),
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
        let stateful = Self::is_stateful_executor(&stream_node);
        let stream_node = Self::rewrite_stream_node(state, stream_node, stateful)?;
        Self::build_and_add_fragment(state, stream_node)?;
        Ok(())
    }

    /// Use the given `stream_node` to create a fragment and add it to graph.
    fn build_and_add_fragment(
        state: &mut BuildFragmentGraphState,
        stream_node: StreamNode,
    ) -> Result<StreamFragment> {
        let mut fragment = state.new_stream_fragment();
        let node = Self::build_fragment(state, &mut fragment, stream_node)?;

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

        Self::assign_local_table_id_to_stream_node(state, &mut stream_node);

        // handle join logic
        if let NodeBody::DeltaIndexJoin(delta_index_join) = stream_node.node_body.as_mut().unwrap()
        {
            if delta_index_join.get_join_type()? == JoinType::Inner
                && delta_index_join.condition.is_none()
            {
                return Self::build_delta_join_without_arrange(
                    state,
                    current_fragment,
                    stream_node,
                );
            } else {
                panic!("only inner join without non-equal condition is supported for delta joins");
            }
        }

        // Visit plan children.
        stream_node.input = stream_node
            .input
            .into_iter()
            .map(|mut child_node| -> Result<StreamNode> {
                match child_node.get_node_body()? {
                    // When exchange node is generated when doing rewrites, it could be having
                    // zero input. In this case, we won't recursively visit its children.
                    NodeBody::Exchange(_) if child_node.input.is_empty() => Ok(child_node),
                    // Exchange node indicates a new child fragment.
                    NodeBody::Exchange(exchange_node) => {
                        let exchange_node_strategy = exchange_node.get_strategy()?.clone();

                        let is_simple_dispatcher =
                            exchange_node_strategy.get_type()? == DispatcherType::Simple;

                        assert_eq!(child_node.input.len(), 1);
                        let child_fragment =
                            Self::build_and_add_fragment(state, child_node.input.remove(0))?;
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
                    _ => Self::build_fragment(state, current_fragment, child_node),
                }
            })
            .try_collect()?;
        Ok(stream_node)
    }

    /// This function assigns the `table_id` based on the type of `StreamNode`
    /// Be careful it has side effects and will change the `StreamNode`
    fn assign_local_table_id_to_stream_node(
        state: &mut BuildFragmentGraphState,
        stream_node: &mut StreamNode,
    ) {
        match stream_node.node_body.as_mut().unwrap() {
            // For HashJoin nodes, attempting to rewrite to delta joins only on inner join
            // with only equal conditions
            NodeBody::HashJoin(hash_join_node) => {
                // Allocate local table id. It will be rewrite to global table id after get table id
                // offset from id generator.
                hash_join_node.left_table.as_mut().unwrap().id = state.gen_table_id();
                hash_join_node.right_table.as_mut().unwrap().id = state.gen_table_id();
                hash_join_node.left_degree_table.as_mut().unwrap().id = state.gen_table_id();
                hash_join_node.right_degree_table.as_mut().unwrap().id = state.gen_table_id();
            }

            NodeBody::Source(node) => {
                node.state_table_id = state.gen_table_id();
            }

            NodeBody::GlobalSimpleAgg(node) => {
                for table in &mut node.internal_tables {
                    table.id = state.gen_table_id();
                }
            }

            // Rewrite hash agg. One agg call -> one table id.
            NodeBody::HashAgg(hash_agg_node) => {
                for table in &mut hash_agg_node.internal_tables {
                    table.id = state.gen_table_id();
                }
            }

            NodeBody::TopN(top_n_node) => {
                if let Some(table) = &mut top_n_node.table {
                    table.id = state.gen_table_id();
                } else {
                    panic!("TopNNode's table shouldn't be None");
                }
            }

            NodeBody::GroupTopN(group_top_n_node) => {
                if let Some(table) = &mut group_top_n_node.table {
                    table.id = state.gen_table_id();
                } else {
                    panic!("GroupTopNNode's table shouldn't be None");
                }
            }

            NodeBody::AppendOnlyTopN(append_only_top_n_node) => {
                append_only_top_n_node.table_id_l = state.gen_table_id();
                append_only_top_n_node.table_id_h = state.gen_table_id();
            }

            NodeBody::DynamicFilter(dynamic_filter_node) => {
                if let Some(left_table) = &mut dynamic_filter_node.left_table {
                    left_table.id = state.gen_table_id();
                }
                if let Some(right_table) = &mut dynamic_filter_node.right_table {
                    right_table.id = state.gen_table_id();
                }
            }

            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use risingwave_pb::catalog::{Table, Table as ProstTable};
    use risingwave_pb::data::data_type::TypeName;
    use risingwave_pb::data::DataType;
    use risingwave_pb::expr::agg_call::{Arg, Type};
    use risingwave_pb::expr::{AggCall, InputRefExpr};
    use risingwave_pb::plan_common::{ColumnCatalog, ColumnDesc, ColumnOrder};
    use risingwave_pb::stream_plan::*;

    use super::*;

    fn make_sum_aggcall(idx: i32) -> AggCall {
        AggCall {
            r#type: Type::Sum as i32,
            args: vec![Arg {
                input: Some(InputRefExpr { column_idx: idx }),
                r#type: Some(DataType {
                    type_name: TypeName::Int64 as i32,
                    ..Default::default()
                }),
            }],
            return_type: Some(DataType {
                type_name: TypeName::Int64 as i32,
                ..Default::default()
            }),
            distinct: false,
            order_by_fields: vec![],
            filter: None,
        }
    }

    fn make_column(column_type: TypeName, column_id: i32) -> ColumnCatalog {
        ColumnCatalog {
            column_desc: Some(ColumnDesc {
                column_type: Some(DataType {
                    type_name: column_type as i32,
                    ..Default::default()
                }),
                column_id,
                ..Default::default()
            }),
            is_hidden: false,
        }
    }

    fn make_internal_table(is_agg_value: bool) -> ProstTable {
        let mut columns = vec![make_column(TypeName::Int64, 0)];
        if !is_agg_value {
            columns.push(make_column(TypeName::Int32, 1));
        }
        ProstTable {
            id: TableId::placeholder().table_id,
            name: String::new(),
            columns,
            order_key: vec![ColumnOrder {
                index: 0,
                order_type: 2,
            }],
            stream_key: vec![2],
            ..Default::default()
        }
    }

    #[test]
    fn test_assign_local_table_id_to_stream_node() {
        // let fragmenter = StreamFragmenter {};
        let mut state = BuildFragmentGraphState::default();
        let mut expect_table_id = 0;
        state.gen_table_id(); // to consume one table_id

        {
            // test HashJoin Type
            let mut stream_node = StreamNode {
                node_body: Some(NodeBody::HashJoin(HashJoinNode {
                    left_table: Some(Table {
                        id: 0,
                        ..Default::default()
                    }),
                    right_table: Some(Table {
                        id: 0,
                        ..Default::default()
                    }),
                    ..Default::default()
                })),
                ..Default::default()
            };
            StreamFragmenter::assign_local_table_id_to_stream_node(&mut state, &mut stream_node);

            if let NodeBody::HashJoin(hash_join_node) = stream_node.node_body.as_ref().unwrap() {
                expect_table_id += 1;
                assert_eq!(
                    expect_table_id,
                    hash_join_node.left_table.as_ref().unwrap().id
                );
                expect_table_id += 1;
                assert_eq!(
                    expect_table_id,
                    hash_join_node.right_table.as_ref().unwrap().id
                );
            }
        }

        {
            // test SimpleAgg Type
            let mut stream_node = StreamNode {
                node_body: Some(NodeBody::GlobalSimpleAgg(SimpleAggNode {
                    agg_calls: vec![
                        make_sum_aggcall(0),
                        make_sum_aggcall(1),
                        make_sum_aggcall(2),
                    ],
                    internal_tables: vec![
                        make_internal_table(true),
                        make_internal_table(false),
                        make_internal_table(false),
                    ],
                    ..Default::default()
                })),
                ..Default::default()
            };
            StreamFragmenter::assign_local_table_id_to_stream_node(&mut state, &mut stream_node);

            if let NodeBody::GlobalSimpleAgg(global_simple_agg_node) =
                stream_node.node_body.as_ref().unwrap()
            {
                assert_eq!(
                    global_simple_agg_node.agg_calls.len(),
                    global_simple_agg_node.internal_tables.len()
                );
                for table in &global_simple_agg_node.internal_tables {
                    expect_table_id += 1;
                    assert_eq!(expect_table_id, table.id);
                }
            }
        }

        {
            // test HashAgg Type
            let mut stream_node = StreamNode {
                node_body: Some(NodeBody::HashAgg(HashAggNode {
                    agg_calls: vec![
                        make_sum_aggcall(0),
                        make_sum_aggcall(1),
                        make_sum_aggcall(2),
                        make_sum_aggcall(3),
                    ],
                    internal_tables: vec![
                        make_internal_table(true),
                        make_internal_table(false),
                        make_internal_table(false),
                        make_internal_table(false),
                    ],
                    ..Default::default()
                })),
                ..Default::default()
            };
            StreamFragmenter::assign_local_table_id_to_stream_node(&mut state, &mut stream_node);

            if let NodeBody::HashAgg(hash_agg_node) = stream_node.node_body.as_ref().unwrap() {
                assert_eq!(
                    hash_agg_node.agg_calls.len(),
                    hash_agg_node.internal_tables.len()
                );
                for table in &hash_agg_node.internal_tables {
                    expect_table_id += 1;
                    assert_eq!(expect_table_id, table.id);
                }
            }
        }

        {
            // test TopN Type
            let mut stream_node = StreamNode {
                node_body: Some(NodeBody::TopN(TopNNode {
                    table: Some(Table {
                        id: 0,
                        ..Default::default()
                    }),
                    ..Default::default()
                })),
                ..Default::default()
            };
            StreamFragmenter::assign_local_table_id_to_stream_node(&mut state, &mut stream_node);
            if let NodeBody::TopN(top_n_node) = stream_node.node_body.as_ref().unwrap() {
                expect_table_id += 1;
                assert_eq!(expect_table_id, top_n_node.table.as_ref().unwrap().id);
            }
        }
        {
            // test Group TopN Type
            let mut stream_node = StreamNode {
                node_body: Some(NodeBody::GroupTopN(GroupTopNNode {
                    table: Some(Table {
                        id: 0,
                        ..Default::default()
                    }),
                    ..Default::default()
                })),
                ..Default::default()
            };
            StreamFragmenter::assign_local_table_id_to_stream_node(&mut state, &mut stream_node);
            if let NodeBody::GroupTopN(node) = stream_node.node_body.as_ref().unwrap() {
                expect_table_id += 1;
                assert_eq!(expect_table_id, node.table.as_ref().unwrap().id);
            }
        }

        {
            // test AppendOnlyTopN Type
            let mut stream_node = StreamNode {
                node_body: Some(NodeBody::AppendOnlyTopN(AppendOnlyTopNNode {
                    ..Default::default()
                })),
                ..Default::default()
            };
            StreamFragmenter::assign_local_table_id_to_stream_node(&mut state, &mut stream_node);

            if let NodeBody::AppendOnlyTopN(append_only_top_n_node) =
                stream_node.node_body.as_ref().unwrap()
            {
                expect_table_id += 2;
                assert_eq!(expect_table_id, append_only_top_n_node.table_id_h);
            }
        }
    }
}

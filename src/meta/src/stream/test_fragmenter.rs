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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::vec;

use risingwave_common::catalog::{DatabaseId, SchemaId, TableId};
use risingwave_pb::catalog::Table as ProstTable;
use risingwave_pb::data::data_type::TypeName;
use risingwave_pb::data::DataType;
use risingwave_pb::expr::agg_call::{Arg, Type};
use risingwave_pb::expr::expr_node::RexNode;
use risingwave_pb::expr::expr_node::Type::{Add, GreaterThan, InputRef};
use risingwave_pb::expr::{AggCall, ExprNode, FunctionCall, InputRefExpr};
use risingwave_pb::plan_common::{ColumnCatalog, ColumnDesc, ColumnOrder, Field, OrderType};
use risingwave_pb::stream_plan::source_node::SourceType;
use risingwave_pb::stream_plan::stream_fragment_graph::{StreamFragment, StreamFragmentEdge};
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_plan::{
    ColumnMapping, DispatchStrategy, DispatcherType, ExchangeNode, FilterNode, FragmentType,
    MaterializeNode, ProjectNode, SimpleAggNode, SourceNode, StreamFragmentGraph, StreamNode,
};

use crate::manager::{FragmentManager, MetaSrvEnv};
use crate::model::TableFragments;
use crate::stream::stream_graph::ActorGraphBuilder;
use crate::stream::CreateMaterializedViewContext;
use crate::MetaResult;

fn make_inputref(idx: i32) -> ExprNode {
    ExprNode {
        expr_type: InputRef as i32,
        return_type: Some(DataType {
            type_name: TypeName::Int32 as i32,
            ..Default::default()
        }),
        rex_node: Some(RexNode::InputRef(InputRefExpr { column_idx: idx })),
    }
}

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

fn make_field(type_name: TypeName) -> Field {
    Field {
        data_type: Some(DataType {
            type_name: type_name as i32,
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn make_column_order(index: u32) -> ColumnOrder {
    ColumnOrder {
        order_type: OrderType::Ascending as i32,
        index,
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

fn make_internal_table(id: u32, is_agg_value: bool) -> ProstTable {
    let mut columns = vec![make_column(TypeName::Int64, 0)];
    if !is_agg_value {
        columns.push(make_column(TypeName::Int32, 1));
    }
    ProstTable {
        id,
        schema_id: SchemaId::placeholder() as u32,
        database_id: DatabaseId::placeholder() as u32,
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

/// [`make_stream_fragments`] build all stream fragments for SQL as follow:
/// ```sql
/// create table t (v1 int, v2 int);
/// create materialized view T_distributed as select sum(v1)+1 as V from t where v1>v2;
/// ```
fn make_stream_fragments() -> Vec<StreamFragment> {
    let mut fragments = vec![];
    // table source node
    let source_node = StreamNode {
        node_body: Some(NodeBody::Source(SourceNode {
            source_id: 1,
            column_ids: vec![1, 2, 0],
            source_type: SourceType::Table as i32,
            state_table_id: 1,
        })),
        stream_key: vec![2],
        ..Default::default()
    };
    fragments.push(StreamFragment {
        fragment_id: 2,
        node: Some(source_node),
        fragment_type: FragmentType::Source as i32,
        is_singleton: false,
        table_ids_cnt: 0,
        upstream_table_ids: vec![],
    });

    // exchange node
    let exchange_node = StreamNode {
        node_body: Some(NodeBody::Exchange(ExchangeNode {
            strategy: Some(DispatchStrategy {
                r#type: DispatcherType::Hash as i32,
                column_indices: vec![0],
            }),
        })),
        fields: vec![
            make_field(TypeName::Int32),
            make_field(TypeName::Int32),
            make_field(TypeName::Int64),
        ],
        input: vec![],
        stream_key: vec![2],
        operator_id: 1,
        identity: "ExchangeExecutor".to_string(),
        ..Default::default()
    };

    // filter node
    let function_call = FunctionCall {
        children: vec![make_inputref(0), make_inputref(1)],
    };
    let filter_node = StreamNode {
        node_body: Some(NodeBody::Filter(FilterNode {
            search_condition: Some(ExprNode {
                expr_type: GreaterThan as i32,
                return_type: Some(DataType {
                    type_name: TypeName::Boolean as i32,
                    ..Default::default()
                }),
                rex_node: Some(RexNode::FuncCall(function_call)),
            }),
        })),
        fields: vec![], // TODO: fill this later
        input: vec![exchange_node],
        stream_key: vec![0, 1],
        operator_id: 2,
        identity: "FilterExecutor".to_string(),
        ..Default::default()
    };

    // simple agg node
    let simple_agg_node = StreamNode {
        node_body: Some(NodeBody::GlobalSimpleAgg(SimpleAggNode {
            agg_calls: vec![make_sum_aggcall(0), make_sum_aggcall(1)],
            distribution_key: Default::default(),
            internal_tables: vec![make_internal_table(2, true), make_internal_table(3, false)],
            // Note: This mappings is not checked yet.
            column_mappings: vec![
                ColumnMapping { indices: vec![0] },
                ColumnMapping { indices: vec![1] },
            ],
            is_append_only: false,
        })),
        input: vec![filter_node],
        fields: vec![], // TODO: fill this later
        stream_key: vec![0, 1],
        operator_id: 3,
        identity: "GlobalSimpleAggExecutor".to_string(),
        ..Default::default()
    };

    fragments.push(StreamFragment {
        fragment_id: 1,
        node: Some(simple_agg_node),
        fragment_type: FragmentType::Others as i32,
        is_singleton: false,
        table_ids_cnt: 0,
        upstream_table_ids: vec![],
    });

    // exchange node
    let exchange_node_1 = StreamNode {
        node_body: Some(NodeBody::Exchange(ExchangeNode {
            strategy: Some(DispatchStrategy {
                r#type: DispatcherType::Simple as i32,
                ..Default::default()
            }),
        })),
        fields: vec![make_field(TypeName::Int64), make_field(TypeName::Int64)],
        input: vec![],
        stream_key: vec![0, 1],
        operator_id: 4,
        identity: "ExchangeExecutor".to_string(),
        ..Default::default()
    };

    // agg node
    let simple_agg_node_1 = StreamNode {
        node_body: Some(NodeBody::GlobalSimpleAgg(SimpleAggNode {
            agg_calls: vec![make_sum_aggcall(0), make_sum_aggcall(1)],
            distribution_key: Default::default(),
            internal_tables: vec![make_internal_table(0, true), make_internal_table(1, false)],
            // Note: This mappings is not checked yet.
            column_mappings: vec![
                ColumnMapping { indices: vec![0] },
                ColumnMapping { indices: vec![1] },
            ],
            is_append_only: false,
        })),
        fields: vec![], // TODO: fill this later
        input: vec![exchange_node_1],
        stream_key: vec![0, 1],
        operator_id: 5,
        identity: "GlobalSimpleAggExecutor".to_string(),
        ..Default::default()
    };

    // project node
    let function_call_1 = FunctionCall {
        children: vec![make_inputref(0), make_inputref(1)],
    };
    let project_node = StreamNode {
        node_body: Some(NodeBody::Project(ProjectNode {
            select_list: vec![
                ExprNode {
                    rex_node: Some(RexNode::FuncCall(function_call_1)),
                    expr_type: Add as i32,
                    return_type: Some(DataType {
                        type_name: TypeName::Int64 as i32,
                        ..Default::default()
                    }),
                },
                make_inputref(0),
                make_inputref(1),
            ],
        })),
        fields: vec![], // TODO: fill this later
        input: vec![simple_agg_node_1],
        stream_key: vec![1, 2],
        operator_id: 6,
        identity: "ProjectExecutor".to_string(),
        ..Default::default()
    };

    // mview node
    let mview_node = StreamNode {
        input: vec![project_node],
        stream_key: vec![],
        node_body: Some(NodeBody::Materialize(MaterializeNode {
            table_id: 1,
            table: Some(make_internal_table(4, true)),
            column_orders: vec![make_column_order(1), make_column_order(2)],
        })),
        fields: vec![], // TODO: fill this later
        operator_id: 7,
        identity: "MaterializeExecutor".to_string(),
        ..Default::default()
    };

    fragments.push(StreamFragment {
        fragment_id: 0,
        node: Some(mview_node),
        fragment_type: FragmentType::Sink as i32,
        is_singleton: true,
        table_ids_cnt: 0,
        upstream_table_ids: vec![],
    });

    fragments
}

fn make_fragment_edges() -> Vec<StreamFragmentEdge> {
    vec![
        StreamFragmentEdge {
            dispatch_strategy: Some(DispatchStrategy {
                r#type: DispatcherType::Simple as i32,
                column_indices: vec![],
            }),
            same_worker_node: false,
            link_id: 4,
            upstream_id: 1,
            downstream_id: 0,
        },
        StreamFragmentEdge {
            dispatch_strategy: Some(DispatchStrategy {
                r#type: DispatcherType::Hash as i32,
                column_indices: vec![0],
            }),
            same_worker_node: false,
            link_id: 1,
            upstream_id: 2,
            downstream_id: 1,
        },
    ]
}

fn make_stream_graph() -> StreamFragmentGraph {
    let fragments = make_stream_fragments();
    StreamFragmentGraph {
        fragments: HashMap::from_iter(fragments.into_iter().map(|f| (f.fragment_id, f))),
        edges: make_fragment_edges(),
        dependent_table_ids: vec![],
        table_ids_cnt: 4,
    }
}

// TODO: enable this test with madsim
#[tokio::test]
async fn test_fragmenter() -> MetaResult<()> {
    let env = MetaSrvEnv::for_test().await;
    let fragment_manager = Arc::new(FragmentManager::new(env.clone()).await?);
    let parallel_degree = 4;
    let mut ctx = CreateMaterializedViewContext::default();
    let graph = make_stream_graph();

    let mut actor_graph_builder =
        ActorGraphBuilder::new(env.id_gen_manager_ref(), &graph, parallel_degree, &mut ctx).await?;

    let graph = actor_graph_builder
        .generate_graph(env.id_gen_manager_ref(), fragment_manager, &mut ctx)
        .await?;

    let table_fragments = TableFragments::new(TableId::default(), graph);
    let actors = table_fragments.actors();
    let source_actor_ids = table_fragments.source_actor_ids();
    let sink_actor_ids = table_fragments.sink_actor_ids();
    let internal_table_ids = ctx.internal_table_ids();
    assert_eq!(actors.len(), 9);
    assert_eq!(source_actor_ids, vec![6, 7, 8, 9]);
    assert_eq!(sink_actor_ids, vec![1]);
    assert_eq!(4, internal_table_ids.len());

    let mut expected_downstream = HashMap::new();
    expected_downstream.insert(1, vec![]);
    expected_downstream.insert(2, vec![1]);
    expected_downstream.insert(3, vec![1]);
    expected_downstream.insert(4, vec![1]);
    expected_downstream.insert(5, vec![1]);
    expected_downstream.insert(6, vec![2, 3, 4, 5]);
    expected_downstream.insert(7, vec![2, 3, 4, 5]);
    expected_downstream.insert(8, vec![2, 3, 4, 5]);
    expected_downstream.insert(9, vec![2, 3, 4, 5]);

    let mut expected_upstream = HashMap::new();
    expected_upstream.insert(1, vec![2, 3, 4, 5]);
    expected_upstream.insert(2, vec![6, 7, 8, 9]);
    expected_upstream.insert(3, vec![6, 7, 8, 9]);
    expected_upstream.insert(4, vec![6, 7, 8, 9]);
    expected_upstream.insert(5, vec![6, 7, 8, 9]);
    expected_upstream.insert(6, vec![]);
    expected_upstream.insert(7, vec![]);
    expected_upstream.insert(8, vec![]);
    expected_upstream.insert(9, vec![]);

    for actor in actors {
        assert_eq!(
            expected_downstream.get(&actor.get_actor_id()).unwrap(),
            actor
                .dispatcher
                .first()
                .map_or(&vec![], |d| d.get_downstream_actor_id()),
        );
        let mut node = actor.get_nodes().unwrap();
        while !node.get_input().is_empty() {
            node = node.get_input().get(0).unwrap();
        }
        match node.get_node_body().unwrap() {
            NodeBody::Merge(merge_node) => {
                assert_eq!(
                    expected_upstream
                        .get(&actor.get_actor_id())
                        .unwrap()
                        .iter()
                        .collect::<HashSet<_>>(),
                    merge_node
                        .get_upstream_actor_id()
                        .iter()
                        .collect::<HashSet<_>>(),
                );
            }
            NodeBody::Source(_) => {
                // check nothing.
            }
            _ => {
                panic!("it should be MergeNode or SourceNode.");
            }
        }
    }

    Ok(())
}

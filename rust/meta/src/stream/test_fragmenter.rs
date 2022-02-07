use std::collections::{HashMap, HashSet};

use risingwave_common::error::Result;
use risingwave_pb::data::data_type::TypeName;
use risingwave_pb::data::DataType;
use risingwave_pb::expr::agg_call::{Arg, Type};
use risingwave_pb::expr::expr_node::RexNode;
use risingwave_pb::expr::expr_node::Type::{Add, GreaterThan, InputRef};
use risingwave_pb::expr::{AggCall, ExprNode, FunctionCall, InputRefExpr};
use risingwave_pb::plan::column_desc::ColumnEncodingType;
use risingwave_pb::plan::{
    ColumnDesc, ColumnOrder, DatabaseRefId, OrderType, SchemaRefId, TableRefId,
};
use risingwave_pb::stream_plan::dispatcher::DispatcherType;
use risingwave_pb::stream_plan::source_node::SourceType;
use risingwave_pb::stream_plan::stream_node::Node;
use risingwave_pb::stream_plan::{
    Dispatcher, ExchangeNode, FilterNode, MViewNode, ProjectNode, SimpleAggNode, SourceNode,
    StreamNode,
};

use crate::manager::MetaSrvEnv;
use crate::stream::fragmenter::StreamFragmenter;

fn make_table_ref_id(id: i32) -> TableRefId {
    TableRefId {
        schema_ref_id: Some(SchemaRefId {
            database_ref_id: Some(DatabaseRefId { database_id: 0 }),
            schema_id: 0,
        }),
        table_id: id,
    }
}

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
    }
}

fn make_column_desc(id: i32, type_name: TypeName) -> ColumnDesc {
    ColumnDesc {
        column_type: Some(DataType {
            type_name: type_name as i32,
            ..Default::default()
        }),
        column_id: id,
        encoding: ColumnEncodingType::Raw as i32,
        ..Default::default()
    }
}

fn make_column_order(idx: i32) -> ColumnOrder {
    ColumnOrder {
        order_type: OrderType::Ascending as i32,
        input_ref: Some(InputRefExpr { column_idx: idx }),
        return_type: Some(DataType {
            type_name: TypeName::Int64 as i32,
            ..Default::default()
        }),
    }
}

/// [`make_stream_node`] build a plan represent in `StreamNode` for SQL as follow:
/// ```sql
/// create table t (v1 int, v2 int);
/// create materialized view T_distributed as select sum(v1)+1 as V from t where v1>v2;
/// ```
///
/// plan:
///
/// ```ignore
///     RwStreamMaterializedView(name=[t_distributed])
///           RwStreamProject(v=[+($STREAM_NULL_BY_ROW_COUNT($0, $1), 1)], $f0=[$0], $f1=[$1])
///             RwStreamAgg(group=[{}], agg#0=[$SUM0($0)], agg#1=[SUM($1)])
///               RwStreamExchange(distribution=[RwDistributionTrait{type=SINGLETON, keys=[]}], collation=[[]])
///                 RwStreamAgg(group=[{}], agg#0=[COUNT()], agg#1=[SUM($0)])
///                   RwStreamFilter(condition=[>($0, $1)])
///                     RwStreamExchange(distribution=[RwDistributionTrait{type=HASH_DISTRIBUTED,keys=[0]}], collation=[[]])
///                       RwStreamTableSource(table=[[test_schema,t]], columns=[`v1,v2,_row_id`])
/// ```
fn make_stream_node() -> StreamNode {
    let table_ref_id = make_table_ref_id(1);
    // table source node
    let source_node = StreamNode {
        node: Some(Node::SourceNode(SourceNode {
            table_ref_id: Some(table_ref_id),
            column_ids: vec![1, 2, 0],
            source_type: SourceType::Table as i32,
        })),
        pk_indices: vec![2],
        ..Default::default()
    };

    // exchange node
    let exchange_node = StreamNode {
        node: Some(Node::ExchangeNode(ExchangeNode {
            dispatcher: Some(Dispatcher {
                r#type: DispatcherType::Hash as i32,
                column_idx: 0,
            }),
            input_column_descs: vec![
                make_column_desc(1, TypeName::Int32),
                make_column_desc(2, TypeName::Int32),
                make_column_desc(0, TypeName::Int64),
            ],
        })),
        input: vec![source_node],
        pk_indices: vec![2],
        node_id: 1,
    };

    // filter node
    let function_call = FunctionCall {
        children: vec![make_inputref(0), make_inputref(1)],
    };
    let filter_node = StreamNode {
        node: Some(Node::FilterNode(FilterNode {
            search_condition: Some(ExprNode {
                expr_type: GreaterThan as i32,
                return_type: Some(DataType {
                    type_name: TypeName::Boolean as i32,
                    ..Default::default()
                }),
                rex_node: Some(RexNode::FuncCall(function_call)),
            }),
        })),
        input: vec![exchange_node],
        pk_indices: vec![0, 1],
        node_id: 2,
    };

    // simple agg node
    let simple_agg_node = StreamNode {
        node: Some(Node::SimpleAggNode(SimpleAggNode {
            agg_calls: vec![make_sum_aggcall(0), make_sum_aggcall(1)],
        })),
        input: vec![filter_node],
        pk_indices: vec![0, 1],
        node_id: 3,
    };

    // exchange node
    let exchange_node_1 = StreamNode {
        node: Some(Node::ExchangeNode(ExchangeNode {
            dispatcher: Some(Dispatcher {
                r#type: DispatcherType::Simple as i32,
                column_idx: 0,
            }),
            input_column_descs: vec![
                make_column_desc(0, TypeName::Int64),
                make_column_desc(0, TypeName::Int64),
            ],
        })),
        input: vec![simple_agg_node],
        pk_indices: vec![0, 1],
        node_id: 4,
    };

    // agg node
    let simple_agg_node_1 = StreamNode {
        node: Some(Node::SimpleAggNode(SimpleAggNode {
            agg_calls: vec![make_sum_aggcall(0), make_sum_aggcall(1)],
        })),
        input: vec![exchange_node_1],
        pk_indices: vec![0, 1],
        node_id: 5,
    };

    // project node
    let function_call_1 = FunctionCall {
        children: vec![make_inputref(0), make_inputref(1)],
    };
    let project_node = StreamNode {
        node: Some(Node::ProjectNode(ProjectNode {
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
        input: vec![simple_agg_node_1],
        pk_indices: vec![1, 2],
        node_id: 6,
    };

    // mview node
    StreamNode {
        input: vec![project_node],
        pk_indices: vec![],
        node: Some(Node::MviewNode(MViewNode {
            table_ref_id: Some(make_table_ref_id(1)),
            associated_table_ref_id: None,
            // ignore STREAM_NULL_BY_ROW_COUNT here. It's not important.
            column_descs: vec![
                make_column_desc(0, TypeName::Int64),
                make_column_desc(0, TypeName::Int64),
            ],
            pk_indices: vec![1, 2],
            column_orders: vec![make_column_order(1), make_column_order(2)],
        })),
        node_id: 7,
    }
}

#[tokio::test]
async fn test_fragmenter() -> Result<()> {
    let env = MetaSrvEnv::for_test().await;
    let stream_node = make_stream_node();
    let mut fragmenter = StreamFragmenter::new(env.id_gen_manager_ref(), 1);

    let (graph, source_actor_ids) = fragmenter.generate_graph(&stream_node).await?;
    assert_eq!(graph.len(), 6);
    assert_eq!(source_actor_ids.len(), 1);
    assert_eq!(source_actor_ids[0], 6);
    let mut expected_downstream = HashMap::new();
    expected_downstream.insert(1, vec![]);
    expected_downstream.insert(2, vec![1]);
    expected_downstream.insert(3, vec![1]);
    expected_downstream.insert(4, vec![1]);
    expected_downstream.insert(5, vec![1]);
    expected_downstream.insert(6, vec![2, 3, 4, 5]);

    let mut expected_upstream = HashMap::new();
    expected_upstream.insert(1, vec![2, 3, 4, 5]);
    expected_upstream.insert(2, vec![6]);
    expected_upstream.insert(3, vec![6]);
    expected_upstream.insert(4, vec![6]);
    expected_upstream.insert(5, vec![6]);
    expected_upstream.insert(6, vec![]);
    for actor in graph {
        assert_eq!(
            expected_downstream.get(&actor.get_actor_id()).unwrap(),
            actor.get_downstream_actor_id(),
        );
        let mut node = actor.get_nodes().unwrap();
        while !node.get_input().is_empty() {
            node = node.get_input().get(0).unwrap();
        }
        let mut source_node_cnt = 0;
        match node.get_node().unwrap() {
            Node::MergeNode(merge_node) => {
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
            Node::SourceNode(_) => {
                source_node_cnt += 1;
                assert_eq!(source_node_cnt, 1);
            }
            _ => {
                panic!("it should be MergeNode.");
            }
        }
    }

    Ok(())
}

#[tokio::test]
/// Test case for multiple compute nodes.
async fn test_fragmenter_case2() -> Result<()> {
    let env = MetaSrvEnv::for_test().await;
    let stream_node = make_stream_node();
    let mut fragmenter = StreamFragmenter::new(env.id_gen_manager_ref(), 3);

    let (graph, source_actor_ids) = fragmenter.generate_graph(&stream_node).await?;
    assert_eq!(graph.len(), 10);
    assert_eq!(source_actor_ids.len(), 3);
    assert_eq!(source_actor_ids, vec![8, 9, 10]);
    let mut expected_downstream = HashMap::new();
    expected_downstream.insert(1, vec![]);
    expected_downstream.insert(2, vec![1]);
    expected_downstream.insert(3, vec![1]);
    expected_downstream.insert(4, vec![1]);
    expected_downstream.insert(5, vec![1]);
    expected_downstream.insert(6, vec![1]);
    expected_downstream.insert(7, vec![1]);
    expected_downstream.insert(8, vec![2, 3, 4, 5, 6, 7]);
    expected_downstream.insert(9, vec![2, 3, 4, 5, 6, 7]);
    expected_downstream.insert(10, vec![2, 3, 4, 5, 6, 7]);

    let mut expected_upstream = HashMap::new();
    expected_upstream.insert(1, vec![2, 3, 4, 5, 6, 7]);
    expected_upstream.insert(2, vec![8, 9, 10]);
    expected_upstream.insert(3, vec![8, 9, 10]);
    expected_upstream.insert(4, vec![8, 9, 10]);
    expected_upstream.insert(5, vec![8, 9, 10]);
    expected_upstream.insert(6, vec![8, 9, 10]);
    expected_upstream.insert(7, vec![8, 9, 10]);
    expected_upstream.insert(8, vec![]);
    expected_upstream.insert(9, vec![]);
    expected_upstream.insert(10, vec![]);

    for actor in graph {
        assert_eq!(
            expected_downstream.get(&actor.get_actor_id()).unwrap(),
            actor.get_downstream_actor_id(),
        );
        let mut node = actor.get_nodes().unwrap();
        while !node.get_input().is_empty() {
            node = node.get_input().get(0).unwrap();
        }
        let mut source_node_cnt = 0;
        match node.get_node().unwrap() {
            Node::MergeNode(merge_node) => {
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
            Node::SourceNode(_) => {
                source_node_cnt += 1;
                assert_eq!(source_node_cnt, 1);
            }
            _ => {
                panic!("it should be MergeNode.");
            }
        }
    }

    Ok(())
}

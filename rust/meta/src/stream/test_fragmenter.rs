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
use risingwave_pb::stream_plan::stream_node::Node;
use risingwave_pb::stream_plan::table_source_node::SourceType;
use risingwave_pb::stream_plan::{
    Dispatcher, ExchangeNode, FilterNode, MViewNode, ProjectNode, SimpleAggNode, StreamNode,
    TableSourceNode,
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
/// create materialized view `T_distributed` as select sum(v1)+1 as V from t where v1>v2
/// ```
/// plan:
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
    let table_source_node = StreamNode {
        node: Some(Node::TableSourceNode(TableSourceNode {
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
                r#type: DispatcherType::Simple as i32,
                column_idx: 0,
            }),
            input_column_descs: vec![
                make_column_desc(1, TypeName::Int32),
                make_column_desc(2, TypeName::Int32),
                make_column_desc(0, TypeName::Int64),
            ],
        })),
        input: vec![table_source_node],
        pk_indices: vec![2],
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
    };

    // simple agg node
    let simple_agg_node = StreamNode {
        node: Some(Node::SimpleAggNode(SimpleAggNode {
            agg_calls: vec![make_sum_aggcall(0), make_sum_aggcall(1)],
        })),
        input: vec![filter_node],
        pk_indices: vec![0, 1],
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
    };

    // agg node
    let simple_agg_node_1 = StreamNode {
        node: Some(Node::SimpleAggNode(SimpleAggNode {
            agg_calls: vec![make_sum_aggcall(0), make_sum_aggcall(1)],
        })),
        input: vec![exchange_node_1],
        pk_indices: vec![0, 1],
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
    };

    // mview node
    StreamNode {
        input: vec![project_node],
        pk_indices: vec![],
        node: Some(Node::MviewNode(MViewNode {
            table_ref_id: Some(make_table_ref_id(1)),
            // ignore STREAM_NULL_BY_ROW_COUNT here. It's not important.
            column_descs: vec![
                make_column_desc(0, TypeName::Int64),
                make_column_desc(0, TypeName::Int64),
            ],
            pk_indices: vec![1, 2],
            column_orders: vec![make_column_order(1), make_column_order(2)],
        })),
    }
}

#[tokio::test]
async fn test_fragmenter() -> Result<()> {
    let env = MetaSrvEnv::for_test().await;
    let stream_node = make_stream_node();
    let mut fragmenter = StreamFragmenter::new(env.id_gen_manager_ref(), 1);

    let graph = fragmenter.generate_graph(&stream_node).await?;
    assert_eq!(graph.len(), 6);
    // assert_eq!(graph, vec![]);
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
    for fragment in graph {
        assert_eq!(
            expected_downstream
                .get(&fragment.get_fragment_id())
                .unwrap()
                .iter()
                .collect::<HashSet<_>>(),
            fragment
                .get_downstream_fragment_id()
                .iter()
                .collect::<HashSet<_>>(),
        );
        let mut node = fragment.get_nodes();
        while !node.get_input().is_empty() {
            node = node.get_input().get(0).unwrap();
        }
        let mut source_node_cnt = 0;
        match node.get_node() {
            Node::MergeNode(merge_node) => {
                assert_eq!(
                    expected_upstream
                        .get(&fragment.get_fragment_id())
                        .unwrap()
                        .iter()
                        .collect::<HashSet<_>>(),
                    merge_node
                        .get_upstream_fragment_id()
                        .iter()
                        .collect::<HashSet<_>>(),
                );
            }
            Node::TableSourceNode(_) => {
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

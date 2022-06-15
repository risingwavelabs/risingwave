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
use risingwave_common::error::Result;
use risingwave_pb::catalog::Table as ProstTable;
use risingwave_pb::data::data_type::TypeName;
use risingwave_pb::data::DataType;
use risingwave_pb::expr::agg_call::{Arg, Type};
use risingwave_pb::expr::expr_node::RexNode;
use risingwave_pb::expr::expr_node::Type::{Add, GreaterThan, InputRef};
use risingwave_pb::expr::{AggCall, ExprNode, FunctionCall, InputRefExpr};
use risingwave_pb::plan_common::{
    ColumnCatalog, ColumnDesc, ColumnOrder, DatabaseRefId, Field, OrderType, SchemaRefId,
    TableRefId,
};
use risingwave_pb::stream_plan::source_node::SourceType;
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_plan::{
    DispatchStrategy, DispatcherType, ExchangeNode, FilterNode, MaterializeNode, ProjectNode,
    SimpleAggNode, SourceNode, StreamNode,
};

use crate::hummock::compaction_group::manager::CompactionGroupManager;
use crate::manager::MetaSrvEnv;
use crate::model::TableFragments;
use crate::stream::stream_graph::ActorGraphBuilder;
use crate::stream::{CreateMaterializedViewContext, FragmentManager};

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

fn make_field(type_name: TypeName) -> Field {
    Field {
        data_type: Some(DataType {
            type_name: type_name as i32,
            ..Default::default()
        }),
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
        schema_id: SchemaId::placeholder() as u32,
        database_id: DatabaseId::placeholder() as u32,
        name: String::new(),
        columns,
        order_column_ids: vec![0],
        orders: vec![2],
        pk: vec![2],
        ..Default::default()
    }
}

/// [`make_stream_node`] build a plan represent in `StreamNode` for SQL as follow:
/// ```sql
/// create table t (v1 int, v2 int);
/// create materialized view T_distributed as select sum(v1)+1 as V from t where v1>v2;
/// ```
fn make_stream_node() -> StreamNode {
    let table_ref_id = make_table_ref_id(1);
    // table source node
    let source_node = StreamNode {
        node_body: Some(NodeBody::Source(SourceNode {
            table_ref_id: Some(table_ref_id),
            column_ids: vec![1, 2, 0],
            source_type: SourceType::Table as i32,
        })),
        pk_indices: vec![2],
        ..Default::default()
    };

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
        input: vec![source_node],
        pk_indices: vec![2],
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
        pk_indices: vec![0, 1],
        operator_id: 2,
        identity: "FilterExecutor".to_string(),
        ..Default::default()
    };

    // simple agg node
    let simple_agg_node = StreamNode {
        node_body: Some(NodeBody::GlobalSimpleAgg(SimpleAggNode {
            agg_calls: vec![make_sum_aggcall(0), make_sum_aggcall(1)],
            distribution_keys: Default::default(),
            internal_tables: vec![make_internal_table(true), make_internal_table(false)],
            column_index2column_id: HashMap::new(),
            is_append_only: false,
        })),
        input: vec![filter_node],
        fields: vec![], // TODO: fill this later
        pk_indices: vec![0, 1],
        operator_id: 3,
        identity: "GlobalSimpleAggExecutor".to_string(),
        ..Default::default()
    };

    // exchange node
    let exchange_node_1 = StreamNode {
        node_body: Some(NodeBody::Exchange(ExchangeNode {
            strategy: Some(DispatchStrategy {
                r#type: DispatcherType::Simple as i32,
                ..Default::default()
            }),
        })),
        fields: vec![make_field(TypeName::Int64), make_field(TypeName::Int64)],
        input: vec![simple_agg_node],
        pk_indices: vec![0, 1],
        operator_id: 4,
        identity: "ExchangeExecutor".to_string(),
        ..Default::default()
    };

    // agg node
    let simple_agg_node_1 = StreamNode {
        node_body: Some(NodeBody::GlobalSimpleAgg(SimpleAggNode {
            agg_calls: vec![make_sum_aggcall(0), make_sum_aggcall(1)],
            distribution_keys: Default::default(),
            internal_tables: vec![make_internal_table(true), make_internal_table(false)],
            column_index2column_id: HashMap::new(),
            is_append_only: false,
        })),
        fields: vec![], // TODO: fill this later
        input: vec![exchange_node_1],
        pk_indices: vec![0, 1],
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
        pk_indices: vec![1, 2],
        operator_id: 6,
        identity: "ProjectExecutor".to_string(),
        ..Default::default()
    };

    // mview node
    StreamNode {
        input: vec![project_node],
        pk_indices: vec![],
        node_body: Some(NodeBody::Materialize(MaterializeNode {
            table_ref_id: Some(make_table_ref_id(1)),
            associated_table_ref_id: None,
            column_ids: vec![0_i32, 1_i32],
            column_orders: vec![make_column_order(1), make_column_order(2)],
            distribution_keys: Default::default(),
        })),
        fields: vec![], // TODO: fill this later
        operator_id: 7,
        identity: "MaterializeExecutor".to_string(),
        ..Default::default()
    }
}

// TODO: enable this test with madsim
// NOTE: frontend is not yet available with madsim
#[cfg(not(madsim))]
#[tokio::test]
async fn test_fragmenter() -> Result<()> {
    use risingwave_frontend::stream_fragmenter::StreamFragmenter;

    let env = MetaSrvEnv::for_test().await;
    let stream_node = make_stream_node();
    let compaction_group_manager = Arc::new(CompactionGroupManager::new(env.clone()).await?);
    let fragment_manager =
        Arc::new(FragmentManager::new(env.clone(), compaction_group_manager).await?);
    let parallel_degree = 4;
    let mut ctx = CreateMaterializedViewContext::default();
    let graph = StreamFragmenter::build_graph(stream_node);
    let graph = ActorGraphBuilder::generate_graph(
        env.id_gen_manager_ref(),
        fragment_manager,
        parallel_degree,
        &graph,
        &mut ctx,
    )
    .await?;
    let table_fragments =
        TableFragments::new(TableId::default(), graph, ctx.internal_table_id_set.clone());
    let actors = table_fragments.actors();
    let source_actor_ids = table_fragments.source_actor_ids();
    let sink_actor_ids = table_fragments.sink_actor_ids();
    let mut internal_table_ids = table_fragments.internal_table_ids();
    assert_eq!(actors.len(), 9);
    assert_eq!(source_actor_ids, vec![6, 7, 8, 9]);
    assert_eq!(sink_actor_ids, vec![1]);
    assert_eq!(4, internal_table_ids.len());
    internal_table_ids.sort();
    let expected_internal_table_ids = vec![0, 1, 2, 3];
    assert_eq!(expected_internal_table_ids, internal_table_ids);

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
            actor.dispatcher[0].get_downstream_actor_id(),
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

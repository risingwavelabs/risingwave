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

use criterion::{black_box, criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};
use futures::StreamExt;
use itertools::Itertools;
use risingwave_batch::executor::test_utils::{gen_data, MockExecutor};
use risingwave_batch::executor::{
    BoxedExecutor, EquiJoinParams, Executor, HashJoinExecutor, JoinType,
};
use risingwave_common::catalog::schema_test_utils::field_n;
use risingwave_common::catalog::Schema;
use risingwave_common::hash::Key64;
use risingwave_common::types::{DataType, ScalarImpl};
use risingwave_expr::expr::build_from_prost;
use risingwave_pb::data::data_type::TypeName;
use risingwave_pb::expr::expr_node::RexNode;
use risingwave_pb::expr::expr_node::Type::{
    ConstantValue as TConstValue, Equal, InputRef, Modulus,
};
use risingwave_pb::expr::{ConstantValue, ExprNode, FunctionCall, InputRefExpr};
use tikv_jemallocator::Jemalloc;
use tokio::runtime::Runtime;
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

fn create_hash_join_executor(
    join_type: JoinType,
    left_chunk_size: usize,
    left_chunk_num: usize,
    right_chunk_size: usize,
    right_chunk_num: usize,
) -> BoxedExecutor {
    let left_input = gen_data(left_chunk_size, left_chunk_num);
    let right_input = gen_data(right_chunk_size, right_chunk_num);

    let mut left_child = Box::new(MockExecutor::new(field_n::<1>(DataType::Int64)));
    left_input.into_iter().for_each(|c| left_child.add(c));

    let mut right_child = Box::new(MockExecutor::new(field_n::<1>(DataType::Int64)));
    right_input.into_iter().for_each(|c| right_child.add(c));

    // Expression: $1 % 2 == $2 % 3
    let join_expr = {
        let left_input_ref = ExprNode {
            expr_type: InputRef as i32,
            return_type: Some(risingwave_pb::data::DataType {
                type_name: TypeName::Int64 as i32,
                ..Default::default()
            }),
            rex_node: Some(RexNode::InputRef(InputRefExpr { column_idx: 0 })),
        };

        let right_input_ref = ExprNode {
            expr_type: InputRef as i32,
            return_type: Some(risingwave_pb::data::DataType {
                type_name: TypeName::Int64 as i32,
                ..Default::default()
            }),
            rex_node: Some(RexNode::InputRef(InputRefExpr { column_idx: 1 })),
        };

        let literal2 = ExprNode {
            expr_type: TConstValue as i32,
            return_type: Some(risingwave_pb::data::DataType {
                type_name: TypeName::Int64 as i32,
                ..Default::default()
            }),
            rex_node: Some(RexNode::Constant(ConstantValue {
                body: ScalarImpl::Int64(2).to_protobuf(),
            })),
        };

        let literal3 = ExprNode {
            expr_type: TConstValue as i32,
            return_type: Some(risingwave_pb::data::DataType {
                type_name: TypeName::Int64 as i32,
                ..Default::default()
            }),
            rex_node: Some(RexNode::Constant(ConstantValue {
                body: ScalarImpl::Int64(3).to_protobuf(),
            })),
        };

        // $1 % 2
        let left_mod2 = ExprNode {
            expr_type: Modulus as i32,
            return_type: Some(risingwave_pb::data::DataType {
                type_name: TypeName::Int64 as i32,
                ..Default::default()
            }),
            rex_node: Some(RexNode::FuncCall(FunctionCall {
                children: vec![left_input_ref, literal2],
            })),
        };

        // $2 % 3
        let right_mod3 = ExprNode {
            expr_type: Modulus as i32,
            return_type: Some(risingwave_pb::data::DataType {
                type_name: TypeName::Int64 as i32,
                ..Default::default()
            }),
            rex_node: Some(RexNode::FuncCall(FunctionCall {
                children: vec![right_input_ref, literal3],
            })),
        };

        // $1 % 2 == $2 % 3
        ExprNode {
            expr_type: Equal as i32,
            return_type: Some(risingwave_pb::data::DataType {
                type_name: TypeName::Boolean as i32,
                ..Default::default()
            }),
            rex_node: Some(RexNode::FuncCall(FunctionCall {
                children: vec![left_mod2, right_mod3],
            })),
        }
    };

    let schema_fields = match join_type {
        JoinType::LeftSemi | JoinType::LeftAnti => left_child.schema().fields.clone(),
        JoinType::RightSemi | JoinType::RightAnti => right_child.schema().fields.clone(),
        _ => [
            left_child.schema().fields.clone(),
            right_child.schema().fields.clone(),
        ]
        .concat(),
    };

    let params = EquiJoinParams::new(
        join_type,
        vec![0],
        vec![DataType::Int64],
        1,
        vec![0],
        vec![DataType::Int64],
        1,
        vec![DataType::Int64, DataType::Int64],
        2,
        Some(build_from_prost(&join_expr).unwrap()),
    );

    let schema = Schema {
        fields: schema_fields,
    };
    let schema_len = schema.len();
    Box::new(HashJoinExecutor::<Key64>::new(
        left_child,
        right_child,
        params,
        schema,
        "HashJoinExecutor".to_string(),
        (0..schema_len).into_iter().collect_vec(),
    ))
}

async fn execute_hash_join_executor(executor: BoxedExecutor) {
    let mut stream = executor.execute();
    while let Some(ret) = stream.next().await {
        black_box(ret.unwrap());
    }
}

fn bench_hash_join(c: &mut Criterion) {
    const LEFT_SIZE: usize = 2 * 1024;
    const RIGHT_SIZE: usize = 1024;
    let rt = Runtime::new().unwrap();
    for join_type in &[
        JoinType::Inner,
        JoinType::LeftOuter,
        JoinType::LeftSemi,
        JoinType::LeftAnti,
        JoinType::RightOuter,
        JoinType::RightSemi,
        JoinType::RightAnti,
        JoinType::FullOuter,
    ] {
        for chunk_size in &[32, 128, 512, 1024] {
            c.bench_with_input(
                BenchmarkId::new(
                    "HashJoinExecutor",
                    format!("{}({:?})", chunk_size, join_type),
                ),
                chunk_size,
                |b, &chunk_size| {
                    let left_chunk_num = LEFT_SIZE / chunk_size;
                    let right_chunk_num = RIGHT_SIZE / chunk_size;
                    b.to_async(&rt).iter_batched(
                        || {
                            create_hash_join_executor(
                                *join_type,
                                chunk_size,
                                left_chunk_num,
                                chunk_size,
                                right_chunk_num,
                            )
                        },
                        |e| execute_hash_join_executor(e),
                        BatchSize::SmallInput,
                    );
                },
            );
        }
    }
}

criterion_group!(benches, bench_hash_join);
criterion_main!(benches);

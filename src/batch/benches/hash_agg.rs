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
pub mod utils;

use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};
use itertools::Itertools;
use risingwave_batch::executor::{BoxedExecutor, HashAggExecutor};
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::hash;
use risingwave_common::types::DataType;
use risingwave_expr::expr::AggKind;
use risingwave_expr::vector_op::agg::AggStateFactory;
use risingwave_pb::expr::agg_call::Arg;
use risingwave_pb::expr::{AggCall, InputRefExpr};
use tikv_jemallocator::Jemalloc;
use tokio::runtime::Runtime;
use utils::{create_input, execute_executor};

#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

fn create_agg_call(
    input_schema: &Schema,
    agg_kind: AggKind,
    args: Vec<usize>,
    return_type: DataType,
) -> AggCall {
    AggCall {
        r#type: agg_kind.to_prost() as i32,
        args: args
            .into_iter()
            .map(|col_idx| Arg {
                input: Some(InputRefExpr {
                    column_idx: col_idx as i32,
                }),
                r#type: Some(input_schema.fields()[col_idx].data_type().to_protobuf()),
            })
            .collect(),
        return_type: Some(return_type.to_protobuf()),
        distinct: false,
        order_by_fields: vec![],
        filter: None,
    }
}

fn create_hash_agg_executor(
    group_key_columns: Vec<usize>,
    agg_kind: AggKind,
    arg_columns: Vec<usize>,
    return_type: DataType,
    chunk_size: usize,
    chunk_num: usize,
) -> BoxedExecutor {
    const CHUNK_SIZE: usize = 1024;
    let input = create_input(
        &[DataType::Int32, DataType::Int64, DataType::Varchar],
        chunk_size,
        chunk_num,
    );
    let input_schema = input.schema();

    let agg_calls = vec![create_agg_call(
        input_schema,
        agg_kind,
        arg_columns,
        return_type,
    )];

    let agg_factories: Vec<_> = agg_calls
        .iter()
        .map(AggStateFactory::new)
        .try_collect()
        .unwrap();

    let group_key_types = group_key_columns
        .iter()
        .map(|i| input_schema.fields()[*i].data_type())
        .collect_vec();

    let fields = group_key_types
        .iter()
        .cloned()
        .chain(agg_factories.iter().map(|fac| fac.get_return_type()))
        .map(Field::unnamed)
        .collect_vec();
    let schema = Schema { fields };

    Box::new(HashAggExecutor::<hash::Key64>::new(
        agg_factories,
        group_key_columns,
        group_key_types,
        schema,
        input,
        "HashAggExecutor".to_string(),
        CHUNK_SIZE,
    ))
}

fn bench_hash_agg(c: &mut Criterion) {
    const SIZE: usize = 1024 * 1024;
    let rt = Runtime::new().unwrap();

    let bench_variants = [
        // (group by, agg, args, return type)
        (vec![0], AggKind::Sum, vec![1], DataType::Int64),
        (vec![0], AggKind::Count, vec![], DataType::Int64),
        (vec![0], AggKind::Count, vec![2], DataType::Int64),
        (vec![0], AggKind::Min, vec![1], DataType::Int64),
        (vec![0], AggKind::StringAgg, vec![2], DataType::Varchar),
        (vec![0, 2], AggKind::Sum, vec![1], DataType::Int64),
        (vec![0, 2], AggKind::Count, vec![], DataType::Int64),
        (vec![0, 2], AggKind::Count, vec![2], DataType::Int64),
        (vec![0, 2], AggKind::Min, vec![1], DataType::Int64),
    ];

    for (group_key_columns, agg_kind, arg_columns, return_type) in bench_variants {
        for chunk_size in &[32, 128, 512, 1024, 2048, 4096] {
            c.bench_with_input(
                BenchmarkId::new("HashAggExecutor", chunk_size),
                chunk_size,
                |b, &chunk_size| {
                    let chunk_num = SIZE / chunk_size;
                    b.to_async(&rt).iter_batched(
                        || {
                            create_hash_agg_executor(
                                group_key_columns.clone(),
                                agg_kind,
                                arg_columns.clone(),
                                return_type.clone(),
                                chunk_size,
                                chunk_num,
                            )
                        },
                        |e| execute_executor(e),
                        BatchSize::SmallInput,
                    );
                },
            );
        }
    }
}

criterion_group!(benches, bench_hash_agg);
criterion_main!(benches);

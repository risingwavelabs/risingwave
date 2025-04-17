// Copyright 2025 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
pub mod utils;

use std::sync::Arc;

use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use itertools::Itertools;
use risingwave_batch::monitor::BatchSpillMetrics;
use risingwave_batch::task::ShutdownToken;
use risingwave_batch_executors::executor::aggregation::build as build_agg;
use risingwave_batch_executors::executor::{BoxedExecutor, HashAggExecutor};
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::memory::MemoryContext;
use risingwave_common::types::DataType;
use risingwave_common::{enable_jemalloc, hash};
use risingwave_expr::aggregate::{AggCall, AggType, PbAggKind};
use risingwave_pb::expr::{PbAggCall, PbInputRef};
use tokio::runtime::Runtime;
use utils::{create_input, execute_executor};

enable_jemalloc!();

fn create_agg_call(
    input_schema: &Schema,
    agg_type: AggType,
    args: Vec<usize>,
    return_type: DataType,
) -> PbAggCall {
    PbAggCall {
        kind: agg_type.to_protobuf_simple() as i32,
        args: args
            .into_iter()
            .map(|col_idx| PbInputRef {
                index: col_idx as _,
                r#type: Some(input_schema.fields()[col_idx].data_type().to_protobuf()),
            })
            .collect(),
        return_type: Some(return_type.to_protobuf()),
        distinct: false,
        order_by: vec![],
        filter: None,
        direct_args: vec![],
        udf: None,
        scalar: None,
    }
}

fn create_hash_agg_executor(
    group_key_columns: Vec<usize>,
    agg_type: AggType,
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
        agg_type,
        arg_columns,
        return_type,
    )];

    let agg_init_states: Vec<_> = agg_calls
        .iter()
        .map(|agg_call| AggCall::from_protobuf(agg_call).and_then(|agg| build_agg(&agg)))
        .try_collect()
        .unwrap();

    let group_key_types = group_key_columns
        .iter()
        .map(|i| input_schema.fields()[*i].data_type())
        .collect_vec();

    let fields = group_key_types
        .iter()
        .cloned()
        .chain(agg_init_states.iter().map(|fac| fac.return_type()))
        .map(Field::unnamed)
        .collect_vec();
    let schema = Schema { fields };

    Box::new(HashAggExecutor::<hash::Key64>::new(
        Arc::new(agg_init_states),
        group_key_columns,
        group_key_types,
        schema,
        input,
        "HashAggExecutor".to_owned(),
        CHUNK_SIZE,
        MemoryContext::none(),
        None,
        BatchSpillMetrics::for_test(),
        ShutdownToken::empty(),
    ))
}

fn bench_hash_agg(c: &mut Criterion) {
    const SIZE: usize = 1024 * 1024;
    let rt = Runtime::new().unwrap();

    let bench_variants = [
        // (group by, agg, args, return type)
        (vec![0], PbAggKind::Sum, vec![1], DataType::Int64),
        (vec![0], PbAggKind::Count, vec![], DataType::Int64),
        (vec![0], PbAggKind::Count, vec![2], DataType::Int64),
        (vec![0], PbAggKind::Min, vec![1], DataType::Int64),
        (vec![0], PbAggKind::StringAgg, vec![2], DataType::Varchar),
        (vec![0, 2], PbAggKind::Sum, vec![1], DataType::Int64),
        (vec![0, 2], PbAggKind::Count, vec![], DataType::Int64),
        (vec![0, 2], PbAggKind::Count, vec![2], DataType::Int64),
        (vec![0, 2], PbAggKind::Min, vec![1], DataType::Int64),
    ];

    for (group_key_columns, agg_type, arg_columns, return_type) in bench_variants {
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
                                agg_type.into(),
                                arg_columns.clone(),
                                return_type.clone(),
                                chunk_size,
                                chunk_num,
                            )
                        },
                        execute_executor,
                        BatchSize::SmallInput,
                    );
                },
            );
        }
    }
}

criterion_group!(benches, bench_hash_agg);
criterion_main!(benches);

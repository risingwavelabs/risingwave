// Copyright 2023 RisingWave Labs
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

use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion};
use futures::executor::block_on;
use futures::StreamExt;
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::types::DataType;
use risingwave_expr::expr::*;
use risingwave_expr::function::aggregate::{AggCall, AggKind};
use risingwave_expr::function::args::FuncArgs;
use risingwave_storage::memory::MemoryStateStore;
use risingwave_storage::StateStore;
use risingwave_stream::executor::test_utils::agg_executor::new_boxed_hash_agg_executor;
use risingwave_stream::executor::test_utils::*;
use risingwave_stream::executor::{BoxedExecutor, PkIndices};
use tokio::runtime::Runtime;

fn bench_hash_agg(c: &mut Criterion) {
    let mut group = c.benchmark_group("Q17");
    group.sample_size(10);

    let rt = Runtime::new().unwrap();
    group.bench_function("benchmark_hash_agg", |b| {
        b.to_async(&rt).iter_batched(
            || setup_bench_hash_agg(MemoryStateStore::new()),
            |e| execute_executor(e),
            BatchSize::SmallInput,
        )
    });
}

/// This aims to mirror `q17`'s aggregator.
/// We can include more executor patterns as needed.
fn setup_bench_hash_agg<S: StateStore>(store: S) -> BoxedExecutor {
    // ---- Define hash agg executor parameters ----
    let input_data_types = vec![
        // to_char(date_time)
        DataType::Varchar,
        // auction
        DataType::Int64,
        // price
        DataType::Int64,
    ];
    let fields = vec![
        // to_char(date_time)
        Field::unnamed(DataType::Varchar),
        // auction
        Field::unnamed(DataType::Int64),
        // price
        Field::unnamed(DataType::Int64),
    ];

    // Aggregation fields
    // let fields = vec![
    //     Field::with_name(DataType::Int64, "auction"),
    //     Field::with_name(DataType::Varchar, "to_char"),
    //
    //     Field::with_name(DataType::Int64, "count"),
    //     Field::with_name(DataType::Int64, "count_filter_below_10_000"),
    //     Field::with_name(DataType::Int64, "count_filter_10_000_to_100_000"),
    //     Field::with_name(DataType::Int64, "count_filter_above_100_000"),
    //
    //     Field::with_name(DataType::Int64, "min"),
    //     Field::with_name(DataType::Int64, "max"),
    //     Field::with_name(DataType::Int64, "avg"),
    //     Field::with_name(DataType::Int64, "sum"),
    // ];

    let schema = Schema { fields };

    let group_key_indices = vec![0, 1];

    let agg_calls = vec![
         AggCall {
            kind: AggKind::Count,
            args: FuncArgs::None,
            return_type: DataType::Int64,
            column_orders: vec![],
            filter: None,
            distinct: false,
         },
         AggCall {
            kind: AggKind::Count,
            args: FuncArgs::None,
            return_type: DataType::Int64,
            column_orders: vec![],
            filter: Some(build_from_pretty("(less_than:boolean $2:int8 10000:int8)").into()),
            distinct: false,
         },
         AggCall {
            kind: AggKind::Count,
            args: FuncArgs::None,
            return_type: DataType::Int64,
            column_orders: vec![],
            filter: Some(build_from_pretty("(and:boolean (greater_than_or_equal:boolean $2:int8 10000:int8) (less_than:boolean $2:int8 100000:int8))").into()),
            distinct: false,
         },
         AggCall {
            kind: AggKind::Count,
            args: FuncArgs::None,
            return_type: DataType::Int64,
            column_orders: vec![],
            filter: Some(build_from_pretty("(greater_than_or_equal:boolean $2:int8 100000:int8)").into()),
            distinct: false,
        },
        // FIXME(kwannoel): Can ignore for now, since it is low cost in q17 (blackhole).
        // It does not work can't diagnose root cause yet.
        // AggCall {
        //     kind: AggKind::Min,
        //     args: FuncArgs::Unary(DataType::Int64, 2),
        //     return_type: DataType::Int64,
        //     column_orders: vec![],
        //     filter: None,
        //     distinct: false,
        // },
        // AggCall {
        //     kind: AggKind::Max,
        //     args: FuncArgs::Unary(DataType::Int64, 2),
        //     return_type: DataType::Int64,
        //     column_orders: vec![],
        //     filter: None,
        //     distinct: false,
        // },
        // Not supported, just use extra sum + count
        // AggCall {
        //     kind: AggKind::Avg,
        //     args: FuncArgs::Unary(DataType::Int64, 2),
        //     return_type: DataType::Int64,
        //     column_orders: vec![],
        //     filter: None,
        //     distinct: false,
        // },
        // avg (sum)
        AggCall {
            kind: AggKind::Sum,
            args: FuncArgs::Unary(DataType::Int64, 2),
            return_type: DataType::Int64,
            column_orders: vec![],
            filter: None,
            distinct: false,
        },
        // avg (count)
        AggCall {
            kind: AggKind::Count,
            args: FuncArgs::Unary(DataType::Int64, 2),
            return_type: DataType::Int64,
            column_orders: vec![],
            filter: None,
            distinct: false,
        },
        AggCall {
            kind: AggKind::Sum,
            args: FuncArgs::Unary(DataType::Int64, 2),
            return_type: DataType::Int64,
            column_orders: vec![],
            filter: None,
            distinct: false,
        },
    ];

    // ---- Generate Data ----

    let num_of_chunks = 1000;
    let chunk_size = 1024;
    let chunks = gen_data(num_of_chunks, chunk_size, &input_data_types);

    // ---- Create MockSourceExecutor ----
    let (mut tx, source) = MockSource::channel(schema, PkIndices::new());
    tx.push_barrier(1, false);
    for chunk in chunks {
        tx.push_chunk(chunk);
    }
    tx.push_barrier_with_prev_epoch_for_test(1002, 1, false);

    // ---- Create HashAggExecutor to be benchmarked ----
    let row_count_index = 0;
    let pk_indices = vec![];
    let extreme_cache_size = 1024;
    let executor_id = 1;

    block_on(new_boxed_hash_agg_executor(
        store,
        Box::new(source),
        false,
        agg_calls,
        row_count_index,
        group_key_indices,
        pk_indices,
        extreme_cache_size,
        false,
        executor_id,
    ))
}

pub async fn execute_executor(executor: BoxedExecutor) {
    let mut stream = executor.execute();
    while let Some(ret) = stream.next().await {
        _ = black_box(ret.unwrap());
    }
}

criterion_group!(benches, bench_hash_agg);
criterion_main!(benches);

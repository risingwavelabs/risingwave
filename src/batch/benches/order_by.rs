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
use risingwave_batch::executor::{BoxedExecutor, OrderByExecutor};
use risingwave_common::types::DataType;
use risingwave_common::util::sort_util::{OrderPair, OrderType};
use tikv_jemallocator::Jemalloc;
use tokio::runtime::Runtime;
use utils::{create_input, execute_executor};

#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

fn create_order_by_executor(
    chunk_size: usize,
    chunk_num: usize,
    single_column: bool,
) -> BoxedExecutor {
    let (child, order_pairs) = if single_column {
        let input = create_input(&[DataType::Int64], chunk_size, chunk_num);
        (input, vec![OrderPair::new(0, OrderType::Ascending)])
    } else {
        let input = create_input(
            &[
                DataType::Int64,
                DataType::Varchar,
                DataType::Float32,
                DataType::Timestamp,
            ],
            chunk_size,
            chunk_num,
        );
        (
            input,
            vec![
                OrderPair::new(0, OrderType::Ascending),
                OrderPair::new(1, OrderType::Descending),
                OrderPair::new(2, OrderType::Ascending),
            ],
        )
    };

    Box::new(OrderByExecutor::new(
        child,
        order_pairs,
        "OrderByExecutor".into(),
    ))
}

fn bench_order_by(c: &mut Criterion) {
    const SIZE: usize = 1024 * 1024;
    let rt = Runtime::new().unwrap();

    for single_column in [true, false] {
        for chunk_size in &[32, 128, 512, 1024, 2048, 4096] {
            c.bench_with_input(
                BenchmarkId::new(
                    "OrderByExecutor",
                    format!("{}(single_column: {})", chunk_size, single_column),
                ),
                chunk_size,
                |b, &chunk_size| {
                    let chunk_num = SIZE / chunk_size;
                    b.to_async(&rt).iter_batched(
                        || create_order_by_executor(chunk_size, chunk_num, single_column),
                        |e| execute_executor(e),
                        BatchSize::SmallInput,
                    );
                },
            );
        }
    }
}

criterion_group!(benches, bench_order_by);
criterion_main!(benches);

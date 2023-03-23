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

pub mod utils;

use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};
use risingwave_batch::executor::{BoxedExecutor, FilterExecutor};
use risingwave_common::enable_jemalloc_on_unix;
use risingwave_common::types::DataType;
use risingwave_expr::expr::build_from_pretty;
use tokio::runtime::Runtime;
use utils::{create_input, execute_executor};

enable_jemalloc_on_unix!();

fn create_filter_executor(chunk_size: usize, chunk_num: usize) -> BoxedExecutor {
    const CHUNK_SIZE: usize = 1024;
    let input = create_input(&[DataType::Int64], chunk_size, chunk_num);
    Box::new(FilterExecutor::new(
        build_from_pretty("(equal:boolean (modulus:int8 #0:int8 2:int8) 0:int8)"),
        input,
        "FilterBenchmark".to_string(),
        CHUNK_SIZE,
    ))
}

fn bench_filter(c: &mut Criterion) {
    const TOTAL_SIZE: usize = 1024 * 1024usize;
    let rt = Runtime::new().unwrap();
    for chunk_size in &[32, 128, 512, 1024, 2048, 4096] {
        c.bench_with_input(
            BenchmarkId::new("FilterExecutor", chunk_size),
            chunk_size,
            |b, &chunk_size| {
                let chunk_num = TOTAL_SIZE / chunk_size;
                b.to_async(&rt).iter_batched(
                    || create_filter_executor(chunk_size, chunk_num),
                    |e| execute_executor(e),
                    BatchSize::SmallInput,
                );
            },
        );
    }
}

criterion_group!(benches, bench_filter);
criterion_main!(benches);

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
use risingwave_batch::executor::{BoxedExecutor, LimitExecutor};
use risingwave_common::types::DataType;
use tikv_jemallocator::Jemalloc;
use tokio::runtime::Runtime;
use utils::{create_input, execute_executor};

#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

fn create_limit_executor(
    chunk_size: usize,
    chunk_num: usize,
    offset: usize,
    limit: usize,
) -> BoxedExecutor {
    let input = create_input(&[DataType::Int64], chunk_size, chunk_num);

    Box::new(LimitExecutor::new(
        input,
        offset,
        limit,
        "LimitExecutor".into(),
    ))
}

fn bench_limit(c: &mut Criterion) {
    const SIZE: usize = 1024 * 1024;
    let rt = Runtime::new().unwrap();

    for chunk_size in &[32, 128, 512, 1024, 2048, 4096] {
        c.bench_with_input(
            BenchmarkId::new("LimitExecutor", chunk_size),
            chunk_size,
            |b, &chunk_size| {
                let chunk_num = SIZE / chunk_size;
                b.to_async(&rt).iter_batched(
                    || create_limit_executor(chunk_size, chunk_num, 128, 128),
                    |e| execute_executor(e),
                    BatchSize::SmallInput,
                );
            },
        );
    }
}

criterion_group!(benches, bench_limit);
criterion_main!(benches);

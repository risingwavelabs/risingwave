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

use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use risingwave_batch_executors::{BoxedExecutor, ExpandExecutor};
use risingwave_common::enable_jemalloc;
use risingwave_common::types::DataType;
use tokio::runtime::Runtime;
use utils::{create_input, execute_executor};

enable_jemalloc!();

fn create_expand_executor(
    column_subsets: Vec<Vec<usize>>,
    chunk_size: usize,
    chunk_num: usize,
) -> BoxedExecutor {
    const CHUNK_SIZE: usize = 1024;
    let input_types = &[DataType::Int32, DataType::Int64, DataType::Varchar];
    let input = create_input(input_types, chunk_size, chunk_num);
    Box::new(ExpandExecutor::new(input, column_subsets, CHUNK_SIZE))
}

fn bench_expand(c: &mut Criterion) {
    const SIZE: usize = 1024 * 1024;
    let rt = Runtime::new().unwrap();
    for chunk_size in &[32, 128, 512, 1024, 2048, 4096] {
        c.bench_with_input(
            BenchmarkId::new("ExpandExecutor", chunk_size),
            chunk_size,
            |b, &chunk_size| {
                let chunk_num = SIZE / chunk_size;
                b.to_async(&rt).iter_batched(
                    || create_expand_executor(vec![vec![0, 1], vec![2]], chunk_size, chunk_num),
                    execute_executor,
                    BatchSize::SmallInput,
                );
            },
        );
    }
}

criterion_group!(benches, bench_expand);
criterion_main!(benches);

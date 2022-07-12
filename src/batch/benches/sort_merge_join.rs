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
use risingwave_batch::executor::row_level_iter::RowLevelIter;
use risingwave_batch::executor::test_utils::{gen_sorted_data, MockExecutor};
use risingwave_batch::executor::{BoxedExecutor, Executor, JoinType, SortMergeJoinExecutor};
use risingwave_common::catalog::schema_test_utils::field_n;
use risingwave_common::catalog::Schema;
use risingwave_common::types::DataType;
use tikv_jemallocator::Jemalloc;
use tokio::runtime::Runtime;

#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

fn create_sort_merge_join_executor(
    left_chunk_size: usize,
    left_chunk_num: usize,
    right_chunk_size: usize,
    right_chunk_num: usize,
) -> BoxedExecutor {
    let left_input = gen_sorted_data(left_chunk_size, left_chunk_num, "0".into(), 10);
    let right_input = gen_sorted_data(right_chunk_size, right_chunk_num, "1000".into(), 20);

    let mut left_child = Box::new(MockExecutor::new(field_n::<1>(DataType::Int64)));
    left_input.into_iter().for_each(|c| left_child.add(c));

    let mut right_child = Box::new(MockExecutor::new(field_n::<1>(DataType::Int64)));
    right_input.into_iter().for_each(|c| right_child.add(c));

    Box::new(SortMergeJoinExecutor::new(
        JoinType::Inner,
        Schema::from_iter(
            left_child
                .schema()
                .fields()
                .iter()
                .chain(right_child.schema().fields().iter())
                .cloned(),
        ),
        vec![0, 1],
        RowLevelIter::new(left_child),
        RowLevelIter::new(right_child),
        vec![0],
        vec![0],
        "SortMergeJoinExecutor".into(),
    ))
}

async fn execute_sort_merge_join_executor(executor: BoxedExecutor) {
    let mut stream = executor.execute();
    while let Some(ret) = stream.next().await {
        black_box(ret.unwrap());
    }
}

fn bench_sort_merge_join(c: &mut Criterion) {
    const LEFT_SIZE: usize = 2 * 1024;
    const RIGHT_SIZE: usize = 2 * 1024;
    let rt = Runtime::new().unwrap();
    for chunk_size in &[32, 128, 512, 1024] {
        c.bench_with_input(
            BenchmarkId::new("SortMergeJoinExecutor", format!("{}", chunk_size)),
            chunk_size,
            |b, &chunk_size| {
                let left_chunk_num = LEFT_SIZE / chunk_size;
                let right_chunk_num = RIGHT_SIZE / chunk_size;
                b.to_async(&rt).iter_batched(
                    || {
                        create_sort_merge_join_executor(
                            chunk_size,
                            left_chunk_num,
                            chunk_size,
                            right_chunk_num,
                        )
                    },
                    |e| execute_sort_merge_join_executor(e),
                    BatchSize::SmallInput,
                );
            },
        );
    }
}

criterion_group!(benches, bench_sort_merge_join);
criterion_main!(benches);

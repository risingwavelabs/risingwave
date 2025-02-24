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

use criterion::{BatchSize, BenchmarkId, Criterion, black_box};
use futures::StreamExt;
use risingwave_batch_executors::test_utils::{MockExecutor, gen_data};
use risingwave_batch_executors::{BoxedExecutor, JoinType};
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::types::DataType;
use tokio::runtime::Runtime;

pub fn bench_join(
    c: &mut Criterion,
    join_name: &str,
    with_conds: Vec<bool>,
    join_types: Vec<JoinType>,
    create_join_executor: fn(JoinType, bool, usize, usize, usize, usize) -> BoxedExecutor,
) {
    const LEFT_SIZE: usize = 2 * 1024;
    const RIGHT_SIZE: usize = 2 * 1024;
    let rt = Runtime::new().unwrap();
    for with_cond in with_conds {
        for join_type in join_types.clone() {
            for chunk_size in &[32, 128, 512, 1024] {
                c.bench_with_input(
                    BenchmarkId::new(
                        join_name,
                        format!("{}({:?})(join: {})", chunk_size, join_type, with_cond),
                    ),
                    chunk_size,
                    |b, &chunk_size| {
                        let left_chunk_num = LEFT_SIZE / chunk_size;
                        let right_chunk_num = RIGHT_SIZE / chunk_size;
                        b.to_async(&rt).iter_batched(
                            || {
                                create_join_executor(
                                    join_type,
                                    with_cond,
                                    chunk_size,
                                    left_chunk_num,
                                    chunk_size,
                                    right_chunk_num,
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
}

pub async fn execute_executor(executor: BoxedExecutor) {
    let mut stream = executor.execute();
    while let Some(ret) = stream.next().await {
        _ = black_box(ret.unwrap());
    }
}

pub fn create_input(
    input_types: &[DataType],
    chunk_size: usize,
    chunk_num: usize,
) -> BoxedExecutor {
    let mut input = MockExecutor::new(Schema {
        fields: input_types.iter().cloned().map(Field::unnamed).collect(),
    });
    for c in gen_data(chunk_size, chunk_num, input_types) {
        input.add(c);
    }
    Box::new(input)
}

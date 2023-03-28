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

use criterion::{black_box, BatchSize, BenchmarkId, Criterion};
use futures::StreamExt;
use risingwave_stream::executor::test_utils::{gen_data, MockExecutor};
use risingwave_stream::executor::{BoxedExecutor, JoinType};
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::types::DataType;
use tokio::runtime::Runtime;


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
        fields: input_types
            .iter()
            .map(Clone::clone)
            .map(Field::unnamed)
            .collect(),
    });
    for c in gen_data(chunk_size, chunk_num, input_types) {
        input.add(c);
    }
    Box::new(input)
}

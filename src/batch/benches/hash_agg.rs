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
use risingwave_batch::executor::test_utils::{gen_data, MockExecutor};
use risingwave_batch::executor::{BoxedExecutor, HashAggExecutorBuilder};
use risingwave_batch::task::TaskId;
use risingwave_common::catalog::schema_test_utils::field_n;
use risingwave_common::types::DataType;
use risingwave_expr::expr::AggKind;
use risingwave_pb::batch_plan::HashAggNode;
use risingwave_pb::expr::agg_call::Arg;
use risingwave_pb::expr::{AggCall, InputRefExpr};
use tikv_jemallocator::Jemalloc;
use tokio::runtime::Runtime;

#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

fn mock_task_id() -> TaskId {
    TaskId {
        task_id: 0,
        stage_id: 0,
        query_id: String::new(),
    }
}

fn create_hash_agg_executor(chunk_size: usize, chunk_num: usize) -> BoxedExecutor {
    let input_data = gen_data(chunk_size, chunk_num, &[DataType::Int64, DataType::Int64]);

    let mut child = MockExecutor::new(field_n::<2>(DataType::Int64));
    input_data.into_iter().for_each(|c| child.add(c));

    let agg_node = HashAggNode {
        group_key: vec![0], // group by $0
        agg_calls: vec![AggCall {
            r#type: AggKind::Count.to_prost() as i32, // count($1)
            args: vec![Arg {
                input: Some(InputRefExpr { column_idx: 1 }),
                r#type: Some(DataType::Int64.to_protobuf()),
            }],
            return_type: Some(DataType::Int64.to_protobuf()),
            distinct: false,
            order_by_fields: vec![],
            filter: None,
        }],
    };

    HashAggExecutorBuilder::deserialize(
        &agg_node,
        Box::new(child),
        mock_task_id(),
        "HashAggExecutor".to_string(),
    )
    .unwrap()
}

async fn execute_hash_agg_executor(executor: BoxedExecutor) {
    let mut stream = executor.execute();
    while let Some(ret) = stream.next().await {
        black_box(ret.unwrap());
    }
}

fn bench_hash_agg(c: &mut Criterion) {
    const SIZE: usize = 1024 * 1024;
    let rt = Runtime::new().unwrap();

    for chunk_size in &[32, 128, 512, 1024, 2048, 4096] {
        c.bench_with_input(
            BenchmarkId::new("HashAggExecutor", chunk_size),
            chunk_size,
            |b, &chunk_size| {
                let chunk_num = SIZE / chunk_size;
                b.to_async(&rt).iter_batched(
                    || create_hash_agg_executor(chunk_size, chunk_num),
                    |e| execute_hash_agg_executor(e),
                    BatchSize::SmallInput,
                );
            },
        );
    }
}

criterion_group!(benches, bench_hash_agg);
criterion_main!(benches);

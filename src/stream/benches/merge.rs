// Copyright 2024 RisingWave Labs
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
use risingwave_common::array::StreamChunk;
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::field_generator::VarcharProperty;
use risingwave_common::test_prelude::StreamChunkTestExt;
use risingwave_common::types::DataType;
use risingwave_common::util::epoch::test_epoch;
use risingwave_expr::aggregate::AggCall;
use risingwave_expr::expr::*;
use risingwave_storage::memory::MemoryStateStore;
use risingwave_storage::StateStore;
use risingwave_stream::executor::exchange::permit::channel_for_test;
use risingwave_stream::executor::test_utils::*;
use risingwave_stream::executor::{
    DispatcherMessage, Executor, ExecutorInfo, MergeExecutor, PkIndices, ReceiverExecutor,
};
use risingwave_stream::task::barrier_test_utils::LocalBarrierTestEnv;
use tokio::runtime::Runtime;
use tokio::sync::mpsc;

risingwave_expr_impl::enable!();

fn bench_merge(c: &mut Criterion) {
    let mut group = c.benchmark_group("Merge vs Receiver");
    group.sample_size(10);

    let rt = Runtime::new().unwrap();
    group.bench_function("benchmark_merge", |b| {
        b.to_async(&rt).iter_batched(
            || block_on(setup_bench_merge(MemoryStateStore::new())),
            execute_executor,
            BatchSize::SmallInput,
        )
    });

    group.bench_function("benchmark_receiver", |b| {
        b.to_async(&rt).iter_batched(
            || block_on(setup_bench_receiver(MemoryStateStore::new())),
            execute_executor,
            BatchSize::SmallInput,
        )
    });
}

async fn setup_bench_merge<S: StateStore>(store: S) -> (LocalBarrierTestEnv, Executor) {
    let input_data_types = vec![DataType::Varchar, DataType::Int64, DataType::Int64];
    let fields = vec![
        Field::unnamed(DataType::Varchar),
        Field::unnamed(DataType::Int64),
        Field::unnamed(DataType::Int64),
    ];
    let schema = Schema { fields };

    // ---- Generate Data ----

    let num_of_chunks = 1;
    let chunk_size = 1024;
    let chunks = StreamChunk::gen_stream_chunks(
        num_of_chunks,
        chunk_size,
        &input_data_types,
        &VarcharProperty::Constant,
    );

    let (mut tx, rx) = channel_for_test();
    for chunk in chunks {
        tx.send(DispatcherMessage::Chunk(chunk)).await.unwrap();
    }

    // ---- Create MergeExecutor ----
    let (barrier_test_env, merge) = MergeExecutor::for_bench(vec![rx], schema.clone()).await;

    let info = ExecutorInfo {
        schema,
        pk_indices: vec![],
        identity: format!("Merge"),
    };

    (barrier_test_env, (info, merge).into())
}

async fn setup_bench_receiver<S: StateStore>(store: S) -> (LocalBarrierTestEnv, Executor) {
    let input_data_types = vec![DataType::Varchar, DataType::Int64, DataType::Int64];
    let fields = vec![
        Field::unnamed(DataType::Varchar),
        Field::unnamed(DataType::Int64),
        Field::unnamed(DataType::Int64),
    ];
    let schema = Schema { fields };

    // ---- Generate Data ----

    let num_of_chunks = 1;
    let chunk_size = 1024;
    let chunks = StreamChunk::gen_stream_chunks(
        num_of_chunks,
        chunk_size,
        &input_data_types,
        &VarcharProperty::Constant,
    );

    let (mut tx, rx) = channel_for_test();
    for chunk in chunks {
        tx.send(DispatcherMessage::Chunk(chunk)).await.unwrap();
    }

    // ---- Create MergeExecutor ----
    let (barrier_test_env, merge) = ReceiverExecutor::for_bench(rx).await;

    let info = ExecutorInfo {
        schema,
        pk_indices: vec![],
        identity: format!("Merge"),
    };

    (barrier_test_env, (info, merge).into())
}

pub async fn execute_executor(executor: (LocalBarrierTestEnv, Executor)) {
    let (barrier_test_env, executor) = executor;
    let mut stream = executor.execute();
    while let Some(ret) = stream.next().await {
        _ = black_box(ret.unwrap());
    }
}

criterion_group!(benches, bench_merge);
criterion_main!(benches);

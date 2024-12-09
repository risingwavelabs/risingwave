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

use crate::prelude::*;
use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion};
use futures::executor::block_on;
use futures::StreamExt;
use risingwave_common::array::{DataChunk, Op, StreamChunk, I64Array};
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::field_generator::VarcharProperty;
use risingwave_common::test_prelude::StreamChunkTestExt;
use risingwave_common::types::DataType;
use risingwave_common::util::epoch::test_epoch;
use risingwave_expr::expr::*;
use risingwave_storage::memory::MemoryStateStore;
use risingwave_storage::StateStore;
use risingwave_stream::executor::test_utils::hash_join_executor::*;
use risingwave_stream::executor::test_utils::*;
use risingwave_stream::executor::{Executor, PkIndices};
use tokio::runtime::Runtime;

risingwave_expr_impl::enable!();

fn bench_hash_join(c: &mut Criterion) {
    let mut group = c.benchmark_group("bench_hash_join");
    group.sample_size(10);

    let rt = Runtime::new().unwrap();
    group.bench_function("cache_refill_memory_utilization", |b| {
        b.to_async(&rt).iter_batched(
            || block_on(setup_bench_stream_hash_join(MemoryStateStore::new())),
            execute_executor,
            BatchSize::SmallInput,
        )
    });
}

/// 1. Refill state table of build side.
/// 2. Init executor.
/// 3. Push data to the probe side.
/// 4. Check memory utilization.
async fn setup_bench_stream_hash_join<S: StateStore>(store: S) -> Executor {
    let fields = vec![DataType::Int64, DataType::Int64, DataType::Int64];
    let orders = vec![OrderType::ascending(), OrderType::ascending(), OrderType::ascending()];
    let state_store = MemoryStateStore::new();

    // Probe side
    let (lhs_state_table, lhs_degree_state_table) = create_in_memory_state_table(
        state_store.clone(),
        &fields,
        &orders,
        &[0, 1],
        0,
    ).await;

    // Build side
    let (mut rhs_state_table, rhs_degree_state_table) = create_in_memory_state_table(
        state_store.clone(),
        &fields,
        &orders,
        &[0, 1],
        2,
    ).await;

    // Insert 100K records into the build side.
    let mut int64_jk_builder = DataType::Int64.create_array_builder(100_000);
    int64_jk_builder.append_array(&I64Array::from_iter([Some(200_000); 100_000].into_iter()).into());
    let arr0 = int64_jk_builder.finish();

    let mut int64_pk_data_chunk_builder = DataType::Int64.create_array_builder(100_000);
    let seq = I64Array::from_iter((0..100_000).map(|i| { Some(i) }));
    int64_pk_data_chunk_builder.append_array(&I64Array::from(seq).into());
    let arr1 = int64_pk_data_chunk_builder.finish();
    let arr2 = arr1.clone();

    let columns = vec![arr0.into(), arr1.into(), arr2.into()];

    let ops = vec![Op::Insert; 100_000];
    let stream_chunk = StreamChunk::new(ops, columns);

    rhs_state_table.write_chunk(stream_chunk);

    todo!()
}

criterion_group!(benches, bench_hash_join);
criterion_main!(benches);

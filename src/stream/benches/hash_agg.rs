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

use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion};
use futures::executor::block_on;
use futures::StreamExt;
use itertools::Itertools;
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::row::{AscentOwnedRow, OwnedRow, Row};
use risingwave_common::types::DataType;
use risingwave_common::util::iter_util::ZipEqDebug;
use risingwave_expr::expr::*;
use risingwave_storage::memory::MemoryStateStore;
use risingwave_storage::StateStore;
use risingwave_stream::executor::aggregation::{AggArgs, AggCall};
use risingwave_stream::executor::new_boxed_hash_agg_executor;
use risingwave_stream::executor::test_utils::*;
use risingwave_stream::executor::{BoxedExecutor, PkIndices};
use tokio::runtime::Runtime;

trait SortedRows {
    fn sorted_rows(self) -> Vec<(Op, OwnedRow)>;
}
impl SortedRows for StreamChunk {
    fn sorted_rows(self) -> Vec<(Op, OwnedRow)> {
        let (chunk, ops) = self.into_parts();
        ops.into_iter()
            .zip_eq_debug(
                chunk
                    .rows()
                    .map(Row::into_owned_row)
                    .map(AscentOwnedRow::from),
            )
            .sorted()
            .map(|(op, row)| (op, row.into_inner()))
            .collect_vec()
    }
}

fn bench_hash_agg(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    c.bench_function("benchmark_hash_agg", |b| {
        b.to_async(&rt).iter_batched(
            || setup_bench_hash_agg(MemoryStateStore::new()),
            |e| execute_executor(e),
            BatchSize::SmallInput,
        )
    });
}

/// Basic case:
/// pk: none
/// group by: 0
/// `chunk_size`: 1000
/// `num_of_chunks`: 1024
/// aggregation: count
fn setup_bench_hash_agg<S: StateStore>(store: S) -> BoxedExecutor {
    // ---- Define hash agg executor parameters ----
    let data_types = vec![DataType::Int64; 3];
    let schema = Schema {
        fields: vec![Field::unnamed(DataType::Int64); 3],
    };

    let group_key_indices = vec![0];

    let append_only = false;

    let agg_calls = vec![
        AggCall {
            kind: AggKind::Count, // as row count, index: 0
            args: AggArgs::None,
            return_type: DataType::Int64,
            column_orders: vec![],
            append_only,
            filter: None,
            distinct: false,
        },
        AggCall {
            kind: AggKind::Sum,
            args: AggArgs::Unary(DataType::Int64, 1),
            return_type: DataType::Int64,
            column_orders: vec![],
            append_only,
            filter: None,
            distinct: false,
        },
        AggCall {
            kind: AggKind::Sum,
            args: AggArgs::Unary(DataType::Int64, 2),
            return_type: DataType::Int64,
            column_orders: vec![],
            append_only,
            filter: None,
            distinct: false,
        },
    ];

    // ---- Generate Data ----
    let num_of_chunks = 1000;
    let chunk_size = 1024;
    let chunks = gen_data(num_of_chunks, chunk_size, &data_types);

    // ---- Create MockSourceExecutor ----
    let (mut tx, source) = MockSource::channel(schema, PkIndices::new());
    tx.push_barrier(1, false);
    for chunk in chunks {
        tx.push_chunk(chunk);
    }
    tx.push_barrier_with_prev_epoch_for_test(1002, 1, false);

    // ---- Create HashAggExecutor to be benchmarked ----
    let row_count_index = 0;
    let pk_indices = vec![];
    let extreme_cache_size = 1024;
    let executor_id = 1;

    block_on(new_boxed_hash_agg_executor(
        store,
        Box::new(source),
        agg_calls,
        row_count_index,
        group_key_indices,
        pk_indices,
        extreme_cache_size,
        executor_id,
    )) as _
}

pub async fn execute_executor(executor: BoxedExecutor) {
    let mut stream = executor.execute();
    while let Some(ret) = stream.next().await {
        _ = black_box(ret.unwrap());
    }
}

criterion_group!(benches, bench_hash_agg);
criterion_main!(benches);

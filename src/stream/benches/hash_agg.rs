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
use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};
// use itertools::Itertools;
// use risingwave_common::catalog::{Field, Schema};
// use risingwave_common::types::DataType;
// use risingwave_common::{enable_jemalloc_on_unix, hash};
// use risingwave_expr::expr::AggKind;
// use risingwave_expr::vector_op::agg::AggStateFactory;
// // use risingwave_pb::expr::{AggCall, InputRef};
// use risingwave_stream::executor::{BoxedExecutor, HashAggExecutor};
use risingwave_stream::executor::new_boxed_hash_agg_executor;
// use tokio::runtime::Runtime;

use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use assert_matches::assert_matches;
use futures::StreamExt;
use itertools::Itertools;
use risingwave_common::array::stream_chunk::StreamChunkTestExt;
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::catalog::{Field, Schema, TableId};
use risingwave_common::hash::SerializedKey;
use risingwave_common::row::{AscentOwnedRow, OwnedRow, Row};
use risingwave_common::types::DataType;
use risingwave_common::util::iter_util::ZipEqDebug;
use risingwave_expr::expr::*;
use risingwave_storage::memory::MemoryStateStore;
use risingwave_storage::StateStore;

use risingwave_stream::executor::aggregation::{AggArgs, AggCall};
use risingwave_stream::executor::monitor::StreamingMetrics;
use risingwave_stream::executor::test_utils::agg_executor::{
    create_agg_state_storage, create_result_table,
};
use risingwave_stream::executor::test_utils::*;
use risingwave_stream::executor::{ActorContext, Executor, HashAggExecutor, Message, PkIndices};

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

}

/// Basic case:
/// pk: none
/// group by: 0
/// chunk_size: 1000
/// num_of_chunks: 1024
/// aggregation: count
async fn setup_bench_hash_agg<S: StateStore>(store: S) {
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

    // ---- Create HashAggExecutor to be benchmarked ----
    let hash_agg = new_boxed_hash_agg_executor(
        store,
        Box::new(source),
        agg_calls,
        0,
        group_key_indices,
        vec![],
        1 << 10,
        1,
    )
    .await;
    let mut hash_agg = hash_agg.execute();

    // Consume the init barrier
    hash_agg.next().await.unwrap().unwrap();
    // Consume stream chunk
    let msg = hash_agg.next().await.unwrap().unwrap();
    assert_eq!(
        msg.into_chunk().unwrap().sorted_rows(),
        StreamChunk::from_pretty(
            " I I I I
            + 1 1 1 1
            + 2 2 4 4"
        )
        .sorted_rows(),
    );

    assert_matches!(
        hash_agg.next().await.unwrap().unwrap(),
        Message::Barrier { .. }
    );

    let msg = hash_agg.next().await.unwrap().unwrap();
    assert_eq!(
        msg.into_chunk().unwrap().sorted_rows(),
        StreamChunk::from_pretty(
            "  I I I I
            -  1 1 1 1
            U- 2 2 4 4
            U+ 2 1 2 2
            +  3 1 3 3"
        )
        .sorted_rows(),
    );
}

criterion_group!(benches, bench_hash_agg);
criterion_main!(benches);

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

use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use futures::executor::block_on;
use risingwave_common::array::{DataChunk, DataChunkTestExt, StreamChunk};
use risingwave_common::catalog::{ColumnDesc, ColumnId, TableId};
use risingwave_common::field_generator::VarcharProperty;
use risingwave_common::row::{OwnedRow, Row};
use risingwave_common::test_prelude::StreamChunkTestExt;
use risingwave_common::types::DataType;
use risingwave_common::util::epoch::{EpochPair, test_epoch};
use risingwave_common::util::sort_util::OrderType;
use risingwave_storage::memory::MemoryStateStore;
use risingwave_stream::common::table::state_table::WatermarkCacheParameterizedStateTable;
use risingwave_stream::common::table::test_utils::gen_pbtable;
use tokio::runtime::Runtime;

type TestStateTable<const USE_WATERMARK_CACHE: bool> =
    WatermarkCacheParameterizedStateTable<MemoryStateStore, USE_WATERMARK_CACHE>;

const FEW_DATA_TYPES: [DataType; 4] = [
    // Keys
    DataType::Int32,
    DataType::Int64,
    // Values
    DataType::Int32,
    DataType::Int64,
];

const MANY_DATA_TYPES: [DataType; 14] = [
    // Keys
    DataType::Timestamp,
    DataType::Int16,
    DataType::Int32,
    DataType::Int64,
    DataType::Float32,
    DataType::Float64,
    DataType::Varchar,
    // Values
    DataType::Timestamp,
    DataType::Int16,
    DataType::Int32,
    DataType::Int64,
    DataType::Float32,
    DataType::Float64,
    DataType::Varchar,
];

async fn create_state_table<const USE_WATERMARK_CACHE: bool>(
    data_types: &[DataType],
) -> TestStateTable<USE_WATERMARK_CACHE> {
    const TEST_TABLE_ID: TableId = TableId { table_id: 233 };

    let column_descs = data_types
        .iter()
        .enumerate()
        .map(|(i, data_type)| ColumnDesc::unnamed(ColumnId::new(i as i32), data_type.clone()))
        .collect::<Vec<_>>();
    let key_length = column_descs.len() / 2;
    let order_types = vec![OrderType::ascending(); key_length];
    let pk_indices = (0..key_length).collect();

    let store = MemoryStateStore::new();
    TestStateTable::<USE_WATERMARK_CACHE>::from_table_catalog_inconsistent_op(
        &gen_pbtable(TEST_TABLE_ID, column_descs, order_types, pk_indices, 0),
        store,
        None,
    )
    .await
}

fn gen_inserts(n: usize, data_types: &[DataType]) -> Vec<OwnedRow> {
    let chunk = DataChunk::gen_data_chunk(0, n, data_types, &VarcharProperty::Constant, 1.0);
    chunk.rows().map(|r| r.into_owned_row()).collect()
}

fn gen_stream_chunks(
    n: usize,
    data_types: &[DataType],
    visibility_percent: f64,
    inserts_percent: f64,
) -> Vec<StreamChunk> {
    StreamChunk::gen_stream_chunks_inner(
        n,
        1024,
        data_types,
        &VarcharProperty::RandomFixedLength(Some(20)),
        visibility_percent,
        inserts_percent,
    )
}

fn setup_bench_state_table<const USE_WATERMARK_CACHE: bool>(
    data_types: &[DataType],
) -> TestStateTable<USE_WATERMARK_CACHE> {
    block_on(create_state_table(data_types))
}

async fn run_bench_state_table_inserts<const USE_WATERMARK_CACHE: bool>(
    mut state_table: TestStateTable<USE_WATERMARK_CACHE>,
    rows: Vec<OwnedRow>,
) {
    let mut epoch = EpochPair::new_test_epoch(test_epoch(1));
    state_table.init_epoch(epoch).await.unwrap();
    for row in rows {
        state_table.insert(row);
    }
    epoch.inc_for_test();
    state_table
        .commit_assert_no_update_vnode_bitmap(epoch)
        .await
        .unwrap();
}

fn bench_state_table_inserts(c: &mut Criterion) {
    let mut group = c.benchmark_group("state_table");
    group.sample_size(100);

    let rt = Runtime::new().unwrap();
    group.bench_function("inserts:col=2,watermark_cache=false", |b| {
        b.to_async(&rt).iter_batched(
            || {
                (
                    setup_bench_state_table::<false>(&FEW_DATA_TYPES),
                    gen_inserts(1, &FEW_DATA_TYPES),
                )
            },
            |(state_table, rows)| run_bench_state_table_inserts(state_table, rows),
            BatchSize::SmallInput,
        )
    });

    let rt = Runtime::new().unwrap();
    group.bench_function("inserts:col=7,watermark_cache=true", |b| {
        b.to_async(&rt).iter_batched(
            || {
                (
                    setup_bench_state_table::<true>(&MANY_DATA_TYPES),
                    gen_inserts(1, &MANY_DATA_TYPES),
                )
            },
            |(state_table, rows)| run_bench_state_table_inserts(state_table, rows),
            BatchSize::SmallInput,
        )
    });

    let rt = Runtime::new().unwrap();
    group.bench_function("inserts:col=7,watermark_cache=false", |b| {
        b.to_async(&rt).iter_batched(
            || {
                (
                    setup_bench_state_table::<false>(&MANY_DATA_TYPES),
                    gen_inserts(1, &MANY_DATA_TYPES),
                )
            },
            |(state_table, rows)| run_bench_state_table_inserts(state_table, rows),
            BatchSize::SmallInput,
        )
    });
}

async fn run_bench_state_table_chunks<const USE_WATERMARK_CACHE: bool>(
    mut state_table: TestStateTable<USE_WATERMARK_CACHE>,
    chunks: Vec<StreamChunk>,
) {
    let mut epoch = EpochPair::new_test_epoch(test_epoch(1));
    state_table.init_epoch(epoch).await.unwrap();
    for chunk in chunks {
        state_table.write_chunk(chunk);
    }
    epoch.inc_for_test();
    state_table
        .commit_assert_no_update_vnode_bitmap(epoch)
        .await
        .unwrap();
}

fn bench_state_table_write_chunk(c: &mut Criterion) {
    let visibilities = [0.5, 0.90, 0.99, 1.0];
    let inserts = [0.5, 0.90, 0.99, 1.0];
    let schemas = [&MANY_DATA_TYPES[..], &FEW_DATA_TYPES[..]];
    let use_watermark_cache_options = [true, false];
    for use_watermark_cache in use_watermark_cache_options {
        for schema in schemas {
            for visibility in visibilities {
                for insert in inserts {
                    bench_state_table_chunks(c, use_watermark_cache, schema, visibility, insert);
                }
            }
        }
    }
}

fn bench_state_table_chunks(
    c: &mut Criterion,
    use_watermark_cache: bool,
    data_types: &[DataType],
    visibility_percent: f64,
    inserts_percent: f64,
) {
    let mut group = c.benchmark_group("state_table");
    group.sample_size(100);

    let data_types_len = data_types.len();
    let deletes_percent = 1.0 - inserts_percent;

    let rt = Runtime::new().unwrap();
    if use_watermark_cache && matches!(data_types[0], DataType::Timestamp) {
        group.bench_function(
            format!(
                "write_chunk:vis={:.2},insert={:.2},delete={:.2},watermark_cache={use_watermark_cache},cols={data_types_len}",
                visibility_percent, inserts_percent, deletes_percent
            ),
            |b| {
                b.to_async(&rt).iter_batched(
                    || {
                        (
                            setup_bench_state_table::<true>(data_types),
                            gen_stream_chunks(100, data_types, visibility_percent, inserts_percent),
                        )
                    },
                    |(state_table, chunks)| run_bench_state_table_chunks(state_table, chunks),
                    BatchSize::SmallInput,
                )
            },
        );
    } else {
        group.bench_function(
            format!(
                "write_chunk:vis={:.2},insert={:.2},delete={:.2},watermark_cache={use_watermark_cache},cols={data_types_len}",
                visibility_percent, inserts_percent, deletes_percent
            ),
            |b| {
                b.to_async(&rt).iter_batched(
                    || {
                        (
                            setup_bench_state_table::<false>(data_types),
                            gen_stream_chunks(100, data_types, visibility_percent, inserts_percent),
                        )
                    },
                    |(state_table, chunks)| run_bench_state_table_chunks(state_table, chunks),
                    BatchSize::SmallInput,
                )
            },
        );
    }
}

criterion_group!(
    benches,
    bench_state_table_inserts,
    bench_state_table_write_chunk
);
criterion_main!(benches);

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

use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use risingwave_common::array::{DataChunk, DataChunkTestExt, StreamChunk};
use risingwave_common::catalog::{ColumnDesc, ColumnId, TableId};
use risingwave_common::field_generator::VarcharProperty;
use risingwave_common::row::{OwnedRow, Row};
use risingwave_common::test_prelude::StreamChunkTestExt;
use risingwave_common::types::DataType;
use risingwave_common::util::epoch::EpochPair;
use risingwave_common::util::sort_util::OrderType;
use risingwave_hummock_test::test_utils::{prepare_hummock_test_env, HummockTestEnv};
use risingwave_storage::hummock::HummockStorage;
use risingwave_stream::common::table::state_table::StateTable;
use risingwave_stream::common::table::test_utils::gen_prost_table;
use tokio::runtime::Runtime;

type TestStateTable = StateTable<HummockStorage>;

async fn create_test_env() -> HummockTestEnv {
    prepare_hummock_test_env().await
}

async fn create_state_table(test_env: &mut HummockTestEnv) -> TestStateTable {
    const TEST_TABLE_ID: TableId = TableId { table_id: 233 };

    let column_descs = vec![
        // TODO: ColumnDesc::unnamed(ColumnId::from(0), DataType::Timestamptz),
        ColumnDesc::unnamed(ColumnId::from(1), DataType::Int32),
        ColumnDesc::unnamed(ColumnId::from(2), DataType::Int64),
        ColumnDesc::unnamed(ColumnId::from(3), DataType::Int64),
    ];
    let order_types = vec![OrderType::ascending(), OrderType::ascending()];
    let pk_index = vec![0_usize, 1_usize];
    let read_prefix_len_hint = 0;
    let table = gen_prost_table(
        TEST_TABLE_ID,
        column_descs,
        order_types,
        pk_index,
        read_prefix_len_hint,
    );

    test_env.register_table(table.clone()).await;
    StateTable::from_table_catalog_inconsistent_op(&table, test_env.storage.clone(), None).await
}

fn gen_inserts(n: usize, data_types: &[DataType]) -> Vec<OwnedRow> {
    let chunk = DataChunk::gen_data_chunk(0, n, data_types, &VarcharProperty::Constant);
    chunk.rows().map(|r| r.into_owned_row()).collect()
}

fn gen_stream_chunks(n: usize, data_types: &[DataType]) -> Vec<StreamChunk> {
    StreamChunk::gen_stream_chunks(n, 1024, data_types, &VarcharProperty::Constant)
}

fn setup_bench_state_table() -> (HummockTestEnv, TestStateTable) {
    let rt = Runtime::new().unwrap();
    let mut test_env = rt.block_on(create_test_env());
    let state_table = rt.block_on(create_state_table(&mut test_env));
    (test_env, state_table)
}

async fn run_bench_state_table_inserts(
    _test_env: HummockTestEnv,
    mut state_table: TestStateTable,
    rows: Vec<OwnedRow>,
) {
    let mut epoch = EpochPair::new_test_epoch(1);
    state_table.init_epoch(epoch);
    for row in rows {
        state_table.insert(row);
    }
    epoch.inc();
    state_table.commit(epoch).await.unwrap();
}

fn bench_state_table_inserts(c: &mut Criterion) {
    let mut group = c.benchmark_group("state_table");
    group.sample_size(10);

    let rt = Runtime::new().unwrap();
    group.bench_function("benchmark_hash_agg", |b| {
        b.to_async(&rt).iter_batched(
            || {
                (
                    setup_bench_state_table(),
                    gen_inserts(10000, &[DataType::Int32, DataType::Int64, DataType::Int64]),
                )
            },
            |((test_env, state_table), rows)| {
                run_bench_state_table_inserts(test_env, state_table, rows)
            },
            BatchSize::SmallInput,
        )
    });
}

async fn run_bench_state_table_chunks(
    _test_env: HummockTestEnv,
    mut state_table: TestStateTable,
    chunks: Vec<StreamChunk>,
) {
    let mut epoch = EpochPair::new_test_epoch(1);
    state_table.init_epoch(epoch);
    for chunk in chunks {
        state_table.write_chunk(chunk);
    }
    epoch.inc();
    state_table.commit(epoch).await.unwrap();
}

fn bench_state_table_insert_chunks(c: &mut Criterion) {
    let mut group = c.benchmark_group("state_table");
    group.sample_size(10);

    let rt = Runtime::new().unwrap();
    group.bench_function("benchmark_hash_agg", |b| {
        b.to_async(&rt).iter_batched(
            || {
                (
                    setup_bench_state_table(),
                    gen_stream_chunks(100, &[DataType::Int32, DataType::Int64, DataType::Int64]),
                )
            },
            |((test_env, state_table), chunks)| {
                run_bench_state_table_chunks(test_env, state_table, chunks)
            },
            BatchSize::SmallInput,
        )
    });
}

criterion_group!(
    benches,
    bench_state_table_inserts,
    bench_state_table_insert_chunks
);
criterion_main!(benches);

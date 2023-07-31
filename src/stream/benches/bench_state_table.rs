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
use risingwave_common::array::StreamChunk;
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::field_generator::VarcharProperty;
use risingwave_common::test_prelude::StreamChunkTestExt;
use risingwave_common::types::DataType;
use risingwave_expr::agg::AggCall;
use risingwave_expr::expr::*;
use risingwave_storage::memory::MemoryStateStore;
use risingwave_storage::StateStore;
use risingwave_stream::executor::test_utils::agg_executor::new_boxed_hash_agg_executor;
use risingwave_stream::executor::test_utils::*;
use risingwave_stream::executor::{BoxedExecutor, PkIndices};
use tokio::runtime::Runtime;

struct TestEnv {

}

async fn test_state_table_update_insert() {
    const TEST_TABLE_ID: TableId = TableId { table_id: 233 };
    let test_env = prepare_hummock_test_env().await;

    let column_descs = vec![
        ColumnDesc::unnamed(ColumnId::from(0), DataType::Timestamptz),
        ColumnDesc::unnamed(ColumnId::from(1), DataType::Int32),
        ColumnDesc::unnamed(ColumnId::from(2), DataType::Int64),
        ColumnDesc::unnamed(ColumnId::from(3), DataType::Int64),
    ];
    let order_types = vec![
        OrderType::ascending(),
        OrderType::ascending(),
        OrderType::ascending()
    ];
    let pk_index = vec![0_usize, 1_usize, 2_usize];
    let read_prefix_len_hint = 0;
    let table = gen_prost_table(
        TEST_TABLE_ID,
        column_descs,
        order_types,
        pk_index,
        read_prefix_len_hint,
    );

    test_env.register_table(table.clone()).await;
    let mut state_table =
        StateTable::from_table_catalog_inconsistent_op(&table, test_env.storage.clone(), None)
            .await;

    let mut epoch = EpochPair::new_test_epoch(1);
    state_table.init_epoch(epoch);

    state_table.insert(OwnedRow::new(vec![
        Some(6_i32.into()),
        Some(66_i32.into()),
        Some(666_i32.into()),
        Some(6666_i32.into()),
    ]));

    state_table.insert(OwnedRow::new(vec![
        Some(7_i32.into()),
        None,
        Some(777_i32.into()),
        None,
    ]));

    epoch.inc();
    state_table.commit(epoch).await.unwrap();

    state_table.delete(OwnedRow::new(vec![
        Some(6_i32.into()),
        Some(66_i32.into()),
        Some(666_i32.into()),
        Some(6666_i32.into()),
    ]));
    state_table.insert(OwnedRow::new(vec![
        Some(6_i32.into()),
        None,
        None,
        Some(6666_i32.into()),
    ]));

    state_table.delete(OwnedRow::new(vec![
        Some(7_i32.into()),
        None,
        Some(777_i32.into()),
        None,
    ]));
    state_table.insert(OwnedRow::new(vec![
        Some(7_i32.into()),
        Some(77_i32.into()),
        Some(7777_i32.into()),
        None,
    ]));
    let row6 = state_table
        .get_row(&OwnedRow::new(vec![Some(6_i32.into())]))
        .await
        .unwrap();
    assert_eq!(
        row6,
        Some(OwnedRow::new(vec![
            Some(6_i32.into()),
            None,
            None,
            Some(6666_i32.into())
        ]))
    );

    let row7 = state_table
        .get_row(&OwnedRow::new(vec![Some(7_i32.into())]))
        .await
        .unwrap();
    assert_eq!(
        row7,
        Some(OwnedRow::new(vec![
            Some(7_i32.into()),
            Some(77_i32.into()),
            Some(7777_i32.into()),
            None
        ]))
    );

    epoch.inc();
    state_table.commit(epoch).await.unwrap();

    let row6_commit = state_table
        .get_row(&OwnedRow::new(vec![Some(6_i32.into())]))
        .await
        .unwrap();
    assert_eq!(
        row6_commit,
        Some(OwnedRow::new(vec![
            Some(6_i32.into()),
            None,
            None,
            Some(6666_i32.into())
        ]))
    );
    let row7_commit = state_table
        .get_row(&OwnedRow::new(vec![Some(7_i32.into())]))
        .await
        .unwrap();
    assert_eq!(
        row7_commit,
        Some(OwnedRow::new(vec![
            Some(7_i32.into()),
            Some(77_i32.into()),
            Some(7777_i32.into()),
            None
        ]))
    );

    state_table.insert(OwnedRow::new(vec![
        Some(1_i32.into()),
        Some(2_i32.into()),
        Some(3_i32.into()),
        Some(4_i32.into()),
    ]));

    epoch.inc();
    state_table.commit(epoch).await.unwrap();

    // one epoch: delete (1, 2, 3, 4), insert (5, 6, 7, None), delete(5, 6, 7, None)
    state_table.delete(OwnedRow::new(vec![
        Some(1_i32.into()),
        Some(2_i32.into()),
        Some(3_i32.into()),
        Some(4_i32.into()),
    ]));
    state_table.insert(OwnedRow::new(vec![
        Some(5_i32.into()),
        Some(6_i32.into()),
        Some(7_i32.into()),
        None,
    ]));
    state_table.delete(OwnedRow::new(vec![
        Some(5_i32.into()),
        Some(6_i32.into()),
        Some(7_i32.into()),
        None,
    ]));

    let row1 = state_table
        .get_row(&OwnedRow::new(vec![Some(1_i32.into())]))
        .await
        .unwrap();
    assert_eq!(row1, None);

    epoch.inc();
    state_table.commit(epoch).await.unwrap();

    let row1_commit = state_table
        .get_row(&OwnedRow::new(vec![Some(1_i32.into())]))
        .await
        .unwrap();
    assert_eq!(row1_commit, None);
}

fn bench_state_table(c: &mut Criterion) {
    let mut group = c.benchmark_group("state_table");
    group.sample_size(10);

    let rt = Runtime::new().unwrap();
    group.bench_function("benchmark_hash_agg", |b| {
        b.to_async(&rt).iter_batched(
            || setup_bench_hash_agg(MemoryStateStore::new()),
            |e| execute_executor(e),
            BatchSize::SmallInput,
        )
    });
}

fn setup_bench_state_table<S: StateStore>(store: S) {}

criterion_group!(benches, bench_state_table);
criterion_main!(benches);

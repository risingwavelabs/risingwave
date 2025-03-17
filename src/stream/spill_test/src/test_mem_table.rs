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

use risingwave_common::catalog::{ColumnDesc, ColumnId, TableId};
use risingwave_common::row::OwnedRow;
use risingwave_common::types::DataType;
use risingwave_common::util::epoch::{EpochPair, test_epoch};
use risingwave_common::util::sort_util::OrderType;
use risingwave_hummock_test::test_utils::prepare_hummock_test_env;
use risingwave_stream::common::table::state_table::StateTable;
use risingwave_stream::common::table::test_utils::gen_pbtable;

#[tokio::test]
async fn test_mem_table_spill_in_streaming() {
    const TEST_TABLE_ID: TableId = TableId { table_id: 233 };
    let test_env = prepare_hummock_test_env().await;

    let column_descs = vec![
        ColumnDesc::unnamed(ColumnId::from(0), DataType::Int32),
        ColumnDesc::unnamed(ColumnId::from(1), DataType::Varchar),
    ];
    let size: usize = 5 * 1024 * 1024; // 5MB
    let mut text1 = String::with_capacity(size);

    for _ in 0..size {
        text1.push('a');
    }

    let mut text2 = String::with_capacity(size);

    for _ in 0..size {
        text2.push('b');
    }

    let mut text3 = String::with_capacity(size);

    for _ in 0..size {
        text3.push('c');
    }
    let order_types = vec![OrderType::ascending()];
    let pk_index = vec![0_usize];
    let read_prefix_len_hint = 1;
    let table = gen_pbtable(
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

    let epoch = EpochPair::new_test_epoch(test_epoch(1));
    state_table.init_epoch(epoch).await.unwrap();

    state_table.insert(OwnedRow::new(vec![
        Some(1_i32.into()),
        Some(text1.clone().into()),
    ]));

    let row1 = state_table
        .get_row(&OwnedRow::new(vec![Some(1_i32.into())]))
        .await
        .unwrap();
    assert_eq!(
        row1,
        Some(OwnedRow::new(vec![
            Some(1_i32.into()),
            Some(text1.clone().into()),
        ]))
    );

    state_table.try_flush().await.unwrap();

    let row1_spill = state_table
        .get_row(&OwnedRow::new(vec![Some(1_i32.into())]))
        .await
        .unwrap();
    assert_eq!(
        row1_spill,
        Some(OwnedRow::new(vec![
            Some(1_i32.into()),
            Some(text1.clone().into()),
        ]))
    );

    state_table.insert(OwnedRow::new(vec![
        Some(2_i32.into()),
        Some(text2.clone().into()),
    ]));

    let row2 = state_table
        .get_row(&OwnedRow::new(vec![Some(2_i32.into())]))
        .await
        .unwrap();
    assert_eq!(
        row2,
        Some(OwnedRow::new(vec![
            Some(2_i32.into()),
            Some(text2.clone().into()),
        ]))
    );

    state_table.try_flush().await.unwrap();

    // spill occurs, the epoch has changed.

    let row2_spill = state_table
        .get_row(&OwnedRow::new(vec![Some(2_i32.into())]))
        .await
        .unwrap();
    assert_eq!(
        row2_spill,
        Some(OwnedRow::new(vec![
            Some(2_i32.into()),
            Some(text2.clone().into()),
        ]))
    );

    state_table.insert(OwnedRow::new(vec![
        Some(3_i32.into()),
        Some(text3.clone().into()),
    ]));

    state_table.try_flush().await.unwrap();

    let row3 = state_table
        .get_row(&OwnedRow::new(vec![Some(3_i32.into())]))
        .await
        .unwrap();
    assert_eq!(
        row3,
        Some(OwnedRow::new(vec![
            Some(3_i32.into()),
            Some(text3.clone().into()),
        ]))
    );
}

#[tokio::test]
async fn test_mem_table_spill_in_streaming_multiple_times() {
    const TEST_TABLE_ID: TableId = TableId { table_id: 233 };
    let test_env = prepare_hummock_test_env().await;

    let column_descs = vec![
        ColumnDesc::unnamed(ColumnId::from(0), DataType::Int32),
        ColumnDesc::unnamed(ColumnId::from(1), DataType::Varchar),
    ];
    let size: usize = 5 * 1024 * 1024; // 5MB
    let mut text1 = String::with_capacity(size);

    for _ in 0..size {
        text1.push('a');
    }

    let mut text2 = String::with_capacity(size);

    for _ in 0..size {
        text2.push('b');
    }

    let mut text3 = String::with_capacity(size);

    for _ in 0..size {
        text3.push('c');
    }
    let order_types = vec![OrderType::ascending()];
    let pk_index = vec![0_usize];
    let read_prefix_len_hint = 1;
    let table = gen_pbtable(
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

    let epoch = EpochPair::new_test_epoch(test_epoch(1));
    state_table.init_epoch(epoch).await.unwrap();

    state_table.insert(OwnedRow::new(vec![
        Some(1_i32.into()),
        Some(text1.clone().into()),
    ]));

    state_table.try_flush().await.unwrap();

    state_table.insert(OwnedRow::new(vec![
        Some(1_i32.into()),
        Some(text2.clone().into()),
    ]));

    state_table.try_flush().await.unwrap();

    state_table.insert(OwnedRow::new(vec![
        Some(1_i32.into()),
        Some(text3.clone().into()),
    ]));

    state_table.try_flush().await.unwrap();

    // spill occurs three times, and the epoch_with_gap is text3 > text2 > text1, should get text3.

    let row = state_table
        .get_row(&OwnedRow::new(vec![Some(1_i32.into())]))
        .await
        .unwrap();
    assert_eq!(
        row,
        Some(OwnedRow::new(vec![
            Some(1_i32.into()),
            Some(text3.clone().into()),
        ]))
    );
}

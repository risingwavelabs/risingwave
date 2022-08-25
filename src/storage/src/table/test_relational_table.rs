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

use futures::{pin_mut, StreamExt};
use risingwave_common::array::Row;
use risingwave_common::catalog::{ColumnDesc, ColumnId, TableId, TableOption};
use risingwave_common::types::DataType;
use risingwave_common::util::sort_util::OrderType;

use crate::memory::MemoryStateStore;
use crate::table::storage_table::RowBasedStorageTable;
use crate::table::streaming_table::state_table::StateTable;
use crate::table::{Distribution, TableIter};

/// There are three struct in relational layer, StateTable, MemTable and CellBasedTable.
/// `StateTable` provides read/write interfaces to the upper layer streaming operator.
/// `MemTable` is an in-memory buffer used to cache operator operations.
/// `CellBasedTable` provides the transform from the kv encoding (hummock) to cell_based row
/// encoding.

#[tokio::test]
async fn test_shuffled_column_id_for_get_row() {
    let state_store = MemoryStateStore::new();
    let column_ids = vec![ColumnId::from(3), ColumnId::from(2), ColumnId::from(1)];
    let column_descs = vec![
        ColumnDesc::unnamed(column_ids[0], DataType::Int32),
        ColumnDesc::unnamed(column_ids[1], DataType::Int32),
        ColumnDesc::unnamed(column_ids[2], DataType::Int32),
    ];

    let order_types = vec![OrderType::Ascending, OrderType::Descending];
    let pk_indices = vec![0_usize, 1_usize];
    let mut state = StateTable::new_without_distribution(
        state_store.clone(),
        TableId::from(0x42),
        column_descs.clone(),
        order_types.clone(),
        pk_indices.clone(),
    );
    let table = RowBasedStorageTable::new_for_test(
        state_store.clone(),
        TableId::from(0x42),
        column_descs.clone(),
        order_types.clone(),
        pk_indices,
    );
    let epoch: u64 = 0;

    state
        .insert(Row(vec![Some(1_i32.into()), None, None]))
        .unwrap();
    state
        .insert(Row(vec![Some(2_i32.into()), None, Some(222_i32.into())]))
        .unwrap();
    state
        .insert(Row(vec![Some(3_i32.into()), None, None]))
        .unwrap();

    state
        .delete(Row(vec![Some(2_i32.into()), None, Some(222_i32.into())]))
        .unwrap();
    state.commit(epoch).await.unwrap();

    let epoch = u64::MAX;

    let get_row1_res = table
        .get_row(&Row(vec![Some(1_i32.into()), None]), epoch)
        .await
        .unwrap();
    assert_eq!(
        get_row1_res,
        Some(Row(vec![Some(1_i32.into()), None, None,]))
    );

    let get_row2_res = table
        .get_row(&Row(vec![Some(2_i32.into()), None]), epoch)
        .await
        .unwrap();
    assert_eq!(get_row2_res, None);

    let get_row3_res = table
        .get_row(&Row(vec![Some(3_i32.into()), None]), epoch)
        .await
        .unwrap();
    assert_eq!(
        get_row3_res,
        Some(Row(vec![Some(3_i32.into()), None, None]))
    );

    let get_no_exist_res = table
        .get_row(&Row(vec![Some(0_i32.into()), Some(00_i32.into())]), epoch)
        .await
        .unwrap();
    assert_eq!(get_no_exist_res, None);
}

#[tokio::test]
async fn test_cell_based_table_iter() {
    let state_store = MemoryStateStore::new();
    let order_types = vec![OrderType::Ascending, OrderType::Descending];
    let column_ids = vec![ColumnId::from(0), ColumnId::from(1), ColumnId::from(2)];
    let column_descs = vec![
        ColumnDesc::unnamed(column_ids[0], DataType::Int32),
        ColumnDesc::unnamed(column_ids[1], DataType::Int32),
        ColumnDesc::unnamed(column_ids[2], DataType::Int32),
    ];
    let pk_indices = vec![0_usize, 1_usize];
    let mut state = StateTable::new_without_distribution(
        state_store.clone(),
        TableId::from(0x42),
        column_descs.clone(),
        order_types.clone(),
        pk_indices.clone(),
    );
    let table = RowBasedStorageTable::new_for_test(
        state_store.clone(),
        TableId::from(0x42),
        column_descs.clone(),
        order_types.clone(),
        pk_indices,
    );
    let epoch: u64 = 0;

    state
        .insert(Row(vec![
            Some(1_i32.into()),
            Some(11_i32.into()),
            Some(111_i32.into()),
        ]))
        .unwrap();
    state
        .insert(Row(vec![
            Some(2_i32.into()),
            Some(22_i32.into()),
            Some(222_i32.into()),
        ]))
        .unwrap();
    state
        .delete(Row(vec![
            Some(2_i32.into()),
            Some(22_i32.into()),
            Some(222_i32.into()),
        ]))
        .unwrap();
    state.commit(epoch).await.unwrap();

    let epoch = u64::MAX;
    let iter = table.batch_iter(epoch).await.unwrap();
    pin_mut!(iter);

    let res = iter.next_row().await.unwrap();
    assert!(res.is_some());
    assert_eq!(
        Row(vec![
            Some(1_i32.into()),
            Some(11_i32.into()),
            Some(111_i32.into())
        ]),
        res.unwrap()
    );

    let res = iter.next_row().await.unwrap();
    assert!(res.is_none());
}

#[tokio::test]
async fn test_multi_cell_based_table_iter() {
    let state_store = MemoryStateStore::new();
    // let pk_columns = vec![0, 1]; leave a message to indicate pk columns
    let order_types = vec![OrderType::Ascending, OrderType::Descending];
    let epoch: u64 = 0;

    let column_ids = vec![ColumnId::from(0), ColumnId::from(1), ColumnId::from(2)];
    let column_descs_1 = vec![
        ColumnDesc::unnamed(column_ids[0], DataType::Int32),
        ColumnDesc::unnamed(column_ids[1], DataType::Int32),
        ColumnDesc::unnamed(column_ids[2], DataType::Int32),
    ];
    let column_descs_2 = vec![
        ColumnDesc::unnamed(column_ids[0], DataType::Varchar),
        ColumnDesc::unnamed(column_ids[1], DataType::Varchar),
        ColumnDesc::unnamed(column_ids[2], DataType::Varchar),
    ];
    let pk_indices = vec![0_usize, 1_usize];
    let mut state_1 = StateTable::new_without_distribution(
        state_store.clone(),
        TableId::from(0x1111),
        column_descs_1.clone(),
        order_types.clone(),
        pk_indices.clone(),
    );
    let mut state_2 = StateTable::new_without_distribution(
        state_store.clone(),
        TableId::from(0x2222),
        column_descs_2.clone(),
        order_types.clone(),
        pk_indices.clone(),
    );

    let table_1 = RowBasedStorageTable::new_for_test(
        state_store.clone(),
        TableId::from(0x1111),
        column_descs_1.clone(),
        order_types.clone(),
        pk_indices.clone(),
    );
    let table_2 = RowBasedStorageTable::new_for_test(
        state_store.clone(),
        TableId::from(0x2222),
        column_descs_2.clone(),
        order_types.clone(),
        pk_indices,
    );

    state_1
        .insert(Row(vec![
            Some(1_i32.into()),
            Some(11_i32.into()),
            Some(111_i32.into()),
        ]))
        .unwrap();
    state_1
        .insert(Row(vec![
            Some(2_i32.into()),
            Some(22_i32.into()),
            Some(222_i32.into()),
        ]))
        .unwrap();
    state_1
        .delete(Row(vec![
            Some(2_i32.into()),
            Some(22_i32.into()),
            Some(222_i32.into()),
        ]))
        .unwrap();

    state_2
        .insert(Row(vec![
            Some("1".to_string().into()),
            Some("11".to_string().into()),
            Some("111".to_string().into()),
        ]))
        .unwrap();
    state_2
        .insert(Row(vec![
            Some("2".to_string().into()),
            Some("22".to_string().into()),
            Some("222".to_string().into()),
        ]))
        .unwrap();
    state_2
        .delete(Row(vec![
            Some("2".to_string().into()),
            Some("22".to_string().into()),
            Some("222".to_string().into()),
        ]))
        .unwrap();

    state_1.commit(epoch).await.unwrap();
    state_2.commit(epoch).await.unwrap();

    let iter_1 = table_1.batch_iter(epoch).await.unwrap();
    let iter_2 = table_2.batch_iter(epoch).await.unwrap();
    pin_mut!(iter_1);
    pin_mut!(iter_2);

    let res_1_1 = iter_1.next_row().await.unwrap();
    assert!(res_1_1.is_some());
    assert_eq!(
        Row(vec![
            Some(1_i32.into()),
            Some(11_i32.into()),
            Some(111_i32.into()),
        ]),
        res_1_1.unwrap()
    );
    let res_1_2 = iter_1.next_row().await.unwrap();
    assert!(res_1_2.is_none());

    let res_2_1 = iter_2.next_row().await.unwrap();
    assert!(res_2_1.is_some());
    assert_eq!(
        Row(vec![
            Some("1".to_string().into()),
            Some("11".to_string().into()),
            Some("111".to_string().into())
        ]),
        res_2_1.unwrap()
    );
    let res_2_2 = iter_2.next_row().await.unwrap();
    assert!(res_2_2.is_none());
}

#[tokio::test]
async fn test_cell_based_scan_empty_column_ids_cardinality() {
    let state_store = MemoryStateStore::new();
    let column_ids = vec![ColumnId::from(0), ColumnId::from(1), ColumnId::from(2)];
    let column_descs = vec![
        ColumnDesc::unnamed(column_ids[0], DataType::Int32),
        ColumnDesc::unnamed(column_ids[1], DataType::Int32),
        ColumnDesc::unnamed(column_ids[2], DataType::Int32),
    ];
    // let pk_columns = vec![0, 1]; leave a message to indicate pk columns
    let order_types = vec![OrderType::Ascending, OrderType::Descending];
    let pk_indices = vec![0_usize, 1_usize];
    let mut state = StateTable::new_without_distribution(
        state_store.clone(),
        TableId::from(0x42),
        column_descs.clone(),
        order_types.clone(),
        pk_indices.clone(),
    );
    let table = RowBasedStorageTable::new_for_test(
        state_store.clone(),
        TableId::from(0x42),
        column_descs.clone(),
        order_types.clone(),
        pk_indices,
    );
    let epoch: u64 = 0;

    state
        .insert(Row(vec![
            Some(1_i32.into()),
            Some(11_i32.into()),
            Some(111_i32.into()),
        ]))
        .unwrap();
    state
        .insert(Row(vec![
            Some(2_i32.into()),
            Some(22_i32.into()),
            Some(222_i32.into()),
        ]))
        .unwrap();
    state.commit(epoch).await.unwrap();

    let chunk = {
        let iter = table.batch_iter(u64::MAX).await.unwrap();
        pin_mut!(iter);
        iter.collect_data_chunk(table.schema(), None)
            .await
            .unwrap()
            .unwrap()
    };
    assert_eq!(chunk.cardinality(), 2);
}

#[tokio::test]
async fn test_state_table_iter_with_prefix() {
    let state_store = MemoryStateStore::new();
    // let pk_columns = vec![0, 1]; leave a message to indicate pk columns
    let order_types = vec![OrderType::Ascending, OrderType::Descending];

    let column_ids = vec![ColumnId::from(0), ColumnId::from(1), ColumnId::from(2)];
    let column_descs = vec![
        ColumnDesc::unnamed(column_ids[0], DataType::Int32),
        ColumnDesc::unnamed(column_ids[1], DataType::Int32),
        ColumnDesc::unnamed(column_ids[2], DataType::Int32),
    ];
    let pk_index = vec![0_usize, 1_usize];
    let mut state = StateTable::new_without_distribution(
        state_store.clone(),
        TableId::from(0x42),
        column_descs.clone(),
        order_types.clone(),
        pk_index,
    );
    let epoch: u64 = 0;

    state
        .insert(Row(vec![
            Some(1_i32.into()),
            Some(11_i32.into()),
            Some(111_i32.into()),
        ]))
        .unwrap();
    state
        .insert(Row(vec![
            Some(1_i32.into()),
            Some(22_i32.into()),
            Some(222_i32.into()),
        ]))
        .unwrap();

    state
        .insert(Row(vec![
            Some(4_i32.into()),
            Some(44_i32.into()),
            Some(444_i32.into()),
        ]))
        .unwrap();

    state
        .insert(Row(vec![
            Some(1_i32.into()),
            Some(55_i32.into()),
            Some(555_i32.into()),
        ]))
        .unwrap();
    state.commit(epoch).await.unwrap();

    state
        .insert(Row(vec![
            Some(1_i32.into()),
            Some(33_i32.into()),
            Some(333_i32.into()),
        ]))
        .unwrap();
    state
        .insert(Row(vec![
            Some(1_i32.into()),
            Some(55_i32.into()),
            Some(5555_i32.into()),
        ]))
        .unwrap();
    state
        .insert(Row(vec![
            Some(6_i32.into()),
            Some(66_i32.into()),
            Some(666_i32.into()),
        ]))
        .unwrap();
    let epoch = u64::MAX;
    let pk_prefix = Row(vec![Some(1_i32.into())]);
    let iter = state.iter_with_pk_prefix(&pk_prefix, epoch).await.unwrap();
    pin_mut!(iter);

    // this row exists in both mem_table and cell_based_table
    let res = iter.next().await.unwrap().unwrap();
    assert_eq!(
        &Row(vec![
            Some(1_i32.into()),
            Some(55_i32.into()),
            Some(5555_i32.into())
        ]),
        res.as_ref()
    );

    // this row exists in mem_table
    let res = iter.next().await.unwrap().unwrap();
    assert_eq!(
        &Row(vec![
            Some(1_i32.into()),
            Some(33_i32.into()),
            Some(333_i32.into())
        ]),
        res.as_ref()
    );

    // this row exists in cell_based_table
    let res = iter.next().await.unwrap().unwrap();
    assert_eq!(
        &Row(vec![
            Some(1_i32.into()),
            Some(22_i32.into()),
            Some(222_i32.into())
        ]),
        res.as_ref()
    );
    // this row exists in cell_based_table
    let res = iter.next().await.unwrap().unwrap();
    assert_eq!(
        &Row(vec![
            Some(1_i32.into()),
            Some(11_i32.into()),
            Some(111_i32.into())
        ]),
        res.as_ref()
    );
    // pk without the prefix the range will not be scan
    let res = iter.next().await;
    assert!(res.is_none());
}

// test row_based table
#[tokio::test]
async fn test_row_based_get_row() {
    let state_store = MemoryStateStore::new();
    let column_ids = vec![ColumnId::from(0), ColumnId::from(1), ColumnId::from(2)];
    let column_descs = vec![
        ColumnDesc::unnamed(column_ids[0], DataType::Int32),
        ColumnDesc::unnamed(column_ids[1], DataType::Int32),
        ColumnDesc::unnamed(column_ids[2], DataType::Int32),
    ];
    let pk_indices = vec![0_usize, 1_usize];
    let order_types = vec![OrderType::Ascending, OrderType::Descending];
    let mut state = StateTable::new_without_distribution(
        state_store.clone(),
        TableId::from(0x42),
        column_descs.clone(),
        order_types.clone(),
        pk_indices.clone(),
    );
    let table = RowBasedStorageTable::new_for_test(
        state_store.clone(),
        TableId::from(0x42),
        column_descs.clone(),
        order_types.clone(),
        pk_indices,
    );
    let epoch: u64 = 0;

    state
        .insert(Row(vec![Some(1_i32.into()), None, None]))
        .unwrap();
    state
        .insert(Row(vec![Some(2_i32.into()), None, Some(222_i32.into())]))
        .unwrap();
    state
        .insert(Row(vec![Some(3_i32.into()), None, None]))
        .unwrap();

    state
        .delete(Row(vec![Some(2_i32.into()), None, Some(222_i32.into())]))
        .unwrap();
    state.commit(epoch).await.unwrap();

    let epoch = u64::MAX;

    let get_row1_res = table
        .get_row(&Row(vec![Some(1_i32.into()), None]), epoch)
        .await
        .unwrap();
    assert_eq!(
        get_row1_res,
        Some(Row(vec![Some(1_i32.into()), None, None,]))
    );

    let get_row2_res = table
        .get_row(&Row(vec![Some(2_i32.into()), None]), epoch)
        .await
        .unwrap();
    assert_eq!(get_row2_res, None);

    let get_row3_res = table
        .get_row(&Row(vec![Some(3_i32.into()), None]), epoch)
        .await
        .unwrap();
    assert_eq!(
        get_row3_res,
        Some(Row(vec![Some(3_i32.into()), None, None]))
    );

    let get_no_exist_res = table
        .get_row(&Row(vec![Some(0_i32.into()), Some(00_i32.into())]), epoch)
        .await
        .unwrap();
    assert_eq!(get_no_exist_res, None);
}

// test row-based encoding in batch mode
#[tokio::test]
async fn test_row_based_storage_table_point_get_in_batch_mode() {
    let state_store = MemoryStateStore::new();
    let column_ids = vec![ColumnId::from(0), ColumnId::from(1), ColumnId::from(2)];
    let column_descs = vec![
        ColumnDesc::unnamed(column_ids[0], DataType::Int32),
        ColumnDesc::unnamed(column_ids[1], DataType::Int32),
        ColumnDesc::unnamed(column_ids[2], DataType::Int32),
    ];
    let pk_indices = vec![0_usize, 1_usize];
    let order_types = vec![OrderType::Ascending, OrderType::Descending];
    let mut state = StateTable::new_without_distribution(
        state_store.clone(),
        TableId::from(0x42),
        column_descs.clone(),
        order_types.clone(),
        pk_indices.clone(),
    );
    let column_ids_partial = vec![ColumnId::from(1), ColumnId::from(2)];
    let table = RowBasedStorageTable::new_partial(
        state_store.clone(),
        TableId::from(0x42),
        column_descs.clone(),
        column_ids_partial,
        order_types.clone(),
        pk_indices,
        Distribution::fallback(),
        TableOption::default(),
    );
    let epoch: u64 = 0;

    state
        .insert(Row(vec![Some(1_i32.into()), None, None]))
        .unwrap();
    state
        .insert(Row(vec![Some(2_i32.into()), None, Some(222_i32.into())]))
        .unwrap();
    state
        .insert(Row(vec![Some(3_i32.into()), None, None]))
        .unwrap();

    state
        .delete(Row(vec![Some(2_i32.into()), None, Some(222_i32.into())]))
        .unwrap();
    state.commit(epoch).await.unwrap();

    let epoch = u64::MAX;

    let get_row1_res = table
        .get_row(&Row(vec![Some(1_i32.into()), None]), epoch)
        .await
        .unwrap();

    // Here only column_ids_partial will be get
    assert_eq!(get_row1_res, Some(Row(vec![None, None,])));

    let get_row2_res = table
        .get_row(&Row(vec![Some(2_i32.into()), None]), epoch)
        .await
        .unwrap();
    assert_eq!(get_row2_res, None);

    let get_row3_res = table
        .get_row(&Row(vec![Some(3_i32.into()), None]), epoch)
        .await
        .unwrap();
    assert_eq!(get_row3_res, Some(Row(vec![None, None])));

    let get_no_exist_res = table
        .get_row(&Row(vec![Some(0_i32.into()), Some(00_i32.into())]), epoch)
        .await
        .unwrap();
    assert_eq!(get_no_exist_res, None);
}

#[tokio::test]
async fn test_row_based_storage_table_scan_in_batch_mode() {
    let state_store = MemoryStateStore::new();
    let order_types = vec![OrderType::Ascending, OrderType::Descending];
    let column_ids = vec![ColumnId::from(0), ColumnId::from(1), ColumnId::from(2)];
    let column_descs = vec![
        ColumnDesc::unnamed(column_ids[0], DataType::Int32),
        ColumnDesc::unnamed(column_ids[1], DataType::Int32),
        ColumnDesc::unnamed(column_ids[2], DataType::Int32),
    ];
    let pk_indices = vec![0_usize, 1_usize];
    let mut state = StateTable::new_without_distribution(
        state_store.clone(),
        TableId::from(0x42),
        column_descs.clone(),
        order_types.clone(),
        pk_indices.clone(),
    );
    let column_ids_partial = vec![ColumnId::from(1), ColumnId::from(2)];
    let table = RowBasedStorageTable::new_partial(
        state_store.clone(),
        TableId::from(0x42),
        column_descs.clone(),
        column_ids_partial,
        order_types.clone(),
        pk_indices,
        Distribution::fallback(),
        TableOption::default(),
    );
    let epoch: u64 = 0;

    state
        .insert(Row(vec![
            Some(1_i32.into()),
            Some(11_i32.into()),
            Some(111_i32.into()),
        ]))
        .unwrap();
    state
        .insert(Row(vec![
            Some(2_i32.into()),
            Some(22_i32.into()),
            Some(222_i32.into()),
        ]))
        .unwrap();
    state
        .delete(Row(vec![
            Some(2_i32.into()),
            Some(22_i32.into()),
            Some(222_i32.into()),
        ]))
        .unwrap();
    state.commit(epoch).await.unwrap();

    let epoch = u64::MAX;
    let iter = table.batch_iter(epoch).await.unwrap();
    pin_mut!(iter);

    let res = iter.next_row().await.unwrap();
    assert!(res.is_some());

    // only scan two columns
    assert_eq!(
        Row(vec![Some(11_i32.into()), Some(111_i32.into())]),
        res.unwrap()
    );

    let res = iter.next_row().await.unwrap();
    assert!(res.is_none());
}

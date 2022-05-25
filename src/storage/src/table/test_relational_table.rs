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

use risingwave_common::array::Row;
use risingwave_common::catalog::{ColumnDesc, ColumnId};
use risingwave_common::types::DataType;
use risingwave_common::util::sort_util::OrderType;

use crate::error::StorageResult;
use crate::memory::MemoryStateStore;
use crate::table::cell_based_table::CellBasedTable;
use crate::table::state_table::StateTable;
use crate::table::TableIter;
use crate::Keyspace;

/// There are three struct in relational layer, StateTable, MemTable and CellBasedTable.
/// `StateTable` provides read/write interfaces to the upper layer streaming operator.
/// `MemTable` is an in-memory buffer used to cache operator operations.
/// `CellBasedTable` provides the transform from the kv encoding (hummock) to cell_based row
/// encoding.

// test state table
#[tokio::test]
async fn test_state_table() -> StorageResult<()> {
    let state_store = MemoryStateStore::new();
    let keyspace = Keyspace::executor_root(state_store.clone(), 0x42);
    let column_descs = vec![
        ColumnDesc::unnamed(ColumnId::from(0), DataType::Int32),
        ColumnDesc::unnamed(ColumnId::from(1), DataType::Int32),
        ColumnDesc::unnamed(ColumnId::from(2), DataType::Int32),
    ];
    let order_types = vec![OrderType::Ascending];
    let mut state_table = StateTable::new(keyspace.clone(), column_descs, order_types, None);
    let mut epoch: u64 = 0;
    state_table
        .insert(
            Row(vec![Some(1_i32.into())]),
            Row(vec![
                Some(1_i32.into()),
                Some(11_i32.into()),
                Some(111_i32.into()),
            ]),
        )
        .unwrap();
    state_table
        .insert(
            Row(vec![Some(2_i32.into())]),
            Row(vec![
                Some(2_i32.into()),
                Some(22_i32.into()),
                Some(222_i32.into()),
            ]),
        )
        .unwrap();
    state_table
        .insert(
            Row(vec![Some(3_i32.into())]),
            Row(vec![
                Some(3_i32.into()),
                Some(33_i32.into()),
                Some(333_i32.into()),
            ]),
        )
        .unwrap();

    // test read visibility
    let row1 = state_table
        .get_row(&Row(vec![Some(1_i32.into())]), epoch)
        .await
        .unwrap();
    assert_eq!(
        row1,
        Some(Row(vec![
            Some(1_i32.into()),
            Some(11_i32.into()),
            Some(111_i32.into())
        ]))
    );

    let row2 = state_table
        .get_row(&Row(vec![Some(2_i32.into())]), epoch)
        .await
        .unwrap();
    assert_eq!(
        row2,
        Some(Row(vec![
            Some(2_i32.into()),
            Some(22_i32.into()),
            Some(222_i32.into())
        ]))
    );

    state_table
        .delete(
            Row(vec![Some(2_i32.into())]),
            Row(vec![
                Some(2_i32.into()),
                Some(22_i32.into()),
                Some(222_i32.into()),
            ]),
        )
        .unwrap();

    let row2_delete = state_table
        .get_row(&Row(vec![Some(2_i32.into())]), epoch)
        .await
        .unwrap();
    assert_eq!(row2_delete, None);

    state_table.commit(epoch).await.unwrap();

    let row2_delete_commit = state_table
        .get_row(&Row(vec![Some(2_i32.into())]), epoch)
        .await
        .unwrap();
    assert_eq!(row2_delete_commit, None);

    epoch += 1;
    state_table
        .delete(
            Row(vec![Some(3_i32.into())]),
            Row(vec![
                Some(3_i32.into()),
                Some(33_i32.into()),
                Some(333_i32.into()),
            ]),
        )
        .unwrap();

    state_table
        .insert(
            Row(vec![Some(4_i32.into())]),
            Row(vec![Some(4_i32.into()), None, None]),
        )
        .unwrap();
    state_table
        .insert(Row(vec![Some(5_i32.into())]), Row(vec![None, None, None]))
        .unwrap();

    let row4 = state_table
        .get_row(&Row(vec![Some(4_i32.into())]), epoch)
        .await
        .unwrap();
    assert_eq!(row4, Some(Row(vec![Some(4_i32.into()), None, None])));

    let row5 = state_table
        .get_row(&Row(vec![Some(5_i32.into())]), epoch)
        .await
        .unwrap();
    assert_eq!(row5, Some(Row(vec![None, None, None])));

    let non_exist_row = state_table
        .get_row(&Row(vec![Some(0_i32.into())]), epoch)
        .await
        .unwrap();
    assert_eq!(non_exist_row, None);

    state_table
        .delete(
            Row(vec![Some(4_i32.into())]),
            Row(vec![Some(4_i32.into()), None, None]),
        )
        .unwrap();

    state_table.commit(epoch).await.unwrap();

    let row3_delete = state_table
        .get_row(&Row(vec![Some(3_i32.into())]), epoch)
        .await
        .unwrap();
    assert_eq!(row3_delete, None);

    let row4_delete = state_table
        .get_row(&Row(vec![Some(4_i32.into())]), epoch)
        .await
        .unwrap();
    assert_eq!(row4_delete, None);

    Ok(())
}

#[tokio::test]
async fn test_state_table_update_insert() -> StorageResult<()> {
    let state_store = MemoryStateStore::new();
    let keyspace = Keyspace::executor_root(state_store.clone(), 0x42);
    let column_descs = vec![
        ColumnDesc::unnamed(ColumnId::from(0), DataType::Int32),
        ColumnDesc::unnamed(ColumnId::from(1), DataType::Int32),
        ColumnDesc::unnamed(ColumnId::from(2), DataType::Int32),
        ColumnDesc::unnamed(ColumnId::from(4), DataType::Int32),
    ];
    let order_types = vec![OrderType::Ascending];
    let mut state_table = StateTable::new(keyspace.clone(), column_descs, order_types, None);
    let mut epoch: u64 = 0;
    state_table
        .insert(
            Row(vec![Some(6_i32.into())]),
            Row(vec![
                Some(6_i32.into()),
                Some(66_i32.into()),
                Some(666_i32.into()),
                Some(6666_i32.into()),
            ]),
        )
        .unwrap();

    state_table
        .insert(
            Row(vec![Some(7_i32.into())]),
            Row(vec![Some(7_i32.into()), None, Some(777_i32.into()), None]),
        )
        .unwrap();
    state_table.commit(epoch).await.unwrap();

    epoch += 1;
    state_table
        .delete(
            Row(vec![Some(6_i32.into())]),
            Row(vec![
                Some(6_i32.into()),
                Some(66_i32.into()),
                Some(666_i32.into()),
                Some(6666_i32.into()),
            ]),
        )
        .unwrap();
    state_table
        .insert(
            Row(vec![Some(6_i32.into())]),
            Row(vec![
                Some(6666_i32.into()),
                None,
                None,
                Some(6666_i32.into()),
            ]),
        )
        .unwrap();

    state_table
        .delete(
            Row(vec![Some(7_i32.into())]),
            Row(vec![Some(7_i32.into()), None, Some(777_i32.into()), None]),
        )
        .unwrap();
    state_table
        .insert(
            Row(vec![Some(7_i32.into())]),
            Row(vec![None, Some(77_i32.into()), Some(7777_i32.into()), None]),
        )
        .unwrap();
    let row6 = state_table
        .get_row(&Row(vec![Some(6_i32.into())]), epoch)
        .await
        .unwrap();
    assert_eq!(
        row6,
        Some(Row(vec![
            Some(6666_i32.into()),
            None,
            None,
            Some(6666_i32.into())
        ]))
    );

    let row7 = state_table
        .get_row(&Row(vec![Some(7_i32.into())]), epoch)
        .await
        .unwrap();
    assert_eq!(
        row7,
        Some(Row(vec![
            None,
            Some(77_i32.into()),
            Some(7777_i32.into()),
            None
        ]))
    );

    state_table.commit(epoch).await.unwrap();

    let row6_commit = state_table
        .get_row(&Row(vec![Some(6_i32.into())]), epoch)
        .await
        .unwrap();
    assert_eq!(
        row6_commit,
        Some(Row(vec![
            Some(6666_i32.into()),
            None,
            None,
            Some(6666_i32.into())
        ]))
    );
    let row7_commit = state_table
        .get_row(&Row(vec![Some(7_i32.into())]), epoch)
        .await
        .unwrap();
    assert_eq!(
        row7_commit,
        Some(Row(vec![
            None,
            Some(77_i32.into()),
            Some(7777_i32.into()),
            None
        ]))
    );

    epoch += 1;

    state_table
        .insert(
            Row(vec![Some(1_i32.into())]),
            Row(vec![
                Some(1_i32.into()),
                Some(2_i32.into()),
                Some(3_i32.into()),
                Some(4_i32.into()),
            ]),
        )
        .unwrap();
    state_table.commit(epoch).await.unwrap();
    // one epoch: delete (1, 2, 3, 4), insert (5, 6, 7, None), delete(5, 6, 7, None)
    state_table
        .delete(
            Row(vec![Some(1_i32.into())]),
            Row(vec![
                Some(1_i32.into()),
                Some(2_i32.into()),
                Some(3_i32.into()),
                Some(4_i32.into()),
            ]),
        )
        .unwrap();
    state_table
        .insert(
            Row(vec![Some(1_i32.into())]),
            Row(vec![
                Some(5_i32.into()),
                Some(6_i32.into()),
                Some(7_i32.into()),
                None,
            ]),
        )
        .unwrap();
    state_table
        .delete(
            Row(vec![Some(1_i32.into())]),
            Row(vec![
                Some(5_i32.into()),
                Some(6_i32.into()),
                Some(7_i32.into()),
                None,
            ]),
        )
        .unwrap();

    let row1 = state_table
        .get_row(&Row(vec![Some(1_i32.into())]), epoch)
        .await
        .unwrap();
    assert_eq!(row1, None);
    state_table.commit(epoch).await.unwrap();

    let row1_commit = state_table
        .get_row(&Row(vec![Some(1_i32.into())]), epoch)
        .await
        .unwrap();
    assert_eq!(row1_commit, None);
    Ok(())
}

#[tokio::test]
async fn test_state_table_iter() {
    let state_store = MemoryStateStore::new();
    let order_types = vec![OrderType::Ascending, OrderType::Descending];
    let keyspace = Keyspace::executor_root(state_store, 0x42);
    let column_ids = vec![ColumnId::from(0), ColumnId::from(1), ColumnId::from(2)];
    let column_descs = vec![
        ColumnDesc::unnamed(column_ids[0], DataType::Int32),
        ColumnDesc::unnamed(column_ids[1], DataType::Int32),
        ColumnDesc::unnamed(column_ids[2], DataType::Int32),
    ];

    let mut state = StateTable::new(
        keyspace.clone(),
        column_descs.clone(),
        order_types.clone(),
        Some(vec![1]),
    );
    let epoch: u64 = 0;

    state
        .insert(
            Row(vec![Some(1_i32.into()), Some(11_i32.into())]),
            Row(vec![
                Some(1_i32.into()),
                Some(11_i32.into()),
                Some(111_i32.into()),
            ]),
        )
        .unwrap();
    state
        .insert(
            Row(vec![Some(2_i32.into()), Some(22_i32.into())]),
            Row(vec![
                Some(2_i32.into()),
                Some(22_i32.into()),
                Some(222_i32.into()),
            ]),
        )
        .unwrap();
    state
        .delete(
            Row(vec![Some(2_i32.into()), Some(22_i32.into())]),
            Row(vec![
                Some(2_i32.into()),
                Some(22_i32.into()),
                Some(222_i32.into()),
            ]),
        )
        .unwrap();

    let mut iter = state.iter(epoch).await.unwrap();

    let res = iter.next().await.unwrap();
    assert!(res.is_some());
    assert_eq!(
        Row(vec![
            Some(1_i32.into()),
            Some(11_i32.into()),
            Some(111_i32.into())
        ]),
        res.unwrap()
    );

    let res = iter.next().await.unwrap();

    assert!(res.is_none());

    state
        .insert(
            Row(vec![Some(3_i32.into()), Some(33_i32.into())]),
            Row(vec![
                Some(3333_i32.into()),
                Some(3333_i32.into()),
                Some(3333_i32.into()),
            ]),
        )
        .unwrap();

    state
        .insert(
            Row(vec![Some(6_i32.into()), Some(66_i32.into())]),
            Row(vec![
                Some(6_i32.into()),
                Some(66_i32.into()),
                Some(666_i32.into()),
            ]),
        )
        .unwrap();

    state
        .insert(
            Row(vec![Some(9_i32.into()), Some(99_i32.into())]),
            Row(vec![
                Some(9_i32.into()),
                Some(99_i32.into()),
                Some(999_i32.into()),
            ]),
        )
        .unwrap();

    state.commit(epoch).await.unwrap();

    let epoch = u64::MAX;

    // write [3, 33, 333], [4, 44, 444], [5, 55, 555], [7, 77, 777], [8, 88, 888]into mem_table,
    // [1, 11, 111], [3333, 3333, 3333], [6, 66, 666], [9, 99, 999] exists in cell_based_table.

    state
        .insert(
            Row(vec![Some(3_i32.into()), Some(33_i32.into())]),
            Row(vec![
                Some(3_i32.into()),
                Some(33_i32.into()),
                Some(333_i32.into()),
            ]),
        )
        .unwrap();

    state
        .insert(
            Row(vec![Some(4_i32.into()), Some(44_i32.into())]),
            Row(vec![
                Some(4_i32.into()),
                Some(44_i32.into()),
                Some(444_i32.into()),
            ]),
        )
        .unwrap();

    state
        .insert(
            Row(vec![Some(5_i32.into()), Some(55_i32.into())]),
            Row(vec![
                Some(5_i32.into()),
                Some(55_i32.into()),
                Some(555_i32.into()),
            ]),
        )
        .unwrap();
    state
        .insert(
            Row(vec![Some(7_i32.into()), Some(77_i32.into())]),
            Row(vec![
                Some(7_i32.into()),
                Some(77_i32.into()),
                Some(777_i32.into()),
            ]),
        )
        .unwrap();

    state
        .insert(
            Row(vec![Some(8_i32.into()), Some(88_i32.into())]),
            Row(vec![
                Some(8_i32.into()),
                Some(88_i32.into()),
                Some(888_i32.into()),
            ]),
        )
        .unwrap();

    let mut iter = state.iter(epoch).await.unwrap();

    let res = iter.next().await.unwrap();
    assert!(res.is_some());
    // this row exists in cell_based_table
    assert_eq!(
        Row(vec![
            Some(1_i32.into()),
            Some(11_i32.into()),
            Some(111_i32.into())
        ]),
        res.unwrap()
    );

    let res = iter.next().await.unwrap();

    // this pk exist in both cell_based_table(shared_storage) and mem_table(buffer)
    assert_eq!(
        Row(vec![
            Some(3_i32.into()),
            Some(33_i32.into()),
            Some(333_i32.into())
        ]),
        res.unwrap()
    );

    // this row exists in mem_table
    let res = iter.next().await.unwrap();
    assert_eq!(
        Row(vec![
            Some(4_i32.into()),
            Some(44_i32.into()),
            Some(444_i32.into())
        ]),
        res.unwrap()
    );

    let res = iter.next().await.unwrap();

    // this row exists in mem_table
    assert_eq!(
        Row(vec![
            Some(5_i32.into()),
            Some(55_i32.into()),
            Some(555_i32.into())
        ]),
        res.unwrap()
    );
    let res = iter.next().await.unwrap();

    // this row exists in cell_based_table
    assert_eq!(
        Row(vec![
            Some(6_i32.into()),
            Some(66_i32.into()),
            Some(666_i32.into())
        ]),
        res.unwrap()
    );

    let res = iter.next().await.unwrap();
    // this row exists in mem_table
    assert_eq!(
        Row(vec![
            Some(7_i32.into()),
            Some(77_i32.into()),
            Some(777.into())
        ]),
        res.unwrap()
    );

    let res = iter.next().await.unwrap();

    // this row exists in mem_table
    assert_eq!(
        Row(vec![
            Some(8_i32.into()),
            Some(88_i32.into()),
            Some(888_i32.into())
        ]),
        res.unwrap()
    );

    let res = iter.next().await.unwrap();

    // this row exists in cell_based_table
    assert_eq!(
        Row(vec![
            Some(9_i32.into()),
            Some(99_i32.into()),
            Some(999_i32.into())
        ]),
        res.unwrap()
    );

    // there is no row in both cell_based_table and mem_table
    let res = iter.next().await.unwrap();
    assert!(res.is_none());
}

#[tokio::test]
async fn test_multi_state_table_iter() {
    let state_store = MemoryStateStore::new();
    // let pk_columns = vec![0, 1]; leave a message to indicate pk columns
    let order_types = vec![OrderType::Ascending, OrderType::Descending];

    let keyspace_1 = Keyspace::executor_root(state_store.clone(), 0x1111);
    let keyspace_2 = Keyspace::executor_root(state_store.clone(), 0x2222);
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

    let mut state_1 = StateTable::new(
        keyspace_1.clone(),
        column_descs_1.clone(),
        order_types.clone(),
        None,
    );
    let mut state_2 = StateTable::new(
        keyspace_2.clone(),
        column_descs_2.clone(),
        order_types.clone(),
        None,
    );

    state_1
        .insert(
            Row(vec![Some(1_i32.into()), Some(11_i32.into())]),
            Row(vec![
                Some(1_i32.into()),
                Some(11_i32.into()),
                Some(111_i32.into()),
            ]),
        )
        .unwrap();
    state_1
        .insert(
            Row(vec![Some(2_i32.into()), Some(22_i32.into())]),
            Row(vec![
                Some(2_i32.into()),
                Some(22_i32.into()),
                Some(222_i32.into()),
            ]),
        )
        .unwrap();
    state_1
        .delete(
            Row(vec![Some(2_i32.into()), Some(22_i32.into())]),
            Row(vec![
                Some(2_i32.into()),
                Some(22_i32.into()),
                Some(222_i32.into()),
            ]),
        )
        .unwrap();

    state_2
        .insert(
            Row(vec![
                Some("1".to_string().into()),
                Some("11".to_string().into()),
            ]),
            Row(vec![
                Some("1".to_string().into()),
                Some("11".to_string().into()),
                Some("111".to_string().into()),
            ]),
        )
        .unwrap();
    state_2
        .insert(
            Row(vec![
                Some("2".to_string().into()),
                Some("22".to_string().into()),
            ]),
            Row(vec![
                Some("2".to_string().into()),
                Some("22".to_string().into()),
                Some("222".to_string().into()),
            ]),
        )
        .unwrap();
    state_2
        .delete(
            Row(vec![
                Some("2".to_string().into()),
                Some("22".to_string().into()),
            ]),
            Row(vec![
                Some("2".to_string().into()),
                Some("22".to_string().into()),
                Some("222".to_string().into()),
            ]),
        )
        .unwrap();

    let mut iter_1 = state_1.iter(epoch).await.unwrap();
    let mut iter_2 = state_2.iter(epoch).await.unwrap();

    let res_1_1 = iter_1.next().await.unwrap();
    assert!(res_1_1.is_some());
    assert_eq!(
        Row(vec![
            Some(1_i32.into()),
            Some(11_i32.into()),
            Some(111_i32.into()),
        ]),
        res_1_1.unwrap()
    );
    let res_1_2 = iter_1.next().await.unwrap();
    assert!(res_1_2.is_none());

    let res_2_1 = iter_2.next().await.unwrap();
    assert!(res_2_1.is_some());
    assert_eq!(
        Row(vec![
            Some("1".to_string().into()),
            Some("11".to_string().into()),
            Some("111".to_string().into())
        ]),
        res_2_1.unwrap()
    );
    let res_2_2 = iter_2.next().await.unwrap();
    assert!(res_2_2.is_none());

    state_1.commit(epoch).await.unwrap();
    state_2.commit(epoch).await.unwrap();
}

#[tokio::test]
async fn test_cell_based_get_row_by_scan() {
    let state_store = MemoryStateStore::new();
    let column_ids = vec![ColumnId::from(0), ColumnId::from(1), ColumnId::from(2)];
    let column_descs = vec![
        ColumnDesc::unnamed(column_ids[0], DataType::Int32),
        ColumnDesc::unnamed(column_ids[1], DataType::Int32),
        ColumnDesc::unnamed(column_ids[2], DataType::Int32),
    ];

    let order_types = vec![OrderType::Ascending, OrderType::Descending];
    let keyspace = Keyspace::executor_root(state_store, 0x42);
    let mut state = StateTable::new(
        keyspace.clone(),
        column_descs.clone(),
        order_types.clone(),
        None,
    );
    let table = CellBasedTable::new_for_test(keyspace.clone(), column_descs, order_types);
    let epoch: u64 = 0;

    state
        .insert(
            Row(vec![Some(1_i32.into()), Some(11_i32.into())]),
            Row(vec![Some(1_i32.into()), None, None]),
        )
        .unwrap();
    state
        .insert(
            Row(vec![Some(2_i32.into()), Some(22_i32.into())]),
            Row(vec![Some(2_i32.into()), None, Some(222_i32.into())]),
        )
        .unwrap();
    state
        .insert(
            Row(vec![Some(3_i32.into()), Some(33_i32.into())]),
            Row(vec![Some(3_i32.into()), None, None]),
        )
        .unwrap();

    state
        .delete(
            Row(vec![Some(2_i32.into()), Some(22_i32.into())]),
            Row(vec![Some(2_i32.into()), None, Some(222_i32.into())]),
        )
        .unwrap();
    state.commit(epoch).await.unwrap();

    let epoch = u64::MAX;

    let get_row1_res = table
        .get_row_by_scan(&Row(vec![Some(1_i32.into()), Some(11_i32.into())]), epoch)
        .await
        .unwrap();
    assert_eq!(
        get_row1_res,
        Some(Row(vec![Some(1_i32.into()), None, None,]))
    );

    let get_row2_res = table
        .get_row_by_scan(&Row(vec![Some(2_i32.into()), Some(22_i32.into())]), epoch)
        .await
        .unwrap();
    assert_eq!(get_row2_res, None);

    let get_row3_res = table
        .get_row_by_scan(&Row(vec![Some(3_i32.into()), Some(33_i32.into())]), epoch)
        .await
        .unwrap();
    assert_eq!(
        get_row3_res,
        Some(Row(vec![Some(3_i32.into()), None, None]))
    );

    let get_no_exist_res = table
        .get_row_by_scan(&Row(vec![Some(0_i32.into()), Some(00_i32.into())]), epoch)
        .await
        .unwrap();
    assert_eq!(get_no_exist_res, None);
}

// test cell_based table
#[tokio::test]
async fn test_cell_based_get_row_by_muti_get() {
    let state_store = MemoryStateStore::new();
    let column_ids = vec![ColumnId::from(0), ColumnId::from(1), ColumnId::from(2)];
    let column_descs = vec![
        ColumnDesc::unnamed(column_ids[0], DataType::Int32),
        ColumnDesc::unnamed(column_ids[1], DataType::Int32),
        ColumnDesc::unnamed(column_ids[2], DataType::Int32),
    ];

    let order_types = vec![OrderType::Ascending, OrderType::Descending];
    let keyspace = Keyspace::executor_root(state_store, 0x42);
    let mut state = StateTable::new(
        keyspace.clone(),
        column_descs.clone(),
        order_types.clone(),
        None,
    );
    let table = CellBasedTable::new_for_test(keyspace.clone(), column_descs, order_types);
    let epoch: u64 = 0;

    state
        .insert(
            Row(vec![Some(1_i32.into()), Some(11_i32.into())]),
            Row(vec![Some(1_i32.into()), None, None]),
        )
        .unwrap();
    state
        .insert(
            Row(vec![Some(2_i32.into()), Some(22_i32.into())]),
            Row(vec![Some(2_i32.into()), None, Some(222_i32.into())]),
        )
        .unwrap();
    state
        .insert(
            Row(vec![Some(3_i32.into()), Some(33_i32.into())]),
            Row(vec![Some(3_i32.into()), None, None]),
        )
        .unwrap();
    state
        .insert(
            Row(vec![Some(4_i32.into()), Some(44_i32.into())]),
            Row(vec![None, None, None]),
        )
        .unwrap();

    state
        .delete(
            Row(vec![Some(2_i32.into()), Some(22_i32.into())]),
            Row(vec![Some(2_i32.into()), None, Some(222_i32.into())]),
        )
        .unwrap();
    state.commit(epoch).await.unwrap();

    let epoch = u64::MAX;

    let get_row1_res = table
        .get_row(&Row(vec![Some(1_i32.into()), Some(11_i32.into())]), epoch)
        .await
        .unwrap();
    assert_eq!(
        get_row1_res,
        Some(Row(vec![Some(1_i32.into()), None, None,]))
    );

    let get_row2_res = table
        .get_row(&Row(vec![Some(2_i32.into()), Some(22_i32.into())]), epoch)
        .await
        .unwrap();
    assert_eq!(get_row2_res, None);

    let get_row3_res = table
        .get_row(&Row(vec![Some(3_i32.into()), Some(33_i32.into())]), epoch)
        .await
        .unwrap();
    assert_eq!(
        get_row3_res,
        Some(Row(vec![Some(3_i32.into()), None, None]))
    );

    let get_row4_res = table
        .get_row(&Row(vec![Some(4_i32.into()), Some(44_i32.into())]), epoch)
        .await
        .unwrap();
    assert_eq!(get_row4_res, Some(Row(vec![None, None, None])));

    let get_no_exist_res = table
        .get_row(&Row(vec![Some(0_i32.into()), Some(00_i32.into())]), epoch)
        .await
        .unwrap();
    assert_eq!(get_no_exist_res, None);
}

#[tokio::test]
async fn test_cell_based_get_row_for_string() {
    let state_store = MemoryStateStore::new();
    let order_types = vec![OrderType::Ascending, OrderType::Descending];
    let keyspace = Keyspace::executor_root(state_store, 0x42);
    let column_ids = vec![ColumnId::from(1), ColumnId::from(4), ColumnId::from(7)];
    let column_descs = vec![
        ColumnDesc::unnamed(column_ids[0], DataType::Varchar),
        ColumnDesc::unnamed(column_ids[1], DataType::Varchar),
        ColumnDesc::unnamed(column_ids[2], DataType::Varchar),
    ];
    let mut state = StateTable::new(
        keyspace.clone(),
        column_descs.clone(),
        order_types.clone(),
        None,
    );
    let table = CellBasedTable::new_for_test(keyspace.clone(), column_descs, order_types);
    let epoch: u64 = 0;

    state
        .insert(
            Row(vec![
                Some("1".to_string().into()),
                Some("11".to_string().into()),
            ]),
            Row(vec![
                Some("1".to_string().into()),
                Some("11".to_string().into()),
                Some("111".to_string().into()),
            ]),
        )
        .unwrap();
    state
        .insert(
            Row(vec![
                Some("4".to_string().into()),
                Some("44".to_string().into()),
            ]),
            Row(vec![
                Some("4".to_string().into()),
                Some("44".to_string().into()),
                Some("444".to_string().into()),
            ]),
        )
        .unwrap();
    state
        .delete(
            Row(vec![
                Some("4".to_string().into()),
                Some("44".to_string().into()),
            ]),
            Row(vec![
                Some("4".to_string().into()),
                Some("44".to_string().into()),
                Some("444".to_string().into()),
            ]),
        )
        .unwrap();
    state.commit(epoch).await.unwrap();

    let epoch = u64::MAX;
    let get_row1_res = table
        .get_row(
            &Row(vec![
                Some("1".to_string().into()),
                Some("11".to_string().into()),
            ]),
            epoch,
        )
        .await
        .unwrap();
    assert_eq!(
        get_row1_res,
        Some(Row(vec![
            Some("1".to_string().into()),
            Some("11".to_string().into()),
            Some("111".to_string().into()),
        ]))
    );

    let get_row2_res = table
        .get_row(
            &Row(vec![
                Some("4".to_string().into()),
                Some("44".to_string().into()),
            ]),
            epoch,
        )
        .await
        .unwrap();
    assert_eq!(get_row2_res, None);
}

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
    let keyspace = Keyspace::executor_root(state_store, 0x42);
    let mut state = StateTable::new(
        keyspace.clone(),
        column_descs.clone(),
        order_types.clone(),
        None,
    );
    let table = CellBasedTable::new_for_test(keyspace.clone(), column_descs, order_types);
    let epoch: u64 = 0;

    state
        .insert(
            Row(vec![Some(1_i32.into()), Some(11_i32.into())]),
            Row(vec![Some(1_i32.into()), None, None]),
        )
        .unwrap();
    state
        .insert(
            Row(vec![Some(2_i32.into()), Some(22_i32.into())]),
            Row(vec![Some(2_i32.into()), None, Some(222_i32.into())]),
        )
        .unwrap();
    state
        .insert(
            Row(vec![Some(3_i32.into()), Some(33_i32.into())]),
            Row(vec![Some(3_i32.into()), None, None]),
        )
        .unwrap();
    state
        .insert(
            Row(vec![Some(4_i32.into()), Some(44_i32.into())]),
            Row(vec![None, None, None]),
        )
        .unwrap();

    state
        .delete(
            Row(vec![Some(2_i32.into()), Some(22_i32.into())]),
            Row(vec![Some(2_i32.into()), None, Some(222_i32.into())]),
        )
        .unwrap();
    state.commit(epoch).await.unwrap();

    let epoch = u64::MAX;

    let get_row1_res = table
        .get_row(&Row(vec![Some(1_i32.into()), Some(11_i32.into())]), epoch)
        .await
        .unwrap();
    assert_eq!(
        get_row1_res,
        Some(Row(vec![Some(1_i32.into()), None, None,]))
    );

    let get_row2_res = table
        .get_row(&Row(vec![Some(2_i32.into()), Some(22_i32.into())]), epoch)
        .await
        .unwrap();
    assert_eq!(get_row2_res, None);

    let get_row3_res = table
        .get_row(&Row(vec![Some(3_i32.into()), Some(33_i32.into())]), epoch)
        .await
        .unwrap();
    assert_eq!(
        get_row3_res,
        Some(Row(vec![Some(3_i32.into()), None, None]))
    );

    let get_row4_res = table
        .get_row(&Row(vec![Some(4_i32.into()), Some(44_i32.into())]), epoch)
        .await
        .unwrap();
    assert_eq!(get_row4_res, Some(Row(vec![None, None, None])));

    let get_no_exist_res = table
        .get_row(&Row(vec![Some(0_i32.into()), Some(00_i32.into())]), epoch)
        .await
        .unwrap();
    assert_eq!(get_no_exist_res, None);
}

#[tokio::test]
async fn test_cell_based_table_iter() {
    let state_store = MemoryStateStore::new();
    // let pk_columns = vec![0, 1]; leave a message to indicate pk columns
    let order_types = vec![OrderType::Ascending, OrderType::Descending];
    let keyspace = Keyspace::executor_root(state_store, 0x42);
    let column_ids = vec![ColumnId::from(0), ColumnId::from(1), ColumnId::from(2)];
    let column_descs = vec![
        ColumnDesc::unnamed(column_ids[0], DataType::Int32),
        ColumnDesc::unnamed(column_ids[1], DataType::Int32),
        ColumnDesc::unnamed(column_ids[2], DataType::Int32),
    ];

    let mut state = StateTable::new(
        keyspace.clone(),
        column_descs.clone(),
        order_types.clone(),
        None,
    );
    let table = CellBasedTable::new_for_test(keyspace.clone(), column_descs, order_types);
    let epoch: u64 = 0;

    state
        .insert(
            Row(vec![Some(1_i32.into()), Some(11_i32.into())]),
            Row(vec![
                Some(1_i32.into()),
                Some(11_i32.into()),
                Some(111_i32.into()),
            ]),
        )
        .unwrap();
    state
        .insert(
            Row(vec![Some(2_i32.into()), Some(22_i32.into())]),
            Row(vec![
                Some(2_i32.into()),
                Some(22_i32.into()),
                Some(222_i32.into()),
            ]),
        )
        .unwrap();
    state
        .delete(
            Row(vec![Some(2_i32.into()), Some(22_i32.into())]),
            Row(vec![
                Some(2_i32.into()),
                Some(22_i32.into()),
                Some(222_i32.into()),
            ]),
        )
        .unwrap();
    state.commit(epoch).await.unwrap();

    let epoch = u64::MAX;
    let mut iter = table.iter(epoch).await.unwrap();

    let res = iter.next().await.unwrap();
    assert!(res.is_some());
    assert_eq!(
        Row(vec![
            Some(1_i32.into()),
            Some(11_i32.into()),
            Some(111_i32.into())
        ]),
        res.unwrap()
    );

    let res = iter.next().await.unwrap();
    assert!(res.is_none());
}

#[tokio::test]
async fn test_multi_cell_based_table_iter() {
    let state_store = MemoryStateStore::new();
    // let pk_columns = vec![0, 1]; leave a message to indicate pk columns
    let order_types = vec![OrderType::Ascending, OrderType::Descending];

    let keyspace_1 = Keyspace::executor_root(state_store.clone(), 0x1111);
    let keyspace_2 = Keyspace::executor_root(state_store.clone(), 0x2222);
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

    let mut state_1 = StateTable::new(
        keyspace_1.clone(),
        column_descs_1.clone(),
        order_types.clone(),
        None,
    );
    let mut state_2 = StateTable::new(
        keyspace_2.clone(),
        column_descs_2.clone(),
        order_types.clone(),
        None,
    );

    let table_1 =
        CellBasedTable::new_for_test(keyspace_1.clone(), column_descs_1, order_types.clone());
    let table_2 = CellBasedTable::new_for_test(keyspace_2.clone(), column_descs_2, order_types);

    state_1
        .insert(
            Row(vec![Some(1_i32.into()), Some(11_i32.into())]),
            Row(vec![
                Some(1_i32.into()),
                Some(11_i32.into()),
                Some(111_i32.into()),
            ]),
        )
        .unwrap();
    state_1
        .insert(
            Row(vec![Some(2_i32.into()), Some(22_i32.into())]),
            Row(vec![
                Some(2_i32.into()),
                Some(22_i32.into()),
                Some(222_i32.into()),
            ]),
        )
        .unwrap();
    state_1
        .delete(
            Row(vec![Some(2_i32.into()), Some(22_i32.into())]),
            Row(vec![
                Some(2_i32.into()),
                Some(22_i32.into()),
                Some(222_i32.into()),
            ]),
        )
        .unwrap();

    state_2
        .insert(
            Row(vec![
                Some("1".to_string().into()),
                Some("11".to_string().into()),
            ]),
            Row(vec![
                Some("1".to_string().into()),
                Some("11".to_string().into()),
                Some("111".to_string().into()),
            ]),
        )
        .unwrap();
    state_2
        .insert(
            Row(vec![
                Some("2".to_string().into()),
                Some("22".to_string().into()),
            ]),
            Row(vec![
                Some("2".to_string().into()),
                Some("22".to_string().into()),
                Some("222".to_string().into()),
            ]),
        )
        .unwrap();
    state_2
        .delete(
            Row(vec![
                Some("2".to_string().into()),
                Some("22".to_string().into()),
            ]),
            Row(vec![
                Some("2".to_string().into()),
                Some("22".to_string().into()),
                Some("222".to_string().into()),
            ]),
        )
        .unwrap();

    state_1.commit(epoch).await.unwrap();
    state_2.commit(epoch).await.unwrap();

    let mut iter_1 = table_1.iter(epoch).await.unwrap();
    let mut iter_2 = table_2.iter(epoch).await.unwrap();

    let res_1_1 = iter_1.next().await.unwrap();
    assert!(res_1_1.is_some());
    assert_eq!(
        Row(vec![
            Some(1_i32.into()),
            Some(11_i32.into()),
            Some(111_i32.into()),
        ]),
        res_1_1.unwrap()
    );
    let res_1_2 = iter_1.next().await.unwrap();
    assert!(res_1_2.is_none());

    let res_2_1 = iter_2.next().await.unwrap();
    assert!(res_2_1.is_some());
    assert_eq!(
        Row(vec![
            Some("1".to_string().into()),
            Some("11".to_string().into()),
            Some("111".to_string().into())
        ]),
        res_2_1.unwrap()
    );
    let res_2_2 = iter_2.next().await.unwrap();
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
    let keyspace = Keyspace::executor_root(state_store, 0x42);

    let mut state = StateTable::new(
        keyspace.clone(),
        column_descs.clone(),
        order_types.clone(),
        None,
    );
    let table = CellBasedTable::new_for_test(keyspace.clone(), column_descs, order_types);
    let epoch: u64 = 0;

    state
        .insert(
            Row(vec![Some(1_i32.into()), Some(11_i32.into())]),
            Row(vec![
                Some(1_i32.into()),
                Some(11_i32.into()),
                Some(111_i32.into()),
            ]),
        )
        .unwrap();
    state
        .insert(
            Row(vec![Some(2_i32.into()), Some(22_i32.into())]),
            Row(vec![
                Some(2_i32.into()),
                Some(22_i32.into()),
                Some(222_i32.into()),
            ]),
        )
        .unwrap();
    state.commit(epoch).await.unwrap();

    let chunk = {
        let mut iter = table.iter(u64::MAX).await.unwrap();
        iter.collect_data_chunk(&table, None)
            .await
            .unwrap()
            .unwrap()
    };
    assert_eq!(chunk.cardinality(), 2);
}

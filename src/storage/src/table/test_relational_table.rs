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

use std::collections::HashSet;

use futures::{pin_mut, StreamExt};
use itertools::Itertools;
use risingwave_common::array::Row;
use risingwave_common::catalog::{ColumnDesc, ColumnId, OrderedColumnDesc, TableId};
use risingwave_common::types::DataType;
use risingwave_common::util::ordered::{serialize_pk, OrderedRowSerializer};
use risingwave_common::util::sort_util::OrderType;

use crate::cell_based_row_serializer::CellBasedRowSerializer;
use crate::error::StorageResult;
use crate::memory::MemoryStateStore;
use crate::storage_value::{StorageValue, ValueMeta};
use crate::store::StateStore;
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
    let keyspace = Keyspace::table_root(state_store.clone(), &TableId::from(0x42));
    let column_descs = vec![
        ColumnDesc::unnamed(ColumnId::from(0), DataType::Int32),
        ColumnDesc::unnamed(ColumnId::from(1), DataType::Int32),
        ColumnDesc::unnamed(ColumnId::from(2), DataType::Int32),
    ];
    let order_types = vec![OrderType::Ascending];
    let pk_index = vec![0_usize];
    let mut state_table =
        StateTable::new(keyspace.clone(), column_descs, order_types, None, pk_index);
    let mut epoch: u64 = 0;
    state_table
        .insert(Row(vec![
            Some(1_i32.into()),
            Some(11_i32.into()),
            Some(111_i32.into()),
        ]))
        .unwrap();
    state_table
        .insert(Row(vec![
            Some(2_i32.into()),
            Some(22_i32.into()),
            Some(222_i32.into()),
        ]))
        .unwrap();
    state_table
        .insert(Row(vec![
            Some(3_i32.into()),
            Some(33_i32.into()),
            Some(333_i32.into()),
        ]))
        .unwrap();

    // test read visibility
    let row1 = state_table
        .get_owned_row(&Row(vec![Some(1_i32.into())]), epoch)
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
        .get_owned_row(&Row(vec![Some(2_i32.into())]), epoch)
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
        .delete(Row(vec![
            Some(2_i32.into()),
            Some(22_i32.into()),
            Some(222_i32.into()),
        ]))
        .unwrap();

    let row2_delete = state_table
        .get_owned_row(&Row(vec![Some(2_i32.into())]), epoch)
        .await
        .unwrap();
    assert_eq!(row2_delete, None);

    state_table.commit(epoch).await.unwrap();

    let row2_delete_commit = state_table
        .get_owned_row(&Row(vec![Some(2_i32.into())]), epoch)
        .await
        .unwrap();
    assert_eq!(row2_delete_commit, None);

    epoch += 1;
    state_table
        .delete(Row(vec![
            Some(3_i32.into()),
            Some(33_i32.into()),
            Some(333_i32.into()),
        ]))
        .unwrap();

    state_table
        .insert(Row(vec![Some(4_i32.into()), None, None]))
        .unwrap();
    let row4 = state_table
        .get_owned_row(&Row(vec![Some(4_i32.into())]), epoch)
        .await
        .unwrap();
    assert_eq!(row4, Some(Row(vec![Some(4_i32.into()), None, None])));

    let non_exist_row = state_table
        .get_owned_row(&Row(vec![Some(0_i32.into())]), epoch)
        .await
        .unwrap();
    assert_eq!(non_exist_row, None);

    state_table
        .delete(Row(vec![Some(4_i32.into()), None, None]))
        .unwrap();

    state_table.commit(epoch).await.unwrap();

    let row3_delete = state_table
        .get_owned_row(&Row(vec![Some(3_i32.into())]), epoch)
        .await
        .unwrap();
    assert_eq!(row3_delete, None);

    let row4_delete = state_table
        .get_owned_row(&Row(vec![Some(4_i32.into())]), epoch)
        .await
        .unwrap();
    assert_eq!(row4_delete, None);

    Ok(())
}

#[tokio::test]
async fn test_state_table_update_insert() -> StorageResult<()> {
    let state_store = MemoryStateStore::new();
    let keyspace = Keyspace::table_root(state_store.clone(), &TableId::from(0x42));
    let column_descs = vec![
        ColumnDesc::unnamed(ColumnId::from(0), DataType::Int32),
        ColumnDesc::unnamed(ColumnId::from(1), DataType::Int32),
        ColumnDesc::unnamed(ColumnId::from(2), DataType::Int32),
        ColumnDesc::unnamed(ColumnId::from(4), DataType::Int32),
    ];
    let order_types = vec![OrderType::Ascending];
    let pk_index = vec![0_usize];
    let mut state_table =
        StateTable::new(keyspace.clone(), column_descs, order_types, None, pk_index);
    let mut epoch: u64 = 0;
    state_table
        .insert(Row(vec![
            Some(6_i32.into()),
            Some(66_i32.into()),
            Some(666_i32.into()),
            Some(6666_i32.into()),
        ]))
        .unwrap();

    state_table
        .insert(Row(vec![
            Some(7_i32.into()),
            None,
            Some(777_i32.into()),
            None,
        ]))
        .unwrap();
    state_table.commit(epoch).await.unwrap();

    epoch += 1;
    state_table
        .delete(Row(vec![
            Some(6_i32.into()),
            Some(66_i32.into()),
            Some(666_i32.into()),
            Some(6666_i32.into()),
        ]))
        .unwrap();
    state_table
        .insert(Row(vec![
            Some(6_i32.into()),
            None,
            None,
            Some(6666_i32.into()),
        ]))
        .unwrap();

    state_table
        .delete(Row(vec![
            Some(7_i32.into()),
            None,
            Some(777_i32.into()),
            None,
        ]))
        .unwrap();
    state_table
        .insert(Row(vec![
            Some(7_i32.into()),
            Some(77_i32.into()),
            Some(7777_i32.into()),
            None,
        ]))
        .unwrap();
    let row6 = state_table
        .get_owned_row(&Row(vec![Some(6_i32.into())]), epoch)
        .await
        .unwrap();
    assert_eq!(
        row6,
        Some(Row(vec![
            Some(6_i32.into()),
            None,
            None,
            Some(6666_i32.into())
        ]))
    );

    let row7 = state_table
        .get_owned_row(&Row(vec![Some(7_i32.into())]), epoch)
        .await
        .unwrap();
    assert_eq!(
        row7,
        Some(Row(vec![
            Some(7_i32.into()),
            Some(77_i32.into()),
            Some(7777_i32.into()),
            None
        ]))
    );

    state_table.commit(epoch).await.unwrap();

    let row6_commit = state_table
        .get_owned_row(&Row(vec![Some(6_i32.into())]), epoch)
        .await
        .unwrap();
    assert_eq!(
        row6_commit,
        Some(Row(vec![
            Some(6_i32.into()),
            None,
            None,
            Some(6666_i32.into())
        ]))
    );
    let row7_commit = state_table
        .get_owned_row(&Row(vec![Some(7_i32.into())]), epoch)
        .await
        .unwrap();
    assert_eq!(
        row7_commit,
        Some(Row(vec![
            Some(7_i32.into()),
            Some(77_i32.into()),
            Some(7777_i32.into()),
            None
        ]))
    );

    epoch += 1;

    state_table
        .insert(Row(vec![
            Some(1_i32.into()),
            Some(2_i32.into()),
            Some(3_i32.into()),
            Some(4_i32.into()),
        ]))
        .unwrap();
    state_table.commit(epoch).await.unwrap();
    // one epoch: delete (1, 2, 3, 4), insert (5, 6, 7, None), delete(5, 6, 7, None)
    state_table
        .delete(Row(vec![
            Some(1_i32.into()),
            Some(2_i32.into()),
            Some(3_i32.into()),
            Some(4_i32.into()),
        ]))
        .unwrap();
    state_table
        .insert(Row(vec![
            Some(5_i32.into()),
            Some(6_i32.into()),
            Some(7_i32.into()),
            None,
        ]))
        .unwrap();
    state_table
        .delete(Row(vec![
            Some(5_i32.into()),
            Some(6_i32.into()),
            Some(7_i32.into()),
            None,
        ]))
        .unwrap();

    let row1 = state_table
        .get_owned_row(&Row(vec![Some(1_i32.into())]), epoch)
        .await
        .unwrap();
    assert_eq!(row1, None);
    state_table.commit(epoch).await.unwrap();

    let row1_commit = state_table
        .get_owned_row(&Row(vec![Some(1_i32.into())]), epoch)
        .await
        .unwrap();
    assert_eq!(row1_commit, None);
    Ok(())
}

#[tokio::test]
async fn test_state_table_iter() {
    let state_store = MemoryStateStore::new();
    let order_types = vec![OrderType::Ascending, OrderType::Descending];
    let keyspace = Keyspace::table_root(state_store, &TableId::from(0x42));
    let column_ids = vec![ColumnId::from(0), ColumnId::from(1), ColumnId::from(2)];
    let column_descs = vec![
        ColumnDesc::unnamed(column_ids[0], DataType::Int32),
        ColumnDesc::unnamed(column_ids[1], DataType::Int32),
        ColumnDesc::unnamed(column_ids[2], DataType::Int32),
    ];
    let pk_index = vec![0_usize, 1_usize];
    let mut state = StateTable::new(
        keyspace.clone(),
        column_descs.clone(),
        order_types.clone(),
        Some(vec![1]),
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

    state
        .insert(Row(vec![
            Some(3_i32.into()),
            Some(33_i32.into()),
            Some(3333_i32.into()),
        ]))
        .unwrap();

    state
        .insert(Row(vec![
            Some(6_i32.into()),
            Some(66_i32.into()),
            Some(666_i32.into()),
        ]))
        .unwrap();

    state
        .insert(Row(vec![
            Some(9_i32.into()),
            Some(99_i32.into()),
            Some(999_i32.into()),
        ]))
        .unwrap();

    {
        let iter = state.iter(epoch).await.unwrap();
        pin_mut!(iter);

        let res = iter.next().await.unwrap().unwrap();
        assert_eq!(
            &Row(vec![
                Some(1_i32.into()),
                Some(11_i32.into()),
                Some(111_i32.into())
            ]),
            res.as_ref()
        );

        // will not get [2, 22, 222]
        let res = iter.next().await.unwrap().unwrap();
        assert_eq!(
            &Row(vec![
                Some(3_i32.into()),
                Some(33_i32.into()),
                Some(3333_i32.into())
            ]),
            res.as_ref()
        );

        let res = iter.next().await.unwrap().unwrap();
        assert_eq!(
            &Row(vec![
                Some(6_i32.into()),
                Some(66_i32.into()),
                Some(666_i32.into())
            ]),
            res.as_ref()
        );
    }

    state.commit(epoch).await.unwrap();

    let epoch = u64::MAX;

    // write [3, 33, 333], [4, 44, 444], [5, 55, 555], [7, 77, 777], [8, 88, 888]into mem_table,
    // [3, 33, 3333], [6, 66, 666], [9, 99, 999] exists in
    // cell_based_table

    state
        .delete(Row(vec![
            Some(1_i32.into()),
            Some(11_i32.into()),
            Some(111_i32.into()),
        ]))
        .unwrap();
    state
        .insert(Row(vec![
            Some(3_i32.into()),
            Some(33_i32.into()),
            Some(333_i32.into()),
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
            Some(5_i32.into()),
            Some(55_i32.into()),
            Some(555_i32.into()),
        ]))
        .unwrap();
    state
        .insert(Row(vec![
            Some(7_i32.into()),
            Some(77_i32.into()),
            Some(777_i32.into()),
        ]))
        .unwrap();

    state
        .insert(Row(vec![
            Some(8_i32.into()),
            Some(88_i32.into()),
            Some(888_i32.into()),
        ]))
        .unwrap();

    let iter = state.iter(epoch).await.unwrap();
    pin_mut!(iter);

    let res = iter.next().await.unwrap().unwrap();

    // this pk exist in both cell_based_table(shared_storage) and mem_table(buffer)
    assert_eq!(
        &Row(vec![
            Some(3_i32.into()),
            Some(33_i32.into()),
            Some(333_i32.into())
        ]),
        res.as_ref()
    );

    // this row exists in mem_table
    let res = iter.next().await.unwrap().unwrap();
    assert_eq!(
        &Row(vec![
            Some(4_i32.into()),
            Some(44_i32.into()),
            Some(444_i32.into())
        ]),
        res.as_ref()
    );

    let res = iter.next().await.unwrap().unwrap();

    // this row exists in mem_table
    assert_eq!(
        &Row(vec![
            Some(5_i32.into()),
            Some(55_i32.into()),
            Some(555_i32.into())
        ]),
        res.as_ref()
    );
    let res = iter.next().await.unwrap().unwrap();

    // this row exists in cell_based_table
    assert_eq!(
        &Row(vec![
            Some(6_i32.into()),
            Some(66_i32.into()),
            Some(666_i32.into())
        ]),
        res.as_ref()
    );

    let res = iter.next().await.unwrap().unwrap();
    // this row exists in mem_table
    assert_eq!(
        &Row(vec![
            Some(7_i32.into()),
            Some(77_i32.into()),
            Some(777.into())
        ]),
        res.as_ref()
    );

    let res = iter.next().await.unwrap().unwrap();

    // this row exists in mem_table
    assert_eq!(
        &Row(vec![
            Some(8_i32.into()),
            Some(88_i32.into()),
            Some(888_i32.into())
        ]),
        res.as_ref()
    );

    let res = iter.next().await.unwrap().unwrap();

    // this row exists in cell_based_table
    assert_eq!(
        &Row(vec![
            Some(9_i32.into()),
            Some(99_i32.into()),
            Some(999_i32.into())
        ]),
        res.as_ref()
    );

    // there is no row in both cell_based_table and mem_table
    let res = iter.next().await;
    assert!(res.is_none());
}

#[tokio::test]
async fn test_multi_state_table_iter() {
    let state_store = MemoryStateStore::new();
    // let pk_columns = vec![0, 1]; leave a message to indicate pk columns
    let order_types = vec![OrderType::Ascending, OrderType::Descending];

    let keyspace_1 = Keyspace::table_root(state_store.clone(), &TableId::from(0x1111));
    let keyspace_2 = Keyspace::table_root(state_store.clone(), &TableId::from(0x2222));
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
    let pk_index = vec![0_usize, 1_usize];
    let mut state_1 = StateTable::new(
        keyspace_1.clone(),
        column_descs_1.clone(),
        order_types.clone(),
        None,
        pk_index.clone(),
    );
    let mut state_2 = StateTable::new(
        keyspace_2.clone(),
        column_descs_2.clone(),
        order_types.clone(),
        None,
        pk_index,
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

    {
        let iter_1 = state_1.iter(epoch).await.unwrap();
        pin_mut!(iter_1);
        let iter_2 = state_2.iter(epoch).await.unwrap();
        pin_mut!(iter_2);

        let res_1_1 = iter_1.next().await.unwrap().unwrap();
        assert_eq!(
            &Row(vec![
                Some(1_i32.into()),
                Some(11_i32.into()),
                Some(111_i32.into()),
            ]),
            res_1_1.as_ref()
        );
        let res_1_2 = iter_1.next().await;
        assert!(res_1_2.is_none());

        let res_2_1 = iter_2.next().await.unwrap().unwrap();
        assert_eq!(
            &Row(vec![
                Some("1".to_string().into()),
                Some("11".to_string().into()),
                Some("111".to_string().into())
            ]),
            res_2_1.as_ref()
        );
        let res_2_2 = iter_2.next().await;
        assert!(res_2_2.is_none());
    }

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
    let pk_index = vec![0_usize, 1_usize];
    let order_types = vec![OrderType::Ascending, OrderType::Descending];
    let keyspace = Keyspace::table_root(state_store, &TableId::from(0x42));
    let mut state = StateTable::new(
        keyspace.clone(),
        column_descs.clone(),
        order_types.clone(),
        None,
        pk_index,
    );
    let table = CellBasedTable::new_for_test(keyspace.clone(), column_descs, order_types);
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
        .get_row_by_scan(&Row(vec![Some(1_i32.into()), None]), epoch)
        .await
        .unwrap();
    assert_eq!(
        get_row1_res,
        Some(Row(vec![Some(1_i32.into()), None, None,]))
    );

    let get_row2_res = table
        .get_row_by_scan(&Row(vec![Some(2_i32.into()), None]), epoch)
        .await
        .unwrap();
    assert_eq!(get_row2_res, None);

    let get_row3_res = table
        .get_row_by_scan(&Row(vec![Some(3_i32.into()), None]), epoch)
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
async fn test_cell_based_get_row_by_multi_get() {
    let state_store = MemoryStateStore::new();
    let column_ids = vec![ColumnId::from(0), ColumnId::from(1), ColumnId::from(2)];
    let column_descs = vec![
        ColumnDesc::unnamed(column_ids[0], DataType::Int32),
        ColumnDesc::unnamed(column_ids[1], DataType::Int32),
        ColumnDesc::unnamed(column_ids[2], DataType::Int32),
    ];
    let pk_index = vec![0_usize, 1_usize];
    let order_types = vec![OrderType::Ascending, OrderType::Descending];
    let keyspace = Keyspace::table_root(state_store, &TableId::from(0x42));
    let mut state = StateTable::new(
        keyspace.clone(),
        column_descs.clone(),
        order_types.clone(),
        None,
        pk_index,
    );
    let table = CellBasedTable::new_for_test(keyspace.clone(), column_descs, order_types);
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
async fn test_cell_based_get_row_for_string() {
    let state_store = MemoryStateStore::new();
    let order_types = vec![OrderType::Ascending, OrderType::Descending];
    let keyspace = Keyspace::table_root(state_store, &TableId::from(0x42));
    let column_ids = vec![ColumnId::from(1), ColumnId::from(4), ColumnId::from(7)];
    let column_descs = vec![
        ColumnDesc::unnamed(column_ids[0], DataType::Varchar),
        ColumnDesc::unnamed(column_ids[1], DataType::Varchar),
        ColumnDesc::unnamed(column_ids[2], DataType::Varchar),
    ];
    let pk_index = vec![0_usize, 1_usize];
    let mut state = StateTable::new(
        keyspace.clone(),
        column_descs.clone(),
        order_types.clone(),
        None,
        pk_index,
    );
    let table = CellBasedTable::new_for_test(keyspace.clone(), column_descs, order_types);
    let epoch: u64 = 0;

    state
        .insert(Row(vec![
            Some("1".to_string().into()),
            Some("11".to_string().into()),
            Some("111".to_string().into()),
        ]))
        .unwrap();
    state
        .insert(Row(vec![
            Some("4".to_string().into()),
            Some("44".to_string().into()),
            Some("444".to_string().into()),
        ]))
        .unwrap();
    state
        .delete(Row(vec![
            Some("4".to_string().into()),
            Some("44".to_string().into()),
            Some("444".to_string().into()),
        ]))
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
    let keyspace = Keyspace::table_root(state_store, &TableId::from(0x42));
    let pk_index = vec![0_usize, 1_usize];
    let mut state = StateTable::new(
        keyspace.clone(),
        column_descs.clone(),
        order_types.clone(),
        None,
        pk_index,
    );
    let table = CellBasedTable::new_for_test(keyspace.clone(), column_descs, order_types);
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
    let keyspace = Keyspace::table_root(state_store, &TableId::from(0x42));
    let column_ids = vec![ColumnId::from(0), ColumnId::from(1), ColumnId::from(2)];
    let column_descs = vec![
        ColumnDesc::unnamed(column_ids[0], DataType::Int32),
        ColumnDesc::unnamed(column_ids[1], DataType::Int32),
        ColumnDesc::unnamed(column_ids[2], DataType::Int32),
    ];
    let pk_index = vec![0_usize, 1_usize];
    let mut state = StateTable::new(
        keyspace.clone(),
        column_descs.clone(),
        order_types.clone(),
        None,
        pk_index,
    );
    let table = CellBasedTable::new_for_test(keyspace.clone(), column_descs, order_types);
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

    let keyspace_1 = Keyspace::table_root(state_store.clone(), &TableId::from(0x1111));
    let keyspace_2 = Keyspace::table_root(state_store.clone(), &TableId::from(0x2222));
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
    let pk_index = vec![0_usize, 1_usize];
    let mut state_1 = StateTable::new(
        keyspace_1.clone(),
        column_descs_1.clone(),
        order_types.clone(),
        None,
        pk_index.clone(),
    );
    let mut state_2 = StateTable::new(
        keyspace_2.clone(),
        column_descs_2.clone(),
        order_types.clone(),
        None,
        pk_index,
    );

    let table_1 =
        CellBasedTable::new_for_test(keyspace_1.clone(), column_descs_1, order_types.clone());
    let table_2 = CellBasedTable::new_for_test(keyspace_2.clone(), column_descs_2, order_types);

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

async fn test_dedup_cell_based_table_iter_with(
    row_ordered_descs: Vec<OrderedColumnDesc>,
    pk_indices: Vec<usize>,
    rows: Vec<Row>,
) {
    // ---------- Declare meta
    let pk_ordered_descs = pk_indices
        .iter()
        .map(|row_idx| row_ordered_descs[*row_idx].clone())
        .collect_vec();
    let pk_indices_set = pk_indices.iter().collect::<HashSet<_>>();
    let order_types = pk_ordered_descs.iter().map(|d| d.order).collect::<Vec<_>>();

    let row_descs = row_ordered_descs
        .into_iter()
        .map(|od| od.column_desc)
        .collect_vec();

    let partial_row_indices = (0..row_descs.len())
        .filter(|i| !(pk_indices_set.contains(i) && row_descs[*i].data_type.mem_cmp_eq_value_enc()))
        .collect_vec();
    let partial_row_descs = partial_row_indices
        .iter()
        .map(|i| row_descs[*i].clone())
        .collect_vec();
    let partial_row_indices_set = partial_row_indices.iter().collect::<HashSet<_>>();
    let partial_row_col_ids = partial_row_descs.iter().map(|d| d.column_id).collect_vec();

    // ---------- Init storage
    let state_store = MemoryStateStore::new();
    let keyspace = Keyspace::table_root(state_store.clone(), &TableId::from(0x1111));
    let epoch: u64 = 0;

    let mut batch = keyspace.state_store().start_write_batch();
    let mut local = batch.prefixify(&keyspace);

    // ---------- Init write serializer
    let mut cell_based_row_serializer = CellBasedRowSerializer::new();
    let ordered_row_serializer = OrderedRowSerializer::new(order_types.clone());

    // ---------- Init table for writes
    let table = CellBasedTable::new_for_test(keyspace.clone(), row_descs, order_types);

    for Row(row) in rows.clone() {
        // ---------- Serialize to cell repr
        let pk = Row(pk_indices
            .iter()
            .map(|row_idx| row[*row_idx].clone())
            .collect_vec());
        let pk_bytes = serialize_pk(&pk, &ordered_row_serializer);

        let partial_row = Row(row
            .iter()
            .enumerate()
            .filter(|(i, _)| partial_row_indices_set.contains(i))
            .map(|(_, row_datum)| row_datum.clone())
            .collect_vec());

        let bytes = cell_based_row_serializer
            .serialize(&pk_bytes, partial_row, &partial_row_col_ids)
            .unwrap();

        // ---------- Batch-write
        for (key, value) in bytes {
            local.put(key, StorageValue::new_put(ValueMeta::default(), value))
        }
    }

    // commit batch
    batch.ingest(epoch).await.unwrap();

    let mut actual_rows = vec![];

    // ---------- Init reader
    let iter = table
        .batch_dedup_pk_iter(epoch, &pk_ordered_descs)
        .await
        .unwrap();
    pin_mut!(iter);

    for _ in 0..rows.len() {
        // ---------- Read + Deserialize from storage
        let actual = iter.next_row().await.unwrap();
        assert!(actual.is_some());
        actual_rows.push(actual.unwrap());
    }

    actual_rows.sort();
    let mut rows = rows;
    rows.sort();

    // ---------- Verify
    assert_eq!(actual_rows, rows);
}

#[tokio::test]
async fn test_dedup_cell_based_table_iter() {
    let pk_indices_permutations = vec![
        vec![0, 1, 2],
        vec![0, 1],
        vec![0, 2],
        vec![0],
        vec![1],
        vec![2],
    ];
    let row_ordered_descs = vec![
        OrderedColumnDesc {
            column_desc: ColumnDesc::unnamed(ColumnId::from(0), DataType::Int32),
            order: OrderType::Ascending,
        },
        OrderedColumnDesc {
            column_desc: ColumnDesc::unnamed(ColumnId::from(1), DataType::Int32),
            order: OrderType::Descending,
        },
        OrderedColumnDesc {
            column_desc: ColumnDesc::unnamed(ColumnId::from(2), DataType::Float64),
            order: OrderType::Descending,
        },
    ];
    let rows = vec![
        Row(vec![
            Some(1_i32.into()),
            Some(11_i32.into()),
            Some(111.001_f64.into()),
        ]),
        Row(vec![
            Some(222_i32.into()),
            Some(22_i32.into()),
            Some(2.002_f64.into()),
        ]),
        Row(vec![
            Some(333_i32.into()),
            Some(33_i32.into()),
            Some(3.003_f64.into()),
        ]),
    ];

    for pk_indices in pk_indices_permutations {
        test_dedup_cell_based_table_iter_with(row_ordered_descs.clone(), pk_indices, rows.clone())
            .await
    }
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
    let keyspace = Keyspace::table_root(state_store, &TableId::from(0x42));
    let pk_index = vec![0_usize, 1_usize];
    let mut state = StateTable::new(
        keyspace.clone(),
        column_descs.clone(),
        order_types.clone(),
        None,
        pk_index,
    );
    let table = CellBasedTable::new_for_test(keyspace.clone(), column_descs, order_types);
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
async fn test_state_table_iter_with_bounds() {
    let state_store = MemoryStateStore::new();
    // let pk_columns = vec![0, 1]; leave a message to indicate pk columns
    let order_types = vec![OrderType::Ascending, OrderType::Descending];
    let keyspace = Keyspace::table_root(state_store, &TableId::from(0x42));
    let column_ids = vec![ColumnId::from(0), ColumnId::from(1), ColumnId::from(2)];
    let column_descs = vec![
        ColumnDesc::unnamed(column_ids[0], DataType::Int32),
        ColumnDesc::unnamed(column_ids[1], DataType::Int32),
        ColumnDesc::unnamed(column_ids[2], DataType::Int32),
    ];
    let pk_index = vec![0_usize, 1_usize];
    let mut state = StateTable::new(
        keyspace.clone(),
        column_descs.clone(),
        order_types.clone(),
        None,
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
            Some(2_i32.into()),
            Some(22_i32.into()),
            Some(222_i32.into()),
        ]))
        .unwrap();
    state
        .insert(Row(vec![
            Some(3_i32.into()),
            Some(33_i32.into()),
            Some(333_i32.into()),
        ]))
        .unwrap();
    state
        .insert(Row(vec![
            Some(5_i32.into()),
            Some(55_i32.into()),
            Some(555_i32.into()),
        ]))
        .unwrap();
    state
        .insert(Row(vec![
            Some(7_i32.into()),
            Some(77_i32.into()),
            Some(777_i32.into()),
        ]))
        .unwrap();
    state.commit(epoch).await.unwrap();
    state
        .insert(Row(vec![
            Some(4_i32.into()),
            Some(44_i32.into()),
            Some(444_i32.into()),
        ]))
        .unwrap();
    state
        .insert(Row(vec![
            Some(5_i32.into()),
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
    let pk_bounds = Row(vec![Some(2_i32.into()), Some(22_i32.into())])
        ..Row(vec![Some(6_i32.into()), Some(66_i32.into())]);
    let iter = state.iter_with_pk_bounds(pk_bounds, epoch).await.unwrap();
    pin_mut!(iter);

    // this row exists in cell_based_table
    let res = iter.next().await.unwrap().unwrap();
    assert_eq!(
        &Row(vec![
            Some(2_i32.into()),
            Some(22_i32.into()),
            Some(222_i32.into())
        ]),
        res.as_ref()
    );
    // this row exists in cell_based_table
    let res = iter.next().await.unwrap().unwrap();
    assert_eq!(
        &Row(vec![
            Some(3_i32.into()),
            Some(33_i32.into()),
            Some(333_i32.into())
        ]),
        res.as_ref()
    );
    // this row exists in mem_table
    let res = iter.next().await.unwrap().unwrap();
    assert_eq!(
        &Row(vec![
            Some(4_i32.into()),
            Some(44_i32.into()),
            Some(444_i32.into())
        ]),
        res.as_ref()
    );
    // this row exists in both mem_table and cell_based_table
    let res = iter.next().await.unwrap().unwrap();
    assert_eq!(
        &Row(vec![
            Some(5_i32.into()),
            Some(55_i32.into()),
            Some(5555_i32.into())
        ]),
        res.as_ref()
    );
    // pk outside the range will not be scan
    let res = iter.next().await;
    assert!(res.is_none());
}

#[tokio::test]
async fn test_state_table_iter_with_unbounded_range() {
    let state_store = MemoryStateStore::new();
    // let pk_columns = vec![0, 1]; leave a message to indicate pk columns
    let order_types = vec![OrderType::Ascending, OrderType::Descending];
    let keyspace = Keyspace::table_root(state_store, &TableId::from(0x42));
    let column_ids = vec![ColumnId::from(0), ColumnId::from(1), ColumnId::from(2)];
    let column_descs = vec![
        ColumnDesc::unnamed(column_ids[0], DataType::Int32),
        ColumnDesc::unnamed(column_ids[1], DataType::Int32),
        ColumnDesc::unnamed(column_ids[2], DataType::Int32),
    ];
    let pk_index = vec![0_usize, 1_usize];
    let mut state = StateTable::new(
        keyspace.clone(),
        column_descs.clone(),
        order_types.clone(),
        None,
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
            Some(2_i32.into()),
            Some(22_i32.into()),
            Some(222_i32.into()),
        ]))
        .unwrap();
    state
        .insert(Row(vec![
            Some(3_i32.into()),
            Some(33_i32.into()),
            Some(333_i32.into()),
        ]))
        .unwrap();
    state
        .insert(Row(vec![
            Some(5_i32.into()),
            Some(55_i32.into()),
            Some(555_i32.into()),
        ]))
        .unwrap();
    state
        .insert(Row(vec![
            Some(7_i32.into()),
            Some(77_i32.into()),
            Some(777_i32.into()),
        ]))
        .unwrap();
    state.commit(epoch).await.unwrap();
    state
        .insert(Row(vec![
            Some(4_i32.into()),
            Some(44_i32.into()),
            Some(444_i32.into()),
        ]))
        .unwrap();
    state
        .insert(Row(vec![
            Some(5_i32.into()),
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
    let pk_bounds = Row(vec![Some(3_i32.into()), Some(33_i32.into())])..;
    let iter = state.iter_with_pk_bounds(pk_bounds, epoch).await.unwrap();
    pin_mut!(iter);

    // this row exists in cell_based_table
    let res = iter.next().await.unwrap().unwrap();
    assert_eq!(
        &Row(vec![
            Some(3_i32.into()),
            Some(33_i32.into()),
            Some(333_i32.into())
        ]),
        res.as_ref()
    );
    // this row exists in mem_table
    let res = iter.next().await.unwrap().unwrap();
    assert_eq!(
        &Row(vec![
            Some(4_i32.into()),
            Some(44_i32.into()),
            Some(444_i32.into())
        ]),
        res.as_ref()
    );
    // this row exists in both mem_table and cell_based_table
    let res = iter.next().await.unwrap().unwrap();
    assert_eq!(
        &Row(vec![
            Some(5_i32.into()),
            Some(55_i32.into()),
            Some(5555_i32.into())
        ]),
        res.as_ref()
    );

    // this row exists in mem_table
    let res = iter.next().await.unwrap().unwrap();
    assert_eq!(
        &Row(vec![
            Some(6_i32.into()),
            Some(66_i32.into()),
            Some(666_i32.into())
        ]),
        res.as_ref()
    );

    // this row exists in cell_based_table
    let res = iter.next().await.unwrap().unwrap();
    assert_eq!(
        &Row(vec![
            Some(7_i32.into()),
            Some(77_i32.into()),
            Some(777_i32.into())
        ]),
        res.as_ref()
    );
    // pk outside the range will not be scan
    let res = iter.next().await;
    assert!(res.is_none());
}

#[tokio::test]
async fn test_state_table_iter_with_prefix() {
    let state_store = MemoryStateStore::new();
    // let pk_columns = vec![0, 1]; leave a message to indicate pk columns
    let order_types = vec![OrderType::Ascending, OrderType::Descending];
    let keyspace = Keyspace::table_root(state_store, &TableId::from(0x42));

    let column_ids = vec![ColumnId::from(0), ColumnId::from(1), ColumnId::from(2)];
    let column_descs = vec![
        ColumnDesc::unnamed(column_ids[0], DataType::Int32),
        ColumnDesc::unnamed(column_ids[1], DataType::Int32),
        ColumnDesc::unnamed(column_ids[2], DataType::Int32),
    ];
    let pk_index = vec![0_usize, 1_usize];
    let mut state = StateTable::new(
        keyspace.clone(),
        column_descs.clone(),
        order_types.clone(),
        None,
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

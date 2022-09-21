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
use risingwave_common::catalog::{ColumnDesc, ColumnId, TableId};
use risingwave_common::types::DataType;
use risingwave_common::util::sort_util::OrderType;

use crate::error::StorageResult;
use crate::memory::MemoryStateStore;
use crate::table::streaming_table::state_table::StateTable;

// test state table
#[tokio::test]
async fn test_state_table() -> StorageResult<()> {
    let state_store = MemoryStateStore::new();
    let column_descs = vec![
        ColumnDesc::unnamed(ColumnId::from(0), DataType::Int32),
        ColumnDesc::unnamed(ColumnId::from(1), DataType::Int32),
        ColumnDesc::unnamed(ColumnId::from(2), DataType::Int32),
    ];
    let order_types = vec![OrderType::Ascending];
    let pk_index = vec![0_usize];
    let mut state_table = StateTable::new_without_distribution(
        state_store.clone(),
        TableId::from(0x42),
        column_descs,
        order_types,
        pk_index,
    );

    let mut epoch = 0;
    state_table.init_epoch(epoch);

    state_table.insert(Row(vec![
        Some(1_i32.into()),
        Some(11_i32.into()),
        Some(111_i32.into()),
    ]));
    state_table.insert(Row(vec![
        Some(2_i32.into()),
        Some(22_i32.into()),
        Some(222_i32.into()),
    ]));
    state_table.insert(Row(vec![
        Some(3_i32.into()),
        Some(33_i32.into()),
        Some(333_i32.into()),
    ]));

    // test read visibility
    let row1 = state_table
        .get_row(&Row(vec![Some(1_i32.into())]))
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
        .get_row(&Row(vec![Some(2_i32.into())]))
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

    state_table.delete(Row(vec![
        Some(2_i32.into()),
        Some(22_i32.into()),
        Some(222_i32.into()),
    ]));

    let row2_delete = state_table
        .get_row(&Row(vec![Some(2_i32.into())]))
        .await
        .unwrap();
    assert_eq!(row2_delete, None);

    epoch += 1;
    state_table.commit(epoch).await.unwrap();

    let row2_delete_commit = state_table
        .get_row(&Row(vec![Some(2_i32.into())]))
        .await
        .unwrap();
    assert_eq!(row2_delete_commit, None);

    state_table.delete(Row(vec![
        Some(3_i32.into()),
        Some(33_i32.into()),
        Some(333_i32.into()),
    ]));

    state_table.insert(Row(vec![Some(4_i32.into()), None, None]));
    let row4 = state_table
        .get_row(&Row(vec![Some(4_i32.into())]))
        .await
        .unwrap();
    assert_eq!(row4, Some(Row(vec![Some(4_i32.into()), None, None])));

    let non_exist_row = state_table
        .get_row(&Row(vec![Some(0_i32.into())]))
        .await
        .unwrap();
    assert_eq!(non_exist_row, None);

    state_table.delete(Row(vec![Some(4_i32.into()), None, None]));

    epoch += 1;
    state_table.commit(epoch).await.unwrap();

    let row3_delete = state_table
        .get_row(&Row(vec![Some(3_i32.into())]))
        .await
        .unwrap();
    assert_eq!(row3_delete, None);

    let row4_delete = state_table
        .get_row(&Row(vec![Some(4_i32.into())]))
        .await
        .unwrap();
    assert_eq!(row4_delete, None);

    Ok(())
}

#[tokio::test]
async fn test_state_table_update_insert() -> StorageResult<()> {
    let state_store = MemoryStateStore::new();
    let column_descs = vec![
        ColumnDesc::unnamed(ColumnId::from(0), DataType::Int32),
        ColumnDesc::unnamed(ColumnId::from(1), DataType::Int32),
        ColumnDesc::unnamed(ColumnId::from(2), DataType::Int32),
        ColumnDesc::unnamed(ColumnId::from(4), DataType::Int32),
    ];
    let order_types = vec![OrderType::Ascending];
    let pk_index = vec![0_usize];
    let mut state_table = StateTable::new_without_distribution(
        state_store.clone(),
        TableId::from(0x42),
        column_descs,
        order_types,
        pk_index,
    );

    let mut epoch: u64 = 0;
    state_table.init_epoch(epoch);

    state_table.insert(Row(vec![
        Some(6_i32.into()),
        Some(66_i32.into()),
        Some(666_i32.into()),
        Some(6666_i32.into()),
    ]));

    state_table.insert(Row(vec![
        Some(7_i32.into()),
        None,
        Some(777_i32.into()),
        None,
    ]));

    epoch += 1;
    state_table.commit(epoch).await.unwrap();

    state_table.delete(Row(vec![
        Some(6_i32.into()),
        Some(66_i32.into()),
        Some(666_i32.into()),
        Some(6666_i32.into()),
    ]));
    state_table.insert(Row(vec![
        Some(6_i32.into()),
        None,
        None,
        Some(6666_i32.into()),
    ]));

    state_table.delete(Row(vec![
        Some(7_i32.into()),
        None,
        Some(777_i32.into()),
        None,
    ]));
    state_table.insert(Row(vec![
        Some(7_i32.into()),
        Some(77_i32.into()),
        Some(7777_i32.into()),
        None,
    ]));
    let row6 = state_table
        .get_row(&Row(vec![Some(6_i32.into())]))
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
        .get_row(&Row(vec![Some(7_i32.into())]))
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

    epoch += 1;
    state_table.commit(epoch).await.unwrap();

    let row6_commit = state_table
        .get_row(&Row(vec![Some(6_i32.into())]))
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
        .get_row(&Row(vec![Some(7_i32.into())]))
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

    state_table.insert(Row(vec![
        Some(1_i32.into()),
        Some(2_i32.into()),
        Some(3_i32.into()),
        Some(4_i32.into()),
    ]));

    epoch += 1;
    state_table.commit(epoch).await.unwrap();

    // one epoch: delete (1, 2, 3, 4), insert (5, 6, 7, None), delete(5, 6, 7, None)
    state_table.delete(Row(vec![
        Some(1_i32.into()),
        Some(2_i32.into()),
        Some(3_i32.into()),
        Some(4_i32.into()),
    ]));
    state_table.insert(Row(vec![
        Some(5_i32.into()),
        Some(6_i32.into()),
        Some(7_i32.into()),
        None,
    ]));
    state_table.delete(Row(vec![
        Some(5_i32.into()),
        Some(6_i32.into()),
        Some(7_i32.into()),
        None,
    ]));

    let row1 = state_table
        .get_row(&Row(vec![Some(1_i32.into())]))
        .await
        .unwrap();
    assert_eq!(row1, None);

    epoch += 1;
    state_table.commit(epoch).await.unwrap();

    let row1_commit = state_table
        .get_row(&Row(vec![Some(1_i32.into())]))
        .await
        .unwrap();
    assert_eq!(row1_commit, None);
    Ok(())
}

#[tokio::test]
async fn test_state_table_iter() {
    let state_store = MemoryStateStore::new();
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

    let mut epoch = 0;
    state.init_epoch(epoch);

    state.insert(Row(vec![
        Some(1_i32.into()),
        Some(11_i32.into()),
        Some(111_i32.into()),
    ]));
    state.insert(Row(vec![
        Some(2_i32.into()),
        Some(22_i32.into()),
        Some(222_i32.into()),
    ]));
    state.delete(Row(vec![
        Some(2_i32.into()),
        Some(22_i32.into()),
        Some(222_i32.into()),
    ]));

    state.insert(Row(vec![
        Some(3_i32.into()),
        Some(33_i32.into()),
        Some(3333_i32.into()),
    ]));

    state.insert(Row(vec![
        Some(6_i32.into()),
        Some(66_i32.into()),
        Some(666_i32.into()),
    ]));

    state.insert(Row(vec![
        Some(9_i32.into()),
        Some(99_i32.into()),
        Some(999_i32.into()),
    ]));

    {
        let iter = state.iter().await.unwrap();
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
    epoch += 1;
    state.commit(epoch).await.unwrap();

    // write [3, 33, 333], [4, 44, 444], [5, 55, 555], [7, 77, 777], [8, 88, 888]into mem_table,
    // [3, 33, 3333], [6, 66, 666], [9, 99, 999] exists in
    // cell_based_table

    state.delete(Row(vec![
        Some(1_i32.into()),
        Some(11_i32.into()),
        Some(111_i32.into()),
    ]));
    state.insert(Row(vec![
        Some(3_i32.into()),
        Some(33_i32.into()),
        Some(333_i32.into()),
    ]));

    state.insert(Row(vec![
        Some(4_i32.into()),
        Some(44_i32.into()),
        Some(444_i32.into()),
    ]));

    state.insert(Row(vec![
        Some(5_i32.into()),
        Some(55_i32.into()),
        Some(555_i32.into()),
    ]));
    state.insert(Row(vec![
        Some(7_i32.into()),
        Some(77_i32.into()),
        Some(777_i32.into()),
    ]));

    state.insert(Row(vec![
        Some(8_i32.into()),
        Some(88_i32.into()),
        Some(888_i32.into()),
    ]));

    let iter = state.iter().await.unwrap();
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

    let mut epoch: u64 = 0;
    state.init_epoch(epoch);

    state.insert(Row(vec![
        Some(1_i32.into()),
        Some(11_i32.into()),
        Some(111_i32.into()),
    ]));
    state.insert(Row(vec![
        Some(1_i32.into()),
        Some(22_i32.into()),
        Some(222_i32.into()),
    ]));

    state.insert(Row(vec![
        Some(4_i32.into()),
        Some(44_i32.into()),
        Some(444_i32.into()),
    ]));

    state.insert(Row(vec![
        Some(1_i32.into()),
        Some(55_i32.into()),
        Some(555_i32.into()),
    ]));

    epoch += 1;
    state.commit(epoch).await.unwrap();

    state.insert(Row(vec![
        Some(1_i32.into()),
        Some(33_i32.into()),
        Some(333_i32.into()),
    ]));
    state.insert(Row(vec![
        Some(1_i32.into()),
        Some(55_i32.into()),
        Some(5555_i32.into()),
    ]));
    state.insert(Row(vec![
        Some(6_i32.into()),
        Some(66_i32.into()),
        Some(666_i32.into()),
    ]));

    let pk_prefix = Row(vec![Some(1_i32.into())]);
    let iter = state.iter_with_pk_prefix(&pk_prefix).await.unwrap();
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

#[tokio::test]
#[should_panic]
async fn test_mem_table_assertion() {
    let state_store = MemoryStateStore::new();
    let column_descs = vec![
        ColumnDesc::unnamed(ColumnId::from(0), DataType::Int32),
        ColumnDesc::unnamed(ColumnId::from(1), DataType::Int32),
        ColumnDesc::unnamed(ColumnId::from(2), DataType::Int32),
    ];
    let order_types = vec![OrderType::Ascending];
    let pk_index = vec![0_usize];
    let mut state_table = StateTable::new_without_distribution(
        state_store,
        TableId::from(0x42),
        column_descs,
        order_types,
        pk_index,
    );
    state_table.init_epoch(0);
    state_table.insert(Row(vec![
        Some(1_i32.into()),
        Some(11_i32.into()),
        Some(111_i32.into()),
    ]));
    // duplicate key: 1
    state_table.insert(Row(vec![
        Some(1_i32.into()),
        Some(22_i32.into()),
        Some(222_i32.into()),
    ]));
}

#[tokio::test]
async fn test_state_table_iter_with_value_indices() {
    let state_store = MemoryStateStore::new();
    let order_types = vec![OrderType::Ascending, OrderType::Descending];
    let column_ids = vec![ColumnId::from(0), ColumnId::from(1), ColumnId::from(2)];
    let column_descs = vec![
        ColumnDesc::unnamed(column_ids[0], DataType::Int32),
        ColumnDesc::unnamed(column_ids[1], DataType::Int32),
        ColumnDesc::unnamed(column_ids[2], DataType::Int32),
    ];
    let pk_index = vec![0_usize, 1_usize];
    let mut state = StateTable::new_without_distribution_partial(
        state_store.clone(),
        TableId::from(0x42),
        column_descs.clone(),
        order_types.clone(),
        pk_index,
        vec![2],
    );
    let mut epoch: u64 = 0;
    state.init_epoch(epoch);

    state.insert(Row(vec![
        Some(1_i32.into()),
        Some(11_i32.into()),
        Some(111_i32.into()),
    ]));
    state.insert(Row(vec![
        Some(2_i32.into()),
        Some(22_i32.into()),
        Some(222_i32.into()),
    ]));
    state.delete(Row(vec![
        Some(2_i32.into()),
        Some(22_i32.into()),
        Some(222_i32.into()),
    ]));

    state.insert(Row(vec![
        Some(3_i32.into()),
        Some(33_i32.into()),
        Some(3333_i32.into()),
    ]));

    state.insert(Row(vec![
        Some(6_i32.into()),
        Some(66_i32.into()),
        Some(666_i32.into()),
    ]));

    state.insert(Row(vec![
        Some(9_i32.into()),
        Some(99_i32.into()),
        Some(999_i32.into()),
    ]));

    {
        let iter = state.iter().await.unwrap();
        pin_mut!(iter);

        let res = iter.next().await.unwrap().unwrap();
        assert_eq!(&Row(vec![Some(111_i32.into())]), res.as_ref());

        // will not get [2, 22, 222]
        let res = iter.next().await.unwrap().unwrap();
        assert_eq!(&Row(vec![Some(3333_i32.into())]), res.as_ref());

        let res = iter.next().await.unwrap().unwrap();
        assert_eq!(&Row(vec![Some(666_i32.into())]), res.as_ref());
    }

    epoch += 1;
    state.commit(epoch).await.unwrap();

    // write [3, 33, 333], [4, 44, 444], [5, 55, 555], [7, 77, 777], [8, 88, 888]into mem_table,
    // [3, 33, 3333], [6, 66, 666], [9, 99, 999] exists in
    // cell_based_table

    state.delete(Row(vec![
        Some(1_i32.into()),
        Some(11_i32.into()),
        Some(111_i32.into()),
    ]));
    state.insert(Row(vec![
        Some(3_i32.into()),
        Some(33_i32.into()),
        Some(333_i32.into()),
    ]));

    state.insert(Row(vec![
        Some(4_i32.into()),
        Some(44_i32.into()),
        Some(444_i32.into()),
    ]));

    state.insert(Row(vec![
        Some(5_i32.into()),
        Some(55_i32.into()),
        Some(555_i32.into()),
    ]));
    state.insert(Row(vec![
        Some(7_i32.into()),
        Some(77_i32.into()),
        Some(777_i32.into()),
    ]));

    state.insert(Row(vec![
        Some(8_i32.into()),
        Some(88_i32.into()),
        Some(888_i32.into()),
    ]));

    let iter = state.iter().await.unwrap();
    pin_mut!(iter);

    let res = iter.next().await.unwrap().unwrap();

    // this pk exist in both cell_based_table(shared_storage) and mem_table(buffer)
    assert_eq!(&Row(vec![Some(333_i32.into())]), res.as_ref());

    // this row exists in mem_table
    let res = iter.next().await.unwrap().unwrap();
    assert_eq!(&Row(vec![Some(444_i32.into())]), res.as_ref());

    let res = iter.next().await.unwrap().unwrap();

    // this row exists in mem_table
    assert_eq!(&Row(vec![Some(555_i32.into())]), res.as_ref());
    let res = iter.next().await.unwrap().unwrap();

    // this row exists in cell_based_table
    assert_eq!(&Row(vec![Some(666_i32.into())]), res.as_ref());

    let res = iter.next().await.unwrap().unwrap();
    // this row exists in mem_table
    assert_eq!(&Row(vec![Some(777.into())]), res.as_ref());

    let res = iter.next().await.unwrap().unwrap();

    // this row exists in mem_table
    assert_eq!(&Row(vec![Some(888_i32.into())]), res.as_ref());

    let res = iter.next().await.unwrap().unwrap();

    // this row exists in cell_based_table
    assert_eq!(&Row(vec![Some(999_i32.into())]), res.as_ref());

    // there is no row in both cell_based_table and mem_table
    let res = iter.next().await;
    assert!(res.is_none());
}

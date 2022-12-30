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
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::{ColumnDesc, ColumnId, TableId};
use risingwave_common::row::{self, OwnedRow};
use risingwave_common::types::DataType;
use risingwave_common::util::epoch::EpochPair;
use risingwave_common::util::sort_util::OrderType;
use risingwave_storage::memory::MemoryStateStore;
use risingwave_storage::table::DEFAULT_VNODE;

use crate::common::table::state_table::StateTable;

// test state table
#[tokio::test]
async fn test_state_table() {
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
    )
    .await;

    let epoch = EpochPair::new_test_epoch(1);
    state_table.init_epoch(epoch);

    state_table.insert(OwnedRow::new(vec![
        Some(1_i32.into()),
        Some(11_i32.into()),
        Some(111_i32.into()),
    ]));
    state_table.insert(OwnedRow::new(vec![
        Some(2_i32.into()),
        Some(22_i32.into()),
        Some(222_i32.into()),
    ]));
    state_table.insert(OwnedRow::new(vec![
        Some(3_i32.into()),
        Some(33_i32.into()),
        Some(333_i32.into()),
    ]));

    // test read visibility
    let row1 = state_table
        .get_row(&OwnedRow::new(vec![Some(1_i32.into())]))
        .await
        .unwrap();
    assert_eq!(
        row1,
        Some(OwnedRow::new(vec![
            Some(1_i32.into()),
            Some(11_i32.into()),
            Some(111_i32.into())
        ]))
    );

    let row2 = state_table
        .get_row(&OwnedRow::new(vec![Some(2_i32.into())]))
        .await
        .unwrap();
    assert_eq!(
        row2,
        Some(OwnedRow::new(vec![
            Some(2_i32.into()),
            Some(22_i32.into()),
            Some(222_i32.into())
        ]))
    );

    state_table.delete(OwnedRow::new(vec![
        Some(2_i32.into()),
        Some(22_i32.into()),
        Some(222_i32.into()),
    ]));

    let row2_delete = state_table
        .get_row(&OwnedRow::new(vec![Some(2_i32.into())]))
        .await
        .unwrap();
    assert_eq!(row2_delete, None);

    epoch.inc();
    state_table.commit_for_test(epoch).await.unwrap();

    let row2_delete_commit = state_table
        .get_row(&OwnedRow::new(vec![Some(2_i32.into())]))
        .await
        .unwrap();
    assert_eq!(row2_delete_commit, None);

    state_table.delete(OwnedRow::new(vec![
        Some(3_i32.into()),
        Some(33_i32.into()),
        Some(333_i32.into()),
    ]));

    state_table.insert(OwnedRow::new(vec![Some(4_i32.into()), None, None]));
    let row4 = state_table
        .get_row(&OwnedRow::new(vec![Some(4_i32.into())]))
        .await
        .unwrap();
    assert_eq!(
        row4,
        Some(OwnedRow::new(vec![Some(4_i32.into()), None, None]))
    );

    let non_exist_row = state_table
        .get_row(&OwnedRow::new(vec![Some(0_i32.into())]))
        .await
        .unwrap();
    assert_eq!(non_exist_row, None);

    state_table.delete(OwnedRow::new(vec![Some(4_i32.into()), None, None]));

    epoch.inc();
    state_table.commit_for_test(epoch).await.unwrap();

    let row3_delete = state_table
        .get_row(&OwnedRow::new(vec![Some(3_i32.into())]))
        .await
        .unwrap();
    assert_eq!(row3_delete, None);

    let row4_delete = state_table
        .get_row(&OwnedRow::new(vec![Some(4_i32.into())]))
        .await
        .unwrap();
    assert_eq!(row4_delete, None);
}

#[tokio::test]
async fn test_state_table_update_insert() {
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
    )
    .await;

    let epoch = EpochPair::new_test_epoch(1);
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
    state_table.commit_for_test(epoch).await.unwrap();

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
    state_table.commit_for_test(epoch).await.unwrap();

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
    state_table.commit_for_test(epoch).await.unwrap();

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
    state_table.commit_for_test(epoch).await.unwrap();

    let row1_commit = state_table
        .get_row(&OwnedRow::new(vec![Some(1_i32.into())]))
        .await
        .unwrap();
    assert_eq!(row1_commit, None);
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
    )
    .await;

    let epoch = EpochPair::new_test_epoch(1);
    state.init_epoch(epoch);

    state.insert(OwnedRow::new(vec![
        Some(1_i32.into()),
        Some(11_i32.into()),
        Some(111_i32.into()),
    ]));
    state.insert(OwnedRow::new(vec![
        Some(2_i32.into()),
        Some(22_i32.into()),
        Some(222_i32.into()),
    ]));
    state.delete(OwnedRow::new(vec![
        Some(2_i32.into()),
        Some(22_i32.into()),
        Some(222_i32.into()),
    ]));

    state.insert(OwnedRow::new(vec![
        Some(3_i32.into()),
        Some(33_i32.into()),
        Some(3333_i32.into()),
    ]));

    state.insert(OwnedRow::new(vec![
        Some(6_i32.into()),
        Some(66_i32.into()),
        Some(666_i32.into()),
    ]));

    state.insert(OwnedRow::new(vec![
        Some(9_i32.into()),
        Some(99_i32.into()),
        Some(999_i32.into()),
    ]));

    {
        let iter = state.iter().await.unwrap();
        pin_mut!(iter);

        let res = iter.next().await.unwrap().unwrap();
        assert_eq!(
            &OwnedRow::new(vec![
                Some(1_i32.into()),
                Some(11_i32.into()),
                Some(111_i32.into())
            ]),
            res.as_ref()
        );

        // will not get [2, 22, 222]
        let res = iter.next().await.unwrap().unwrap();
        assert_eq!(
            &OwnedRow::new(vec![
                Some(3_i32.into()),
                Some(33_i32.into()),
                Some(3333_i32.into())
            ]),
            res.as_ref()
        );

        let res = iter.next().await.unwrap().unwrap();
        assert_eq!(
            &OwnedRow::new(vec![
                Some(6_i32.into()),
                Some(66_i32.into()),
                Some(666_i32.into())
            ]),
            res.as_ref()
        );
    }
    epoch.inc();
    state.commit_for_test(epoch).await.unwrap();

    // write [3, 33, 333], [4, 44, 444], [5, 55, 555], [7, 77, 777], [8, 88, 888]into mem_table,
    // [1, 11, 111], [3, 33, 3333], [6, 66, 666], [9, 99, 999] exists in
    // shared_storage

    state.delete(OwnedRow::new(vec![
        Some(1_i32.into()),
        Some(11_i32.into()),
        Some(111_i32.into()),
    ]));
    state.insert(OwnedRow::new(vec![
        Some(3_i32.into()),
        Some(33_i32.into()),
        Some(333_i32.into()),
    ]));

    state.insert(OwnedRow::new(vec![
        Some(4_i32.into()),
        Some(44_i32.into()),
        Some(444_i32.into()),
    ]));

    state.insert(OwnedRow::new(vec![
        Some(5_i32.into()),
        Some(55_i32.into()),
        Some(555_i32.into()),
    ]));
    state.insert(OwnedRow::new(vec![
        Some(7_i32.into()),
        Some(77_i32.into()),
        Some(777_i32.into()),
    ]));

    state.insert(OwnedRow::new(vec![
        Some(8_i32.into()),
        Some(88_i32.into()),
        Some(888_i32.into()),
    ]));
    let iter = state.iter().await.unwrap();
    pin_mut!(iter);

    let res = iter.next().await.unwrap().unwrap();
    // this pk exist in both shared_storage and mem_table
    assert_eq!(
        &OwnedRow::new(vec![
            Some(3_i32.into()),
            Some(33_i32.into()),
            Some(333_i32.into())
        ]),
        res.as_ref()
    );
    // this row exists in mem_table
    let res = iter.next().await.unwrap().unwrap();
    assert_eq!(
        &OwnedRow::new(vec![
            Some(4_i32.into()),
            Some(44_i32.into()),
            Some(444_i32.into())
        ]),
        res.as_ref()
    );
    let res = iter.next().await.unwrap().unwrap();

    // this row exists in mem_table
    assert_eq!(
        &OwnedRow::new(vec![
            Some(5_i32.into()),
            Some(55_i32.into()),
            Some(555_i32.into())
        ]),
        res.as_ref()
    );
    let res = iter.next().await.unwrap().unwrap();

    // this row exists in shared_storage
    assert_eq!(
        &OwnedRow::new(vec![
            Some(6_i32.into()),
            Some(66_i32.into()),
            Some(666_i32.into())
        ]),
        res.as_ref()
    );

    let res = iter.next().await.unwrap().unwrap();
    // this row exists in mem_table
    assert_eq!(
        &OwnedRow::new(vec![
            Some(7_i32.into()),
            Some(77_i32.into()),
            Some(777.into())
        ]),
        res.as_ref()
    );

    let res = iter.next().await.unwrap().unwrap();

    // this row exists in mem_table
    assert_eq!(
        &OwnedRow::new(vec![
            Some(8_i32.into()),
            Some(88_i32.into()),
            Some(888_i32.into())
        ]),
        res.as_ref()
    );

    let res = iter.next().await.unwrap().unwrap();

    // this row exists in shared_storage
    assert_eq!(
        &OwnedRow::new(vec![
            Some(9_i32.into()),
            Some(99_i32.into()),
            Some(999_i32.into())
        ]),
        res.as_ref()
    );

    // there is no row in both shared_storage and mem_table
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
    )
    .await;

    let epoch = EpochPair::new_test_epoch(1);
    state.init_epoch(epoch);

    state.insert(OwnedRow::new(vec![
        Some(1_i32.into()),
        Some(11_i32.into()),
        Some(111_i32.into()),
    ]));
    state.insert(OwnedRow::new(vec![
        Some(1_i32.into()),
        Some(22_i32.into()),
        Some(222_i32.into()),
    ]));

    state.insert(OwnedRow::new(vec![
        Some(4_i32.into()),
        Some(44_i32.into()),
        Some(444_i32.into()),
    ]));

    state.insert(OwnedRow::new(vec![
        Some(1_i32.into()),
        Some(55_i32.into()),
        Some(555_i32.into()),
    ]));

    epoch.inc();
    state.commit_for_test(epoch).await.unwrap();

    state.insert(OwnedRow::new(vec![
        Some(1_i32.into()),
        Some(33_i32.into()),
        Some(333_i32.into()),
    ]));
    state.insert(OwnedRow::new(vec![
        Some(1_i32.into()),
        Some(55_i32.into()),
        Some(5555_i32.into()),
    ]));
    state.insert(OwnedRow::new(vec![
        Some(6_i32.into()),
        Some(66_i32.into()),
        Some(666_i32.into()),
    ]));

    let pk_prefix = OwnedRow::new(vec![Some(1_i32.into())]);
    let iter = state.iter_with_pk_prefix(&pk_prefix).await.unwrap();
    pin_mut!(iter);

    // this row exists in both mem_table and shared_storage
    let res = iter.next().await.unwrap().unwrap();
    assert_eq!(
        &OwnedRow::new(vec![
            Some(1_i32.into()),
            Some(55_i32.into()),
            Some(5555_i32.into())
        ]),
        res.as_ref()
    );

    // this row exists in mem_table
    let res = iter.next().await.unwrap().unwrap();
    assert_eq!(
        &OwnedRow::new(vec![
            Some(1_i32.into()),
            Some(33_i32.into()),
            Some(333_i32.into())
        ]),
        res.as_ref()
    );

    // this row exists in shared_storage
    let res = iter.next().await.unwrap().unwrap();
    assert_eq!(
        &OwnedRow::new(vec![
            Some(1_i32.into()),
            Some(22_i32.into()),
            Some(222_i32.into())
        ]),
        res.as_ref()
    );
    // this row exists in shared_storage
    let res = iter.next().await.unwrap().unwrap();
    assert_eq!(
        &OwnedRow::new(vec![
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
async fn test_state_table_iter_with_pk_range() {
    let state_store = MemoryStateStore::new();
    // let pk_columns = vec![0, 1]; leave a message to indicate pk columns
    let order_types = vec![OrderType::Ascending, OrderType::Descending];

    let column_ids = vec![ColumnId::from(0), ColumnId::from(1), ColumnId::from(2)];
    let column_descs = vec![
        ColumnDesc::unnamed(column_ids[0], DataType::Int32), // This is the range prefix key
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
    )
    .await;

    let epoch = EpochPair::new_test_epoch(1);
    state.init_epoch(epoch);

    state.insert(OwnedRow::new(vec![
        Some(1_i32.into()),
        Some(11_i32.into()),
        Some(111_i32.into()),
    ]));
    state.insert(OwnedRow::new(vec![
        Some(1_i32.into()),
        Some(22_i32.into()),
        Some(222_i32.into()),
    ]));

    state.insert(OwnedRow::new(vec![
        Some(4_i32.into()),
        Some(44_i32.into()),
        Some(444_i32.into()),
    ]));

    state.insert(OwnedRow::new(vec![
        Some(1_i32.into()),
        Some(55_i32.into()),
        Some(555_i32.into()),
    ]));

    epoch.inc();
    state.commit_for_test(epoch).await.unwrap();

    state.insert(OwnedRow::new(vec![
        Some(1_i32.into()),
        Some(33_i32.into()),
        Some(333_i32.into()),
    ]));
    state.insert(OwnedRow::new(vec![
        Some(1_i32.into()),
        Some(55_i32.into()),
        Some(5555_i32.into()),
    ]));
    state.insert(OwnedRow::new(vec![
        Some(6_i32.into()),
        Some(66_i32.into()),
        Some(666_i32.into()),
    ]));

    let pk_range = (
        std::ops::Bound::Excluded(OwnedRow::new(vec![Some(1_i32.into())])),
        std::ops::Bound::Included(OwnedRow::new(vec![Some(4_i32.into())])),
    );
    let iter = state
        .iter_with_pk_range(&pk_range, DEFAULT_VNODE)
        .await
        .unwrap();
    pin_mut!(iter);

    // this row exists in both mem_table and cell_based_table
    let res = iter.next().await.unwrap().unwrap();
    assert_eq!(
        &OwnedRow::new(vec![
            Some(4_i32.into()),
            Some(44_i32.into()),
            Some(444_i32.into()),
        ]),
        res.as_ref()
    );

    // pk without the prefix the range will not be scan
    let res = iter.next().await;
    assert!(res.is_none());

    let pk_range = (
        std::ops::Bound::Included(OwnedRow::new(vec![Some(2_i32.into())])),
        std::ops::Bound::<row::Empty>::Unbounded,
    );
    let iter = state
        .iter_with_pk_range(&pk_range, DEFAULT_VNODE)
        .await
        .unwrap();
    pin_mut!(iter);

    // this row exists in both mem_table and cell_based_table
    let res = iter.next().await.unwrap().unwrap();
    assert_eq!(
        &OwnedRow::new(vec![
            Some(4_i32.into()),
            Some(44_i32.into()),
            Some(444_i32.into()),
        ]),
        res.as_ref()
    );

    // this row exists in mem_table
    let res = iter.next().await.unwrap().unwrap();
    assert_eq!(
        &OwnedRow::new(vec![
            Some(6_i32.into()),
            Some(66_i32.into()),
            Some(666_i32.into()),
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
    )
    .await;
    let epoch = EpochPair::new_test_epoch(1);
    state_table.init_epoch(epoch);
    state_table.insert(OwnedRow::new(vec![
        Some(1_i32.into()),
        Some(11_i32.into()),
        Some(111_i32.into()),
    ]));
    // duplicate key: 1
    state_table.insert(OwnedRow::new(vec![
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
    )
    .await;
    let epoch = EpochPair::new_test_epoch(1);
    state.init_epoch(epoch);

    state.insert(OwnedRow::new(vec![
        Some(1_i32.into()),
        Some(11_i32.into()),
        Some(111_i32.into()),
    ]));
    state.insert(OwnedRow::new(vec![
        Some(2_i32.into()),
        Some(22_i32.into()),
        Some(222_i32.into()),
    ]));
    state.delete(OwnedRow::new(vec![
        Some(2_i32.into()),
        Some(22_i32.into()),
        Some(222_i32.into()),
    ]));

    state.insert(OwnedRow::new(vec![
        Some(3_i32.into()),
        Some(33_i32.into()),
        Some(3333_i32.into()),
    ]));

    state.insert(OwnedRow::new(vec![
        Some(6_i32.into()),
        Some(66_i32.into()),
        Some(666_i32.into()),
    ]));

    state.insert(OwnedRow::new(vec![
        Some(9_i32.into()),
        Some(99_i32.into()),
        Some(999_i32.into()),
    ]));

    {
        let iter = state.iter().await.unwrap();
        pin_mut!(iter);

        let res = iter.next().await.unwrap().unwrap();
        assert_eq!(&OwnedRow::new(vec![Some(111_i32.into())]), res.as_ref());

        // will not get [2, 22, 222]
        let res = iter.next().await.unwrap().unwrap();
        assert_eq!(&OwnedRow::new(vec![Some(3333_i32.into())]), res.as_ref());

        let res = iter.next().await.unwrap().unwrap();
        assert_eq!(&OwnedRow::new(vec![Some(666_i32.into())]), res.as_ref());
    }

    epoch.inc();
    state.commit_for_test(epoch).await.unwrap();

    // write [3, 33, 333], [4, 44, 444], [5, 55, 555], [7, 77, 777], [8, 88, 888]into mem_table,
    // [3, 33, 3333], [6, 66, 666], [9, 99, 999] exists in
    // shared_storage

    state.delete(OwnedRow::new(vec![
        Some(1_i32.into()),
        Some(11_i32.into()),
        Some(111_i32.into()),
    ]));
    state.insert(OwnedRow::new(vec![
        Some(3_i32.into()),
        Some(33_i32.into()),
        Some(333_i32.into()),
    ]));

    state.insert(OwnedRow::new(vec![
        Some(4_i32.into()),
        Some(44_i32.into()),
        Some(444_i32.into()),
    ]));

    state.insert(OwnedRow::new(vec![
        Some(5_i32.into()),
        Some(55_i32.into()),
        Some(555_i32.into()),
    ]));
    state.insert(OwnedRow::new(vec![
        Some(7_i32.into()),
        Some(77_i32.into()),
        Some(777_i32.into()),
    ]));

    state.insert(OwnedRow::new(vec![
        Some(8_i32.into()),
        Some(88_i32.into()),
        Some(888_i32.into()),
    ]));

    let iter = state.iter().await.unwrap();
    pin_mut!(iter);

    let res = iter.next().await.unwrap().unwrap();

    // this pk exist in both shared_storage and mem_table
    assert_eq!(&OwnedRow::new(vec![Some(333_i32.into())]), res.as_ref());

    // this row exists in mem_table
    let res = iter.next().await.unwrap().unwrap();
    assert_eq!(&OwnedRow::new(vec![Some(444_i32.into())]), res.as_ref());

    let res = iter.next().await.unwrap().unwrap();

    // this row exists in mem_table
    assert_eq!(&OwnedRow::new(vec![Some(555_i32.into())]), res.as_ref());
    let res = iter.next().await.unwrap().unwrap();

    // this row exists in shared_storage
    assert_eq!(&OwnedRow::new(vec![Some(666_i32.into())]), res.as_ref());

    let res = iter.next().await.unwrap().unwrap();
    // this row exists in mem_table
    assert_eq!(&OwnedRow::new(vec![Some(777.into())]), res.as_ref());

    let res = iter.next().await.unwrap().unwrap();

    // this row exists in mem_table
    assert_eq!(&OwnedRow::new(vec![Some(888_i32.into())]), res.as_ref());

    let res = iter.next().await.unwrap().unwrap();

    // this row exists in shared_storage
    assert_eq!(&OwnedRow::new(vec![Some(999_i32.into())]), res.as_ref());

    // there is no row in both shared_storage and mem_table
    let res = iter.next().await;
    assert!(res.is_none());
}

#[tokio::test]
async fn test_state_table_iter_with_shuffle_value_indices() {
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
        vec![2, 1, 0],
    )
    .await;
    let epoch = EpochPair::new_test_epoch(1);
    state.init_epoch(epoch);

    state.insert(OwnedRow::new(vec![
        Some(1_i32.into()),
        Some(11_i32.into()),
        Some(111_i32.into()),
    ]));
    state.insert(OwnedRow::new(vec![
        Some(2_i32.into()),
        Some(22_i32.into()),
        Some(222_i32.into()),
    ]));
    state.delete(OwnedRow::new(vec![
        Some(2_i32.into()),
        Some(22_i32.into()),
        Some(222_i32.into()),
    ]));

    state.insert(OwnedRow::new(vec![
        Some(3_i32.into()),
        Some(33_i32.into()),
        Some(3333_i32.into()),
    ]));

    state.insert(OwnedRow::new(vec![
        Some(6_i32.into()),
        Some(66_i32.into()),
        Some(666_i32.into()),
    ]));

    state.insert(OwnedRow::new(vec![
        Some(9_i32.into()),
        Some(99_i32.into()),
        Some(999_i32.into()),
    ]));

    {
        let iter = state.iter().await.unwrap();
        pin_mut!(iter);

        let res = iter.next().await.unwrap().unwrap();
        assert_eq!(
            &OwnedRow::new(vec![
                Some(111_i32.into()),
                Some(11_i32.into()),
                Some(1_i32.into())
            ]),
            res.as_ref()
        );

        // will not get [2, 22, 222]
        let res = iter.next().await.unwrap().unwrap();
        assert_eq!(
            &OwnedRow::new(vec![
                Some(3333_i32.into()),
                Some(33_i32.into()),
                Some(3_i32.into())
            ]),
            res.as_ref()
        );

        let res = iter.next().await.unwrap().unwrap();
        assert_eq!(
            &OwnedRow::new(vec![
                Some(666_i32.into()),
                Some(66_i32.into()),
                Some(6_i32.into())
            ]),
            res.as_ref()
        );
    }

    epoch.inc();
    state.commit_for_test(epoch).await.unwrap();

    // write [3, 33, 333], [4, 44, 444], [5, 55, 555], [7, 77, 777], [8, 88, 888]into mem_table,
    // [3, 33, 3333], [6, 66, 666], [9, 99, 999] exists in
    // shared_storage

    state.delete(OwnedRow::new(vec![
        Some(1_i32.into()),
        Some(11_i32.into()),
        Some(111_i32.into()),
    ]));
    state.insert(OwnedRow::new(vec![
        Some(3_i32.into()),
        Some(33_i32.into()),
        Some(333_i32.into()),
    ]));

    state.insert(OwnedRow::new(vec![
        Some(4_i32.into()),
        Some(44_i32.into()),
        Some(444_i32.into()),
    ]));

    state.insert(OwnedRow::new(vec![
        Some(5_i32.into()),
        Some(55_i32.into()),
        Some(555_i32.into()),
    ]));
    state.insert(OwnedRow::new(vec![
        Some(7_i32.into()),
        Some(77_i32.into()),
        Some(777_i32.into()),
    ]));

    state.insert(OwnedRow::new(vec![
        Some(8_i32.into()),
        Some(88_i32.into()),
        Some(888_i32.into()),
    ]));

    let iter = state.iter().await.unwrap();
    pin_mut!(iter);

    let res = iter.next().await.unwrap().unwrap();

    assert_eq!(
        &OwnedRow::new(vec![
            Some(333_i32.into()),
            Some(33_i32.into()),
            Some(3_i32.into())
        ]),
        res.as_ref()
    );

    // this row exists in mem_table
    let res = iter.next().await.unwrap().unwrap();
    assert_eq!(
        &OwnedRow::new(vec![
            Some(444_i32.into()),
            Some(44_i32.into()),
            Some(4_i32.into())
        ]),
        res.as_ref()
    );

    let res = iter.next().await.unwrap().unwrap();

    // this row exists in mem_table
    assert_eq!(
        &OwnedRow::new(vec![
            Some(555_i32.into()),
            Some(55_i32.into()),
            Some(5_i32.into())
        ]),
        res.as_ref()
    );
    let res = iter.next().await.unwrap().unwrap();

    // this row exists in shared_storage
    assert_eq!(
        &OwnedRow::new(vec![
            Some(666_i32.into()),
            Some(66_i32.into()),
            Some(6_i32.into())
        ]),
        res.as_ref()
    );

    let res = iter.next().await.unwrap().unwrap();
    // this row exists in mem_table
    assert_eq!(
        &OwnedRow::new(vec![
            Some(777.into()),
            Some(77_i32.into()),
            Some(7_i32.into())
        ]),
        res.as_ref()
    );

    let res = iter.next().await.unwrap().unwrap();

    // this row exists in mem_table
    assert_eq!(
        &OwnedRow::new(vec![
            Some(888_i32.into()),
            Some(88_i32.into()),
            Some(8_i32.into())
        ]),
        res.as_ref()
    );

    let res = iter.next().await.unwrap().unwrap();

    // this row exists in shared_storage
    assert_eq!(
        &OwnedRow::new(vec![
            Some(999_i32.into()),
            Some(99_i32.into()),
            Some(9_i32.into())
        ]),
        res.as_ref()
    );

    // there is no row in both shared_storage and mem_table
    let res = iter.next().await;
    assert!(res.is_none());
}

#[tokio::test]
async fn test_state_table_write_chunk() {
    let state_store = MemoryStateStore::new();
    let column_descs = vec![
        ColumnDesc::unnamed(ColumnId::from(0), DataType::Int32),
        ColumnDesc::unnamed(ColumnId::from(1), DataType::Int64),
        ColumnDesc::unnamed(ColumnId::from(2), DataType::Boolean),
        ColumnDesc::unnamed(ColumnId::from(3), DataType::Float32),
    ];
    let data_types = [
        DataType::Int32,
        DataType::Int64,
        DataType::Boolean,
        DataType::Float32,
    ];
    let order_types = vec![OrderType::Ascending];
    let pk_index = vec![0_usize];
    let mut state_table = StateTable::new_without_distribution(
        state_store.clone(),
        TableId::from(0x42),
        column_descs,
        order_types,
        pk_index,
    )
    .await;

    let epoch = EpochPair::new_test_epoch(1);
    state_table.init_epoch(epoch);

    let chunk = StreamChunk::from_rows(
        &[
            (
                Op::Insert,
                OwnedRow::new(vec![
                    Some(123i32.into()),
                    Some(456i64.into()),
                    Some(true.into()),
                    Some(3.15f32.into()),
                ]),
            ),
            (
                Op::Insert, // mark 1
                OwnedRow::new(vec![
                    Some(365i32.into()),
                    Some(4888i64.into()),
                    Some(false.into()),
                    Some(0.123f32.into()),
                ]),
            ),
            (
                Op::Insert, // mark 2
                OwnedRow::new(vec![
                    Some(8i32.into()),
                    Some(1000i64.into()),
                    Some(true.into()),
                    Some(123.987f32.into()),
                ]),
            ),
            (
                Op::UpdateDelete, // update the row with `mark 1`
                OwnedRow::new(vec![
                    Some(365i32.into()),
                    Some(4888i64.into()),
                    Some(false.into()),
                    Some(0.123f32.into()),
                ]),
            ),
            (
                Op::UpdateInsert,
                OwnedRow::new(vec![
                    Some(365i32.into()),
                    Some(4999i64.into()),
                    Some(false.into()),
                    Some(0.123456f32.into()),
                ]),
            ),
            (
                Op::Delete, // delete the row with `mark 2`
                OwnedRow::new(vec![
                    Some(8i32.into()),
                    Some(1000i64.into()),
                    Some(true.into()),
                    Some(123.987f32.into()),
                ]),
            ),
        ],
        &data_types,
    );

    state_table.write_chunk(chunk);

    let rows: Vec<_> = state_table
        .iter()
        .await
        .unwrap()
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|row| row.unwrap())
        .collect();

    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows[0].as_ref(),
        &OwnedRow::new(vec![
            Some(123i32.into()),
            Some(456i64.into()),
            Some(true.into()),
            Some(3.15f32.into()),
        ])
    );
    assert_eq!(
        rows[1].as_ref(),
        &OwnedRow::new(vec![
            Some(365i32.into()),
            Some(4999i64.into()),
            Some(false.into()),
            Some(0.123456f32.into()),
        ])
    );
}

#[tokio::test]
async fn test_state_table_write_chunk_visibility() {
    let state_store = MemoryStateStore::new();
    let column_descs = vec![
        ColumnDesc::unnamed(ColumnId::from(0), DataType::Int32),
        ColumnDesc::unnamed(ColumnId::from(1), DataType::Int64),
        ColumnDesc::unnamed(ColumnId::from(2), DataType::Boolean),
        ColumnDesc::unnamed(ColumnId::from(3), DataType::Float32),
    ];
    let data_types = [
        DataType::Int32,
        DataType::Int64,
        DataType::Boolean,
        DataType::Float32,
    ];
    let order_types = vec![OrderType::Ascending];
    let pk_index = vec![0_usize];
    let mut state_table = StateTable::new_without_distribution(
        state_store.clone(),
        TableId::from(0x42),
        column_descs,
        order_types,
        pk_index,
    )
    .await;

    let epoch = EpochPair::new_test_epoch(1);
    state_table.init_epoch(epoch);

    let chunk = StreamChunk::from_rows(
        &[
            (
                Op::Insert,
                OwnedRow::new(vec![
                    Some(123i32.into()),
                    Some(456i64.into()),
                    Some(true.into()),
                    Some(3.15f32.into()),
                ]),
            ),
            (
                Op::Insert, // mark 1
                OwnedRow::new(vec![
                    Some(365i32.into()),
                    Some(4888i64.into()),
                    Some(false.into()),
                    Some(0.123f32.into()),
                ]),
            ),
            (
                Op::Insert,
                OwnedRow::new(vec![
                    Some(8i32.into()),
                    Some(1000i64.into()),
                    Some(true.into()),
                    Some(123.987f32.into()),
                ]),
            ),
            (
                Op::Delete, // delete the row with `mark 1`, but hidden
                OwnedRow::new(vec![
                    Some(8i32.into()),
                    Some(1000i64.into()),
                    Some(true.into()),
                    Some(123.987f32.into()),
                ]),
            ),
        ],
        &data_types,
    );
    let (ops, columns, _) = chunk.into_inner();
    let chunk = StreamChunk::new(
        ops,
        columns,
        Some(Bitmap::from_iter([true, true, true, false])),
    );

    state_table.write_chunk(chunk);

    let rows: Vec<_> = state_table
        .iter()
        .await
        .unwrap()
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|row| row.unwrap())
        .collect();

    assert_eq!(rows.len(), 3);
    assert_eq!(
        rows[0].as_ref(),
        &OwnedRow::new(vec![
            Some(8i32.into()),
            Some(1000i64.into()),
            Some(true.into()),
            Some(123.987f32.into()),
        ])
    );
    assert_eq!(
        rows[1].as_ref(),
        &OwnedRow::new(vec![
            Some(123i32.into()),
            Some(456i64.into()),
            Some(true.into()),
            Some(3.15f32.into()),
        ])
    );
    assert_eq!(
        rows[2].as_ref(),
        &OwnedRow::new(vec![
            Some(365i32.into()),
            Some(4888i64.into()),
            Some(false.into()),
            Some(0.123f32.into()),
        ])
    );
}

#[tokio::test]
async fn test_state_table_write_chunk_value_indices() {
    let state_store = MemoryStateStore::new();
    let column_descs = vec![
        ColumnDesc::unnamed(ColumnId::from(0), DataType::Int32),
        ColumnDesc::unnamed(ColumnId::from(1), DataType::Int64),
        ColumnDesc::unnamed(ColumnId::from(2), DataType::Boolean),
        ColumnDesc::unnamed(ColumnId::from(3), DataType::Float32),
    ];
    let data_types = [
        DataType::Int32,
        DataType::Int64,
        DataType::Boolean,
        DataType::Float32,
    ];
    let order_types = vec![OrderType::Ascending];
    let pk_index = vec![0_usize];
    let mut state_table = StateTable::new_without_distribution_partial(
        state_store.clone(),
        TableId::from(0x42),
        column_descs,
        order_types,
        pk_index,
        vec![2, 1],
    )
    .await;

    let epoch = EpochPair::new_test_epoch(1);
    state_table.init_epoch(epoch);

    let chunk = StreamChunk::from_rows(
        &[
            (
                Op::Insert,
                OwnedRow::new(vec![
                    Some(123i32.into()),
                    Some(456i64.into()),
                    Some(true.into()),
                    Some(3.15f32.into()),
                ]),
            ),
            (
                Op::Insert,
                OwnedRow::new(vec![
                    Some(365i32.into()),
                    Some(4888i64.into()),
                    Some(false.into()),
                    Some(0.123f32.into()),
                ]),
            ),
            (
                Op::Insert,
                OwnedRow::new(vec![
                    Some(8i32.into()),
                    Some(1000i64.into()),
                    Some(true.into()),
                    Some(123.987f32.into()),
                ]),
            ),
        ],
        &data_types,
    );

    state_table.write_chunk(chunk);

    let rows: Vec<_> = state_table
        .iter()
        .await
        .unwrap()
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|row| row.unwrap())
        .collect();

    assert_eq!(rows.len(), 3);
    assert_eq!(
        rows[0].as_ref(),
        &OwnedRow::new(vec![Some(true.into()), Some(1000i64.into()),])
    );
    assert_eq!(
        rows[1].as_ref(),
        &OwnedRow::new(vec![Some(true.into()), Some(456i64.into()),])
    );
    assert_eq!(
        rows[2].as_ref(),
        &OwnedRow::new(vec![Some(false.into()), Some(4888i64.into()),])
    );
}

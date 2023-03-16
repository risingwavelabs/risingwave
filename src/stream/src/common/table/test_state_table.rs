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

use futures::{pin_mut, StreamExt};
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::{ColumnDesc, ColumnId, TableId};
use risingwave_common::row::{self, OwnedRow};
use risingwave_common::types::DataType;
use risingwave_common::util::epoch::EpochPair;
use risingwave_common::util::sort_util::OrderType;
use risingwave_hummock_test::test_utils::prepare_hummock_test_env;
use risingwave_rpc_client::HummockMetaClient;
use risingwave_storage::store::PrefetchOptions;
use risingwave_storage::table::DEFAULT_VNODE;
use risingwave_storage::StateStore;

use crate::common::table::state_table::StateTable;
use crate::common::table::test_utils::{gen_prost_table, gen_prost_table_with_value_indices};

#[tokio::test]
async fn test_state_table_update_insert() {
    const TEST_TABLE_ID: TableId = TableId { table_id: 233 };
    let test_env = prepare_hummock_test_env().await;

    let column_descs = vec![
        ColumnDesc::unnamed(ColumnId::from(0), DataType::Int32),
        ColumnDesc::unnamed(ColumnId::from(1), DataType::Int32),
        ColumnDesc::unnamed(ColumnId::from(2), DataType::Int32),
        ColumnDesc::unnamed(ColumnId::from(4), DataType::Int32),
    ];
    let order_types = vec![OrderType::default_ascending()];
    let pk_index = vec![0_usize];
    let read_prefix_len_hint = 1;
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

#[tokio::test]
async fn test_state_table_iter_with_prefix() {
    const TEST_TABLE_ID: TableId = TableId { table_id: 233 };
    let test_env = prepare_hummock_test_env().await;

    // let pk_columns = vec![0, 1]; leave a message to indicate pk columns
    let order_types = vec![
        OrderType::default_ascending(),
        OrderType::default_descending(),
    ];

    let column_ids = vec![ColumnId::from(0), ColumnId::from(1), ColumnId::from(2)];
    let column_descs = vec![
        ColumnDesc::unnamed(column_ids[0], DataType::Int32),
        ColumnDesc::unnamed(column_ids[1], DataType::Int32),
        ColumnDesc::unnamed(column_ids[2], DataType::Int32),
    ];
    let pk_index = vec![0_usize, 1_usize];
    let read_prefix_len_hint = 1;
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
        Some(1_i32.into()),
        Some(11_i32.into()),
        Some(111_i32.into()),
    ]));
    state_table.insert(OwnedRow::new(vec![
        Some(1_i32.into()),
        Some(22_i32.into()),
        Some(222_i32.into()),
    ]));

    state_table.insert(OwnedRow::new(vec![
        Some(4_i32.into()),
        Some(44_i32.into()),
        Some(444_i32.into()),
    ]));

    state_table.insert(OwnedRow::new(vec![
        Some(1_i32.into()),
        Some(55_i32.into()),
        Some(555_i32.into()),
    ]));

    epoch.inc();
    state_table.commit(epoch).await.unwrap();

    state_table.insert(OwnedRow::new(vec![
        Some(1_i32.into()),
        Some(33_i32.into()),
        Some(333_i32.into()),
    ]));
    state_table.insert(OwnedRow::new(vec![
        Some(1_i32.into()),
        Some(55_i32.into()),
        Some(5555_i32.into()),
    ]));
    state_table.insert(OwnedRow::new(vec![
        Some(6_i32.into()),
        Some(66_i32.into()),
        Some(666_i32.into()),
    ]));

    let pk_prefix = OwnedRow::new(vec![Some(1_i32.into())]);
    let iter = state_table
        .iter_with_pk_prefix(&pk_prefix, Default::default())
        .await
        .unwrap();
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
    const TEST_TABLE_ID: TableId = TableId { table_id: 233 };
    let test_env = prepare_hummock_test_env().await;

    // let pk_columns = vec![0, 1]; leave a message to indicate pk columns
    let order_types = vec![
        OrderType::default_ascending(),
        OrderType::default_descending(),
    ];

    let column_ids = vec![ColumnId::from(0), ColumnId::from(1), ColumnId::from(2)];
    let column_descs = vec![
        ColumnDesc::unnamed(column_ids[0], DataType::Int32), // This is the range prefix key
        ColumnDesc::unnamed(column_ids[1], DataType::Int32),
        ColumnDesc::unnamed(column_ids[2], DataType::Int32),
    ];
    let pk_index = vec![0_usize, 1_usize];
    let read_prefix_len_hint = 1;
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
        Some(1_i32.into()),
        Some(11_i32.into()),
        Some(111_i32.into()),
    ]));
    state_table.insert(OwnedRow::new(vec![
        Some(1_i32.into()),
        Some(22_i32.into()),
        Some(222_i32.into()),
    ]));

    state_table.insert(OwnedRow::new(vec![
        Some(4_i32.into()),
        Some(44_i32.into()),
        Some(444_i32.into()),
    ]));

    state_table.insert(OwnedRow::new(vec![
        Some(1_i32.into()),
        Some(55_i32.into()),
        Some(555_i32.into()),
    ]));

    epoch.inc();
    state_table.commit(epoch).await.unwrap();

    state_table.insert(OwnedRow::new(vec![
        Some(1_i32.into()),
        Some(33_i32.into()),
        Some(333_i32.into()),
    ]));
    state_table.insert(OwnedRow::new(vec![
        Some(1_i32.into()),
        Some(55_i32.into()),
        Some(5555_i32.into()),
    ]));
    state_table.insert(OwnedRow::new(vec![
        Some(6_i32.into()),
        Some(66_i32.into()),
        Some(666_i32.into()),
    ]));

    let pk_range = (
        std::ops::Bound::Excluded(OwnedRow::new(vec![Some(1_i32.into())])),
        std::ops::Bound::Included(OwnedRow::new(vec![Some(4_i32.into())])),
    );
    let iter = state_table
        .iter_with_pk_range(&pk_range, DEFAULT_VNODE, Default::default())
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
    let iter = state_table
        .iter_with_pk_range(&pk_range, DEFAULT_VNODE, Default::default())
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
    const TEST_TABLE_ID: TableId = TableId { table_id: 233 };
    let test_env = prepare_hummock_test_env().await;

    let column_descs = vec![
        ColumnDesc::unnamed(ColumnId::from(0), DataType::Int32),
        ColumnDesc::unnamed(ColumnId::from(1), DataType::Int32),
        ColumnDesc::unnamed(ColumnId::from(2), DataType::Int32),
    ];
    let order_types = vec![OrderType::default_ascending()];
    let pk_index = vec![0_usize];
    let read_prefix_len_hint = 1;
    let table = gen_prost_table(
        TEST_TABLE_ID,
        column_descs,
        order_types,
        pk_index,
        read_prefix_len_hint,
    );

    test_env.register_table(table.clone()).await;
    let mut state_table =
        StateTable::from_table_catalog(&table, test_env.storage.clone(), None).await;

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
    const TEST_TABLE_ID: TableId = TableId { table_id: 233 };
    let test_env = prepare_hummock_test_env().await;

    let order_types = vec![
        OrderType::default_ascending(),
        OrderType::default_descending(),
    ];
    let column_ids = vec![ColumnId::from(0), ColumnId::from(1), ColumnId::from(2)];
    let column_descs = vec![
        ColumnDesc::unnamed(column_ids[0], DataType::Int32),
        ColumnDesc::unnamed(column_ids[1], DataType::Int32),
        ColumnDesc::unnamed(column_ids[2], DataType::Int32),
    ];
    let pk_index = vec![0_usize, 1_usize];
    let read_prefix_len_hint = 0;
    let table = gen_prost_table_with_value_indices(
        TEST_TABLE_ID,
        column_descs,
        order_types,
        pk_index,
        read_prefix_len_hint,
        vec![2],
    );

    test_env.register_table(table.clone()).await;
    let mut state_table =
        StateTable::from_table_catalog_inconsistent_op(&table, test_env.storage.clone(), None)
            .await;

    let mut epoch = EpochPair::new_test_epoch(1);
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
    state_table.delete(OwnedRow::new(vec![
        Some(2_i32.into()),
        Some(22_i32.into()),
        Some(222_i32.into()),
    ]));

    state_table.insert(OwnedRow::new(vec![
        Some(3_i32.into()),
        Some(33_i32.into()),
        Some(3333_i32.into()),
    ]));

    state_table.insert(OwnedRow::new(vec![
        Some(6_i32.into()),
        Some(66_i32.into()),
        Some(666_i32.into()),
    ]));

    state_table.insert(OwnedRow::new(vec![
        Some(9_i32.into()),
        Some(99_i32.into()),
        Some(999_i32.into()),
    ]));

    {
        let iter = state_table.iter(Default::default()).await.unwrap();
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
    state_table.commit(epoch).await.unwrap();

    // write [3, 33, 333], [4, 44, 444], [5, 55, 555], [7, 77, 777], [8, 88, 888]into mem_table,
    // [3, 33, 3333], [6, 66, 666], [9, 99, 999] exists in
    // shared_storage

    state_table.delete(OwnedRow::new(vec![
        Some(1_i32.into()),
        Some(11_i32.into()),
        Some(111_i32.into()),
    ]));
    state_table.insert(OwnedRow::new(vec![
        Some(3_i32.into()),
        Some(33_i32.into()),
        Some(333_i32.into()),
    ]));

    state_table.insert(OwnedRow::new(vec![
        Some(4_i32.into()),
        Some(44_i32.into()),
        Some(444_i32.into()),
    ]));

    state_table.insert(OwnedRow::new(vec![
        Some(5_i32.into()),
        Some(55_i32.into()),
        Some(555_i32.into()),
    ]));
    state_table.insert(OwnedRow::new(vec![
        Some(7_i32.into()),
        Some(77_i32.into()),
        Some(777_i32.into()),
    ]));

    state_table.insert(OwnedRow::new(vec![
        Some(8_i32.into()),
        Some(88_i32.into()),
        Some(888_i32.into()),
    ]));

    let iter = state_table.iter(Default::default()).await.unwrap();
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
    const TEST_TABLE_ID: TableId = TableId { table_id: 233 };
    let test_env = prepare_hummock_test_env().await;

    let order_types = vec![
        OrderType::default_ascending(),
        OrderType::default_descending(),
    ];
    let column_ids = vec![ColumnId::from(0), ColumnId::from(1), ColumnId::from(2)];
    let column_descs = vec![
        ColumnDesc::unnamed(column_ids[0], DataType::Int32),
        ColumnDesc::unnamed(column_ids[1], DataType::Int32),
        ColumnDesc::unnamed(column_ids[2], DataType::Int32),
    ];
    let pk_index = vec![0_usize, 1_usize];
    let read_prefix_len_hint = 0;
    let table = gen_prost_table_with_value_indices(
        TEST_TABLE_ID,
        column_descs,
        order_types,
        pk_index,
        read_prefix_len_hint,
        vec![2, 1, 0],
    );

    test_env.register_table(table.clone()).await;
    let mut state_table =
        StateTable::from_table_catalog_inconsistent_op(&table, test_env.storage.clone(), None)
            .await;

    let mut epoch = EpochPair::new_test_epoch(1);
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
    state_table.delete(OwnedRow::new(vec![
        Some(2_i32.into()),
        Some(22_i32.into()),
        Some(222_i32.into()),
    ]));

    state_table.insert(OwnedRow::new(vec![
        Some(3_i32.into()),
        Some(33_i32.into()),
        Some(3333_i32.into()),
    ]));

    state_table.insert(OwnedRow::new(vec![
        Some(6_i32.into()),
        Some(66_i32.into()),
        Some(666_i32.into()),
    ]));

    state_table.insert(OwnedRow::new(vec![
        Some(9_i32.into()),
        Some(99_i32.into()),
        Some(999_i32.into()),
    ]));

    {
        let iter = state_table.iter(Default::default()).await.unwrap();
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
    state_table.commit(epoch).await.unwrap();

    // write [3, 33, 333], [4, 44, 444], [5, 55, 555], [7, 77, 777], [8, 88, 888]into mem_table,
    // [3, 33, 3333], [6, 66, 666], [9, 99, 999] exists in
    // shared_storage

    state_table.delete(OwnedRow::new(vec![
        Some(1_i32.into()),
        Some(11_i32.into()),
        Some(111_i32.into()),
    ]));
    state_table.insert(OwnedRow::new(vec![
        Some(3_i32.into()),
        Some(33_i32.into()),
        Some(333_i32.into()),
    ]));

    state_table.insert(OwnedRow::new(vec![
        Some(4_i32.into()),
        Some(44_i32.into()),
        Some(444_i32.into()),
    ]));

    state_table.insert(OwnedRow::new(vec![
        Some(5_i32.into()),
        Some(55_i32.into()),
        Some(555_i32.into()),
    ]));
    state_table.insert(OwnedRow::new(vec![
        Some(7_i32.into()),
        Some(77_i32.into()),
        Some(777_i32.into()),
    ]));

    state_table.insert(OwnedRow::new(vec![
        Some(8_i32.into()),
        Some(88_i32.into()),
        Some(888_i32.into()),
    ]));

    let iter = state_table.iter(Default::default()).await.unwrap();
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
    const TEST_TABLE_ID: TableId = TableId { table_id: 233 };
    let test_env = prepare_hummock_test_env().await;

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
    let order_types = vec![OrderType::default_ascending()];
    let pk_index = vec![0_usize];
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
        .iter(PrefetchOptions::new_for_exhaust_iter())
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
    const TEST_TABLE_ID: TableId = TableId { table_id: 233 };
    let test_env = prepare_hummock_test_env().await;

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
    let order_types = vec![OrderType::default_ascending()];
    let pk_index = vec![0_usize];
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
        .iter(PrefetchOptions::new_for_exhaust_iter())
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
    const TEST_TABLE_ID: TableId = TableId { table_id: 233 };
    let test_env = prepare_hummock_test_env().await;

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
    let order_types = vec![OrderType::default_ascending()];
    let pk_index = vec![0_usize];
    let read_prefix_len_hint = 0;
    let table = gen_prost_table_with_value_indices(
        TEST_TABLE_ID,
        column_descs,
        order_types,
        pk_index,
        read_prefix_len_hint,
        vec![2, 1],
    );

    test_env.register_table(table.clone()).await;
    let mut state_table =
        StateTable::from_table_catalog_inconsistent_op(&table, test_env.storage.clone(), None)
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
        .iter(PrefetchOptions::new_for_exhaust_iter())
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

async fn check_may_exist<S>(
    state_table: &StateTable<S>,
    existent_prefix: Vec<i32>,
    non_existent_prefix: Vec<i32>,
) where
    S: StateStore,
{
    for prefix in existent_prefix {
        let pk_prefix = OwnedRow::new(vec![Some(prefix.into())]);
        assert!(state_table.may_exist(&pk_prefix).await.unwrap());
    }
    for prefix in non_existent_prefix {
        let pk_prefix = OwnedRow::new(vec![Some(prefix.into())]);
        assert!(!state_table.may_exist(&pk_prefix).await.unwrap());
    }
}

#[tokio::test]
async fn test_state_table_may_exist() {
    const TEST_TABLE_ID: TableId = TableId { table_id: 233 };
    let test_env = prepare_hummock_test_env().await;

    // let pk_columns = vec![0, 1]; leave a message to indicate pk columns
    let order_types = vec![
        OrderType::default_ascending(),
        OrderType::default_descending(),
    ];

    let column_ids = vec![ColumnId::from(0), ColumnId::from(1), ColumnId::from(2)];
    let column_descs = vec![
        ColumnDesc::unnamed(column_ids[0], DataType::Int32),
        ColumnDesc::unnamed(column_ids[1], DataType::Int32),
        ColumnDesc::unnamed(column_ids[2], DataType::Int32),
    ];
    let pk_index = vec![0_usize, 1_usize];
    let read_prefix_len_hint = 1;
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
        Some(1_i32.into()),
        Some(11_i32.into()),
        Some(111_i32.into()),
    ]));
    state_table.insert(OwnedRow::new(vec![
        Some(1_i32.into()),
        Some(22_i32.into()),
        Some(222_i32.into()),
    ]));

    state_table.insert(OwnedRow::new(vec![
        Some(4_i32.into()),
        Some(44_i32.into()),
        Some(444_i32.into()),
    ]));

    state_table.insert(OwnedRow::new(vec![
        Some(1_i32.into()),
        Some(55_i32.into()),
        Some(555_i32.into()),
    ]));

    // test may_exist with data only in memtable (e1)
    check_may_exist(&state_table, vec![1, 4], vec![2, 3, 6, 12]).await;

    epoch.inc();
    state_table.commit(epoch).await.unwrap();
    let e1 = epoch.prev;

    // test may_exist with data only in immutable memtable (e1)
    check_may_exist(&state_table, vec![1, 4], vec![2, 3, 6, 12]).await;

    let e1_res = test_env.storage.seal_and_sync_epoch(e1).await.unwrap();

    // test may_exist with data only in uncommitted ssts (e1)
    check_may_exist(&state_table, vec![1, 4], vec![2, 3, 6, 12]).await;

    test_env
        .meta_client
        .commit_epoch(e1, e1_res.uncommitted_ssts)
        .await
        .unwrap();
    test_env.storage.try_wait_epoch_for_test(e1).await;

    // test may_exist with data only in committed ssts (e1)
    check_may_exist(&state_table, vec![1, 4], vec![2, 3, 6, 12]).await;

    state_table.insert(OwnedRow::new(vec![
        Some(1_i32.into()),
        Some(33_i32.into()),
        Some(333_i32.into()),
    ]));
    state_table.insert(OwnedRow::new(vec![
        Some(1_i32.into()),
        Some(55_i32.into()),
        Some(5555_i32.into()),
    ]));
    state_table.insert(OwnedRow::new(vec![
        Some(6_i32.into()),
        Some(66_i32.into()),
        Some(666_i32.into()),
    ]));

    // test may_exist with data in memtable (e2), committed ssts (e1)
    check_may_exist(&state_table, vec![1, 4, 6], vec![2, 3, 12]).await;

    epoch.inc();
    state_table.commit(epoch).await.unwrap();
    let e2 = epoch.prev;

    // test may_exist with data in immutable memtable (e2), committed ssts (e1)
    check_may_exist(&state_table, vec![1, 4, 6], vec![2, 3, 12]).await;

    state_table.insert(OwnedRow::new(vec![
        Some(1_i32.into()),
        Some(44_i32.into()),
        Some(444_i32.into()),
    ]));
    state_table.insert(OwnedRow::new(vec![
        Some(3_i32.into()),
        Some(1_i32.into()),
        Some(111_i32.into()),
    ]));

    // test may_exist with data in memtable (e3), immutable memtable (e2), committed ssts (e1)
    check_may_exist(&state_table, vec![1, 3, 4, 6], vec![2, 12]).await;

    let e2_res = test_env.storage.seal_and_sync_epoch(e2).await.unwrap();

    // test may_exist with data in memtable (e3), uncommitted ssts (e2), committed ssts (e1)
    check_may_exist(&state_table, vec![1, 3, 4, 6], vec![2, 12]).await;

    epoch.inc();
    state_table.commit(epoch).await.unwrap();
    let e3 = epoch.prev;

    // test may_exist with data in immutable memtable (e3), uncommitted ssts (e2), committed
    // ssts (e1)
    check_may_exist(&state_table, vec![1, 3, 4, 6], vec![2, 12]).await;

    state_table.insert(OwnedRow::new(vec![
        Some(1_i32.into()),
        Some(55_i32.into()),
        Some(555_i32.into()),
    ]));
    state_table.insert(OwnedRow::new(vec![
        Some(2_i32.into()),
        Some(1_i32.into()),
        Some(111_i32.into()),
    ]));

    // test may_exist with data in memtable (e4), immutable memtable (e3), uncommitted ssts
    // (e2), committed ssts (e1)
    check_may_exist(&state_table, vec![1, 3, 4, 6], vec![12]).await;

    test_env
        .meta_client
        .commit_epoch(e2, e2_res.uncommitted_ssts)
        .await
        .unwrap();
    test_env.storage.try_wait_epoch_for_test(e2).await;

    epoch.inc();
    state_table.commit(epoch).await.unwrap();
    let e4 = epoch.prev;

    let e3_res = test_env.storage.seal_and_sync_epoch(e3).await.unwrap();
    let e4_res = test_env.storage.seal_and_sync_epoch(e4).await.unwrap();

    // test may_exist with data in uncommitted ssts (e3, e4), committed ssts (e1, e2, e3, e4)
    check_may_exist(&state_table, vec![1, 3, 4, 6], vec![12]).await;

    test_env
        .meta_client
        .commit_epoch(e3, e3_res.uncommitted_ssts)
        .await
        .unwrap();
    test_env.storage.try_wait_epoch_for_test(e3).await;

    test_env
        .meta_client
        .commit_epoch(e4, e4_res.uncommitted_ssts)
        .await
        .unwrap();
    test_env.storage.try_wait_epoch_for_test(e4).await;

    // test may_exist with data in committed ssts (e1, e2, e3, e4)
    check_may_exist(&state_table, vec![1, 3, 4, 6], vec![12]).await;
}

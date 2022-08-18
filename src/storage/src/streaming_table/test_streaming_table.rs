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
use risingwave_common::catalog::{ColumnDesc, ColumnId, TableId};
use risingwave_common::types::DataType;
use risingwave_common::util::sort_util::OrderType;

use crate::error::StorageResult;
use crate::memory::MemoryStateStore;
use crate::streaming_table::state_table::StateTable;

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
        .delete(Row(vec![
            Some(2_i32.into()),
            Some(22_i32.into()),
            Some(222_i32.into()),
        ]))
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
        .get_row(&Row(vec![Some(4_i32.into())]), epoch)
        .await
        .unwrap();
    assert_eq!(row4, Some(Row(vec![Some(4_i32.into()), None, None])));

    let non_exist_row = state_table
        .get_row(&Row(vec![Some(0_i32.into())]), epoch)
        .await
        .unwrap();
    assert_eq!(non_exist_row, None);

    state_table
        .delete(Row(vec![Some(4_i32.into()), None, None]))
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
        .get_row(&Row(vec![Some(6_i32.into())]), epoch)
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
        .get_row(&Row(vec![Some(7_i32.into())]), epoch)
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
        .get_row(&Row(vec![Some(6_i32.into())]), epoch)
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
        .get_row(&Row(vec![Some(7_i32.into())]), epoch)
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
        .get_row(&Row(vec![Some(1_i32.into())]), epoch)
        .await
        .unwrap();
    assert_eq!(row1, None);

    epoch += 1;
    state_table.commit(epoch).await.unwrap();

    let row1_commit = state_table
        .get_row(&Row(vec![Some(1_i32.into())]), epoch)
        .await
        .unwrap();
    assert_eq!(row1_commit, None);
    Ok(())
}

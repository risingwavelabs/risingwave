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

use risingwave_common::catalog::{ColumnDesc, TableId};
use risingwave_common::row::OwnedRow;
use risingwave_common::types::DataType;
use risingwave_common::util::epoch::EpochPair;
use risingwave_common::util::sort_util::OrderType;
use risingwave_storage::memory::MemoryStateStore;
use risingwave_storage::table::batch_table::storage_table::StorageTable;

use crate::common::table::state_table::StateTable;

pub async fn gen_basic_table(row_count: usize) -> StorageTable<MemoryStateStore> {
    let state_store = MemoryStateStore::new();

    let order_types = vec![
        OrderType::default_ascending(),
        OrderType::default_descending(),
    ];
    let column_ids = vec![0.into(), 1.into(), 2.into()];
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
        order_types,
        pk_indices,
    )
    .await;
    let table = StorageTable::for_test(
        state_store.clone(),
        TableId::from(0x42),
        column_descs.clone(),
        vec![OrderType::default_ascending()],
        vec![0],
        vec![0, 1, 2],
    );
    let mut epoch = EpochPair::new_test_epoch(1);
    state.init_epoch(epoch);

    for idx in 0..row_count {
        let idx = idx as i32;
        state.insert(OwnedRow::new(vec![
            Some(idx.into()),
            Some(idx.into()),
            Some(idx.into()),
        ]));
    }

    epoch.inc();
    state.commit(epoch).await.unwrap();

    table
}

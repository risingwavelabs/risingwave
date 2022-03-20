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
//
use risingwave_common::array::Row;
use risingwave_common::catalog::ColumnDesc;
use risingwave_common::types::DataType;
use risingwave_common::util::sort_util::OrderType;
use risingwave_storage::memory::MemoryStateStore;
use risingwave_storage::Keyspace;

use super::{MViewTable, ManagedMViewState};

pub async fn gen_basic_table(row_count: usize) -> MViewTable<MemoryStateStore> {
    let state_store = MemoryStateStore::new();
    // let pk_columns = vec![0, 1]; leave a message to indicate pk columns
    let orderings = vec![OrderType::Ascending, OrderType::Descending];
    let keyspace = Keyspace::executor_root(state_store, 0x42);
    let column_ids = vec![0.into(), 1.into(), 2.into()];
    let mut state = ManagedMViewState::new(
        keyspace.clone(),
        column_ids.clone(),
        vec![OrderType::Ascending, OrderType::Descending],
    );
    let column_descs = vec![
        ColumnDesc::unnamed(column_ids[0], DataType::Int32),
        ColumnDesc::unnamed(column_ids[1], DataType::Int32),
        ColumnDesc::unnamed(column_ids[2], DataType::Int32),
    ];
    let table = MViewTable::new_for_test(keyspace.clone(), column_descs, orderings);
    let epoch: u64 = 0;

    for idx in 0..row_count {
        let idx = idx as i32;
        state.put(
            Row(vec![Some(idx.into()), Some(idx.into())]),
            Row(vec![Some(idx.into()), Some(idx.into()), Some(idx.into())]),
        );
    }
    state.flush(epoch).await.unwrap();
    table
}

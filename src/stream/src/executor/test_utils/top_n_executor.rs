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

use itertools::Itertools;
use risingwave_common::catalog::{ColumnDesc, ColumnId, TableId};
use risingwave_common::types::DataType;
use risingwave_common::util::sort_util::OrderType;
use risingwave_storage::memory::MemoryStateStore;

use crate::common::table::state_table::StateTable;
use crate::common::table::test_utils::gen_pbtable;

pub async fn create_in_memory_state_table(
    data_types: &[DataType],
    order_types: &[OrderType],
    pk_indices: &[usize],
) -> StateTable<MemoryStateStore> {
    create_in_memory_state_table_from_state_store(
        data_types,
        order_types,
        pk_indices,
        MemoryStateStore::new(),
    )
    .await
}

pub async fn create_in_memory_state_table_from_state_store(
    data_types: &[DataType],
    order_types: &[OrderType],
    pk_indices: &[usize],
    state_store: MemoryStateStore,
) -> StateTable<MemoryStateStore> {
    let column_descs = data_types
        .iter()
        .enumerate()
        .map(|(id, data_type)| ColumnDesc::unnamed(ColumnId::new(id as i32), data_type.clone()))
        .collect_vec();
    StateTable::from_table_catalog(
        &gen_pbtable(
            TableId::new(0),
            column_descs,
            order_types.to_vec(),
            pk_indices.to_vec(),
            0,
        ),
        state_store,
        None,
    )
    .await
}

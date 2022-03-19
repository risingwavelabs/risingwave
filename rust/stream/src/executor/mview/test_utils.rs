use risingwave_common::array::Row;
use risingwave_common::catalog::ColumnDesc;
use risingwave_common::types::DataType;
use risingwave_common::util::sort_util::OrderType;
use risingwave_storage::memory::MemoryStateStore;
use risingwave_storage::table::cell_based_table::CellBasedTable;
use risingwave_storage::Keyspace;

use super::ManagedMViewState;

pub async fn gen_basic_table(row_count: usize) -> CellBasedTable<MemoryStateStore> {
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
    let table = CellBasedTable::new_for_test(keyspace.clone(), column_descs, orderings);
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

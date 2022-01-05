use risingwave_common::array::Row;
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::types::Int32Type;
use risingwave_common::util::sort_util::OrderType;
use risingwave_storage::memory::MemoryStateStore;
use risingwave_storage::Keyspace;

use super::{MViewTable, ManagedMViewState};

pub async fn gen_basic_table(row_count: usize) -> MViewTable<MemoryStateStore> {
    let state_store = MemoryStateStore::new();
    let schema = Schema::new(vec![
        Field::new(Int32Type::create(false)),
        Field::new(Int32Type::create(false)),
        Field::new(Int32Type::create(false)),
    ]);
    let pk_columns = vec![0, 1];
    let orderings = vec![OrderType::Ascending, OrderType::Descending];
    let keyspace = Keyspace::executor_root(state_store, 0x42);
    let mut state = ManagedMViewState::new(
        keyspace.clone(),
        schema.clone(),
        pk_columns.clone(),
        orderings.clone(),
    );
    let table = MViewTable::new(keyspace.clone(), schema, pk_columns.clone(), orderings);
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

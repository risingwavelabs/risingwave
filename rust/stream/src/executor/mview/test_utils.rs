use risingwave_common::array::Row;
use risingwave_common::util::sort_util::{OrderPair, OrderType};
use risingwave_storage::memory::MemoryStateStore;
use risingwave_storage::Keyspace;

use super::super::test_utils::schemas;
use super::{MViewTable, ManagedMViewState};

pub async fn gen_basic_table(row_count: usize) -> MViewTable<MemoryStateStore> {
    let state_store = MemoryStateStore::new();
    let schema = schemas::iii();
    let pk_columns = vec![0, 1];
    let orderings = vec![OrderType::Ascending, OrderType::Descending];
    let keyspace = Keyspace::executor_root(state_store, 0x42);
    let mut state = ManagedMViewState::new(
        keyspace.clone(),
        vec![0.into(), 1.into(), 2.into()],
        vec![
            OrderPair::new(0, OrderType::Ascending),
            OrderPair::new(1, OrderType::Descending),
        ],
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

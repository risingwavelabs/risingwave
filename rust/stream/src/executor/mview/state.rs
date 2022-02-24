use std::collections::HashMap;

use risingwave_common::array::Row;
use risingwave_common::catalog::ColumnId;
use risingwave_common::error::Result;
use risingwave_common::util::ordered::*;
use risingwave_common::util::sort_util::OrderPair;
use risingwave_storage::{Keyspace, StateStore};

use crate::executor::managed_state::flush_status::HashMapFlushStatus as FlushStatus;

/// `ManagedMviewState` buffers recent mutations. Data will be written
/// to backend storage on calling `flush`.
pub struct ManagedMViewState<S: StateStore> {
    keyspace: Keyspace<S>,

    /// Column IDs of each column in the input schema
    column_ids: Vec<ColumnId>,

    /// Serializer to serialize keys from input rows
    key_serializer: OrderedRowsSerializer,

    /// Cached key/values
    cache: HashMap<Row, FlushStatus<Row>>,
}

impl<S: StateStore> ManagedMViewState<S> {
    pub fn new(keyspace: Keyspace<S>, column_ids: Vec<ColumnId>, keys: Vec<OrderPair>) -> Self {
        Self {
            keyspace,
            column_ids,
            cache: HashMap::new(),
            key_serializer: OrderedRowsSerializer::new(keys),
        }
    }

    pub fn put(&mut self, pk: Row, value: Row) {
        assert_eq!(self.column_ids.len(), value.size());
        FlushStatus::do_insert(self.cache.entry(pk), value);
    }

    pub fn delete(&mut self, pk: Row) {
        FlushStatus::do_delete(self.cache.entry(pk));
    }

    // TODO(MrCroxx): flush can be performed in another coroutine by taking the snapshot of
    // cache.
    pub async fn flush(&mut self, epoch: u64) -> Result<()> {
        let mut batch = self.keyspace.state_store().start_write_batch();
        batch.reserve(self.cache.len() * self.column_ids.len());
        let mut local = batch.prefixify(&self.keyspace);

        for (pk, cells) in self.cache.drain() {
            let row = cells.into_option();
            debug_assert_eq!(self.column_ids.len(), pk.0.len());
            let pk_buf = serialize_pk(&pk, &self.key_serializer)?;
            for (index, column_id) in self.column_ids.iter().enumerate() {
                // TODO(MrCroxx): More efficient encoding is needed.
                // format: [ pk_buf | cell_idx (4B)]
                let key = [
                    pk_buf.as_slice(),
                    serialize_column_id(column_id)?.as_slice(),
                ]
                .concat();

                match &row {
                    Some(values) => {
                        let value = serialize_cell(&values[index])?;
                        local.put(key, value)
                    }
                    None => local.delete(key),
                };
            }
        }

        batch.ingest(epoch).await.unwrap();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::util::sort_util::OrderType;
    use risingwave_storage::memory::MemoryStateStore;

    use super::*;
    use crate::executor::test_utils::schemas;

    #[tokio::test]
    async fn test_mview_state() {
        // Only assert pk and columns can be successfully put/delete/flush,
        // and the ammount of rows is expected.
        let state_store = MemoryStateStore::new();
        let _schema = schemas::ii();
        let keyspace = Keyspace::executor_root(state_store.clone(), 0x42);

        let mut state = ManagedMViewState::new(
            keyspace.clone(),
            vec![0.into(), 1.into()],
            vec![OrderPair::new(0, OrderType::Ascending)],
        );
        let mut epoch: u64 = 0;
        state.put(
            Row(vec![Some(1_i32.into())]),
            Row(vec![Some(1_i32.into()), Some(11_i32.into())]),
        );
        state.put(
            Row(vec![Some(2_i32.into())]),
            Row(vec![Some(2_i32.into()), Some(22_i32.into())]),
        );
        state.put(
            Row(vec![Some(3_i32.into())]),
            Row(vec![Some(3_i32.into()), Some(33_i32.into())]),
        );
        state.delete(Row(vec![Some(2_i32.into())]));

        state.flush(epoch).await.unwrap();
        let data = keyspace.scan(None, epoch).await.unwrap();
        // cell-based storage has 4 cells
        assert_eq!(data.len(), 4);

        epoch += 1;
        state.delete(Row(vec![Some(3_i32.into())]));
        state.flush(epoch).await.unwrap();
        let data = keyspace.scan(None, epoch).await.unwrap();
        assert_eq!(data.len(), 2);
    }
}

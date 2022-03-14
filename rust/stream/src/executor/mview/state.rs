use std::collections::HashMap;

use risingwave_common::array::Row;
use risingwave_common::catalog::ColumnId;
use risingwave_common::error::Result;
use risingwave_common::util::ordered::*;
use risingwave_common::util::sort_util::OrderType;
use risingwave_storage::{Keyspace, StateStore};

use crate::executor::managed_state::flush_status::HashMapFlushStatus as FlushStatus;

/// `ManagedMViewState` buffers recent mutations. Data will be written
/// to backend storage on calling `flush`.
pub struct ManagedMViewState<S: StateStore> {
    keyspace: Keyspace<S>,

    /// Column IDs of each column in the input schema
    column_ids: Vec<ColumnId>,

    /// Ordering of primary key (for assertion)
    order_types: Vec<OrderType>,

    /// Serializer to serialize keys from input rows
    key_serializer: OrderedRowSerializer,

    /// Cached key/values
    cache: HashMap<Row, FlushStatus<Row>>,
}

impl<S: StateStore> ManagedMViewState<S> {
    /// Create a [`ManagedMViewState`].
    pub fn new(
        keyspace: Keyspace<S>,
        column_ids: Vec<ColumnId>,
        order_types: Vec<OrderType>,
    ) -> Self {
        // TODO(eric): refactor this later...
        Self {
            keyspace,
            column_ids,
            cache: HashMap::new(),
            order_types: order_types.clone(),
            key_serializer: OrderedRowSerializer::new(order_types),
        }
    }

    /// Put a key into the managed mview state. [`arrange_keys`] is composed of group keys and
    /// primary keys.
    pub fn put(&mut self, pk: Row, value: Row) {
        assert_eq!(self.order_types.len(), pk.size());
        assert_eq!(self.column_ids.len(), value.size());

        FlushStatus::do_insert(self.cache.entry(pk), value);
    }

    /// Delete a key from the managed mview state. [`arrange_keys`] is composed of group keys and
    /// primary keys.
    pub fn delete(&mut self, pk: Row) {
        assert_eq!(self.order_types.len(), pk.size());

        FlushStatus::do_delete(self.cache.entry(pk));
    }

    pub fn clear_cache(&mut self) {
        self.cache.clear();
    }

    pub async fn flush(&mut self, epoch: u64) -> Result<()> {
        let mut batch = self.keyspace.state_store().start_write_batch();
        batch.reserve(self.cache.len() * self.column_ids.len());
        let mut local = batch.prefixify(&self.keyspace);

        for (arrange_keys, cells) in self.cache.drain() {
            let row = cells.into_option();
            let arrange_key_buf = serialize_pk(&arrange_keys, &self.key_serializer)?;
            let bytes = serialize_pk_and_row(&arrange_key_buf, &row, &self.column_ids)?;
            for (key, value) in bytes {
                match value {
                    Some(val) => local.put(key, val),
                    None => local.delete(key),
                }
            }
        }
        batch.ingest(epoch).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::catalog::schema_test_utils;
    use risingwave_common::util::sort_util::OrderType;
    use risingwave_storage::memory::MemoryStateStore;

    use super::*;

    #[tokio::test]
    async fn test_mview_state() {
        // Only assert pk and columns can be successfully put/delete/flush,
        // and the ammount of rows is expected.
        let state_store = MemoryStateStore::new();
        let _schema = schema_test_utils::ii();
        let keyspace = Keyspace::executor_root(state_store.clone(), 0x42);

        let mut state = ManagedMViewState::new(
            keyspace.clone(),
            vec![0.into(), 1.into()],
            vec![OrderType::Ascending],
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

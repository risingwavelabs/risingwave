use std::collections::{btree_map, BTreeMap};

use itertools::Itertools;
use risingwave_common::array::data_chunk_iter::RowDeserializer;
use risingwave_common::array::Row;
use risingwave_common::error::Result;
use risingwave_common::types::DataTypeKind;
use risingwave_storage::write_batch::WriteBatch;
use risingwave_storage::{Keyspace, StateStore};

use super::{PkType, ValueType};
use crate::executor::managed_state::flush_status::BtreeMapFlushStatus as FlushStatus;

type AllOrNoneStateIter<'a> = btree_map::Iter<'a, Row, Row>;

type AllOrNoneStateValues<'a> = btree_map::Values<'a, Row, Row>;

/// Manages a `BTreeMap` in memory for all entries. When evicted, `BTreeMap` does not hold any
/// entries.
pub struct AllOrNoneState<S: StateStore> {
    /// The full copy of the state. If evicted, it will be empty.
    cached: BTreeMap<PkType, ValueType>,

    /// The actions that will be taken on next flush
    flush_buffer: BTreeMap<PkType, FlushStatus<ValueType>>,

    /// Number of items in the state including the cache and state store.
    total_count: usize,

    /// if the cache are evicted
    cache_evicted: bool,

    /// Data types of the sort column
    data_types: Vec<DataTypeKind>,

    /// Data types of primary keys
    pk_data_types: Vec<DataTypeKind>,

    /// Indices of primary keys
    pk_indices: Vec<usize>,

    /// The keyspace to operate on.
    keyspace: Keyspace<S>,
}

impl<S: StateStore> AllOrNoneState<S> {
    pub fn new(
        keyspace: Keyspace<S>,
        data_types: Vec<DataTypeKind>,
        pk_indices: Vec<usize>,
    ) -> Self {
        let pk_data_types = pk_indices.iter().map(|idx| data_types[*idx]).collect_vec();
        Self {
            cached: BTreeMap::new(),
            flush_buffer: BTreeMap::new(),
            total_count: 0,
            cache_evicted: false,
            data_types,
            pk_data_types,
            pk_indices,
            keyspace,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.total_count > 0
    }

    /// The state is dirty means there are unflush
    pub fn is_dirty(&self) -> bool {
        !self.flush_buffer.is_empty()
    }

    // Insert into the cache and flush buffer.
    pub fn insert(&mut self, value: ValueType) {
        let pk = self
            .pk_indices
            .iter()
            .map(|idx| value[*idx].clone())
            .collect_vec();
        let pk = Row(pk);
        if !self.cache_evicted {
            self.cached.insert(pk.clone(), value.clone());
        }
        self.total_count += 1;
        FlushStatus::do_insert(self.flush_buffer.entry(pk), value);
    }

    pub fn remove(&mut self, pk: PkType) {
        if !self.cache_evicted {
            self.cached.remove(&pk);
        }
        // If no cache maintained, only update the flush buffer.
        self.total_count -= 1;
        FlushStatus::do_delete(self.flush_buffer.entry(pk));
    }

    // Flush data to the state store
    pub fn flush(&mut self, write_batch: &mut WriteBatch<S>) -> Result<()> {
        let mut local = write_batch.local(&self.keyspace);

        for (pk, v) in std::mem::take(&mut self.flush_buffer) {
            let key_encoded = pk.serialize_not_null()?;

            match v.into_option() {
                Some(v) => {
                    let value = v.serialize()?;
                    local.put(key_encoded, value);
                }
                None => {
                    local.delete(key_encoded);
                }
            }
        }
        Ok(())
    }

    // Fetch cache from the state store.
    async fn fetch_cache(&mut self) {
        let all_data = self.keyspace.scan_strip_prefix(None).await.unwrap();

        for (raw_key, raw_value) in all_data {
            let pk_deserializer = RowDeserializer::new(self.pk_data_types.clone());
            let key = pk_deserializer.deserialize_not_null(&raw_key).unwrap();
            let deserializer = RowDeserializer::new(self.data_types.clone());
            let value = deserializer.deserialize(&raw_value).unwrap();
            self.cached.insert(key, value);
        }
    }

    pub async fn iter(&mut self) -> AllOrNoneStateIter<'_> {
        if self.cache_evicted {
            self.fetch_cache().await;
        }
        self.cached.iter()
    }

    pub async fn values(&mut self) -> AllOrNoneStateValues<'_> {
        if self.cache_evicted {
            self.fetch_cache().await;
        }
        self.cached.values()
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::*;
    use risingwave_common::column_nonnull;
    use risingwave_common::types::ScalarImpl;
    use risingwave_storage::memory::MemoryStateStore;

    use super::*;

    async fn test_managed_all_or_none_state() {
        let store = MemoryStateStore::new();
        let keyspace = Keyspace::executor_root(store.clone(), 0x2333);
        let mut managed_state = AllOrNoneState::new(
            keyspace,
            vec![DataTypeKind::Int64, DataTypeKind::Int64],
            vec![0],
        );
        assert!(!managed_state.is_dirty());
        let columns = vec![
            column_nonnull! { I64Array, [3, 2, 1] },
            column_nonnull! { I64Array, [4, 5, 6] },
        ];

        let col1 = [1, 2, 3];
        let col2 = [6, 5, 4];
        let data_chunk_builder = DataChunk::builder().columns(columns);
        let data_chunk = data_chunk_builder.build();

        for row_ref in data_chunk.rows() {
            let row = row_ref.into();
            managed_state.insert(row);
        }

        for state in managed_state.iter().await.zip(col1.iter().zip(col2.iter())) {
            let ((key, value), (d1, d2)) = state;
            assert_eq!(key.0[0], Some(ScalarImpl::Int64(*d1)));
            assert_eq!(value.0[0], Some(ScalarImpl::Int64(*d1)));
            assert_eq!(value.0[1], Some(ScalarImpl::Int64(*d2)));
        }

        // flush to write batch and write to state store
        let epoch: u64 = 0;
        let mut write_batch = store.start_write_batch();
        managed_state.flush(&mut write_batch).unwrap();
        write_batch.ingest(epoch).await.unwrap();

        assert!(!managed_state.is_dirty());
    }
}

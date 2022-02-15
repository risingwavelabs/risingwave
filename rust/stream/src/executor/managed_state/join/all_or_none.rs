use std::collections::{btree_map, BTreeMap};
use std::sync::Arc;

use risingwave_common::array::data_chunk_iter::RowDeserializer;
use risingwave_common::error::Result;
use risingwave_common::types::DataType;
use risingwave_storage::write_batch::WriteBatch;
use risingwave_storage::{Keyspace, StateStore};

use super::*;
use crate::executor::managed_state::flush_status::BtreeMapFlushStatus as FlushStatus;

type AllOrNoneStateIter<'a> = btree_map::Iter<'a, PkType, StateValueType>;

type AllOrNoneStateValues<'a> = btree_map::Values<'a, PkType, StateValueType>;

type AllOrNoneStateValuesMut<'a> = btree_map::ValuesMut<'a, PkType, StateValueType>;

/// Manages a `BTreeMap` in memory for all entries. When evicted, `BTreeMap` does not hold any
/// entries.
pub struct AllOrNoneState<S: StateStore> {
    /// The full copy of the state. If evicted, it will be `None`.
    cached: Option<BTreeMap<PkType, StateValueType>>,

    /// The actions that will be taken on next flush
    flush_buffer: BTreeMap<PkType, FlushStatus<StateValueType>>,

    /// Number of items in the state including the cache and state store.
    total_count: usize,

    /// Data types of the sort column
    data_types: Arc<[DataType]>,

    /// Data types of primary keys
    pk_data_types: Arc<[DataType]>,

    /// The keyspace to operate on.
    keyspace: Keyspace<S>,
}

impl<S: StateStore> AllOrNoneState<S> {
    pub fn new(
        keyspace: Keyspace<S>,
        total_count: usize,
        data_types: Arc<[DataType]>,
        pk_data_types: Arc<[DataType]>,
    ) -> Self {
        Self {
            cached: None,
            flush_buffer: BTreeMap::new(),
            total_count,
            data_types,
            pk_data_types,
            keyspace,
        }
    }

    pub fn with_cached_state(
        keyspace: Keyspace<S>,
        cached: BTreeMap<PkType, StateValueType>,
        data_types: Arc<[DataType]>,
        pk_data_types: Arc<[DataType]>,
    ) -> Self {
        let len = cached.len();
        Self {
            cached: Some(cached),
            flush_buffer: BTreeMap::new(),
            total_count: len,
            data_types,
            pk_data_types,
            keyspace,
        }
    }

    pub async fn is_empty(&mut self) -> bool {
        self.len().await == 0
    }

    pub async fn len(&mut self) -> usize {
        if self.cached.is_none() {
            self.fetch_cache().await.unwrap();
        }
        self.total_count as usize
    }

    /// The state is dirty means there are unflush
    #[allow(dead_code)]
    pub fn is_dirty(&self) -> bool {
        !self.flush_buffer.is_empty()
    }

    // Insert into the cache and flush buffer.
    pub fn insert(&mut self, key: PkType, value: StateValueType) {
        if let Some(cached) = self.cached.as_mut() {
            cached.insert(key.clone(), value.clone());
        }
        // If no cache maintained, only update the flush buffer.
        self.total_count += 1;
        FlushStatus::do_insert(self.flush_buffer.entry(key), value);
    }

    pub fn remove(&mut self, pk: PkType) {
        if let Some(cached) = self.cached.as_mut() {
            cached.remove(&pk);
        }
        // If no cache maintained, only update the flush buffer.
        self.total_count -= 1;
        FlushStatus::do_delete(self.flush_buffer.entry(pk));
    }

    // Flush data to the state store
    pub fn flush(&mut self, write_batch: &mut WriteBatch<S>) -> Result<()> {
        let mut local = write_batch.prefixify(&self.keyspace);

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
    async fn fetch_cache(&mut self) -> Result<()> {
        let epoch = u64::MAX;
        assert!(self.cached.is_none());

        let all_data = self.keyspace.scan_strip_prefix(None, epoch).await?;

        // Fetch cached states.
        let mut cached = BTreeMap::new();
        for (raw_key, raw_value) in all_data {
            let pk_deserializer = RowDeserializer::new(self.pk_data_types.to_vec());
            let key = pk_deserializer.deserialize_not_null(&raw_key)?;
            let deserializer = JoinRowDeserializer::new(self.data_types.to_vec());
            let value = deserializer.deserialize(&raw_value)?;
            cached.insert(key, value);
        }

        // Apply current flush buffer to cached states.
        for (pk, row) in &self.flush_buffer {
            match row.as_option() {
                Some(row) => {
                    cached.insert(pk.clone(), row.clone());
                }
                None => {
                    cached.remove(pk);
                }
            }
        }

        self.total_count = cached.len();
        self.cached = Some(cached);
        Ok(())
    }

    pub fn clear_cache(&mut self) {
        assert!(
            !self.is_dirty(),
            "cannot clear cache while all or none state is dirty"
        );
        self.cached = None;
    }

    #[allow(dead_code)]
    pub async fn iter(&mut self) -> AllOrNoneStateIter<'_> {
        if self.cached.is_none() {
            self.fetch_cache().await.unwrap();
        }
        self.cached.as_ref().unwrap().iter()
    }

    pub async fn values(&mut self) -> AllOrNoneStateValues<'_> {
        if self.cached.is_none() {
            self.fetch_cache().await.unwrap();
        }
        self.cached.as_ref().unwrap().values()
    }

    pub async fn values_mut(&mut self) -> AllOrNoneStateValuesMut<'_> {
        if self.cached.is_none() {
            self.fetch_cache().await.unwrap();
        }
        self.cached.as_mut().unwrap().values_mut()
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::*;
    use risingwave_common::column_nonnull;
    use risingwave_common::types::ScalarImpl;
    use risingwave_storage::memory::MemoryStateStore;

    use super::*;

    #[tokio::test]
    async fn test_managed_all_or_none_state() {
        let store = MemoryStateStore::new();
        let keyspace = Keyspace::executor_root(store.clone(), 0x2333);
        let mut managed_state = AllOrNoneState::new(
            keyspace,
            0,
            vec![DataType::Int64, DataType::Int64].into(),
            vec![DataType::Int64].into(),
        );
        assert!(!managed_state.is_dirty());
        let columns = vec![
            column_nonnull! { I64Array, [3, 2, 1] },
            column_nonnull! { I64Array, [4, 5, 6] },
        ];
        let pk_indices = [0];
        let col1 = [1, 2, 3];
        let col2 = [6, 5, 4];
        let data_chunk_builder = DataChunk::builder().columns(columns);
        let data_chunk = data_chunk_builder.build();

        for row_ref in data_chunk.rows() {
            let row: Row = row_ref.into();
            let pk = pk_indices.iter().map(|idx| row[*idx].clone()).collect_vec();
            let pk = Row(pk);
            let join_row = JoinRow { row, degree: 0 };
            managed_state.insert(pk, join_row);
        }

        for state in managed_state
            .iter()
            .await
            .zip_eq(col1.iter().zip_eq(col2.iter()))
        {
            let ((key, value), (d1, d2)) = state;
            assert_eq!(key.0[0], Some(ScalarImpl::Int64(*d1)));
            assert_eq!(value.row[0], Some(ScalarImpl::Int64(*d1)));
            assert_eq!(value.row[1], Some(ScalarImpl::Int64(*d2)));
            assert_eq!(value.degree, 0);
        }

        // flush to write batch and write to state store
        let epoch: u64 = 0;
        let mut write_batch = store.start_write_batch();
        managed_state.flush(&mut write_batch).unwrap();
        write_batch.ingest(epoch).await.unwrap();

        assert!(!managed_state.is_dirty());
    }
}

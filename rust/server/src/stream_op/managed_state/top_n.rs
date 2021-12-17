use std::collections::BTreeMap;

use bytes::Bytes;
use risingwave_common::array::{Row, RowDeserializer};
use risingwave_common::catalog::Schema;
use risingwave_common::error::Result;
use risingwave_storage::{Keyspace, StateStore};

use crate::stream_op::{serialize_cell, serialize_cell_idx};

/// This state maintains a cache and a flush buffer with top-n cache policy.
/// This is because we need to compare the largest element to decide
/// whether a new element should be put into the state.
/// Additionally, it provides ability to query the largest element. Also, its value is a
/// row instead of a single `ScalarImpl`.
pub struct ManagedTopNState<S: StateStore> {
    /// Cache.
    top_n: BTreeMap<Bytes, Row>,
    /// Buffer for updates.
    flush_buffer: BTreeMap<Bytes, Option<Row>>,
    /// The number of elements in both cache and storage.
    total_count: usize,
    /// Number of entries to retain in memory after each flush.
    top_n_count: Option<usize>,
    /// The keyspace to operate on.
    keyspace: Keyspace<S>,
    /// Schema for serializing `Option<Row>`.
    schema: Schema,
}

impl<S: StateStore> ManagedTopNState<S> {
    pub fn new(
        top_n_count: Option<usize>,
        total_count: usize,
        keyspace: Keyspace<S>,
        schema: Schema,
    ) -> Self {
        Self {
            top_n: BTreeMap::new(),
            flush_buffer: BTreeMap::new(),
            total_count,
            top_n_count,
            keyspace,
            schema,
        }
    }

    #[cfg(test)]
    fn get_cache_len(&self) -> usize {
        self.top_n.len()
    }

    pub fn total_count(&self) -> usize {
        self.total_count
    }

    pub fn is_dirty(&self) -> bool {
        !self.flush_buffer.is_empty()
    }

    pub fn retain_top_n(&mut self) {
        if let Some(count) = self.top_n_count {
            while self.top_n.len() > count {
                // Although it seems to pop the last element(supposedly with the largest key),
                // it is actually popping the element with the smallest key.
                // This is because we reverse serialize the key so that `scan` can fetch from
                // the larger end.
                self.top_n.pop_last();
            }
        }
    }

    pub async fn pop_top_element(&mut self) -> Result<Option<(Bytes, Row)>> {
        if self.total_count == 0 {
            Ok(None)
        } else {
            // Cache must always be non-empty when the state is not empty.
            debug_assert!(!self.top_n.is_empty(), "top_n is empty");
            // Similar as the comments in `retain_top_n`, it is actually popping
            // the element with the largest key.
            let element_to_pop = self.top_n.pop_first().unwrap();
            // We need to delete the element from the storage.
            self.flush_buffer.insert(element_to_pop.0.clone(), None);
            self.total_count -= 1;
            // If we have nothing in the cache, we have to scan from the storage.
            if self.top_n.is_empty() && self.total_count > 0 {
                self.flush().await?;
                self.fill_in_cache().await?;
            }
            Ok(Some(element_to_pop))
        }
    }

    pub fn top_element(&mut self) -> Option<(&Bytes, &Row)> {
        if self.total_count == 0 {
            None
        } else {
            self.top_n.first_key_value()
        }
    }

    pub async fn insert(&mut self, element: (Bytes, Row)) {
        self.top_n.insert(element.0.clone(), element.1.clone());
        self.flush_buffer.insert(element.0, Some(element.1));
        self.total_count += 1;
    }

    /// We can fill in the cache from storage only when state is not dirty, i.e. right after
    /// `flush`. We don't need to care about whether `self.top_n` is empty or not as the key is
    /// unique. An element with duplicated key scanned from the storage would just override the
    /// element with the same key in the cache, and their value must be the same.
    pub async fn fill_in_cache(&mut self) -> Result<()> {
        debug_assert!(!self.is_dirty());
        let pk_row_bytes = self
            .keyspace
            .scan_strip_prefix(
                self.top_n_count
                    .map(|top_n_count| top_n_count * self.schema.len()),
            )
            .await?;
        // We must have enough cells to restore a complete row.
        debug_assert_eq!(pk_row_bytes.len() % self.schema.len(), 0);
        // cell-based storage format, so `self.schema.len()`
        let mut row_bytes = vec![];
        let mut cell_restored = 0;
        let schema = self
            .schema
            .data_types_clone()
            .into_iter()
            .map(|data_type| data_type.data_type_kind())
            .collect::<Vec<_>>();
        for (pk, cell_bytes) in pk_row_bytes {
            row_bytes.extend_from_slice(&cell_bytes);
            cell_restored += 1;
            if cell_restored == self.schema.len() {
                cell_restored = 0;
                let deserializer = RowDeserializer::new(schema.clone());
                let row = deserializer.deserialize(&std::mem::take(&mut row_bytes))?;
                // format: [pk_buf | cell_idx (4B)]
                // Take `pk_buf` out.
                let pk_without_cell_idx = pk.slice(0..pk.len() - 4);
                let prev_element = self.top_n.insert(pk_without_cell_idx, row.clone());
                if let Some(prev_row) = prev_element {
                    debug_assert_eq!(prev_row, row);
                }
            }
        }
        self.retain_top_n();
        Ok(())
    }

    /// `Flush` can be called by the executor when it receives a barrier and thus needs to
    /// checkpoint. TODO: `Flush` should also be called internally when `top_n` and
    /// `flush_buffer` exceeds certain limit.
    pub async fn flush(&mut self) -> Result<()> {
        if !self.is_dirty() {
            self.retain_top_n();
            return Ok(());
        }

        let mut write_batches = vec![];
        for (pk_buf, cells) in std::mem::take(&mut self.flush_buffer) {
            for cell_idx in 0..self.schema.len() {
                // format: [pk_buf | cell_idx (4B)]
                let key = [&pk_buf[..], &serialize_cell_idx(cell_idx as u32)?[..]].concat();
                // format: [keyspace prefix | pk_buf | cell_idx (4B)]
                let key = self.keyspace.prefixed_key(&key);
                let value = match &cells {
                    Some(cells) => Some(serialize_cell(&cells[cell_idx])?),
                    None => None,
                };
                write_batches.push((key.into(), value.map(Bytes::from)));
            }
        }
        self.keyspace
            .state_store()
            .ingest_batch(write_batches)
            .await?;

        self.retain_top_n();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::types::{DataTypeKind, Int64Type, StringType};
    use risingwave_common::util::sort_util::OrderType;
    use risingwave_storage::memory::MemoryStateStore;
    use risingwave_storage::{Keyspace, StateStore};

    use crate::row_nonnull;
    use crate::stream_op::managed_state::top_n::ManagedTopNState;
    use crate::stream_op::OrderedRowsSerializer;

    fn create_managed_top_n_state<S: StateStore>(
        store: &S,
        row_count: usize,
    ) -> ManagedTopNState<S> {
        let schema = Schema::new(vec![
            Field::new(Arc::new(Int64Type::new(false))),
            Field::new(StringType::create(false, 5, DataTypeKind::Varchar)),
        ]);

        ManagedTopNState::new(
            Some(2),
            row_count,
            Keyspace::new(store.clone(), b"test_2333".to_vec()),
            schema,
        )
    }

    #[tokio::test]
    async fn test_managed_top_n_state() {
        let store = MemoryStateStore::new();
        let mut managed_state = create_managed_top_n_state(&store, 0);
        let row1 = row_nonnull![2i64, "abc".to_string()];
        let row2 = row_nonnull![3i64, "abc".to_string()];
        let row3 = row_nonnull![3i64, "abd".to_string()];
        let row4 = row_nonnull![4i64, "ab".to_string()];
        let rows = vec![&row1, &row2, &row3, &row4];
        let orderings = vec![OrderType::Ascending, OrderType::Descending];
        let pk_indices = vec![1, 0];
        let order_pairs = orderings
            .into_iter()
            .zip(pk_indices.into_iter())
            .collect::<Vec<_>>();
        let ordered_row_serializer = OrderedRowsSerializer::new(order_pairs);
        let mut rows_bytes = vec![];
        ordered_row_serializer.order_based_scehmaed_serialize(&rows, &mut rows_bytes);
        managed_state
            .insert((rows_bytes[3].clone().into(), row4.clone()))
            .await;
        // now (4, "ab")

        assert_eq!(
            managed_state.top_element(),
            Some((&Bytes::from(rows_bytes[3].clone()), &row4))
        );
        assert!(managed_state.is_dirty());
        assert_eq!(managed_state.get_cache_len(), 1);

        managed_state
            .insert((rows_bytes[2].clone().into(), row3.clone()))
            .await;
        // now (3, "abd") -> (4, "ab")

        assert_eq!(
            managed_state.top_element(),
            Some((&Bytes::from(rows_bytes[2].clone()), &row3))
        );
        assert!(managed_state.is_dirty());
        assert_eq!(managed_state.get_cache_len(), 2);

        managed_state
            .insert((rows_bytes[1].clone().into(), row2.clone()))
            .await;
        // now (3, "abd") -> (3, "abc") -> (4, "ab")

        assert_eq!(
            managed_state.top_element(),
            Some((&Bytes::from(rows_bytes[2].clone()), &row3))
        );
        assert_eq!(managed_state.get_cache_len(), 3);
        managed_state.flush().await.unwrap();
        assert!(!managed_state.is_dirty());
        let row_count = managed_state.total_count;
        assert_eq!(row_count, 3);
        // After flush, only 2 elements should be kept in the cache.
        assert_eq!(managed_state.get_cache_len(), 2);

        drop(managed_state);
        let mut managed_state = create_managed_top_n_state(&store, row_count);
        assert_eq!(managed_state.top_element(), None);
        managed_state.fill_in_cache().await.unwrap();
        // now (3, "abd") -> (3, "abc") -> (4, "ab")
        assert_eq!(
            managed_state.top_element(),
            Some((&Bytes::from(rows_bytes[2].clone()), &row3))
        );
        // Right after recovery.
        assert!(!managed_state.is_dirty());
        assert_eq!(managed_state.get_cache_len(), 2);

        assert_eq!(
            managed_state.pop_top_element().await.unwrap(),
            Some((Bytes::from(rows_bytes[2].clone()), row3))
        );
        assert!(managed_state.is_dirty());
        assert_eq!(managed_state.total_count, 2);
        assert_eq!(managed_state.get_cache_len(), 1);
        assert_eq!(
            managed_state.pop_top_element().await.unwrap(),
            Some((Bytes::from(rows_bytes[1].clone()), row2.clone()))
        );
        // Popping to 0 element but automatically get at most `2` elements from the storage.
        // However, here we only have one element left as the `total_count` indicates.
        // The state is not dirty as we first flush and then scan from the storage.
        assert!(!managed_state.is_dirty());
        assert_eq!(managed_state.total_count, 1);
        assert_eq!(managed_state.get_cache_len(), 1);

        assert_eq!(
            managed_state.top_element(),
            Some((&Bytes::from(rows_bytes[3].clone()), &row4))
        );

        managed_state
            .insert((rows_bytes[0].clone().into(), row1.clone()))
            .await;
        assert_eq!(
            managed_state.top_element(),
            Some((&Bytes::from(rows_bytes[0].clone()), &row1))
        );

        // Exclude the last `insert` as the state crashes before recovery.
        let row_count = managed_state.total_count - 1;
        drop(managed_state);
        let mut managed_state = create_managed_top_n_state(&store, row_count);
        managed_state.fill_in_cache().await.unwrap();
        assert_eq!(
            managed_state.top_element(),
            Some((&Bytes::from(rows_bytes[3].clone()), &row4))
        );
    }
}

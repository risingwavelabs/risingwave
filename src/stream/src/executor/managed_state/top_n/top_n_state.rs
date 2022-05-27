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

use std::cmp::Ordering;

use madsim::collections::BTreeMap;
use risingwave_common::array::Row;
use risingwave_common::catalog::ColumnId;
use risingwave_common::error::Result;
use risingwave_common::types::DataType;
use risingwave_common::util::ordered::*;
use risingwave_storage::cell_based_row_deserializer::CellBasedRowDeserializer;
use risingwave_storage::storage_value::StorageValue;
use risingwave_storage::{Keyspace, StateStore};

use super::super::flush_status::BtreeMapFlushStatus as FlushStatus;
use super::variants::*;
use super::PkAndRowIterator;

/// This state is used for several ranges (e.g `[0, offset)`, `[offset+limit, +inf)` of elements in
/// the `AppendOnlyTopNExecutor` and `TopNExecutor`. For these ranges, we only care about one of the
/// ends of the range, either the largest or the smallest, as that end would frequently deal with
/// elements being removed from or inserted into the range. If interested in both ends, one should
/// refer to `ManagedTopNBottomNState`.
///
/// We remark that `TOP_N_TYPE` indicates which end we are interested in, and how we should
/// serialize and deserialize the `OrderedRow` and its binary representations. Since `scan` from the
/// storage always starts with the least key, we need to reversely serialize an `OrderedRow` if we
/// are interested in the larger end. This can also be solved by a `reverse_scan` api
/// from the storage. However, `reverse_scan` is typically slower than `forward_scan` when it comes
/// to LSM tree based storage.
pub struct ManagedTopNState<S: StateStore, const TOP_N_TYPE: usize> {
    /// Cache.
    top_n: BTreeMap<OrderedRow, Row>,
    /// Buffer for updates.
    flush_buffer: BTreeMap<OrderedRow, FlushStatus<Row>>,
    /// The number of elements in both cache and storage.
    total_count: usize,
    /// Number of entries to retain in memory after each flush.
    top_n_count: Option<usize>,
    /// The keyspace to operate on.
    keyspace: Keyspace<S>,
    /// `DataType`s use for deserializing `Row`.
    data_types: Vec<DataType>,
    /// For deserializing `OrderedRow`.
    ordered_row_deserializer: OrderedRowDeserializer,
    /// For deserializing `Row`.
    cell_based_row_deserializer: CellBasedRowDeserializer,
}

impl<S: StateStore, const TOP_N_TYPE: usize> ManagedTopNState<S, TOP_N_TYPE> {
    pub fn new(
        top_n_count: Option<usize>,
        total_count: usize,
        keyspace: Keyspace<S>,
        data_types: Vec<DataType>,
        ordered_row_deserializer: OrderedRowDeserializer,
        cell_based_row_deserializer: CellBasedRowDeserializer,
    ) -> Self {
        Self {
            top_n: BTreeMap::new(),
            flush_buffer: BTreeMap::new(),
            total_count,
            top_n_count,
            keyspace,
            data_types,
            ordered_row_deserializer,
            cell_based_row_deserializer,
        }
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
                match TOP_N_TYPE {
                    TOP_N_MIN => {
                        self.top_n.pop_last();
                    }
                    TOP_N_MAX => {
                        self.top_n.pop_first();
                    }
                    _ => unreachable!(),
                }
            }
        }
    }

    pub async fn pop_top_element(&mut self, epoch: u64) -> Result<Option<(OrderedRow, Row)>> {
        if self.total_count == 0 {
            Ok(None)
        } else {
            // Cache must always be non-empty when the state is not empty.
            debug_assert!(!self.top_n.is_empty(), "top_n is empty");
            // Similar as the comments in `retain_top_n`, it is actually popping
            // the element with the largest key.
            let key = match TOP_N_TYPE {
                TOP_N_MIN => self.top_n.first_key_value().unwrap().0.clone(),
                TOP_N_MAX => self.top_n.last_key_value().unwrap().0.clone(),
                _ => unreachable!(),
            };
            let value = self.delete(&key, epoch).await?;
            Ok(Some((key, value.unwrap())))
        }
    }

    pub fn top_element(&mut self) -> Option<(&OrderedRow, &Row)> {
        if self.total_count == 0 {
            None
        } else {
            match TOP_N_TYPE {
                TOP_N_MIN => self.top_n.first_key_value(),
                TOP_N_MAX => self.top_n.last_key_value(),
                _ => unreachable!(),
            }
        }
    }

    fn bottom_element(&mut self) -> Option<(&OrderedRow, &Row)> {
        if self.total_count == 0 {
            None
        } else {
            match TOP_N_TYPE {
                TOP_N_MIN => self.top_n.last_key_value(),
                TOP_N_MAX => self.top_n.first_key_value(),
                _ => unreachable!(),
            }
        }
    }

    pub async fn insert(&mut self, key: OrderedRow, value: Row, _epoch: u64) -> Result<()> {
        let have_key_on_storage = self.total_count > self.top_n.len();
        let need_to_flush = if have_key_on_storage {
            // It is impossible that the cache is empty.
            let bottom_key = self.bottom_element().unwrap().0;
            match TOP_N_TYPE {
                TOP_N_MIN => key > *bottom_key,
                TOP_N_MAX => key < *bottom_key,
                _ => unreachable!(),
            }
        } else {
            false
        };
        // If there may be other keys between `key` and `bottom_key` in the storage,
        // we cannot insert `key` into cache. Instead, we have to flush it onto the storage.
        // This is because other keys may be more qualified to stay in cache.
        // TODO: This needs to be changed when transaction on Hummock is implemented.
        FlushStatus::do_insert(self.flush_buffer.entry(key.clone()), value.clone());
        if !need_to_flush {
            self.top_n.insert(key, value);
        }
        self.total_count += 1;
        Ok(())
    }

    /// This function is a temporary implementation to bypass the about-to-be-implemented
    /// transaction layer of Hummock.
    ///
    /// This function scans kv pairs from the storage, and properly deal with them
    /// according to the flush buffer.
    pub async fn scan_and_merge(&mut self, epoch: u64) -> Result<()> {
        // For a key scanned from the storage,
        // 1. Not touched by flush buffer. Do nothing.
        // 2. Deleted by flush buffer. Do not go into cache.
        // 3. Overridden by flush buffer. Go into cache with the new value.
        // We remark that:
        // 1. if TOP_N_MIN, kv_pairs is sorted in ascending order.
        // 2. if TOP_N_MAX, kv_pairs is sorted in descending order.
        // while flush_buffer is always sorted in ascending order.
        // This `order` is defined by the order between two `OrderedRow`.
        // We have to scan all because the top n on the storage may have been deleted by the flush
        // buffer.
        let iter = self.keyspace.iter(epoch).await?;
        let mut pk_and_row_iter = PkAndRowIterator::<_, TOP_N_TYPE>::new(
            iter,
            &mut self.ordered_row_deserializer,
            &mut self.cell_based_row_deserializer,
        );
        match TOP_N_TYPE {
            TOP_N_MIN => {
                let mut flush_buffer_iter = self.flush_buffer.iter().peekable();
                while let Some((key_from_storage, row_from_storage)) =
                    pk_and_row_iter.next().await?
                {
                    // If we inserted enough values, break as we will only retain `top_n_count`
                    // elements in the cache.
                    if let Some(top_n_count) = self.top_n_count && self.top_n.len() >= top_n_count {
                        break;
                    }
                    let mut encounter_same_key = false;
                    while let Some((key_from_buffer, value_from_buffer)) = flush_buffer_iter.peek()
                    {
                        match key_from_buffer.cmp(&&key_from_storage) {
                            Ordering::Greater => {
                                // We need to break to determine whether the next `key_from_storage`
                                // will be shadowed by `key_from_buffer` in the next for loop.
                                break;
                            }
                            Ordering::Equal => {
                                // The `key_from_storage` is shadowed by `key_from_buffer`.
                                // We do not want to add `key_from_storage` into the result set
                                // after the while loop.
                                encounter_same_key = true;
                            }
                            _ => {
                                // do nothing
                            }
                        }
                        match value_from_buffer {
                            FlushStatus::Insert(row) | FlushStatus::DeleteInsert(row) => {
                                self.top_n.insert((*key_from_buffer).clone(), row.clone());
                            }
                            FlushStatus::Delete => {
                                // do nothing
                            }
                        }
                        flush_buffer_iter.next();
                    }
                    if !encounter_same_key {
                        self.top_n.insert(key_from_storage, row_from_storage);
                    }
                }
            }
            TOP_N_MAX => {
                let mut flush_buffer_iter = self.flush_buffer.iter().rev().peekable();
                while let Some((key_from_storage, row_from_storage)) =
                    pk_and_row_iter.next().await?
                {
                    if let Some(top_n_count) = self.top_n_count && self.top_n.len() >= top_n_count {
                        break;
                    }
                    let mut encounter_same_key = false;
                    // This is similar to `TOP_N_MIN` branch above. May refer to its comments for
                    // the code below.
                    while let Some((key_from_buffer, value_from_buffer)) = flush_buffer_iter.peek()
                    {
                        match key_from_buffer.cmp(&&key_from_storage) {
                            Ordering::Less => {
                                break;
                            }
                            Ordering::Equal => {
                                encounter_same_key = true;
                            }
                            _ => {
                                // do nothing
                            }
                        }
                        match value_from_buffer {
                            FlushStatus::Insert(row) | FlushStatus::DeleteInsert(row) => {
                                self.top_n.insert((*key_from_buffer).clone(), row.clone());
                            }
                            FlushStatus::Delete => {
                                // do nothing
                            }
                        }
                        flush_buffer_iter.next();
                    }
                    if !encounter_same_key {
                        self.top_n.insert(key_from_storage, row_from_storage);
                    }
                }
            }
            _ => unreachable!(),
        }
        Ok(())
    }

    pub async fn delete(&mut self, key: &OrderedRow, epoch: u64) -> Result<Option<Row>> {
        let prev_entry = self.top_n.remove(key);
        FlushStatus::do_delete(self.flush_buffer.entry(key.clone()));
        self.total_count -= 1;
        // If we have nothing in the cache, we have to scan from the storage.
        if self.top_n.is_empty() && self.total_count > 0 {
            self.scan_and_merge(epoch).await?;
            self.retain_top_n();
        }
        Ok(prev_entry)
    }

    /// We can fill in the cache from storage only when state is not dirty, i.e. right after
    /// `flush`.
    ///
    /// We don't need to care about whether `self.top_n` is empty or not as the key is unique.
    /// An element with duplicated key scanned from the storage would just override the element with
    /// the same key in the cache, and their value must be the same.
    pub async fn fill_in_cache(&mut self, epoch: u64) -> Result<()> {
        debug_assert!(!self.is_dirty());
        let iter = self.keyspace.iter(epoch).await?;
        let mut pk_and_row_iter = PkAndRowIterator::<_, TOP_N_TYPE>::new(
            iter,
            &mut self.ordered_row_deserializer,
            &mut self.cell_based_row_deserializer,
        );
        while let Some((pk, row)) = pk_and_row_iter.next().await? {
            let prev_row = self.top_n.insert(pk, row.clone());
            if let Some(prev_row) = prev_row {
                debug_assert_eq!(prev_row, row);
            }
            if let Some(top_n_count) = self.top_n_count && top_n_count == self.top_n.len() {
                break;
            }
        }
        Ok(())
    }

    async fn flush_inner(
        &mut self,
        iterator: impl Iterator<Item = (OrderedRow, FlushStatus<Row>)>,
        epoch: u64,
    ) -> Result<()> {
        let mut write_batch = self.keyspace.state_store().start_write_batch();
        let mut local = write_batch.prefixify(&self.keyspace);
        for (pk, cells) in iterator {
            let row = cells.into_option();
            println!("---1 flush inner write pk = {:?}", pk);
            let pk_buf = match TOP_N_TYPE {
                TOP_N_MIN => pk.serialize(),
                TOP_N_MAX => pk.reverse_serialize(),
                _ => unreachable!(),
            }?;
            let column_ids = (0..self.data_types.len() as i32)
                .map(ColumnId::from)
                .collect::<Vec<_>>();
            let bytes = serialize_pk_and_row_state(&pk_buf, &row, &column_ids)?;
            println!("--- 2flush inner write pk = {:?}", pk_buf);
            for (key, value) in bytes {
                match value {
                    // TODO(Yuanxin): Implement value meta
                    Some(val) => local.put(key, StorageValue::new_default_put(val)),
                    None => local.delete(key),
                }
            }
        }
        write_batch.ingest(epoch).await.unwrap();
        Ok(())
    }

    /// `Flush` can be called by the executor when it receives a barrier and thus needs to
    /// checkpoint.
    ///
    /// TODO: `Flush` should also be called internally when `top_n` and `flush_buffer` exceeds
    /// certain limit.
    pub async fn flush(&mut self, epoch: u64) -> Result<()> {
        if !self.is_dirty() {
            self.retain_top_n();
            return Ok(());
        }

        let iterator = std::mem::take(&mut self.flush_buffer).into_iter();
        self.flush_inner(iterator, epoch).await?;

        self.retain_top_n();
        Ok(())
    }
}

/// Test-related methods
impl<S: StateStore, const TOP_N_TYPE: usize> ManagedTopNState<S, TOP_N_TYPE> {
    #[cfg(test)]
    fn get_cache_len(&self) -> usize {
        self.top_n.len()
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::catalog::ColumnDesc;
    use risingwave_common::types::DataType;
    use risingwave_common::util::sort_util::OrderType;
    use risingwave_storage::memory::MemoryStateStore;
    use risingwave_storage::{Keyspace, StateStore};

    use super::super::variants::TOP_N_MAX;
    use super::*;
    use crate::row_nonnull;

    fn create_managed_top_n_state<S: StateStore, const TOP_N_TYPE: usize>(
        store: &S,
        row_count: usize,
        data_types: Vec<DataType>,
        order_types: Vec<OrderType>,
    ) -> ManagedTopNState<S, TOP_N_TYPE> {
        let ordered_row_deserializer = OrderedRowDeserializer::new(data_types.clone(), order_types);
        let table_column_descs = data_types
            .iter()
            .enumerate()
            .map(|(id, data_type)| {
                ColumnDesc::unnamed(ColumnId::from(id as i32), data_type.clone())
            })
            .collect::<Vec<_>>();
        let cell_based_row_deserializer = CellBasedRowDeserializer::new(table_column_descs);

        ManagedTopNState::<S, TOP_N_TYPE>::new(
            Some(2),
            row_count,
            Keyspace::executor_root(store.clone(), 0x2333),
            data_types,
            ordered_row_deserializer,
            cell_based_row_deserializer,
        )
    }

    #[tokio::test]
    async fn test_managed_top_n_state() {
        let store = MemoryStateStore::new();
        let data_types = vec![DataType::Varchar, DataType::Int64];
        let order_types = vec![OrderType::Descending, OrderType::Ascending];

        let mut managed_state = create_managed_top_n_state::<_, TOP_N_MAX>(
            &store,
            0,
            data_types.clone(),
            order_types.clone(),
        );

        let row1 = row_nonnull!["abc".to_string(), 2i64];
        let row2 = row_nonnull!["abc".to_string(), 3i64];
        let row3 = row_nonnull!["abd".to_string(), 3i64];
        let row4 = row_nonnull!["ab".to_string(), 4i64];
        let rows = vec![row1, row2, row3, row4];
        let ordered_rows = rows
            .clone()
            .into_iter()
            .map(|row| OrderedRow::new(row, &order_types))
            .collect::<Vec<_>>();

        let epoch = 0;
        managed_state
            .insert(ordered_rows[3].clone(), rows[3].clone(), epoch)
            .await
            .unwrap();
        // now ("ab", 4)

        assert_eq!(
            managed_state.top_element(),
            Some((&ordered_rows[3], &rows[3]))
        );
        assert!(managed_state.is_dirty());
        assert_eq!(managed_state.get_cache_len(), 1);

        managed_state
            .insert(ordered_rows[2].clone(), rows[2].clone(), epoch)
            .await
            .unwrap();
        // now ("abd", 3) -> ("ab", 4)

        assert_eq!(
            managed_state.top_element(),
            Some((&ordered_rows[3], &rows[3]))
        );
        assert!(managed_state.is_dirty());
        assert_eq!(managed_state.get_cache_len(), 2);

        managed_state
            .insert(ordered_rows[1].clone(), rows[1].clone(), epoch)
            .await
            .unwrap();
        // now ("abd", 3) -> ("abc", 3) -> ("ab", 4)
        let epoch: u64 = 0;

        assert_eq!(
            managed_state.top_element(),
            Some((&ordered_rows[3], &rows[3]))
        );
        assert_eq!(managed_state.get_cache_len(), 3);
        managed_state.flush(epoch).await.unwrap();
        assert!(!managed_state.is_dirty());
        let row_count = managed_state.total_count;
        assert_eq!(row_count, 3);
        // After flush, only 2 elements should be kept in the cache.
        assert_eq!(managed_state.get_cache_len(), 2);

        drop(managed_state);
        let mut managed_state = create_managed_top_n_state::<_, TOP_N_MAX>(
            &store,
            row_count,
            data_types.clone(),
            order_types.clone(),
        );
        assert_eq!(managed_state.top_element(), None);
        managed_state.fill_in_cache(epoch).await.unwrap();
        // now ("abd", 3) on storage -> ("abc", 3) in memory -> ("ab", 4) in memory
        assert_eq!(
            managed_state.top_element(),
            Some((&ordered_rows[3], &rows[3]))
        );
        // Right after recovery.
        assert!(!managed_state.is_dirty());
        assert_eq!(managed_state.get_cache_len(), 2);
        assert_eq!(managed_state.total_count, 3);

        assert_eq!(
            managed_state.pop_top_element(epoch).await.unwrap(),
            Some((ordered_rows[3].clone(), rows[3].clone()))
        );
        // now ("abd", 3) on storage -> ("abc", 3) in memory
        assert!(managed_state.is_dirty());
        assert_eq!(managed_state.total_count, 2);
        assert_eq!(managed_state.get_cache_len(), 1);
        assert_eq!(
            managed_state.pop_top_element(epoch).await.unwrap(),
            Some((ordered_rows[1].clone(), rows[1].clone()))
        );
        // now ("abd", 3) on storage
        // Popping to 0 element but automatically get at most `2` elements from the storage.
        // However, here we only have one element left as the `total_count` indicates.
        // The state is dirty as we didn't flush.
        assert!(managed_state.is_dirty());
        assert_eq!(managed_state.total_count, 1);
        assert_eq!(managed_state.get_cache_len(), 1);
        // now ("abd", 3) in memory

        assert_eq!(
            managed_state.top_element(),
            Some((&ordered_rows[2], &rows[2]))
        );

        managed_state
            .insert(ordered_rows[0].clone(), rows[0].clone(), epoch)
            .await
            .unwrap();
        // now ("abd", 3) in memory -> ("abc", 2)
        assert_eq!(
            managed_state.top_element(),
            Some((&ordered_rows[0], &rows[0]))
        );

        // Exclude the last `insert` as the state crashes before recovery.
        let row_count = managed_state.total_count - 1;
        drop(managed_state);
        let mut managed_state =
            create_managed_top_n_state::<_, TOP_N_MAX>(&store, row_count, data_types, order_types);
        managed_state.fill_in_cache(epoch).await.unwrap();
        assert_eq!(
            managed_state.top_element(),
            Some((&ordered_rows[3], &rows[3]))
        );
    }
}

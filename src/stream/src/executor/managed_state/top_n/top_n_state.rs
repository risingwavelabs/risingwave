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

use std::collections::BTreeMap;
use std::ops::Index;

use futures::pin_mut;
use futures::stream::StreamExt;
use itertools::Itertools;
use risingwave_common::array::Row;
use risingwave_common::catalog::{ColumnDesc, ColumnId, TableId};
use risingwave_common::types::DataType;
use risingwave_common::util::ordered::*;
use risingwave_common::util::sort_util::OrderType;
use risingwave_storage::table::streaming_table::state_table::StateTable;
use risingwave_storage::StateStore;

use super::variants::*;
use crate::executor::error::StreamExecutorResult;
use crate::executor::PkIndices;

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
    /// Relational table.
    state_table: StateTable<S>,
    /// The number of elements in both cache and storage.
    total_count: usize,
    /// Number of entries to retain in memory after each flush.
    top_n_count: Option<usize>,
    /// For deserializing `OrderedRow`.
    ordered_row_deserializer: OrderedRowDeserializer,
}

impl<S: StateStore, const TOP_N_TYPE: usize> ManagedTopNState<S, TOP_N_TYPE> {
    pub fn new(
        top_n_count: Option<usize>,
        total_count: usize,
        store: S,
        table_id: TableId,
        data_types: Vec<DataType>,
        ordered_row_deserializer: OrderedRowDeserializer,
        pk_indices: PkIndices,
    ) -> Self {
        let order_types = match TOP_N_TYPE {
            TOP_N_MIN => ordered_row_deserializer.get_order_types().to_vec(),
            TOP_N_MAX => ordered_row_deserializer
                .get_order_types()
                .to_vec()
                .iter()
                .map(|t| match *t {
                    OrderType::Ascending => OrderType::Descending,
                    OrderType::Descending => OrderType::Ascending,
                })
                .collect_vec(),
            _ => unreachable!(),
        };

        let column_descs = data_types
            .iter()
            .enumerate()
            .map(|(id, data_type)| {
                ColumnDesc::unnamed(ColumnId::from(id as i32), data_type.clone())
            })
            .collect::<Vec<_>>();
        let state_table = StateTable::new_without_distribution(
            store,
            table_id,
            column_descs,
            order_types,
            pk_indices,
        );
        Self {
            top_n: BTreeMap::new(),
            state_table,
            total_count,
            top_n_count,
            ordered_row_deserializer,
        }
    }

    pub fn total_count(&self) -> usize {
        self.total_count
    }

    pub fn is_dirty(&self) -> bool {
        self.state_table.is_dirty()
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

    pub async fn pop_top_element(
        &mut self,
        epoch: u64,
    ) -> StreamExecutorResult<Option<(OrderedRow, Row)>> {
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
            let row = match TOP_N_TYPE {
                TOP_N_MIN => self.top_n.first_key_value().unwrap().1.clone(),
                TOP_N_MAX => self.top_n.last_key_value().unwrap().1.clone(),
                _ => unreachable!(),
            };
            let value = self.delete(&key, row, epoch).await?;
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

    pub fn insert(&mut self, key: OrderedRow, value: Row, _epoch: u64) -> StreamExecutorResult<()> {
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
        self.state_table.insert(value.clone())?;
        if !need_to_flush {
            self.top_n.insert(key, value);
        }
        self.total_count += 1;
        Ok(())
    }

    /// This function scans rows by `StateTableRowIter`, which scan rows from the
    /// `shared_storage`(`cell_based_table`) and memory(`mem_table`) .
    pub async fn scan_from_relational_table(&mut self, epoch: u64) -> StreamExecutorResult<()> {
        let state_table_iter = self.state_table.iter(epoch).await?;
        pin_mut!(state_table_iter);

        loop {
            if let Some(top_n_count) = self.top_n_count && self.top_n.len() >= top_n_count {
                    break;
                }
            match state_table_iter.next().await {
                Some(next_res) => {
                    let row = next_res.unwrap().into_owned();
                    let mut datums = vec![];
                    for pk_index in self.state_table.pk_indices() {
                        datums.push(row.index(*pk_index).clone());
                    }
                    let pk = Row::new(datums);
                    let pk_ordered =
                        OrderedRow::new(pk, self.ordered_row_deserializer.get_order_types());
                    self.top_n.insert(pk_ordered, row);
                }
                None => {
                    break;
                }
            }
        }
        Ok(())
    }

    pub async fn delete(
        &mut self,
        key: &OrderedRow,
        value: Row,
        epoch: u64,
    ) -> StreamExecutorResult<Option<Row>> {
        let prev_entry = self.top_n.remove(key);
        self.state_table.delete(value)?;

        self.total_count -= 1;
        // If we have nothing in the cache, we have to scan from the storage.
        if self.top_n.is_empty() && self.total_count > 0 {
            self.scan_from_relational_table(epoch).await?;
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
    pub async fn fill_in_cache(&mut self, epoch: u64) -> StreamExecutorResult<()> {
        debug_assert!(!self.is_dirty());
        let state_table_iter = self.state_table.iter(epoch).await?;
        pin_mut!(state_table_iter);
        while let Some(res) = state_table_iter.next().await {
            let row = res.unwrap().into_owned();
            let mut datums = vec![];
            for pk_index in self.state_table.pk_indices() {
                datums.push(row.index(*pk_index).clone());
            }
            let pk = Row::new(datums);
            let pk_ordered = OrderedRow::new(pk, self.ordered_row_deserializer.get_order_types());
            let prev_row = self.top_n.insert(pk_ordered, row.clone());
            if let Some(prev_row) = prev_row {
                debug_assert_eq!(prev_row, row);
            }
            if let Some(top_n_count) = self.top_n_count && top_n_count == self.top_n.len() {
                break;
            }
        }
        Ok(())
    }

    /// `Flush` can be called by the executor when it receives a barrier and thus needs to
    /// checkpoint.
    ///
    /// TODO: `Flush` should also be called internally when `top_n` and `flush_buffer` exceeds
    /// certain limit.
    pub async fn flush(&mut self, epoch: u64) -> StreamExecutorResult<()> {
        if !self.is_dirty() {
            self.retain_top_n();
            return Ok(());
        }
        self.state_table.commit(epoch).await?;

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
    use risingwave_common::catalog::TableId;
    use risingwave_common::types::DataType;
    use risingwave_common::util::sort_util::OrderType;
    use risingwave_storage::memory::MemoryStateStore;
    use risingwave_storage::StateStore;

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

        ManagedTopNState::<S, TOP_N_TYPE>::new(
            Some(2),
            row_count,
            store.clone(),
            TableId::from(0x2333),
            data_types,
            ordered_row_deserializer,
            vec![0_usize, 1_usize],
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

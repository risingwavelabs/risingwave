// Copyright 2025 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::ops::Bound;

use futures::{pin_mut, StreamExt};
use risingwave_common::row::{OwnedRow, Row, RowExt};
use risingwave_common::types::ScalarImpl;
use risingwave_common::util::epoch::EpochPair;
use risingwave_storage::store::PrefetchOptions;
use risingwave_storage::StateStore;

use super::top_n_cache::CacheKey;
use super::{serialize_pk_to_cache_key, CacheKeySerde, GroupKey, TopNCache};
use crate::common::table::state_table::{StateTable, StateTablePostCommit};
use crate::executor::error::StreamExecutorResult;
use crate::executor::top_n::top_n_cache::Cache;

/// * For TopN, the storage key is: `[ order_by + remaining columns of pk ]`
/// * For group TopN, the storage key is: `[ group_key + order_by + remaining columns of pk ]`
///
/// The key in [`TopNCache`] is [`CacheKey`], which is `[ order_by|remaining columns of pk ]`, and
/// `group_key` is not included.
pub struct ManagedTopNState<S: StateStore> {
    /// Relational table.
    state_table: StateTable<S>,

    /// Used for serializing pk into `CacheKey`.
    cache_key_serde: CacheKeySerde,
}

#[derive(Clone, PartialEq, Debug)]
pub struct TopNStateRow {
    pub cache_key: CacheKey,
    pub row: OwnedRow,
}

impl TopNStateRow {
    pub fn new(cache_key: CacheKey, row: OwnedRow) -> Self {
        Self { cache_key, row }
    }
}

impl<S: StateStore> ManagedTopNState<S> {
    pub fn new(state_table: StateTable<S>, cache_key_serde: CacheKeySerde) -> Self {
        Self {
            state_table,
            cache_key_serde,
        }
    }

    /// Get the immutable reference of managed state table.
    pub fn table(&self) -> &StateTable<S> {
        &self.state_table
    }

    /// Init epoch for the managed state table.
    pub async fn init_epoch(&mut self, epoch: EpochPair) -> StreamExecutorResult<()> {
        self.state_table.init_epoch(epoch).await
    }

    /// Update watermark for the managed state table.
    pub fn update_watermark(&mut self, watermark: ScalarImpl) {
        self.state_table.update_watermark(watermark)
    }

    pub fn insert(&mut self, value: impl Row) {
        self.state_table.insert(value);
    }

    pub fn delete(&mut self, value: impl Row) {
        self.state_table.delete(value);
    }

    fn get_topn_row(&self, row: OwnedRow, group_key_len: usize) -> TopNStateRow {
        let pk = (&row).project(&self.state_table.pk_indices()[group_key_len..]);
        let cache_key = serialize_pk_to_cache_key(pk, &self.cache_key_serde);

        TopNStateRow::new(cache_key, row)
    }

    /// This function will return the rows in the range of [`offset`, `offset` + `limit`).
    ///
    /// If `group_key` is None, it will scan rows from the very beginning.
    /// Otherwise it will scan rows with prefix `group_key`.
    #[cfg(test)]
    pub async fn find_range(
        &self,
        group_key: Option<impl GroupKey>,
        offset: usize,
        limit: Option<usize>,
    ) -> StreamExecutorResult<Vec<TopNStateRow>> {
        let sub_range: &(Bound<OwnedRow>, Bound<OwnedRow>) = &(Bound::Unbounded, Bound::Unbounded);
        let state_table_iter = self
            .state_table
            .iter_with_prefix(&group_key, sub_range, Default::default())
            .await?;
        pin_mut!(state_table_iter);

        // here we don't expect users to have large OFFSET.
        let (mut rows, mut stream) = if let Some(limit) = limit {
            (
                Vec::with_capacity(limit.min(1024)),
                state_table_iter.skip(offset).take(limit),
            )
        } else {
            (
                Vec::with_capacity(1024),
                state_table_iter.skip(offset).take(1024),
            )
        };
        while let Some(item) = stream.next().await {
            rows.push(self.get_topn_row(item?.into_owned_row(), group_key.len()));
        }
        Ok(rows)
    }

    /// # Arguments
    ///
    /// * `group_key` - Used as the prefix of the key to scan. Only for group TopN.
    /// * `start_key` - The start point of the key to scan. It should be the last key of the middle
    ///   cache. It doesn't contain the group key.
    pub async fn fill_high_cache<const WITH_TIES: bool>(
        &self,
        group_key: Option<impl GroupKey>,
        topn_cache: &mut TopNCache<WITH_TIES>,
        start_key: Option<CacheKey>,
        cache_size_limit: usize,
    ) -> StreamExecutorResult<()> {
        let high_cache = &mut topn_cache.high;
        assert!(high_cache.is_empty());

        // TODO(rc): iterate from `start_key`
        let sub_range: &(Bound<OwnedRow>, Bound<OwnedRow>) = &(Bound::Unbounded, Bound::Unbounded);
        let state_table_iter = self
            .state_table
            .iter_with_prefix(
                &group_key,
                sub_range,
                PrefetchOptions {
                    prefetch: cache_size_limit == usize::MAX,
                    for_large_query: false,
                },
            )
            .await?;
        pin_mut!(state_table_iter);

        let mut group_row_count = 0;

        while let Some(item) = state_table_iter.next().await {
            group_row_count += 1;

            // Note(bugen): should first compare with start key before constructing TopNStateRow.
            let topn_row = self.get_topn_row(item?.into_owned_row(), group_key.len());
            if let Some(start_key) = start_key.as_ref()
                && &topn_row.cache_key <= start_key
            {
                continue;
            }
            high_cache.insert(topn_row.cache_key, (&topn_row.row).into());
            if high_cache.len() == cache_size_limit {
                break;
            }
        }

        if WITH_TIES && topn_cache.high_is_full() {
            let high_last_sort_key = topn_cache.high.last_key_value().unwrap().0 .0.clone();
            while let Some(item) = state_table_iter.next().await {
                group_row_count += 1;

                let topn_row = self.get_topn_row(item?.into_owned_row(), group_key.len());
                if topn_row.cache_key.0 == high_last_sort_key {
                    topn_cache
                        .high
                        .insert(topn_row.cache_key, (&topn_row.row).into());
                } else {
                    break;
                }
            }
        }

        if state_table_iter.next().await.is_none() {
            // We can only update the row count when we have seen all rows of the group in the table.
            topn_cache.update_table_row_count(group_row_count);
        }

        Ok(())
    }

    pub async fn init_topn_cache<const WITH_TIES: bool>(
        &self,
        group_key: Option<impl GroupKey>,
        topn_cache: &mut TopNCache<WITH_TIES>,
    ) -> StreamExecutorResult<()> {
        assert!(topn_cache.low.as_ref().map(Cache::is_empty).unwrap_or(true));
        assert!(topn_cache.middle.is_empty());
        assert!(topn_cache.high.is_empty());

        let sub_range: &(Bound<OwnedRow>, Bound<OwnedRow>) = &(Bound::Unbounded, Bound::Unbounded);
        let state_table_iter = self
            .state_table
            .iter_with_prefix(
                &group_key,
                sub_range,
                PrefetchOptions {
                    prefetch: topn_cache.limit == usize::MAX,
                    for_large_query: false,
                },
            )
            .await?;
        pin_mut!(state_table_iter);

        let mut group_row_count = 0;

        if let Some(low) = &mut topn_cache.low {
            while let Some(item) = state_table_iter.next().await {
                group_row_count += 1;
                let topn_row = self.get_topn_row(item?.into_owned_row(), group_key.len());
                low.insert(topn_row.cache_key, (&topn_row.row).into());
                if low.len() == topn_cache.offset {
                    break;
                }
            }
        }

        assert!(topn_cache.limit > 0, "topn cache limit should always > 0");
        while let Some(item) = state_table_iter.next().await {
            group_row_count += 1;
            let topn_row = self.get_topn_row(item?.into_owned_row(), group_key.len());
            topn_cache
                .middle
                .insert(topn_row.cache_key, (&topn_row.row).into());
            if topn_cache.middle.len() == topn_cache.limit {
                break;
            }
        }
        if WITH_TIES && topn_cache.middle_is_full() {
            let middle_last_sort_key = topn_cache.middle.last_key_value().unwrap().0 .0.clone();
            while let Some(item) = state_table_iter.next().await {
                group_row_count += 1;
                let topn_row = self.get_topn_row(item?.into_owned_row(), group_key.len());
                if topn_row.cache_key.0 == middle_last_sort_key {
                    topn_cache
                        .middle
                        .insert(topn_row.cache_key, (&topn_row.row).into());
                } else {
                    topn_cache
                        .high
                        .insert(topn_row.cache_key, (&topn_row.row).into());
                    break;
                }
            }
        }

        assert!(
            topn_cache.high_cache_capacity > 0,
            "topn cache high_capacity should always > 0"
        );
        while !topn_cache.high_is_full()
            && let Some(item) = state_table_iter.next().await
        {
            group_row_count += 1;
            let topn_row = self.get_topn_row(item?.into_owned_row(), group_key.len());
            topn_cache
                .high
                .insert(topn_row.cache_key, (&topn_row.row).into());
        }
        if WITH_TIES && topn_cache.high_is_full() {
            let high_last_sort_key = topn_cache.high.last_key_value().unwrap().0 .0.clone();
            while let Some(item) = state_table_iter.next().await {
                group_row_count += 1;
                let topn_row = self.get_topn_row(item?.into_owned_row(), group_key.len());
                if topn_row.cache_key.0 == high_last_sort_key {
                    topn_cache
                        .high
                        .insert(topn_row.cache_key, (&topn_row.row).into());
                } else {
                    break;
                }
            }
        }

        if state_table_iter.next().await.is_none() {
            // After trying to initially fill in the cache, all table entries are in the cache,
            // we then get the precise table row count.
            topn_cache.update_table_row_count(group_row_count);
        }

        Ok(())
    }

    pub async fn flush(
        &mut self,
        epoch: EpochPair,
    ) -> StreamExecutorResult<StateTablePostCommit<'_, S>> {
        self.state_table.commit(epoch).await
    }

    pub async fn try_flush(&mut self) -> StreamExecutorResult<()> {
        self.state_table.try_flush().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::types::DataType;
    use risingwave_common::util::epoch::test_epoch;
    use risingwave_common::util::sort_util::{ColumnOrder, OrderType};

    use super::*;
    use crate::executor::test_utils::top_n_executor::create_in_memory_state_table;
    use crate::executor::top_n::top_n_cache::{TopNCacheTrait, TopNStaging};
    use crate::executor::top_n::{create_cache_key_serde, NO_GROUP_KEY};
    use crate::row_nonnull;

    fn cache_key_serde() -> CacheKeySerde {
        let data_types = vec![DataType::Varchar, DataType::Int64];
        let schema = Schema::new(data_types.into_iter().map(Field::unnamed).collect());
        let storage_key = vec![
            ColumnOrder::new(0, OrderType::ascending()),
            ColumnOrder::new(1, OrderType::ascending()),
        ];
        let order_by = vec![ColumnOrder::new(0, OrderType::ascending())];

        create_cache_key_serde(&storage_key, &schema, &order_by, &[])
    }

    #[tokio::test]
    async fn test_managed_top_n_state() {
        let state_table = {
            let mut tb = create_in_memory_state_table(
                &[DataType::Varchar, DataType::Int64],
                &[OrderType::ascending(), OrderType::ascending()],
                &[0, 1],
            )
            .await;
            tb.init_epoch(EpochPair::new_test_epoch(test_epoch(1)))
                .await
                .unwrap();
            tb
        };

        let cache_key_serde = cache_key_serde();
        let mut managed_state = ManagedTopNState::new(state_table, cache_key_serde.clone());

        let row1 = row_nonnull!["abc", 2i64];
        let row2 = row_nonnull!["abc", 3i64];
        let row3 = row_nonnull!["abd", 3i64];
        let row4 = row_nonnull!["ab", 4i64];

        let row1_bytes = serialize_pk_to_cache_key(row1.clone(), &cache_key_serde);
        let row2_bytes = serialize_pk_to_cache_key(row2.clone(), &cache_key_serde);
        let row3_bytes = serialize_pk_to_cache_key(row3.clone(), &cache_key_serde);
        let row4_bytes = serialize_pk_to_cache_key(row4.clone(), &cache_key_serde);
        let rows = [row1, row2, row3, row4];
        let ordered_rows = [row1_bytes, row2_bytes, row3_bytes, row4_bytes];
        managed_state.insert(rows[3].clone());

        // now ("ab", 4)
        let valid_rows = managed_state
            .find_range(NO_GROUP_KEY, 0, Some(1))
            .await
            .unwrap();

        assert_eq!(valid_rows.len(), 1);
        assert_eq!(valid_rows[0].cache_key, ordered_rows[3].clone());

        managed_state.insert(rows[2].clone());
        let valid_rows = managed_state
            .find_range(NO_GROUP_KEY, 1, Some(1))
            .await
            .unwrap();
        assert_eq!(valid_rows.len(), 1);
        assert_eq!(valid_rows[0].cache_key, ordered_rows[2].clone());

        managed_state.insert(rows[1].clone());

        let valid_rows = managed_state
            .find_range(NO_GROUP_KEY, 1, Some(2))
            .await
            .unwrap();
        assert_eq!(valid_rows.len(), 2);
        assert_eq!(
            valid_rows.first().unwrap().cache_key,
            ordered_rows[1].clone()
        );
        assert_eq!(
            valid_rows.last().unwrap().cache_key,
            ordered_rows[2].clone()
        );

        // delete ("abc", 3)
        managed_state.delete(rows[1].clone());

        // insert ("abc", 2)
        managed_state.insert(rows[0].clone());

        let valid_rows = managed_state
            .find_range(NO_GROUP_KEY, 0, Some(3))
            .await
            .unwrap();

        assert_eq!(valid_rows.len(), 3);
        assert_eq!(valid_rows[0].cache_key, ordered_rows[3].clone());
        assert_eq!(valid_rows[1].cache_key, ordered_rows[0].clone());
        assert_eq!(valid_rows[2].cache_key, ordered_rows[2].clone());
    }

    #[tokio::test]
    async fn test_managed_top_n_state_fill_cache() {
        let data_types = vec![DataType::Varchar, DataType::Int64];
        let state_table = {
            let mut tb = create_in_memory_state_table(
                &data_types,
                &[OrderType::ascending(), OrderType::ascending()],
                &[0, 1],
            )
            .await;
            tb.init_epoch(EpochPair::new_test_epoch(test_epoch(1)))
                .await
                .unwrap();
            tb
        };

        let cache_key_serde = cache_key_serde();
        let mut managed_state = ManagedTopNState::new(state_table, cache_key_serde.clone());

        let row1 = row_nonnull!["abc", 2i64];
        let row2 = row_nonnull!["abc", 3i64];
        let row3 = row_nonnull!["abd", 3i64];
        let row4 = row_nonnull!["ab", 4i64];
        let row5 = row_nonnull!["abcd", 5i64];

        let row1_bytes = serialize_pk_to_cache_key(row1.clone(), &cache_key_serde);
        let row2_bytes = serialize_pk_to_cache_key(row2.clone(), &cache_key_serde);
        let row3_bytes = serialize_pk_to_cache_key(row3.clone(), &cache_key_serde);
        let row4_bytes = serialize_pk_to_cache_key(row4.clone(), &cache_key_serde);
        let row5_bytes = serialize_pk_to_cache_key(row5.clone(), &cache_key_serde);
        let rows = [row1, row2, row3, row4, row5];
        let ordered_rows = vec![row1_bytes, row2_bytes, row3_bytes, row4_bytes, row5_bytes];

        let mut cache = TopNCache::<false>::new(1, 1, data_types);

        managed_state.insert(rows[3].clone());
        managed_state.insert(rows[1].clone());
        managed_state.insert(rows[2].clone());
        managed_state.insert(rows[4].clone());

        managed_state
            .fill_high_cache(NO_GROUP_KEY, &mut cache, Some(ordered_rows[3].clone()), 2)
            .await
            .unwrap();
        assert_eq!(cache.high.len(), 2);
        assert_eq!(cache.high.first_key_value().unwrap().0, &ordered_rows[1]);
        assert_eq!(cache.high.last_key_value().unwrap().0, &ordered_rows[4]);
    }

    #[tokio::test]
    async fn test_top_n_cache_limit_1() {
        let data_types = vec![DataType::Varchar, DataType::Int64];
        let state_table = {
            let mut tb = create_in_memory_state_table(
                &data_types,
                &[OrderType::ascending(), OrderType::ascending()],
                &[0, 1],
            )
            .await;
            tb.init_epoch(EpochPair::new_test_epoch(test_epoch(1)))
                .await
                .unwrap();
            tb
        };

        let cache_key_serde = cache_key_serde();
        let mut managed_state = ManagedTopNState::new(state_table, cache_key_serde.clone());

        let row1 = row_nonnull!["abc", 2i64];
        let row1_bytes = serialize_pk_to_cache_key(row1.clone(), &cache_key_serde);

        let mut cache = TopNCache::<true>::new(0, 1, data_types);
        cache.insert(row1_bytes.clone(), row1.clone(), &mut TopNStaging::new());
        cache
            .delete(
                NO_GROUP_KEY,
                &mut managed_state,
                row1_bytes,
                row1,
                &mut TopNStaging::new(),
            )
            .await
            .unwrap();
    }
}

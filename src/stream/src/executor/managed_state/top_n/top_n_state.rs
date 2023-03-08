// Copyright 2023 RisingWave Labs
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

use futures::{pin_mut, StreamExt};
use risingwave_common::row::{OwnedRow, Row, RowExt};
use risingwave_common::util::epoch::EpochPair;
use risingwave_storage::store::PrefetchOptions;
use risingwave_storage::StateStore;

use crate::common::table::state_table::StateTable;
use crate::executor::error::StreamExecutorResult;
use crate::executor::managed_state::top_n::GroupKey;
use crate::executor::top_n::{serialize_pk_to_cache_key, CacheKey, CacheKeySerde, TopNCache};

/// * For TopN, the storage key is: `[ order_by + remaining columns of pk ]`
/// * For group TopN, the storage key is: `[ group_key + order_by + remaining columns of pk ]`
///
/// The key in [`TopNCache`] is [`CacheKey`], which is `[ order_by|remaining columns of pk ]`, and
/// `group_key` is not included.
pub struct ManagedTopNState<S: StateStore> {
    /// Relational table.
    pub(crate) state_table: StateTable<S>,

    /// Used for serializing pk into CacheKey.
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
        let state_table_iter = self
            .state_table
            .iter_with_pk_prefix(&group_key, Default::default())
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
            rows.push(self.get_topn_row(item?, group_key.len()));
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
        start_key: CacheKey,
        cache_size_limit: usize,
    ) -> StreamExecutorResult<()> {
        let cache = &mut topn_cache.high;
        let state_table_iter = self
            .state_table
            .iter_with_pk_prefix(
                &group_key,
                PrefetchOptions {
                    exhaust_iter: cache_size_limit == usize::MAX,
                },
            )
            .await?;
        pin_mut!(state_table_iter);
        while let Some(item) = state_table_iter.next().await {
            // Note(bugen): should first compare with start key before constructing TopNStateRow.
            let topn_row = self.get_topn_row(item?, group_key.len());
            if topn_row.cache_key <= start_key {
                continue;
            }
            // let row= &topn_row.row;
            cache.insert(topn_row.cache_key, (&topn_row.row).into());
            if cache.len() == cache_size_limit {
                break;
            }
        }
        if WITH_TIES && topn_cache.is_high_cache_full() {
            let high_last_sort_key = topn_cache.high.last_key_value().unwrap().0 .0.clone();
            while let Some(item) = state_table_iter.next().await {
                let topn_row = self.get_topn_row(item?, group_key.len());
                if topn_row.cache_key.0 == high_last_sort_key {
                    topn_cache
                        .high
                        .insert(topn_row.cache_key, (&topn_row.row).into());
                } else {
                    break;
                }
            }
        }

        Ok(())
    }

    pub async fn init_topn_cache<const WITH_TIES: bool>(
        &self,
        group_key: Option<impl GroupKey>,
        topn_cache: &mut TopNCache<WITH_TIES>,
    ) -> StreamExecutorResult<()> {
        assert!(topn_cache.low.is_empty());
        assert!(topn_cache.middle.is_empty());
        assert!(topn_cache.high.is_empty());

        let state_table_iter = self
            .state_table
            .iter_with_pk_prefix(
                &group_key,
                PrefetchOptions {
                    exhaust_iter: topn_cache.limit == usize::MAX,
                },
            )
            .await?;
        pin_mut!(state_table_iter);
        if topn_cache.offset > 0 {
            while let Some(item) = state_table_iter.next().await {
                let topn_row = self.get_topn_row(item?, group_key.len());
                topn_cache
                    .low
                    .insert(topn_row.cache_key, (&topn_row.row).into());
                if topn_cache.low.len() == topn_cache.offset {
                    break;
                }
            }
        }

        assert!(topn_cache.limit > 0, "topn cache limit should always > 0");
        while let Some(item) = state_table_iter.next().await {
            let topn_row = self.get_topn_row(item?, group_key.len());
            topn_cache
                .middle
                .insert(topn_row.cache_key, (&topn_row.row).into());
            if topn_cache.middle.len() == topn_cache.limit {
                break;
            }
        }
        if WITH_TIES && topn_cache.is_middle_cache_full() {
            let middle_last_sort_key = topn_cache.middle.last_key_value().unwrap().0 .0.clone();
            while let Some(item) = state_table_iter.next().await {
                let topn_row = self.get_topn_row(item?, group_key.len());
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
            topn_cache.high_capacity > 0,
            "topn cache high_capacity should always > 0"
        );
        while !topn_cache.is_high_cache_full() && let Some(item) = state_table_iter.next().await {
            let topn_row = self.get_topn_row(item?, group_key.len());
            topn_cache.high.insert(topn_row.cache_key, (&topn_row.row).into());
        }
        if WITH_TIES && topn_cache.is_high_cache_full() {
            let high_last_sort_key = topn_cache.high.last_key_value().unwrap().0 .0.clone();
            while let Some(item) = state_table_iter.next().await {
                let topn_row = self.get_topn_row(item?, group_key.len());
                if topn_row.cache_key.0 == high_last_sort_key {
                    topn_cache
                        .high
                        .insert(topn_row.cache_key, (&topn_row.row).into());
                } else {
                    break;
                }
            }
        }

        Ok(())
    }

    pub async fn flush(&mut self, epoch: EpochPair) -> StreamExecutorResult<()> {
        self.state_table.commit(epoch).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::types::DataType;
    use risingwave_common::util::sort_util::{OrderPair, OrderType};

    // use std::collections::BTreeMap;
    use super::*;
    use crate::executor::managed_state::top_n::NO_GROUP_KEY;
    use crate::executor::test_utils::top_n_executor::create_in_memory_state_table;
    use crate::executor::top_n::create_cache_key_serde;
    use crate::row_nonnull;

    fn cache_key_serde() -> CacheKeySerde {
        let data_types = vec![DataType::Varchar, DataType::Int64];
        let schema = Schema::new(data_types.into_iter().map(Field::unnamed).collect());
        let storage_key = vec![
            OrderPair::new(0, OrderType::ascending()),
            OrderPair::new(1, OrderType::ascending()),
        ];
        let pk = vec![0, 1];
        let order_by = vec![OrderPair::new(0, OrderType::ascending())];

        create_cache_key_serde(&storage_key, &pk, &schema, &order_by, &[])
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
            tb.init_epoch(EpochPair::new_test_epoch(1));
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
        let rows = vec![row1, row2, row3, row4];
        let ordered_rows = vec![row1_bytes, row2_bytes, row3_bytes, row4_bytes];
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
        let state_table = {
            let mut tb = create_in_memory_state_table(
                &[DataType::Varchar, DataType::Int64],
                &[OrderType::ascending(), OrderType::ascending()],
                &[0, 1],
            )
            .await;
            tb.init_epoch(EpochPair::new_test_epoch(1));
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
        let rows = vec![row1, row2, row3, row4, row5];
        let ordered_rows = vec![row1_bytes, row2_bytes, row3_bytes, row4_bytes, row5_bytes];

        let mut cache = TopNCache::<false>::new(1, 1);

        managed_state.insert(rows[3].clone());
        managed_state.insert(rows[1].clone());
        managed_state.insert(rows[2].clone());
        managed_state.insert(rows[4].clone());

        managed_state
            .fill_high_cache(NO_GROUP_KEY, &mut cache, ordered_rows[3].clone(), 2)
            .await
            .unwrap();
        assert_eq!(cache.high.len(), 2);
        assert_eq!(cache.high.first_key_value().unwrap().0, &ordered_rows[1]);
        assert_eq!(cache.high.last_key_value().unwrap().0, &ordered_rows[4]);
    }
}

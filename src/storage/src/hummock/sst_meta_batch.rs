// Copyright 2026 RisingWave Labs
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

use std::collections::HashMap;

use parking_lot::Mutex;
use risingwave_common::config::storage::ReadBatchConfig;
use risingwave_hummock_sdk::HummockSstableObjectId;
use risingwave_hummock_sdk::sstable_info::SstableInfo;

use super::{HummockResult, SstableStoreRef, TableHolder};
use crate::monitor::StoreLocalStatistic;

/// Ready metadata handles shared by independent key reads in one `MultiGet`.
/// Foyer owns loading and single-flight. The batch retains handles until it is dropped.
/// Active reads can hold their own handles independently of the retention budget.
pub(super) struct SstMetaBatch {
    config: ReadBatchConfig,
    inner: Mutex<Metadata>,
}

#[derive(Default)]
struct Metadata {
    entries: HashMap<HummockSstableObjectId, TableHolder>,
    regular_bytes: usize,
    oversized: Option<TableHolder>,
}

impl SstMetaBatch {
    pub(super) fn new(config: ReadBatchConfig) -> Self {
        Self {
            config,
            inner: Mutex::new(Metadata::default()),
        }
    }

    pub(super) async fn sstable(
        &self,
        store: &SstableStoreRef,
        info: &SstableInfo,
        stats: &mut StoreLocalStatistic,
    ) -> HummockResult<TableHolder> {
        if !self.config.meta_reuse {
            return store.sstable(info, stats).await;
        }
        let object_id = info.object_id;
        let max_entries = self.config.meta_max_entries.get();
        let max_bytes = self.config.meta_max_bytes.get();
        let (available_bytes, oversized_slot_free) = {
            let inner = self.inner.lock();
            let oversized = inner
                .oversized
                .as_ref()
                .filter(|table| table.id == object_id);
            if let Some(table) = inner.entries.get(&object_id).or(oversized) {
                stats.cache_meta_batch_ready += 1;
                return Ok(table.clone());
            }
            (
                (inner.entries.len() < max_entries).then_some(max_bytes - inner.regular_bytes),
                inner.oversized.is_none(),
            )
        };
        let table = store.sstable(info, stats).await?;
        // Foyer already weighed the decoded metadata when it created the entry.
        let bytes = table.weight();
        let oversized = bytes > max_bytes;
        let can_retain = if oversized {
            oversized_slot_free
        } else {
            available_bytes.is_some_and(|available| bytes <= available)
        };
        // Retained handles live until batch end, so capacity can only shrink.
        if !can_retain {
            stats.cache_meta_batch_budget_reject += 1;
            return Ok(table);
        }

        // Other reads may have retained a handle or used the remaining budget during the fetch.
        let mut inner = self.inner.lock();
        if oversized {
            // Keep the first oversized handle. Rotating this slot churns when keys
            // visit multiple oversized SSTs in the same batch.
            match &inner.oversized {
                None => {
                    inner.oversized = Some(table.clone());
                    stats.cache_meta_batch_oversized += 1;
                    stats.cache_meta_batch_retained += 1;
                }
                Some(entry) if entry.id != object_id => stats.cache_meta_batch_budget_reject += 1,
                Some(_) => {}
            }
        } else if !inner.entries.contains_key(&object_id) {
            if inner.entries.len() < max_entries && bytes <= max_bytes - inner.regular_bytes {
                inner.regular_bytes += bytes;
                inner.entries.insert(object_id, table.clone());
                stats.cache_meta_batch_retained += 1;
            } else {
                stats.cache_meta_batch_budget_reject += 1;
            }
        }
        Ok(table)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hummock::iterator::test_utils::mock_sstable_store;
    use crate::hummock::test_utils::{default_builder_opt_for_test, gen_default_test_sstable};

    #[tokio::test]
    async fn test_batch_metadata_retains_completed_foyer_fetch() {
        use futures::FutureExt;
        use tokio::sync::oneshot;

        use crate::hummock::{Sstable, SstableMetaCacheValue};

        let store = mock_sstable_store().await;
        let (table, info) =
            gen_default_test_sstable(default_builder_opt_for_test(), 0, store.clone()).await;
        let (_, other_info) =
            gen_default_test_sstable(default_builder_opt_for_test(), 1, store.clone()).await;
        for cancel_first in [false, true] {
            store.clear_meta_cache().await.unwrap();
            let (release, released) = oneshot::channel();
            let value = SstableMetaCacheValue::from(Sstable::clone(&table));
            // Hold Foyer's producer pending. Both batch reads wait through Foyer.
            let mut fetch = Box::pin(store.meta_cache().get_or_fetch(
                &info.object_id,
                || async move {
                    released.await.unwrap();
                    Ok::<_, anyhow::Error>(value)
                },
            ));
            assert!(fetch.as_mut().now_or_never().is_none());
            let batch = SstMetaBatch::new(ReadBatchConfig {
                meta_reuse: true,
                ..Default::default()
            });
            let mut first_stats = StoreLocalStatistic::default();
            let mut second_stats = StoreLocalStatistic::default();
            let mut first = Box::pin(batch.sstable(&store, &info, &mut first_stats));
            let mut second = Box::pin(batch.sstable(&store, &info, &mut second_stats));
            assert!(first.as_mut().now_or_never().is_none());
            assert!(second.as_mut().now_or_never().is_none());
            // An unrelated SST must not wait for this load's completion.
            let mut other_stats = StoreLocalStatistic::default();
            tokio::time::timeout(
                std::time::Duration::from_secs(10),
                batch.sstable(&store, &other_info, &mut other_stats),
            )
            .await
            .unwrap()
            .unwrap();
            if cancel_first {
                drop(first);
                release.send(()).unwrap();
                second.await.unwrap();
            } else {
                release.send(()).unwrap();
                let (a, b) = futures::future::join(first, second).await;
                assert_eq!(a.unwrap().id, b.unwrap().id);
            }
            fetch.await.unwrap();
            assert_eq!(first_stats.cache_meta_block_total, 1);
            assert_eq!(second_stats.cache_meta_block_total, 1);
            assert_eq!(
                first_stats.cache_meta_batch_retained + second_stats.cache_meta_batch_retained,
                1
            );
            let mut reused = StoreLocalStatistic::default();
            batch.sstable(&store, &info, &mut reused).await.unwrap();
            assert_eq!(reused.cache_meta_block_total, 0);
        }
    }

    #[tokio::test]
    async fn test_batch_metadata_reuse_is_bounded() {
        let store = mock_sstable_store().await;
        let (table, info) =
            gen_default_test_sstable(default_builder_opt_for_test(), 0, store.clone()).await;
        let batch = SstMetaBatch::new(ReadBatchConfig {
            meta_reuse: true,
            meta_max_entries: 3.try_into().unwrap(),
            ..Default::default()
        });
        let mut stats = StoreLocalStatistic::default();
        for id in 0..=batch.config.meta_max_entries.get() {
            let mut inner = info.get_inner();
            inner.object_id = (id as u64).into();
            let info: SstableInfo = inner.into();
            store.insert_meta_cache(info.object_id, table.meta.clone());
            batch.sstable(&store, &info, &mut stats).await.unwrap();
            batch.sstable(&store, &info, &mut stats).await.unwrap();
            // The configured first three are reused; a full batch still reads subsequent SSTs normally.
            assert_eq!(
                stats.cache_meta_block_total,
                (id + 1 + usize::from(id == batch.config.meta_max_entries.get())) as u64
            );
        }
        {
            let inner = batch.inner.lock();
            assert_eq!(inner.entries.len(), batch.config.meta_max_entries.get());
            assert!(inner.regular_bytes <= batch.config.meta_max_bytes.get());
        }

        assert!(stats.cache_meta_batch_ready > 0);
        assert!(stats.cache_meta_batch_budget_reject > 0);
        assert_eq!(
            stats.cache_meta_batch_retained,
            batch.config.meta_max_entries.get() as u64
        );
        let disabled = SstMetaBatch::new(ReadBatchConfig {
            meta_reuse: false,
            ..batch.config
        });
        let mut disabled_stats = StoreLocalStatistic::default();
        disabled
            .sstable(&store, &info, &mut disabled_stats)
            .await
            .unwrap();
        disabled
            .sstable(&store, &info, &mut disabled_stats)
            .await
            .unwrap();
        assert_eq!(disabled_stats.cache_meta_block_total, 2);
        assert_eq!(disabled_stats.cache_meta_batch_ready, 0);
        assert!(disabled.inner.lock().entries.is_empty());

        // One large SST can still be retained after the regular budget fills.
        let batch = SstMetaBatch::new(ReadBatchConfig {
            meta_reuse: true,
            meta_max_entries: 1.try_into().unwrap(),
            meta_max_bytes: 65536.try_into().unwrap(),
            ..Default::default()
        });
        let mut stats = StoreLocalStatistic::default();
        batch.sstable(&store, &info, &mut stats).await.unwrap();
        let large_sstables = [1_u64, 2].map(|id| {
            let mut large_meta = table.meta.clone();
            large_meta.block_metas.reserve(
                batch.config.meta_max_bytes.get() / std::mem::size_of::<super::super::BlockMeta>()
                    + 1,
            );
            let mut large_info = info.get_inner();
            large_info.object_id = id.into();
            let large_info: SstableInfo = large_info.into();
            store.insert_meta_cache(large_info.object_id, large_meta);
            large_info
        });
        let [first, second] = &large_sstables;
        batch.sstable(&store, first, &mut stats).await.unwrap();
        for _ in 0..3 {
            // The second large SST falls back to Foyer without replacing the first.
            assert_eq!(
                batch.sstable(&store, second, &mut stats).await.unwrap().id,
                second.object_id
            );
            let before = stats.cache_meta_block_total;
            batch.sstable(&store, &info, &mut stats).await.unwrap();
            batch.sstable(&store, first, &mut stats).await.unwrap();
            assert_eq!(stats.cache_meta_block_total, before);
        }
        assert_eq!(stats.cache_meta_block_total, 5);
        assert_eq!(stats.cache_meta_batch_budget_reject, 3);
        assert_eq!(stats.cache_meta_batch_oversized, 1);
        assert_eq!(stats.cache_meta_batch_retained, 2);
    }
}

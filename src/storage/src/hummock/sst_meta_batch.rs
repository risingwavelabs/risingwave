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
use std::sync::Arc;

use parking_lot::Mutex;
use risingwave_common::config::storage::ReadBatchConfig;
use risingwave_hummock_sdk::HummockSstableObjectId;
use risingwave_hummock_sdk::sstable_info::SstableInfo;
use tokio::sync::OnceCell;

use super::{HummockResult, SstableStoreRef, TableHolder};
use crate::monitor::StoreLocalStatistic;

/// Metadata shared by independent key reads in one `MultiGet`. This owner does not spawn
/// work: dropping the batch drops its waiters and releases all retained handles. Ready
/// retention uses the configured entry/byte budgets plus one oversized SST; active reads hold
/// their own handles independently of this retention budget.
pub(super) struct SstMetaBatch {
    config: ReadBatchConfig,
    inner: Mutex<Metadata>,
}

#[derive(Default)]
struct Metadata {
    entries: HashMap<HummockSstableObjectId, Entry>,
    regular_bytes: usize,
}

enum Entry {
    Ready(TableHolder),
    Oversized(TableHolder),
    Loading(Arc<OnceCell<TableHolder>>),
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
        let mut wait_timer = None;
        let loading = {
            let mut inner = self.inner.lock();
            match inner.entries.entry(info.object_id) {
                std::collections::hash_map::Entry::Occupied(entry) => match entry.get() {
                    Entry::Ready(table) | Entry::Oversized(table) => {
                        stats.cache_meta_batch_ready += 1;
                        return Ok(table.clone());
                    }
                    Entry::Loading(loading) => {
                        stats.cache_meta_batch_loading += 1;
                        wait_timer =
                            Some(store.read_metrics.meta_batch_wait_duration.start_timer());
                        loading.clone()
                    }
                },
                std::collections::hash_map::Entry::Vacant(entry) => {
                    let loading = Arc::new(OnceCell::new());
                    entry.insert(Entry::Loading(loading.clone()));
                    loading
                }
            }
        };
        // Only same-SST callers wait together, and only for metadata. Different SSTs and
        // each key's data-block reads progress independently. No map lock crosses an await.
        let table = loading
            .get_or_try_init(|| async {
                stats.cache_meta_batch_load += 1;
                let table = store.sstable(info, stats).await?;
                let mut inner = self.inner.lock();
                let bytes = table.estimated_meta_cache_memory_weight();
                // Keep one oversized SST in addition to the normal budget. Keys often
                // check a small SST before a large one; retaining only the large SST would
                // discard that useful small metadata. Never replace pending load cells.
                if bytes > self.config.meta_max_bytes.get() {
                    stats.cache_meta_batch_oversized += 1;
                    inner
                        .entries
                        .retain(|_, entry| !matches!(entry, Entry::Oversized(_)));
                    inner
                        .entries
                        .insert(info.object_id, Entry::Oversized(table.clone()));
                } else if inner.entries.len() <= self.config.meta_max_entries.get()
                    && bytes <= self.config.meta_max_bytes.get() - inner.regular_bytes
                {
                    inner.regular_bytes += bytes;
                    inner
                        .entries
                        .insert(info.object_id, Entry::Ready(table.clone()));
                } else {
                    stats.cache_meta_batch_budget_reject += 1;
                    inner.entries.remove(&info.object_id);
                }
                Ok::<_, super::HummockError>(table)
            })
            .await?;
        drop(wait_timer);
        Ok(table.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hummock::iterator::test_utils::mock_sstable_store;
    use crate::hummock::test_utils::{default_builder_opt_for_test, gen_default_test_sstable};

    #[tokio::test]
    async fn test_batch_metadata_shares_loading_and_survives_cancelled_waiter() {
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
            // Hold an actual Foyer fetch pending, so the batch must join an in-flight load.
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
            assert_eq!(second_stats.cache_meta_block_total, u64::from(cancel_first));
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

        // A large SST shares the batch with ordinary metadata. Replacing it must not
        // discard useful small metadata or a concurrent load.
        let batch = SstMetaBatch::new(ReadBatchConfig {
            meta_reuse: true,
            meta_max_bytes: 65536.try_into().unwrap(),
            ..Default::default()
        });
        let pending = Arc::new(OnceCell::new());
        let pending_id = ((batch.config.meta_max_entries.get() + 3) as u64).into();
        batch
            .inner
            .lock()
            .entries
            .insert(pending_id, Entry::Loading(pending.clone()));
        let mut stats = StoreLocalStatistic::default();
        batch.sstable(&store, &info, &mut stats).await.unwrap();
        for id in [
            batch.config.meta_max_entries.get() + 1,
            batch.config.meta_max_entries.get() + 2,
        ] {
            let mut large_meta = table.meta.clone();
            large_meta.block_metas.reserve(
                batch.config.meta_max_bytes.get() / std::mem::size_of::<super::super::BlockMeta>()
                    + 1,
            );
            let mut large_info = info.get_inner();
            large_info.object_id = (id as u64).into();
            let large_info: SstableInfo = large_info.into();
            store.insert_meta_cache(large_info.object_id, large_meta);
            let before = stats.cache_meta_block_total;
            batch
                .sstable(&store, &large_info, &mut stats)
                .await
                .unwrap();
            batch
                .sstable(&store, &large_info, &mut stats)
                .await
                .unwrap();
            assert_eq!(stats.cache_meta_block_total, before + 1);
            // Both the small and large SST are reused when each key visits them in turn.
            batch.sstable(&store, &info, &mut stats).await.unwrap();
            batch
                .sstable(&store, &large_info, &mut stats)
                .await
                .unwrap();
            assert_eq!(stats.cache_meta_block_total, before + 1);
            let inner = batch.inner.lock();
            assert_eq!(inner.entries.len(), 3); // Normal, oversized, and pending.
            assert!(inner.regular_bytes <= batch.config.meta_max_bytes.get());
            assert_eq!(
                inner
                    .entries
                    .values()
                    .filter(|entry| matches!(entry, Entry::Oversized(_)))
                    .count(),
                1
            );
            assert!(
                matches!(inner.entries.get(&pending_id), Some(Entry::Loading(cell)) if Arc::ptr_eq(cell, &pending))
            );
        }
    }
}

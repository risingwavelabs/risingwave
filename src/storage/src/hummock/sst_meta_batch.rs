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
use std::sync::{Arc, Weak};

use parking_lot::Mutex;
use risingwave_common::config::storage::ReadBatchConfig;
use risingwave_hummock_sdk::HummockSstableObjectId;
use risingwave_hummock_sdk::sstable_info::SstableInfo;

use super::block_stream::MemoryUsageTracker;
use super::{HummockResult, SstableStore, SstableStoreRef, TableHolder};
use crate::monitor::StoreLocalStatistic;

tokio::task_local! {
    static ITER_META_BATCH: Arc<SstMetaBatch>;
}

/// Ready metadata handles shared by a batch of reads.
/// Foyer owns loading and single-flight. The batch retains handles until it is dropped.
/// Active reads can hold their own handles independently of the retention budget.
pub struct SstMetaBatch {
    config: ReadBatchConfig,
    inner: Mutex<Metadata>,
}

struct RetainedMeta {
    table: TableHolder,
    _charge: MemoryUsageTracker,
}

#[derive(Default)]
struct Metadata {
    owner: Option<Weak<SstableStore>>,
    entries: HashMap<HummockSstableObjectId, RetainedMeta>,
    regular_bytes: usize,
    has_oversized: bool,
}

impl Metadata {
    fn available_regular_bytes(&self, config: ReadBatchConfig) -> Option<usize> {
        let regular_entries = self.entries.len() - usize::from(self.has_oversized);
        (regular_entries < config.meta_max_entries.get())
            .then_some(config.meta_max_bytes.get() - self.regular_bytes)
    }
}

impl SstMetaBatch {
    pub fn new(config: ReadBatchConfig) -> Self {
        Self {
            config,
            inner: Mutex::new(Metadata::default()),
        }
    }

    /// Reuse metadata during iterator construction, including its initial seek.
    /// The iterator reads the current state normally and does not retain the batch.
    pub async fn scope<T>(batch: Option<&Arc<Self>>, init: impl Future<Output = T>) -> T {
        match batch {
            Some(batch) => ITER_META_BATCH.scope(batch.clone(), init).await,
            None => init.await,
        }
    }

    pub(super) fn current() -> Option<Arc<Self>> {
        ITER_META_BATCH.try_with(Arc::clone).ok()
    }

    pub(super) async fn sstable_for_iter(
        batch: Option<&Self>,
        store: &SstableStoreRef,
        info: &SstableInfo,
        stats: &mut StoreLocalStatistic,
    ) -> HummockResult<TableHolder> {
        match batch {
            Some(batch) => batch.sstable(store, info, stats).await,
            None => store.sstable(info, stats).await,
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
        let max_bytes = self.config.meta_max_bytes.get();
        let retention = {
            let mut inner = self.inner.lock();
            let owner = inner.owner.get_or_insert_with(|| Arc::downgrade(store));
            if !std::ptr::eq(owner.as_ptr(), Arc::as_ptr(store)) {
                None
            } else {
                if let Some(entry) = inner.entries.get(&object_id) {
                    stats.cache_meta_batch_ready += 1;
                    return Ok(entry.table.clone());
                }
                Some((
                    inner.available_regular_bytes(self.config),
                    !inner.has_oversized,
                ))
            }
        };
        let Some((available_bytes, oversized_slot_free)) = retention else {
            return store.sstable(info, stats).await;
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
        if inner.entries.contains_key(&object_id) {
            return Ok(table);
        }
        let has_room = if oversized {
            !inner.has_oversized
        } else {
            inner
                .available_regular_bytes(self.config)
                .is_some_and(|available| bytes <= available)
        };
        if !has_room {
            stats.cache_meta_batch_budget_reject += 1;
            return Ok(table);
        }
        let Some(charge) = store.try_reserve_meta_batch(bytes) else {
            stats.cache_meta_batch_shared_budget_reject += 1;
            return Ok(table);
        };
        let retained = RetainedMeta {
            table: table.clone(),
            _charge: charge,
        };
        inner.entries.insert(object_id, retained);
        if oversized {
            inner.has_oversized = true;
            stats.cache_meta_batch_oversized += 1;
        } else {
            inner.regular_bytes += bytes;
        }
        stats.cache_meta_batch_retained += 1;
        Ok(table)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hummock::iterator::test_utils::mock_sstable_store;
    use crate::hummock::test_utils::{default_builder_opt_for_test, gen_default_test_sstable};

    #[tokio::test]
    async fn test_iterator_metadata_scope_reuses_handles_from_the_same_store() {
        let store = mock_sstable_store().await;
        let other_store = mock_sstable_store().await;
        let (_, info) =
            gen_default_test_sstable(default_builder_opt_for_test(), 0, store.clone()).await;
        let (_, other_info) =
            gen_default_test_sstable(default_builder_opt_for_test(), 0, other_store.clone()).await;
        let batch = Arc::new(SstMetaBatch::new(ReadBatchConfig {
            meta_reuse: true,
            ..Default::default()
        }));

        let mut stats = StoreLocalStatistic::default();
        for _ in 0..2 {
            SstMetaBatch::scope(Some(&batch), async {
                let current = SstMetaBatch::current();
                SstMetaBatch::sstable_for_iter(current.as_deref(), &store, &info, &mut stats)
                    .await
                    .unwrap();
            })
            .await;
            assert!(SstMetaBatch::current().is_none());
        }
        assert_eq!(stats.cache_meta_block_total, 1);
        assert_eq!(stats.cache_meta_batch_ready, 1);

        let mut other_stats = StoreLocalStatistic::default();
        SstMetaBatch::scope(Some(&batch), async {
            let current = SstMetaBatch::current();
            SstMetaBatch::sstable_for_iter(
                current.as_deref(),
                &other_store,
                &other_info,
                &mut other_stats,
            )
            .await
            .unwrap();
        })
        .await;
        assert_eq!(other_stats.cache_meta_block_total, 1);
        assert_eq!(other_stats.cache_meta_batch_ready, 0);
    }

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
    async fn test_shared_budget_charges_oversized_and_releases_on_cancel() {
        let mut store = mock_sstable_store().await;
        let (table, info) =
            gen_default_test_sstable(default_builder_opt_for_test(), 0, store.clone()).await;
        let weight = table.weight();
        let config = &mut Arc::get_mut(&mut store).unwrap().read_batch;
        config.meta_reuse = true;
        config.meta_max_bytes = (weight - 1).try_into().unwrap();
        config.meta_shared_max_bytes = weight.try_into().unwrap();
        let config = *config;

        let (ready, retained) = tokio::sync::oneshot::channel();
        let holder = tokio::spawn({
            let store = store.clone();
            let info = info.clone();
            async move {
                let batch = SstMetaBatch::new(config);
                let mut stats = StoreLocalStatistic::default();
                batch.sstable(&store, &info, &mut stats).await.unwrap();
                assert_eq!(stats.cache_meta_batch_oversized, 1);
                ready.send(()).unwrap();
                std::future::pending::<()>().await;
                drop(batch);
            }
        });
        retained.await.unwrap();
        let batch = SstMetaBatch::new(config);
        let mut stats = StoreLocalStatistic::default();
        assert_eq!(
            batch.sstable(&store, &info, &mut stats).await.unwrap().id,
            info.object_id
        );
        assert_eq!(stats.cache_meta_batch_shared_budget_reject, 1);
        assert_eq!(stats.cache_meta_batch_budget_reject, 0);
        holder.abort();
        assert!(holder.await.unwrap_err().is_cancelled());
        assert_eq!(store.meta_batch_usage(), 0);
        batch.sstable(&store, &info, &mut stats).await.unwrap();
        batch.sstable(&store, &info, &mut stats).await.unwrap();
        assert_eq!(stats.cache_meta_batch_ready, 1);
        assert_eq!(store.meta_batch_usage(), weight);
        drop(batch);
        assert_eq!(store.meta_batch_usage(), 0);
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

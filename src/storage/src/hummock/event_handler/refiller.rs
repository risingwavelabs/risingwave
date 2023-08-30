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

use std::collections::{HashSet, VecDeque};
use std::ops::DerefMut;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{ready, Context, Poll};
use std::time::Duration;

use futures::future::{join_all, try_join_all};
use futures::{Future, FutureExt};
use itertools::Itertools;
use risingwave_hummock_sdk::compaction_group::hummock_version_ext::SstDeltaInfo;
use risingwave_object_store::object::object_metrics::GLOBAL_OBJECT_STORE_METRICS;
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;

use crate::hummock::local_version::pinned_version::PinnedVersion;
use crate::hummock::{HummockResult, SstableStoreRef, TableHolder};
use crate::monitor::StoreLocalStatistic;

#[derive(Debug)]
pub struct CacheRefillConfig {
    pub timeout: Duration,
    pub data_refill_levels: HashSet<u32>,
    pub concurrency: usize,
}

struct Item {
    handle: JoinHandle<()>,
    event: CacheRefillerEvent,
}

/// A cache refiller for hummock data.
pub struct CacheRefiller {
    /// order: old => new
    queue: VecDeque<Item>,

    config: Arc<CacheRefillConfig>,

    concurrency: Arc<Semaphore>,

    sstable_store: SstableStoreRef,
}

impl CacheRefiller {
    pub fn new(config: CacheRefillConfig, sstable_store: SstableStoreRef) -> Self {
        let config = Arc::new(config);
        let concurrency = Arc::new(Semaphore::new(config.concurrency));
        Self {
            queue: VecDeque::new(),
            config,
            concurrency,
            sstable_store,
        }
    }

    pub fn start_cache_refill(
        &mut self,
        deltas: Vec<SstDeltaInfo>,
        pinned_version: Arc<PinnedVersion>,
        new_pinned_version: PinnedVersion,
    ) {
        let task = CacheRefillTask {
            deltas,

            context: Arc::new(CacheRefillContext {
                config: self.config.clone(),
                concurrency: self.concurrency.clone(),
                sstable_store: self.sstable_store.clone(),
            }),
        };
        let event = CacheRefillerEvent {
            pinned_version,
            new_pinned_version,
        };
        let handle = tokio::spawn(async move { task.run().await });
        let item = Item { handle, event };
        self.queue.push_back(item);
    }

    pub fn last_new_pinned_version(&self) -> Option<&PinnedVersion> {
        self.queue.back().map(|item| &item.event.new_pinned_version)
    }

    pub fn next_event(&mut self) -> NextCacheRefillerEvent<'_> {
        NextCacheRefillerEvent { refiller: self }
    }
}

pub struct NextCacheRefillerEvent<'a> {
    refiller: &'a mut CacheRefiller,
}

impl<'a> Future for NextCacheRefillerEvent<'a> {
    type Output = CacheRefillerEvent;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let refiller = &mut self.deref_mut().refiller;

        if let Some(item) = refiller.queue.front_mut() {
            ready!(item.handle.poll_unpin(cx)).unwrap();
            let item = refiller.queue.pop_front().unwrap();
            return Poll::Ready(item.event);
        }
        Poll::Pending
    }
}

pub struct CacheRefillerEvent {
    pub pinned_version: Arc<PinnedVersion>,
    pub new_pinned_version: PinnedVersion,
}

struct CacheRefillContext {
    config: Arc<CacheRefillConfig>,
    concurrency: Arc<Semaphore>,
    sstable_store: SstableStoreRef,
}

pub struct CacheRefillTask {
    deltas: Vec<SstDeltaInfo>,

    context: Arc<CacheRefillContext>,
}

impl CacheRefillTask {
    async fn run(self) {
        let tasks = self
            .deltas
            .iter()
            .map(|delta| {
                let context = self.context.clone();
                async move {
                    let holders = match Self::meta_cache_refill(&context, delta).await {
                        Ok(holders) => holders,
                        Err(e) => {
                            tracing::warn!("meeta cache refill error: {:?}", e);
                            return;
                        }
                    };
                    Self::data_cache_refill(&context, delta, holders).await;
                }
            })
            .collect_vec();
        let future = join_all(tasks);

        let _ = tokio::time::timeout(self.context.config.timeout, future).await;
    }

    async fn meta_cache_refill(
        context: &Arc<CacheRefillContext>,
        delta: &SstDeltaInfo,
    ) -> HummockResult<Vec<TableHolder>> {
        let stats = StoreLocalStatistic::default();
        let tasks = delta
            .insert_sst_infos
            .iter()
            .map(|info| context.sstable_store.sstable_syncable(info, &stats))
            .collect_vec();
        let res = try_join_all(tasks).await?;
        let holders = res.into_iter().map(|(holder, _, _)| holder).collect_vec();
        Ok(holders)
    }

    async fn data_cache_refill(
        context: &Arc<CacheRefillContext>,
        delta: &SstDeltaInfo,
        holders: Vec<TableHolder>,
    ) {
        // return if data file cache is disabled
        let Some(filter) = context.sstable_store.data_file_cache_refill_filter() else {
            return;
        };

        // return if no data to refill
        if delta.insert_sst_infos.is_empty() || delta.delete_sst_object_ids.is_empty() {
            return;
        }

        // return if filtered
        if !context
            .config
            .data_refill_levels
            .contains(&delta.insert_sst_level)
            || !delta
                .delete_sst_object_ids
                .iter()
                .any(|id| filter.contains(id))
        {
            let blocks = holders
                .iter()
                .map(|meta| meta.value().block_count() as u64)
                .sum();
            GLOBAL_OBJECT_STORE_METRICS
                .refill_data_file_cache_count
                .with_label_values(&["filtered"])
                .inc_by(blocks);
            return;
        }

        let mut tasks = vec![];
        for sst_info in &holders {
            for block_index in 0..sst_info.value().block_count() {
                let meta = sst_info.value();
                let mut stat = StoreLocalStatistic::default();
                let task = async move {
                    let permit = context.concurrency.acquire().await.unwrap();
                    match context
                        .sstable_store
                        .may_fill_data_file_cache(meta, block_index, &mut stat)
                        .await
                    {
                        Ok(true) => GLOBAL_OBJECT_STORE_METRICS
                            .refill_data_file_cache_count
                            .with_label_values(&["admitted"])
                            .inc(),
                        Ok(false) => GLOBAL_OBJECT_STORE_METRICS
                            .refill_data_file_cache_count
                            .with_label_values(&["rejected"])
                            .inc(),
                        Err(e) => {
                            tracing::warn!("data cache refill error: {:?}", e);
                        }
                    }
                    drop(permit);
                };
                tasks.push(task);
            }
        }

        join_all(tasks).await;
    }
}

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

use std::time::Instant;

use futures::future::try_join_all;
use risingwave_hummock_sdk::sstable_info::SstableInfo;
use tokio::sync::Semaphore;

use crate::hummock::{HummockResult, SstableStoreRef, TableHolder};
use crate::monitor::{GLOBAL_CACHE_REFILL_METRICS, StoreLocalStatistic};

pub(super) async fn meta_cache_refill<'a>(
    store: &SstableStoreRef,
    concurrency: Option<&Semaphore>,
    infos: impl IntoIterator<Item = &'a SstableInfo>,
) -> HummockResult<Vec<TableHolder>> {
    let tasks = infos.into_iter().map(|info| async {
        let mut stats = StoreLocalStatistic::default();
        GLOBAL_CACHE_REFILL_METRICS.meta_refill_attempts_total.inc();

        let permit = if let Some(c) = concurrency {
            Some(c.acquire().await.unwrap())
        } else {
            None
        };

        let now = Instant::now();
        let res = store.sstable(info, &mut stats).await;
        stats.discard();
        if res.is_ok() {
            GLOBAL_CACHE_REFILL_METRICS
                .meta_refill_success_duration
                .observe(now.elapsed().as_secs_f64());
        }
        drop(permit);

        res
    });
    try_join_all(tasks).await
}

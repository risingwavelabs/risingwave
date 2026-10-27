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

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use futures::FutureExt;
use risingwave_common::config::streaming::CacheRefillPolicy;
use risingwave_common::license::Feature;
use risingwave_hummock_sdk::compaction_group::hummock_version_ext::SstDeltaInfo;
use risingwave_hummock_sdk::version::HummockVersionDelta;
use risingwave_pb::id::TableId;
use thiserror_ext::AsReport;
use tokio::sync::Semaphore;

mod foyer;
mod pin;

use foyer::FoyerRefiller;
#[cfg(test)]
pub(super) use foyer::SpawnRefillTask;
#[cfg(test)]
use foyer::default_spawn_refill_task;
pub(super) use foyer::table_cache_refill_monitor_snapshot;
pub use foyer::{
    TableCacheRefillContext, TableCacheRefillContextMap, TableCacheRefillMonitorSnapshot,
};
use pin::PinCacheRefiller;

use super::version_update::VersionUpdateCondition;
use crate::hummock::SstableStoreRef;
use crate::hummock::local_version::pinned_version::PinnedVersion;
use crate::hummock::pin_cache_refill::PinCacheRefillPlan;
use crate::hummock::table_cache_metadata::TableCacheMetadata;
use crate::opts::StorageOpts;

#[derive(Debug)]
pub struct CacheRefillConfig {
    /// Timeout for each backend refill attempt, starting at event submission.
    pub timeout: Duration,

    /// Data file cache refill levels.
    pub data_refill_levels: HashSet<u32>,

    /// Meta file cache refill concurrency.
    pub meta_refill_concurrency: usize,

    /// Data file cache refill concurrency.
    pub concurrency: usize,

    /// Data file cache refill unit (blocks).
    pub unit: usize,

    /// Data file cache reill unit threshold.
    ///
    /// Only units whose admit rate > threshold will be refilled.
    pub threshold: f64,

    /// Skip recent filter.
    pub skip_recent_filter: bool,

    /// Skip inheritance filter.
    pub skip_inheritance_filter: bool,

    /// Default table cache refill policy.
    pub table_cache_refill_default_policy: CacheRefillPolicy,
}

impl CacheRefillConfig {
    pub fn from_storage_opts(options: &StorageOpts) -> Self {
        let data_refill_levels = match Feature::ElasticDiskCache.check_available() {
            Ok(_) => options
                .cache_refill_data_refill_levels
                .iter()
                .copied()
                .collect(),
            Err(e) => {
                tracing::warn!(error = %e.as_report(), "ElasticDiskCache is not available.");
                HashSet::new()
            }
        };

        Self {
            timeout: Duration::from_millis(options.cache_refill_timeout_ms),
            data_refill_levels,
            concurrency: options.cache_refill_concurrency,
            meta_refill_concurrency: options.cache_refill_meta_refill_concurrency,
            unit: options.cache_refill_unit,
            threshold: options.cache_refill_threshold,
            skip_recent_filter: options.cache_refill_skip_recent_filter,
            skip_inheritance_filter: options.cache_refill_skip_inheritance_filter,
            table_cache_refill_default_policy: options
                .cache_refill_table_cache_refill_default_policy,
        }
    }
}

/// Constructs independent refill backends and combines their publication conditions.
/// Backends consume shared table policies and ownership; membership stays with the handler.
pub(crate) struct CacheRefiller {
    foyer: FoyerRefiller,
    pin_cache_refill: Option<PinCacheRefiller>,
}

impl CacheRefiller {
    pub(crate) fn new(config: CacheRefillConfig, sstable_store: SstableStoreRef) -> Self {
        let config = Arc::new(config);
        let concurrency = Arc::new(Semaphore::new(config.concurrency));
        let meta_refill_concurrency = if config.meta_refill_concurrency == 0 {
            None
        } else {
            Some(Arc::new(Semaphore::new(config.meta_refill_concurrency)))
        };
        let pin_cache_refill = sstable_store
            .pin_cache()
            .map(|_| PinCacheRefiller::new(&config, sstable_store.clone(), concurrency.clone()));
        let foyer = FoyerRefiller::new(
            config,
            sstable_store,
            meta_refill_concurrency,
            concurrency,
            pin_cache_refill.is_none(),
        );
        Self {
            foyer,
            pin_cache_refill,
        }
    }

    #[cfg(test)]
    pub(super) fn with_foyer_spawn_task(mut self, spawn: SpawnRefillTask) -> Self {
        self.foyer.spawn_refill_task = Some(spawn);
        self
    }

    pub(super) fn start_cache_refill(
        &self,
        deltas: Vec<SstDeltaInfo>,
        new_pinned_version: PinnedVersion,
        version_deltas: Option<&[HummockVersionDelta]>,
        metadata: &TableCacheMetadata,
    ) -> VersionUpdateCondition {
        let pin = self.pin_cache_refill.as_ref().map(|refiller| {
            let plan = PinCacheRefillPlan::from_version_update(
                &deltas,
                &new_pinned_version,
                version_deltas,
                metadata.pinned_tables(),
            );
            refiller.start_cache_refill(plan, metadata)
        });
        let foyer = self.foyer.start_cache_refill(deltas, metadata);
        async move {
            if let Some(pin) = pin {
                futures::join!(foyer, pin);
            } else {
                foyer.await;
            }
        }
        .boxed()
    }

    pub(super) fn invalidate_pin_refills(&self, changed_tables: &HashSet<TableId>) {
        if let Some(refiller) = &self.pin_cache_refill {
            refiller.invalidate_refills(changed_tables);
        }
    }

    pub(super) fn cancel_objects(
        &self,
        objects: impl IntoIterator<Item = risingwave_hummock_sdk::HummockSstableObjectId>,
    ) {
        if let Some(refiller) = &self.pin_cache_refill {
            refiller.cancel_objects(objects);
        }
    }
}

#[cfg(test)]
pub(super) mod test_utils;
#[cfg(test)]
mod tests;

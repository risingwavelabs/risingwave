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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use futures::FutureExt;
use risingwave_common::bitmap::Bitmap;
use risingwave_common::config::streaming::CacheRefillPolicy;
use risingwave_common::license::Feature;
use risingwave_hummock_sdk::compaction_group::hummock_version_ext::SstDeltaInfo;
use risingwave_hummock_sdk::version::HummockVersionDelta;
use risingwave_pb::id::TableId;
use thiserror_ext::AsReport;
use tokio::sync::Semaphore;

mod admission;
mod foyer;
mod pin;

pub(super) use admission::foyer::table_cache_refill_monitor_snapshot;
pub use admission::foyer::{
    TableCacheRefillContext, TableCacheRefillContextMap, TableCacheRefillMonitorSnapshot,
};
use foyer::FoyerRefiller;
#[cfg(test)]
pub(super) use foyer::SpawnRefillTask;
#[cfg(test)]
use foyer::default_spawn_refill_task;
use pin::PinCacheRefiller;

use super::ReadOnlyRwLockRef;
use super::version_update::VersionUpdateCondition;
use crate::hummock::SstableStoreRef;
use crate::hummock::local_version::pinned_version::PinnedVersion;
use crate::hummock::table_cache_metadata::TableCacheMetadata;
use crate::opts::StorageOpts;

#[derive(Debug)]
pub struct CacheRefillConfig {
    /// Timeout for each backend refill attempt, excluding queued Pin events.
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

/// Applies shared admission, dispatches independent backends and combines their conditions.
/// Version application and membership stay with the handler and always see complete deltas.
pub(crate) struct CacheRefiller {
    foyer: FoyerRefiller,
    pin_cache_refill: Option<PinCacheRefiller>,
}

impl CacheRefiller {
    pub(crate) fn new(
        config: CacheRefillConfig,
        sstable_store: SstableStoreRef,
        metadata: ReadOnlyRwLockRef<TableCacheMetadata>,
    ) -> Self {
        let config = Arc::new(config);
        let concurrency = Arc::new(Semaphore::new(config.concurrency));
        let meta_refill_concurrency = if config.meta_refill_concurrency == 0 {
            None
        } else {
            Some(Arc::new(Semaphore::new(config.meta_refill_concurrency)))
        };
        let pin_cache_refill = sstable_store.pin_cache().map(|_| {
            PinCacheRefiller::new(
                config.clone(),
                sstable_store.clone(),
                metadata,
                concurrency.clone(),
                meta_refill_concurrency.clone(),
            )
        });
        let foyer = FoyerRefiller::new(config, sstable_store, meta_refill_concurrency, concurrency);
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
        mut deltas: Vec<SstDeltaInfo>,
        new_pinned_version: PinnedVersion,
        version_deltas: Option<&[HummockVersionDelta]>,
        metadata: &TableCacheMetadata,
    ) -> VersionUpdateCondition {
        // Resolve shared admission in each backend's input preparation below. Filtering
        // whole SSTs first would repeat those checks and cannot replace table projection.
        let pin = self.pin_cache_refill.as_ref().map(|refiller| {
            // Raw structural deltas can supply insertions absent from the summary. Bound
            // their table admission too, before Pin constructs per-object intent.
            let admitted_pin_tables = metadata
                .pinned_tables()
                .iter()
                .copied()
                .filter(|table| metadata.ownership().has_vnodes(table))
                .collect();
            refiller.start_cache_refill(
                &deltas,
                new_pinned_version,
                version_deltas,
                &admitted_pin_tables,
            )
        });
        let table_contexts = admission::foyer::select_table_inputs(&mut deltas, metadata);
        let foyer = self.foyer.start_cache_refill(deltas, table_contexts);
        // Preserve main's detached-on-drop behavior without Pin. A composite condition owns
        // its Foyer waiter; install cleanup before polling so queue shutdown also aborts it.
        let abort_on_drop = pin.is_some();
        let mut foyer = scopeguard::guard(foyer, move |handle| {
            if abort_on_drop {
                handle.abort();
            }
        });
        async move {
            match pin {
                // Owner shutdown closes receivers and drops their publication queue together.
                Some(pin) => {
                    let (foyer, pin) = futures::join!(&mut *foyer, pin);
                    if let Err(error) = foyer {
                        tracing::warn!(error = %error.as_report(), "Foyer refill task failed; using remote fallback");
                    }
                    let _ = pin;
                }
                None => {
                    (&mut *foyer).await.unwrap();
                }
            }
        }
        .boxed()
    }

    pub(super) fn on_pinned_tables_changed(
        &self,
        previous: &HashSet<TableId>,
        metadata: &TableCacheMetadata,
    ) {
        if let Some(refiller) = &self.pin_cache_refill {
            refiller.on_pinned_tables_changed(previous, metadata);
        }
    }

    pub(super) fn on_serving_mapping_changed(
        &self,
        previous: &HashMap<TableId, Bitmap>,
        metadata: &TableCacheMetadata,
    ) {
        if let Some(refiller) = &self.pin_cache_refill {
            refiller.on_serving_mapping_changed(previous, metadata);
        }
    }

    pub(super) fn on_streaming_vnodes_changed(
        &self,
        table: TableId,
        previous: Option<&Bitmap>,
        metadata: &TableCacheMetadata,
    ) {
        if let Some(refiller) = &self.pin_cache_refill {
            refiller.on_streaming_vnodes_changed(table, previous, metadata);
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

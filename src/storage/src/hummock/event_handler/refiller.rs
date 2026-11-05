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

use risingwave_common::bitmap::Bitmap;
use risingwave_common::config::streaming::CacheRefillPolicy;
use risingwave_common::license::Feature;
use risingwave_hummock_sdk::compaction_group::hummock_version_ext::SstDeltaInfo;
use risingwave_pb::id::TableId;
use thiserror_ext::AsReport;
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;

use crate::hummock::SstableStoreRef;
use crate::hummock::table_cache_metadata::TableCacheMetadata;
pub use crate::monitor::{CacheRefillMetrics, GLOBAL_CACHE_REFILL_METRICS};
use crate::opts::StorageOpts;

// Foyer tasks consume immutable snapshots of the handler-owned policy and vnode maps.
mod foyer;

#[derive(Debug)]
pub struct CacheRefillConfig {
    /// Cache refill timeout.
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
        }
    }
}

#[cfg(test)]
type SpawnRefillTask =
    Arc<dyn Fn(Vec<SstDeltaInfo>, CacheRefillContext) -> JoinHandle<()> + Send + Sync + 'static>;

pub type TableCacheRefillContextMap = HashMap<TableId, TableCacheRefillContext>;

/// Per-table metadata captured for a refill task to decide whether an sstable block should be
/// refilled. Mutable runtime state used to build this snapshot is owned by the event handler.
#[derive(Clone)]
pub struct TableCacheRefillContext {
    /// Vnodes covered by local streaming read versions on this compute node.
    pub streaming_vnode_bitmap: Option<Bitmap>,
    /// Vnodes served by this compute node according to the serving vnode mapping.
    pub serving_vnode_bitmap: Option<Bitmap>,
    /// Effective refill policy after applying the default policy and per-table overrides.
    pub policy: CacheRefillPolicy,
}

/// Current CN metadata and its Foyer projection for monitor/debugging APIs.
#[derive(Clone)]
pub struct TableCacheRefillMonitorSnapshot {
    pub contexts: TableCacheRefillContextMap,
    pub policies: HashMap<TableId, CacheRefillPolicy>,
    pub default_policy: CacheRefillPolicy,
    pub streaming_table_vnode_mapping: HashMap<TableId, Bitmap>,
    pub serving_table_vnode_mapping: HashMap<TableId, Bitmap>,
}

/// Admits SSTs using borrowed CN metadata, then runs Foyer tasks with immutable snapshots.
pub(crate) struct CacheRefiller {
    #[cfg(test)]
    spawn_refill_task: Option<SpawnRefillTask>,

    config: Arc<CacheRefillConfig>,
    meta_refill_concurrency: Option<Arc<Semaphore>>,
    concurrency: Arc<Semaphore>,
    sstable_store: SstableStoreRef,
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
        Self {
            #[cfg(test)]
            spawn_refill_task: None,
            config,
            meta_refill_concurrency,
            concurrency,
            sstable_store,
        }
    }

    #[cfg(test)]
    pub(super) fn with_foyer_spawn_task(mut self, spawn: SpawnRefillTask) -> Self {
        self.spawn_refill_task = Some(spawn);
        self
    }

    pub(crate) fn start_cache_refill(
        &self,
        mut deltas: Vec<SstDeltaInfo>,
        metadata: &TableCacheMetadata,
    ) -> JoinHandle<()> {
        for delta in &mut deltas {
            let for_serving = metadata.role.for_serving();
            // Writer-appended L0 SSTs are already warm on the streaming side. Their data refill
            // may therefore only be needed by serving workers.
            let for_streaming =
                metadata.role.for_streaming() && !delta.delete_sst_object_ids.is_empty();

            if !for_serving && !for_streaming {
                delta.insert_sst_infos.clear();
                continue;
            }

            // This is deliberately a whole-SST admission check before Meta load. The serving
            // mapping key set identifies result tables, but bitmap contents remain for the exact
            // block/vnode decision in DataCacheRefillTaskGenerator after Meta load. An SST stays
            // when any contained table matches either the serving or streaming refill lane.
            delta.insert_sst_infos.retain(|sst| {
                sst.table_ids.iter().any(|table_id| {
                    // A missing entry means there is no table override, not that the table is
                    // absent. Preserve the configured legacy/default policy in that case.
                    let policy = metadata
                        .table_cache_refill_policies
                        .get(table_id)
                        .copied()
                        .unwrap_or(metadata.default_policy);

                    // Enabled preserves legacy full refill. For scoped policies, mapping keys are
                    // only a whole-SST coarse gate; bitmap bits still filter blocks post-Meta.
                    match policy {
                        CacheRefillPolicy::Enabled => for_streaming || for_serving,
                        CacheRefillPolicy::Disabled => false,
                        CacheRefillPolicy::Streaming => {
                            for_streaming
                                && metadata
                                    .streaming_table_vnode_mapping
                                    .contains_key(table_id)
                        }
                        CacheRefillPolicy::Serving => {
                            for_serving
                                && metadata.serving_table_vnode_mapping.contains_key(table_id)
                        }
                        CacheRefillPolicy::Both => {
                            (for_streaming
                                && metadata
                                    .streaming_table_vnode_mapping
                                    .contains_key(table_id))
                                || (for_serving
                                    && metadata.serving_table_vnode_mapping.contains_key(table_id))
                        }
                    }
                })
            });
        }
        let context = self.new_cache_refill_context(&deltas, metadata);
        #[cfg(test)]
        if let Some(spawn) = &self.spawn_refill_task {
            return spawn(deltas, context);
        }
        foyer::spawn_refill_task(deltas, context)
    }

    fn new_cache_refill_context(
        &self,
        deltas: &[SstDeltaInfo],
        metadata: &TableCacheMetadata,
    ) -> CacheRefillContext {
        let table_ids = deltas.iter().flat_map(|delta| {
            delta
                .insert_sst_infos
                .iter()
                .flat_map(|sst| sst.table_ids.iter().copied())
        });
        CacheRefillContext {
            config: self.config.clone(),
            meta_refill_concurrency: self.meta_refill_concurrency.clone(),
            concurrency: self.concurrency.clone(),
            sstable_store: self.sstable_store.clone(),
            table_cache_refill_context_map: Arc::new(Self::table_cache_refill_contexts(
                metadata, table_ids,
            )),
        }
    }

    fn table_cache_refill_contexts(
        metadata: &TableCacheMetadata,
        table_ids: impl IntoIterator<Item = TableId>,
    ) -> TableCacheRefillContextMap {
        let for_streaming = metadata.role.for_streaming();
        let for_serving = metadata.role.for_serving();
        let mut contexts = HashMap::new();
        for table_id in table_ids {
            if for_serving
                && !for_streaming
                && !metadata.serving_table_vnode_mapping.contains_key(&table_id)
            {
                continue;
            }
            contexts.entry(table_id).or_insert_with(|| {
                let policy = metadata
                    .table_cache_refill_policies
                    .get(&table_id)
                    .copied()
                    .unwrap_or(metadata.default_policy);
                let streaming_vnode_bitmap = (for_streaming && policy.is_streaming_scoped())
                    .then(|| {
                        metadata
                            .streaming_table_vnode_mapping
                            .get(&table_id)
                            .cloned()
                    })
                    .flatten();
                // `Enabled` normally does not use bitmap filtering. The only exception is L0
                // insert-only refill, where serving workers still need serving-locality evidence.
                let serving_vnode_bitmap = (for_serving
                    && (policy.is_serving_scoped() || policy.is_unscoped_enabled()))
                .then(|| metadata.serving_table_vnode_mapping.get(&table_id).cloned())
                .flatten();
                TableCacheRefillContext {
                    streaming_vnode_bitmap,
                    serving_vnode_bitmap,
                    policy,
                }
            });
        }
        contexts
    }

    pub(crate) fn table_cache_refill_monitor_snapshot(
        metadata: &TableCacheMetadata,
    ) -> TableCacheRefillMonitorSnapshot {
        let table_ids = metadata
            .table_cache_refill_policies
            .keys()
            .chain(metadata.streaming_table_vnode_mapping.keys())
            .chain(metadata.serving_table_vnode_mapping.keys())
            .copied();
        TableCacheRefillMonitorSnapshot {
            contexts: Self::table_cache_refill_contexts(metadata, table_ids),
            policies: metadata.table_cache_refill_policies.clone(),
            default_policy: metadata.default_policy,
            streaming_table_vnode_mapping: metadata.streaming_table_vnode_mapping.clone(),
            serving_table_vnode_mapping: metadata.serving_table_vnode_mapping.clone(),
        }
    }
}

#[derive(Clone)]
pub(crate) struct CacheRefillContext {
    config: Arc<CacheRefillConfig>,
    meta_refill_concurrency: Option<Arc<Semaphore>>,
    concurrency: Arc<Semaphore>,
    sstable_store: SstableStoreRef,
    table_cache_refill_context_map: Arc<TableCacheRefillContextMap>,
}

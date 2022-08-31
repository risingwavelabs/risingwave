// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt::Debug;
use std::future::pending;
use std::sync::Arc;

use enum_as_inner::EnumAsInner;
use futures::future::{select_all, BoxFuture};
use futures::FutureExt;
use itertools::Itertools;
use risingwave_common::config::StorageConfig;
use risingwave_common::error::RwError;
use risingwave_hummock_sdk::filter_key_extractor::FilterKeyExtractorManagerRef;
use risingwave_object_store::object::{
    parse_local_object_store, parse_remote_object_store, ObjectStoreImpl,
};
use risingwave_rpc_client::HummockMetaClient;

use crate::error::StorageResult;
use crate::hummock::compaction_group_client::CompactionGroupClientImpl;
use crate::hummock::{
    HummockStorage, LocalHummockStorageWrapper, SstableStore, TieredCache,
    TieredCacheMetricsBuilder,
};
use crate::memory::MemoryStateStore;
use crate::monitor::{MonitoredStateStore as Monitored, ObjectStoreMetrics, StateStoreMetrics};
use crate::store::StateStore;

pub enum StorageControlMessage {}

/// The type erased [`StateStore`].
#[derive(Clone, EnumAsInner)]
pub enum StateStoreImpl {
    /// The Hummock state store, which operates on an S3-like service. URLs beginning with
    /// `hummock` will be automatically recognized as Hummock state store.
    ///
    /// Example URLs:
    ///
    /// * `hummock+s3://bucket`
    /// * `hummock+minio://KEY:SECRET@minio-ip:port`
    /// * `hummock+memory` (should only be used in 1 compute node mode)
    HummockStateStore(Monitored<HummockStorage>),
    /// In-memory B-Tree state store. Should only be used in unit and integration tests. If you
    /// want speed up e2e test, you should use Hummock in-memory mode instead. Also, this state
    /// store misses some critical implementation to ensure the correctness of persisting streaming
    /// state. (e.g., no read_epoch support, no async checkpoint)
    MemoryStateStore(Monitored<MemoryStateStore>),
}

impl StateStoreImpl {
    pub fn shared_in_memory_store(state_store_metrics: Arc<StateStoreMetrics>) -> Self {
        Self::MemoryStateStore(MemoryStateStore::shared().monitored(state_store_metrics))
    }
}

impl Debug for StateStoreImpl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StateStoreImpl::HummockStateStore(_) => write!(f, "HummockStateStore"),
            StateStoreImpl::MemoryStateStore(_) => write!(f, "MemoryStateStore"),
        }
    }
}

#[macro_export]
macro_rules! dispatch_state_store_inner {
    ($impl:expr, $store:ident, $body:tt, $type_name:ident) => {
        match $impl {
            $type_name::MemoryStateStore($store) => {
                // WARNING: don't change this. Enabling memory backend will cause monomorphization
                // explosion and thus slow compile time in release mode.
                #[cfg(debug_assertions)]
                {
                    $body
                }
                #[cfg(not(debug_assertions))]
                {
                    let _store = $store;
                    unimplemented!("memory state store should never be used in release mode");
                }
            }
            $type_name::HummockStateStore($store) => $body,
        }
    };
}

#[macro_export]
macro_rules! dispatch_state_store {
    ($impl:expr, $store:ident, $body:tt) => {
        $crate::dispatch_state_store_inner!($impl, $store, $body, StateStoreImpl)
    };
}

impl StateStoreImpl {
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        s: &str,
        #[allow(unused)] file_cache_dir: &str,
        config: Arc<StorageConfig>,
        hummock_meta_client: Arc<dyn HummockMetaClient>,
        state_store_stats: Arc<StateStoreMetrics>,
        object_store_metrics: Arc<ObjectStoreMetrics>,
        filter_key_extractor_manager: FilterKeyExtractorManagerRef,
        #[allow(unused)] tiered_cache_metrics_builder: TieredCacheMetricsBuilder,
    ) -> StorageResult<Self> {
        #[cfg(not(target_os = "linux"))]
        let tiered_cache = TieredCache::none();

        #[cfg(target_os = "linux")]
        let tiered_cache = if file_cache_dir.is_empty() {
            TieredCache::none()
        } else {
            use crate::hummock::file_cache::cache::FileCacheOptions;
            use crate::hummock::HummockError;

            let options = FileCacheOptions {
                dir: file_cache_dir.to_string(),
                capacity: config.file_cache.capacity,
                total_buffer_capacity: config.file_cache.total_buffer_capacity,
                cache_file_fallocate_unit: config.file_cache.cache_file_fallocate_unit,
                flush_buffer_hooks: vec![],
            };
            let metrics = Arc::new(tiered_cache_metrics_builder.file());
            TieredCache::file(options, metrics)
                .await
                .map_err(HummockError::tiered_cache)?
        };

        let store = match s {
            hummock if hummock.starts_with("hummock+") => {
                let remote_object_store = parse_remote_object_store(
                    hummock.strip_prefix("hummock+").unwrap(),
                    object_store_metrics.clone(),
                )
                .await;
                let object_store = if config.enable_local_spill {
                    let local_object_store = parse_local_object_store(
                        config.local_object_store.as_str(),
                        object_store_metrics.clone(),
                    )
                    .await;
                    ObjectStoreImpl::hybrid(local_object_store, remote_object_store)
                } else {
                    remote_object_store
                };

                let sstable_store = Arc::new(SstableStore::new(
                    Arc::new(object_store),
                    config.data_directory.to_string(),
                    config.block_cache_capacity_mb * (1 << 20),
                    config.meta_cache_capacity_mb * (1 << 20),
                    tiered_cache,
                ));
                let compaction_group_client =
                    Arc::new(CompactionGroupClientImpl::new(hummock_meta_client.clone()));
                let inner = HummockStorage::new(
                    config.clone(),
                    sstable_store.clone(),
                    hummock_meta_client.clone(),
                    state_store_stats.clone(),
                    compaction_group_client,
                    filter_key_extractor_manager,
                )
                .await?;
                StateStoreImpl::HummockStateStore(inner.monitored(state_store_stats))
            }

            "in_memory" | "in-memory" => {
                panic!("in-memory state backend should never be used in end-to-end environment, use `hummock+memory` instead.")
            }

            other => unimplemented!("{} state store is not supported", other),
        };

        Ok(store)
    }

    pub fn start_local_state_store(&self) -> LocalStateStoreImpl {
        match self {
            StateStoreImpl::HummockStateStore(store) => LocalStateStoreImpl::HummockStateStore(
                store
                    .state_store()
                    .start_local_state_store()
                    .monitored(store.stats()),
            ),
            StateStoreImpl::MemoryStateStore(store) => {
                LocalStateStoreImpl::MemoryStateStore(store.clone())
            }
        }
    }
}

/// The type erased [`StateStore`].
#[derive(Clone, EnumAsInner)]
pub enum LocalStateStoreImpl {
    // TODO: implement new local state store builder for hummock
    HummockStateStore(Monitored<LocalHummockStorageWrapper>),
    MemoryStateStore(Monitored<MemoryStateStore>),
}

impl LocalStateStoreImpl {
    pub fn into_poll_control_future(
        self,
    ) -> BoxFuture<'static, risingwave_common::error::Result<()>> {
        match self {
            LocalStateStoreImpl::HummockStateStore(state_store) => {
                let instances = state_store
                    .state_store()
                    .collect_local_state_store_instance();
                let futures = instances
                    .into_iter()
                    .map(|(instance, mut rx)| {
                        async move {
                            while let Some(control_msg) = rx.recv().await {
                                instance.apply_control_msg(control_msg)
                            }
                            Ok::<(), RwError>(())
                        }
                        .boxed()
                    })
                    .collect_vec();
                select_all(futures).map(|_| Ok::<(), RwError>(())).boxed()
            }
            LocalStateStoreImpl::MemoryStateStore(_) => pending().boxed(),
        }
    }
}

#[macro_export]
macro_rules! dispatch_local_state_store {
    ($impl:expr, $store:ident, $body:tt) => {
        $crate::dispatch_state_store_inner!($impl, $store, $body, LocalStateStoreImpl)
    };
}

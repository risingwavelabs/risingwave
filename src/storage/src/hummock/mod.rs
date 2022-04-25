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

//! Hummock is the state store of the streaming system.

use std::fmt;
use std::sync::Arc;

use bytes::Bytes;
use risingwave_common::config::StorageConfig;
use risingwave_hummock_sdk::*;
use risingwave_rpc_client::HummockMetaClient;

mod block_cache;
pub use block_cache::*;
mod sstable;
pub use sstable::*;
mod cache;
pub mod compactor;
#[cfg(test)]
mod compactor_tests;
mod conflict_detector;
mod error;
pub mod hummock_meta_client;
pub mod iterator;
mod local_version;
pub mod local_version_manager;
pub mod shared_buffer;
#[cfg(test)]
mod snapshot_tests;
mod sstable_store;
mod state_store;
#[cfg(test)]
mod state_store_tests;
pub mod test_runner;
#[cfg(test)]
pub(crate) mod test_utils;
mod utils;
mod vacuum;
pub mod value;
pub use cache::{CachableEntry, LookupResult, LruCache};
pub use error::*;
use value::*;

use self::iterator::HummockIterator;
use self::key::user_key;
pub use self::sstable_store::*;
pub use self::state_store::HummockStateStoreIter;
use super::monitor::StateStoreMetrics;
use crate::hummock::local_version_manager::LocalVersionManager;

/// Hummock is the state store backend.
#[derive(Clone)]
pub struct HummockStorage {
    options: Arc<StorageConfig>,

    local_version_manager: Arc<LocalVersionManager>,

    hummock_meta_client: Arc<dyn HummockMetaClient>,

    sstable_store: SstableStoreRef,

    /// Statistics
    stats: Arc<StateStoreMetrics>,
}

impl HummockStorage {
    /// Creates a [`HummockStorage`] with default stats. Should only be used by tests.
    pub async fn with_default_stats(
        options: Arc<StorageConfig>,
        sstable_store: SstableStoreRef,
        hummock_meta_client: Arc<dyn HummockMetaClient>,
        hummock_metrics: Arc<StateStoreMetrics>,
    ) -> HummockResult<Self> {
        Self::new(options, sstable_store, hummock_meta_client, hummock_metrics).await
    }

    /// Creates a [`HummockStorage`].
    pub async fn new(
        options: Arc<StorageConfig>,
        sstable_store: SstableStoreRef,
        hummock_meta_client: Arc<dyn HummockMetaClient>,
        // TODO: separate `HummockStats` from `StateStoreMetrics`.
        stats: Arc<StateStoreMetrics>,
    ) -> HummockResult<Self> {
        let local_version_manager = Arc::new(LocalVersionManager::new(options.clone()));

        LocalVersionManager::start_workers(
            options.clone(),
            sstable_store.clone(),
            local_version_manager.clone(),
            stats.clone(),
            hummock_meta_client.clone(),
        );

        // Ensure at least one available version in cache.
        local_version_manager.wait_epoch(HummockEpoch::MIN).await?;

        let instance = Self {
            options: options.clone(),
            local_version_manager,
            hummock_meta_client,
            sstable_store,
            stats,
        };
        Ok(instance)
    }

    fn get_builder(options: &StorageConfig) -> SSTableBuilder {
        SSTableBuilder::new(SSTableBuilderOptions {
            capacity: options.sstable_size as usize,
            block_capacity: options.block_size as usize,
            restart_interval: DEFAULT_RESTART_INTERVAL,
            bloom_false_positive: options.bloom_false_positive,
            // TODO: Make this configurable.
            compression_algorithm: CompressionAlgorithm::None,
        })
    }

    async fn get_from_table(
        &self,
        table: TableHolder,
        internal_key: &[u8],
        key: &[u8],
    ) -> HummockResult<Option<Bytes>> {
        if table.value().surely_not_have_user_key(key) {
            self.stats.bloom_filter_true_negative_counts.inc();
            return Ok(None);
        }
        // Might have the key, take it as might positive.
        self.stats.bloom_filter_might_positive_counts.inc();
        let mut iter = SSTableIterator::new(table, self.sstable_store.clone());
        iter.seek(internal_key).await?;
        // Iterator has seeked passed the borders.
        if !iter.is_valid() {
            return Ok(None);
        }

        // Iterator gets us the key, we tell if it's the key we want
        // or key next to it.
        let value = match user_key(iter.key()) == key {
            true => iter.value().into_user_value().map(Bytes::copy_from_slice),
            false => None,
        };
        Ok(value)
    }

    pub fn hummock_meta_client(&self) -> &Arc<dyn HummockMetaClient> {
        &self.hummock_meta_client
    }

    pub fn options(&self) -> &Arc<StorageConfig> {
        &self.options
    }

    pub fn sstable_store(&self) -> SstableStoreRef {
        self.sstable_store.clone()
    }

    pub fn local_version_manager(&self) -> &Arc<LocalVersionManager> {
        &self.local_version_manager
    }
}

impl fmt::Debug for HummockStorage {
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

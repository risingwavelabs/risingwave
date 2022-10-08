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

use std::sync::Arc;

use bytes::Bytes;
use risingwave_common::config::StorageConfig;
use risingwave_hummock_sdk::*;
use risingwave_pb::hummock::SstableInfo;
use risingwave_rpc_client::HummockMetaClient;

mod block_cache;
pub use block_cache::*;

#[cfg(target_os = "linux")]
pub mod file_cache;

mod tiered_cache;
pub use tiered_cache::*;

pub mod sstable;
pub use sstable::*;

pub mod compaction_group_client;
pub mod compactor;
pub mod conflict_detector;
mod error;
pub mod hummock_meta_client;
pub mod iterator;
pub mod shared_buffer;
pub mod sstable_store;
mod state_store;
#[cfg(any(test, feature = "test"))]
pub mod test_utils;
pub mod utils;
pub use compactor::{CompactorMemoryCollector, CompactorSstableStore};
pub use utils::MemoryLimiter;
pub mod local_version;
pub mod observer_manager;
pub mod store;
pub mod vacuum;
mod validator;
pub mod value;

pub use error::*;
use local_version::local_version_manager::{LocalVersionManager, LocalVersionManagerRef};
pub use risingwave_common::cache::{CacheableEntry, LookupResult, LruCache};
use risingwave_common::catalog::TableId;
use risingwave_common_service::observer_manager::NotificationClient;
use risingwave_hummock_sdk::compaction_group::StaticCompactionGroupId;
pub use validator::*;
use value::*;

use self::iterator::HummockIterator;
use self::key::user_key;
pub use self::sstable_store::*;
pub use self::state_store::HummockStateStoreIter;
use super::monitor::StateStoreMetrics;
use crate::error::StorageResult;
use crate::hummock::compaction_group_client::{
    CompactionGroupClientImpl, DummyCompactionGroupClient,
};
use crate::hummock::conflict_detector::ConflictDetector;
use crate::hummock::shared_buffer::shared_buffer_batch::SharedBufferBatch;
use crate::hummock::shared_buffer::{OrderSortedUncommittedData, UncommittedData};
use crate::hummock::sstable::SstableIteratorReadOptions;
use crate::hummock::sstable_store::{SstableStoreRef, TableHolder};
use crate::monitor::StoreLocalStatistic;

/// Hummock is the state store backend.
#[derive(Clone)]
pub struct HummockStorage {
    options: Arc<StorageConfig>,

    local_version_manager: LocalVersionManagerRef,

    hummock_meta_client: Arc<dyn HummockMetaClient>,

    sstable_store: SstableStoreRef,

    /// Statistics
    stats: Arc<StateStoreMetrics>,

    compaction_group_client: Arc<CompactionGroupClientImpl>,

    sstable_id_manager: SstableIdManagerRef,

    #[cfg(not(madsim))]
    tracing: Arc<risingwave_tracing::RwTracingService>,
}

impl HummockStorage {
    /// Creates a [`HummockStorage`] with default stats. Should only be used by tests.
    #[cfg(any(test, feature = "test"))]
    pub async fn for_test(
        options: Arc<StorageConfig>,
        sstable_store: SstableStoreRef,
        hummock_meta_client: Arc<dyn HummockMetaClient>,
        notification_client: impl NotificationClient,
    ) -> HummockResult<Self> {
        Self::new(
            options,
            sstable_store,
            hummock_meta_client,
            notification_client,
            Arc::new(StateStoreMetrics::unused()),
            Arc::new(CompactionGroupClientImpl::Dummy(
                DummyCompactionGroupClient::new(StaticCompactionGroupId::StateDefault.into()),
            )),
        )
        .await
    }

    /// Creates a [`HummockStorage`].
    pub async fn new(
        options: Arc<StorageConfig>,
        sstable_store: SstableStoreRef,
        hummock_meta_client: Arc<dyn HummockMetaClient>,
        notification_client: impl NotificationClient,
        // TODO: separate `HummockStats` from `StateStoreMetrics`.
        stats: Arc<StateStoreMetrics>,
        compaction_group_client: Arc<CompactionGroupClientImpl>,
    ) -> HummockResult<Self> {
        // For conflict key detection. Enabled by setting `write_conflict_detection_enabled` to
        // true in `StorageConfig`
        let write_conflict_detector = ConflictDetector::new_from_config(options.clone());
        let sstable_id_manager = Arc::new(SstableIdManager::new(
            hummock_meta_client.clone(),
            options.sstable_id_remote_fetch_number,
        ));
        let local_version_manager = LocalVersionManager::new(
            options.clone(),
            sstable_store.clone(),
            stats.clone(),
            hummock_meta_client.clone(),
            notification_client,
            write_conflict_detector,
            sstable_id_manager.clone(),
        )
        .await;

        let instance = Self {
            options,
            local_version_manager,
            hummock_meta_client,
            sstable_store,
            stats,
            compaction_group_client,
            sstable_id_manager,
            #[cfg(not(madsim))]
            tracing: Arc::new(risingwave_tracing::RwTracingService::new()),
        };
        Ok(instance)
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

    pub fn local_version_manager(&self) -> &LocalVersionManagerRef {
        &self.local_version_manager
    }

    async fn get_compaction_group_id(&self, table_id: TableId) -> HummockResult<CompactionGroupId> {
        self.compaction_group_client
            .get_compaction_group_id(table_id.table_id)
            .await
    }

    pub fn sstable_id_manager(&self) -> &SstableIdManagerRef {
        &self.sstable_id_manager
    }
}

pub async fn get_from_sstable_info(
    sstable_store_ref: SstableStoreRef,
    sstable_info: &SstableInfo,
    internal_key: &[u8],
    check_bloom_filter: bool,
    local_stats: &mut StoreLocalStatistic,
) -> HummockResult<Option<HummockValue<Bytes>>> {
    let sstable = sstable_store_ref.sstable(sstable_info, local_stats).await?;

    let ukey = user_key(internal_key);
    if check_bloom_filter && !hit_sstable_bloom_filter(sstable.value(), ukey, local_stats) {
        return Ok(None);
    }

    // TODO: now SstableIterator does not use prefetch through SstableIteratorReadOptions, so we
    // use default before refinement.
    let mut iter = SstableIterator::create(
        sstable,
        sstable_store_ref.clone(),
        Arc::new(SstableIteratorReadOptions::default()),
    );
    iter.seek(internal_key).await?;
    // Iterator has sought passed the borders.
    if !iter.is_valid() {
        return Ok(None);
    }

    // Iterator gets us the key, we tell if it's the key we want
    // or key next to it.
    let value = match key::user_key(iter.key()) == ukey {
        true => Some(iter.value().to_bytes()),
        false => None,
    };
    iter.collect_local_statistic(local_stats);

    Ok(value)
}

pub fn hit_sstable_bloom_filter(
    sstable_info_ref: &Sstable,
    key: &[u8],
    local_stats: &mut StoreLocalStatistic,
) -> bool {
    local_stats.bloom_filter_check_counts += 1;
    let surely_not_have = sstable_info_ref.surely_not_have_user_key(key);

    if surely_not_have {
        local_stats.bloom_filter_true_negative_count += 1;
    }

    !surely_not_have
}

/// Get `user_value` from `OrderSortedUncommittedData`. If not get successful, return None.
pub async fn get_from_order_sorted_uncommitted_data(
    sstable_store_ref: SstableStoreRef,
    order_sorted_uncommitted_data: OrderSortedUncommittedData,
    internal_key: &[u8],
    local_stats: &mut StoreLocalStatistic,
    key: &[u8],
    check_bloom_filter: bool,
) -> StorageResult<(Option<HummockValue<Bytes>>, i32)> {
    let mut table_counts = 0;
    let epoch = key::get_epoch(internal_key);
    for data_list in order_sorted_uncommitted_data {
        for data in data_list {
            match data {
                UncommittedData::Batch(batch) => {
                    assert!(batch.epoch() <= epoch, "batch'epoch greater than epoch");
                    if let Some(data) = get_from_batch(&batch, key, local_stats) {
                        return Ok((Some(data), table_counts));
                    }
                }

                UncommittedData::Sst((_, sstable_info)) => {
                    table_counts += 1;

                    if let Some(data) = get_from_sstable_info(
                        sstable_store_ref.clone(),
                        &sstable_info,
                        internal_key,
                        check_bloom_filter,
                        local_stats,
                    )
                    .await?
                    {
                        return Ok((Some(data), table_counts));
                    }
                }
            }
        }
    }
    Ok((None, table_counts))
}

/// Get `user_value` from `SharedBufferBatch`
pub fn get_from_batch(
    batch: &SharedBufferBatch,
    key: &[u8],
    local_stats: &mut StoreLocalStatistic,
) -> Option<HummockValue<Bytes>> {
    batch.get(key).map(|v| {
        local_stats.get_shared_buffer_hit_counts += 1;
        v
    })
}

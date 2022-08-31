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

use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;

use bytes::Bytes;
use parking_lot::Mutex;
use risingwave_common::config::StorageConfig;
use risingwave_hummock_sdk::*;
use risingwave_rpc_client::HummockMetaClient;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

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
pub mod local_version;
pub mod local_version_manager;
pub mod shared_buffer;
pub mod sstable_store;
mod state_store;
#[cfg(any(test, feature = "test"))]
pub mod test_utils;
pub mod utils;
pub use compactor::{CompactorMemoryCollector, CompactorSstableStore};
pub use utils::MemoryLimiter;
pub mod store;
pub mod vacuum;
mod validator;
pub mod value;
pub use error::*;
pub use risingwave_common::cache::{CacheableEntry, LookupResult, LruCache};
use risingwave_common::catalog::TableId;
use risingwave_hummock_sdk::compaction_group::StaticCompactionGroupId;
use risingwave_hummock_sdk::filter_key_extractor::FilterKeyExtractorManagerRef;
pub use validator::*;
use value::*;

use self::iterator::HummockIterator;
use self::key::user_key;
pub use self::sstable_store::*;
pub use self::state_store::HummockStateStoreIter;
use super::monitor::StateStoreMetrics;
use crate::hummock::compaction_group_client::{CompactionGroupClient, DummyCompactionGroupClient};
use crate::hummock::conflict_detector::ConflictDetector;
use crate::hummock::local_version_manager::LocalVersionManager;
use crate::hummock::sstable::SstableIteratorReadOptions;
use crate::hummock::sstable_store::{SstableStoreRef, TableHolder};
use crate::monitor::StoreLocalStatistic;

// TODO: add more msg type
pub enum StorageControlMsg {}

/// Hummock is the state store backend.
#[derive(Clone)]
pub struct HummockStorage {
    options: Arc<StorageConfig>,

    local_version_manager: Arc<LocalVersionManager>,

    hummock_meta_client: Arc<dyn HummockMetaClient>,

    sstable_store: SstableStoreRef,

    /// Statistics
    stats: Arc<StateStoreMetrics>,

    compaction_group_client: Arc<dyn CompactionGroupClient>,

    sstable_id_manager: SstableIdManagerRef,

    #[cfg(not(madsim))]
    tracing: Arc<risingwave_tracing::RwTracingService>,

    hummock_event_tx: UnboundedSender<HummockEvent>,

    next_local_instance_id: Arc<AtomicU64>,
}

impl HummockStorage {
    /// Creates a [`HummockStorage`] with default stats. Should only be used by tests.
    pub async fn for_test(
        options: Arc<StorageConfig>,
        sstable_store: SstableStoreRef,
        hummock_meta_client: Arc<dyn HummockMetaClient>,
        filter_key_extractor_manager: FilterKeyExtractorManagerRef,
    ) -> HummockResult<Self> {
        Self::new(
            options,
            sstable_store,
            hummock_meta_client,
            Arc::new(StateStoreMetrics::unused()),
            Arc::new(DummyCompactionGroupClient::new(
                StaticCompactionGroupId::StateDefault.into(),
            )),
            filter_key_extractor_manager,
        )
        .await
    }

    /// Creates a [`HummockStorage`].
    pub async fn new(
        options: Arc<StorageConfig>,
        sstable_store: SstableStoreRef,
        hummock_meta_client: Arc<dyn HummockMetaClient>,
        // TODO: separate `HummockStats` from `StateStoreMetrics`.
        stats: Arc<StateStoreMetrics>,
        compaction_group_client: Arc<dyn CompactionGroupClient>,
        filter_key_extractor_manager: FilterKeyExtractorManagerRef,
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
            write_conflict_detector,
            sstable_id_manager.clone(),
            filter_key_extractor_manager,
        )
        .await;

        let (tx, mut rx) = unbounded_channel();
        tokio::spawn(async move {
            let mut control_msg_tx_holder = HashMap::new();
            while let Some(event) = rx.recv().await {
                match event {
                    HummockEvent::NewInstance {
                        local_instance_id,
                        control_msg_tx,
                    } => {
                        assert!(control_msg_tx_holder
                            .insert(local_instance_id, control_msg_tx)
                            .is_none());
                    }
                    HummockEvent::InstanceDropped { local_instance_id } => {
                        assert!(control_msg_tx_holder.remove(&local_instance_id).is_some());
                    }
                }
            }
        });

        let instance = Self {
            options: options.clone(),
            local_version_manager,
            hummock_meta_client,
            sstable_store,
            stats,
            compaction_group_client,
            sstable_id_manager,
            #[cfg(not(madsim))]
            tracing: Arc::new(risingwave_tracing::RwTracingService::new()),
            hummock_event_tx: tx,
            next_local_instance_id: Arc::new(AtomicU64::new(0)),
        };
        Ok(instance)
    }

    async fn get_from_table(
        &self,
        sstable: TableHolder,
        internal_key: &[u8],
        key: &[u8],
        check_bloom_filter: bool,
        stats: &mut StoreLocalStatistic,
    ) -> HummockResult<Option<Option<Bytes>>> {
        if check_bloom_filter && !Self::hit_sstable_bloom_filter(sstable.value(), key, stats) {
            return Ok(None);
        }

        // TODO: now SstableIterator does not use prefetch through SstableIteratorReadOptions, so we
        // use default before refinement.
        let mut iter = SstableIterator::create(
            sstable,
            self.sstable_store.clone(),
            Arc::new(SstableIteratorReadOptions::default()),
        );
        iter.seek(internal_key).await?;
        // Iterator has seeked passed the borders.
        if !iter.is_valid() {
            return Ok(None);
        }

        // Iterator gets us the key, we tell if it's the key we want
        // or key next to it.
        let value = match user_key(iter.key()) == key {
            true => Some(iter.value().into_user_value().map(Bytes::copy_from_slice)),
            false => None,
        };
        iter.collect_local_statistic(stats);
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

    async fn get_compaction_group_id(&self, table_id: TableId) -> HummockResult<CompactionGroupId> {
        self.compaction_group_client
            .get_compaction_group_id(table_id.table_id)
            .await
    }

    pub fn sstable_id_manager(&self) -> &SstableIdManagerRef {
        &self.sstable_id_manager
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

    pub fn start_local_state_store(&self) -> LocalHummockStorageWrapper {
        let local_instance = self.new_local_state_store_inner();
        LocalHummockStorageWrapper {
            inner: Arc::new(local_instance),
            instance_collector: Arc::new(Default::default()),
        }
    }

    fn new_local_state_store_inner(&self) -> LocalHummockStorageInner {
        let local_instance_id = self.next_local_instance_id.fetch_add(1, Relaxed);
        let (control_tx, control_rx) = unbounded_channel();
        let _ = self.hummock_event_tx.send(HummockEvent::NewInstance {
            local_instance_id,
            control_msg_tx: control_tx,
        });

        LocalHummockStorageInner {
            local_instance_id,
            event_tx: self.hummock_event_tx.clone(),
            global_instance: self.clone(),
            control_msg_rx_holder: Some(control_rx),
        }
    }
}

pub(crate) enum HummockEvent {
    NewInstance {
        local_instance_id: u64,
        control_msg_tx: UnboundedSender<StorageControlMsg>,
    },
    InstanceDropped {
        local_instance_id: u64,
    },
}

pub struct LocalHummockStorageInner {
    local_instance_id: u64,
    event_tx: UnboundedSender<HummockEvent>,
    // TODO: use the local state store
    // This a just a temporary way to access data.
    // Will be replaced after we implement local state store
    global_instance: HummockStorage,
    control_msg_rx_holder: Option<UnboundedReceiver<StorageControlMsg>>,
}

impl LocalHummockStorageInner {
    pub fn apply_control_msg(&self, msg: StorageControlMsg) {
        match msg {}
    }
}

impl Drop for LocalHummockStorageInner {
    fn drop(&mut self) {
        let _ = self.event_tx.send(HummockEvent::InstanceDropped {
            local_instance_id: self.local_instance_id,
        });
    }
}

pub struct LocalHummockStorageWrapper {
    inner: Arc<LocalHummockStorageInner>,
    #[allow(clippy::type_complexity)]
    instance_collector: Arc<
        Mutex<
            Option<
                Vec<(
                    Arc<LocalHummockStorageInner>,
                    UnboundedReceiver<StorageControlMsg>,
                )>,
            >,
        >,
    >,
}

impl LocalHummockStorageWrapper {
    pub fn collect_local_state_store_instance(
        &self,
    ) -> Vec<(
        Arc<LocalHummockStorageInner>,
        UnboundedReceiver<StorageControlMsg>,
    )> {
        self.instance_collector
            .try_lock()
            .expect("should not have any contention")
            .take()
            .expect("should be some when collecting instance")
    }
}

impl Clone for LocalHummockStorageWrapper {
    fn clone(&self) -> Self {
        let mut new_local_instance = self.inner.global_instance.new_local_state_store_inner();
        let control_rx = new_local_instance
            .control_msg_rx_holder
            .take()
            .expect("should be some when the local instance is just created");
        let new_local_instance = Arc::new(new_local_instance);

        self.instance_collector
            .try_lock()
            .expect("should not have any contention")
            .as_mut()
            .expect("should be some when we are still creating new local instance")
            .push((new_local_instance.clone(), control_rx));
        Self {
            inner: new_local_instance,
            instance_collector: self.instance_collector.clone(),
        }
    }
}

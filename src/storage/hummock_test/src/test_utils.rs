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

use std::sync::Arc;

use bytes::Bytes;
use itertools::Itertools;
use risingwave_common::catalog::TableId;
use risingwave_common_service::observer_manager::ObserverManager;
use risingwave_hummock_sdk::compaction_group::StaticCompactionGroupId;
use risingwave_hummock_sdk::filter_key_extractor::{
    FilterKeyExtractorManager, FilterKeyExtractorManagerRef,
};
use risingwave_meta::hummock::test_utils::{
    register_table_ids_to_compaction_group, setup_compute_env,
    update_filter_key_extractor_for_table_ids, update_filter_key_extractor_for_tables,
};
use risingwave_meta::hummock::{HummockManagerRef, MockHummockMetaClient};
use risingwave_meta::manager::MetaSrvEnv;
use risingwave_meta::storage::{MemStore, MetaStore};
use risingwave_pb::catalog::Table as ProstTable;
use risingwave_pb::common::WorkerNode;
use risingwave_pb::hummock::version_update_payload;
use risingwave_rpc_client::HummockMetaClient;
use risingwave_storage::error::StorageResult;
use risingwave_storage::hummock::backup_reader::BackupReader;
use risingwave_storage::hummock::event_handler::HummockEvent;
use risingwave_storage::hummock::iterator::test_utils::mock_sstable_store;
use risingwave_storage::hummock::local_version::pinned_version::PinnedVersion;
use risingwave_storage::hummock::observer_manager::HummockObserverNode;
use risingwave_storage::hummock::test_utils::default_opts_for_test;
use risingwave_storage::hummock::write_limiter::WriteLimiter;
use risingwave_storage::hummock::HummockStorage;
use risingwave_storage::storage_value::StorageValue;
use risingwave_storage::store::*;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

use crate::mock_notification_client::get_notification_client_for_test;

pub async fn prepare_first_valid_version(
    env: MetaSrvEnv<MemStore>,
    hummock_manager_ref: HummockManagerRef<MemStore>,
    worker_node: WorkerNode,
) -> (
    PinnedVersion,
    UnboundedSender<HummockEvent>,
    UnboundedReceiver<HummockEvent>,
) {
    let (tx, mut rx) = unbounded_channel();
    let notification_client =
        get_notification_client_for_test(env, hummock_manager_ref.clone(), worker_node.clone());
    let backup_manager = BackupReader::unused();
    let write_limiter = WriteLimiter::unused();
    let observer_manager = ObserverManager::new(
        notification_client,
        HummockObserverNode::new(
            Arc::new(FilterKeyExtractorManager::default()),
            backup_manager,
            tx.clone(),
            write_limiter,
        ),
    )
    .await;
    observer_manager.start().await;
    let hummock_version = match rx.recv().await {
        Some(HummockEvent::VersionUpdate(version_update_payload::Payload::PinnedVersion(
            version,
        ))) => version,
        _ => unreachable!("should be full version"),
    };

    (
        PinnedVersion::new(hummock_version, unbounded_channel().0),
        tx,
        rx,
    )
}

#[async_trait::async_trait]
pub trait TestIngestBatch: LocalStateStore {
    async fn ingest_batch(
        &mut self,
        kv_pairs: Vec<(Bytes, StorageValue)>,
        delete_ranges: Vec<(Bytes, Bytes)>,
        write_options: WriteOptions,
    ) -> StorageResult<usize>;
}

#[async_trait::async_trait]
impl<S: LocalStateStore> TestIngestBatch for S {
    async fn ingest_batch(
        &mut self,
        kv_pairs: Vec<(Bytes, StorageValue)>,
        delete_ranges: Vec<(Bytes, Bytes)>,
        write_options: WriteOptions,
    ) -> StorageResult<usize> {
        assert_eq!(self.epoch(), write_options.epoch);
        for (key, value) in kv_pairs {
            match value.user_value {
                None => self.delete(key, Bytes::new())?,
                Some(value) => self.insert(key, value, None)?,
            }
        }
        self.flush(delete_ranges).await
    }
}

#[async_trait::async_trait]
pub(crate) trait HummockStateStoreTestTrait: StateStore {
    fn get_pinned_version(&self) -> PinnedVersion;
    async fn seal_and_sync_epoch(&self, epoch: u64) -> StorageResult<SyncResult> {
        self.seal_epoch(epoch, true);
        self.sync(epoch).await
    }
}

impl HummockStateStoreTestTrait for HummockStorage {
    fn get_pinned_version(&self) -> PinnedVersion {
        self.get_pinned_version()
    }
}

pub async fn with_hummock_storage_v2(
    table_id: TableId,
) -> (HummockStorage, Arc<MockHummockMetaClient>) {
    let sstable_store = mock_sstable_store();
    let hummock_options = Arc::new(default_opts_for_test());
    let (env, hummock_manager_ref, _cluster_manager_ref, worker_node) =
        setup_compute_env(8080).await;
    let meta_client = Arc::new(MockHummockMetaClient::new(
        hummock_manager_ref.clone(),
        worker_node.id,
    ));

    let hummock_storage = HummockStorage::for_test(
        hummock_options,
        sstable_store,
        meta_client.clone(),
        get_notification_client_for_test(env, hummock_manager_ref.clone(), worker_node),
    )
    .await
    .unwrap();

    register_tables_with_id_for_test(
        hummock_storage.filter_key_extractor_manager(),
        &hummock_manager_ref,
        &[table_id.table_id()],
    )
    .await;

    (hummock_storage, meta_client)
}

pub async fn register_tables_with_id_for_test<S: MetaStore>(
    filter_key_extractor_manager: &FilterKeyExtractorManagerRef,
    hummock_manager_ref: &HummockManagerRef<S>,
    table_ids: &[u32],
) {
    update_filter_key_extractor_for_table_ids(filter_key_extractor_manager, table_ids);
    register_table_ids_to_compaction_group(
        hummock_manager_ref,
        table_ids,
        StaticCompactionGroupId::StateDefault.into(),
    )
    .await;
}

pub async fn register_tables_with_catalog_for_test<S: MetaStore>(
    filter_key_extractor_manager: &FilterKeyExtractorManagerRef,
    hummock_manager_ref: &HummockManagerRef<S>,
    tables: &[ProstTable],
) {
    update_filter_key_extractor_for_tables(filter_key_extractor_manager, tables);
    let table_ids = tables.iter().map(|t| t.id).collect_vec();
    register_table_ids_to_compaction_group(
        hummock_manager_ref,
        &table_ids,
        StaticCompactionGroupId::StateDefault.into(),
    )
    .await;
}

pub struct HummockTestEnv {
    pub storage: HummockStorage,
    pub manager: HummockManagerRef<MemStore>,
    pub meta_client: Arc<MockHummockMetaClient>,
}

impl HummockTestEnv {
    pub async fn register_table_id(&self, table_id: TableId) {
        register_tables_with_id_for_test(
            self.storage.filter_key_extractor_manager(),
            &self.manager,
            &[table_id.table_id()],
        )
        .await;
    }

    pub async fn register_table(&self, table: ProstTable) {
        register_tables_with_catalog_for_test(
            self.storage.filter_key_extractor_manager(),
            &self.manager,
            &[table],
        )
        .await;
    }

    // Seal, sync and commit a epoch.
    // On completion of this function call, the provided epoch should be committed and visible.
    pub async fn commit_epoch(&self, epoch: u64) {
        let res = self.storage.seal_and_sync_epoch(epoch).await.unwrap();
        self.meta_client
            .commit_epoch(epoch, res.uncommitted_ssts)
            .await
            .unwrap();
        self.storage.try_wait_epoch_for_test(epoch).await;
    }
}

pub async fn prepare_hummock_test_env() -> HummockTestEnv {
    let sstable_store = mock_sstable_store();
    let hummock_options = Arc::new(default_opts_for_test());
    let (env, hummock_manager_ref, _cluster_manager_ref, worker_node) =
        setup_compute_env(8080).await;

    let hummock_meta_client = Arc::new(MockHummockMetaClient::new(
        hummock_manager_ref.clone(),
        worker_node.id,
    ));

    let notification_client =
        get_notification_client_for_test(env, hummock_manager_ref.clone(), worker_node.clone());

    let storage = HummockStorage::for_test(
        hummock_options,
        sstable_store,
        hummock_meta_client.clone(),
        notification_client,
    )
    .await
    .unwrap();

    HummockTestEnv {
        storage,
        manager: hummock_manager_ref,
        meta_client: hummock_meta_client,
    }
}

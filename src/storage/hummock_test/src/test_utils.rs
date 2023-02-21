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

use std::collections::Bound;
use std::fmt::Debug;
use std::future::Future;
use std::ops::Deref;
use std::sync::Arc;

use async_stream::try_stream;
use bytes::Bytes;
use futures::TryStreamExt;
use itertools::Itertools;
use risingwave_common::catalog::TableId;
use risingwave_common_service::observer_manager::ObserverManager;
use risingwave_hummock_sdk::compaction_group::StaticCompactionGroupId;
use risingwave_hummock_sdk::filter_key_extractor::{
    FilterKeyExtractorManager, FilterKeyExtractorManagerRef,
};
use risingwave_hummock_sdk::HummockReadEpoch;
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
use risingwave_storage::hummock::store::state_store::LocalHummockStorage;
use risingwave_storage::hummock::test_utils::default_opts_for_test;
use risingwave_storage::hummock::HummockStorage;
use risingwave_storage::storage_value::StorageValue;
use risingwave_storage::store::*;
use risingwave_storage::{
    define_state_store_associated_type, define_state_store_read_associated_type,
    define_state_store_write_associated_type,
};
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
    let observer_manager = ObserverManager::new(
        notification_client,
        HummockObserverNode::new(
            Arc::new(FilterKeyExtractorManager::default()),
            backup_manager,
            tx.clone(),
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
pub(crate) trait HummockStateStoreTestTrait: StateStore + StateStoreWrite {
    fn get_pinned_version(&self) -> PinnedVersion;
    async fn seal_and_sync_epoch(&self, epoch: u64) -> StorageResult<SyncResult> {
        self.seal_epoch(epoch, true);
        self.sync(epoch).await
    }
}

fn assert_result_eq<Item: PartialEq + Debug, E>(
    first: &std::result::Result<Item, E>,
    second: &std::result::Result<Item, E>,
) {
    match (first, second) {
        (Ok(first), Ok(second)) => {
            assert_eq!(first, second);
        }
        (Err(_), Err(_)) => {}
        _ => panic!("result not equal"),
    }
}

pub struct LocalGlobalStateStoreHolder<L, G> {
    pub(crate) local: L,
    pub(crate) global: G,
}

impl<L: StateStoreReadIterStream, G: StateStoreReadIterStream> LocalGlobalStateStoreHolder<L, G> {
    fn into_stream(self) -> impl StateStoreReadIterStream {
        try_stream! {
            let local = self.local;
            let global = self.global;
            futures::pin_mut!(local);
            futures::pin_mut!(global);
            loop {
                let local_result = local.try_next().await;
                let global_result = global.try_next().await;
                assert_result_eq(&local_result, &global_result);
                let local_next = local_result?;
                match local_next {
                    Some(local_next) => {
                        yield local_next;
                    },
                    None => {
                        break;
                    }
                }
            }
        }
    }
}

pub(crate) type LocalGlobalStateStoreIterStream<L: StateStoreRead, G: StateStoreRead> =
    impl StateStoreReadIterStream;
impl<L: StateStoreRead, G: StateStoreRead> StateStoreRead for LocalGlobalStateStoreHolder<L, G> {
    type IterStream = LocalGlobalStateStoreIterStream<L, G>;

    define_state_store_read_associated_type!();

    fn get<'a>(
        &'a self,
        key: &'a [u8],
        epoch: u64,
        read_options: ReadOptions,
    ) -> Self::GetFuture<'_> {
        async move {
            let local = self.local.get(key, epoch, read_options.clone()).await;
            let global = self.global.get(key, epoch, read_options).await;
            assert_result_eq(&local, &global);
            local
        }
    }

    fn iter(
        &self,
        key_range: (Bound<Vec<u8>>, Bound<Vec<u8>>),
        epoch: u64,
        read_options: ReadOptions,
    ) -> Self::IterFuture<'_> {
        async move {
            let local_iter = self
                .local
                .iter(key_range.clone(), epoch, read_options.clone())
                .await?;
            let global_iter = self.global.iter(key_range, epoch, read_options).await?;
            Ok(LocalGlobalStateStoreHolder {
                local: local_iter,
                global: global_iter,
            }
            .into_stream())
        }
    }
}

impl<L: StateStoreWrite, G: StaticSendSync> StateStoreWrite for LocalGlobalStateStoreHolder<L, G> {
    define_state_store_write_associated_type!();

    fn ingest_batch(
        &self,
        kv_pairs: Vec<(Bytes, StorageValue)>,
        delete_ranges: Vec<(Bytes, Bytes)>,
        write_options: WriteOptions,
    ) -> Self::IngestBatchFuture<'_> {
        self.local
            .ingest_batch(kv_pairs, delete_ranges, write_options)
    }
}

impl<G, L> Clone for LocalGlobalStateStoreHolder<G, L> {
    fn clone(&self) -> Self {
        unimplemented!()
    }
}

impl<G: StateStore> StateStore for LocalGlobalStateStoreHolder<G::Local, G> {
    type Local = G::Local;

    type NewLocalFuture<'a> = impl Future<Output = G::Local> + Send;

    define_state_store_associated_type!();

    fn try_wait_epoch(&self, epoch: HummockReadEpoch) -> Self::WaitEpochFuture<'_> {
        self.global.try_wait_epoch(epoch)
    }

    fn sync(&self, epoch: u64) -> Self::SyncFuture<'_> {
        self.global.sync(epoch)
    }

    fn seal_epoch(&self, epoch: u64, is_checkpoint: bool) {
        self.global.seal_epoch(epoch, is_checkpoint)
    }

    fn clear_shared_buffer(&self) -> Self::ClearSharedBufferFuture<'_> {
        async move { self.global.clear_shared_buffer().await }
    }

    fn new_local(&self, _table_id: TableId) -> Self::NewLocalFuture<'_> {
        async { unimplemented!("should not be called new local again") }
    }

    fn validate_read_epoch(&self, epoch: HummockReadEpoch) -> StorageResult<()> {
        self.global.validate_read_epoch(epoch)
    }
}

impl<G: StateStore> LocalGlobalStateStoreHolder<G::Local, G> {
    pub async fn new(state_store: G, table_id: TableId) -> Self {
        LocalGlobalStateStoreHolder {
            local: state_store.new_local(table_id).await,
            global: state_store,
        }
    }
}

pub type HummockV2MixedStateStore =
    LocalGlobalStateStoreHolder<LocalHummockStorage, HummockStorage>;

impl Deref for HummockV2MixedStateStore {
    type Target = HummockStorage;

    fn deref(&self) -> &Self::Target {
        &self.global
    }
}

impl HummockStateStoreTestTrait for HummockV2MixedStateStore {
    fn get_pinned_version(&self) -> PinnedVersion {
        self.global.get_pinned_version()
    }
}

pub async fn with_hummock_storage_v2(
    table_id: TableId,
) -> (HummockV2MixedStateStore, Arc<MockHummockMetaClient>) {
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

    (
        HummockV2MixedStateStore::new(hummock_storage, table_id).await,
        meta_client,
    )
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

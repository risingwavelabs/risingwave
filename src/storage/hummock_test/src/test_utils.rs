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

use std::collections::Bound;
use std::fmt::Debug;
use std::future::Future;
use std::ops::Deref;
use std::sync::Arc;

use bytes::{BufMut, Bytes};
use risingwave_common::catalog::TableId;
use risingwave_common::error::Result;
use risingwave_common::util::addr::HostAddr;
use risingwave_common_service::observer_manager::{Channel, NotificationClient, ObserverManager};
use risingwave_hummock_sdk::filter_key_extractor::FilterKeyExtractorManager;
use risingwave_hummock_sdk::HummockReadEpoch;
use risingwave_meta::hummock::test_utils::setup_compute_env;
use risingwave_meta::hummock::{HummockManager, HummockManagerRef, MockHummockMetaClient};
use risingwave_meta::manager::{MessageStatus, MetaSrvEnv, NotificationManagerRef, WorkerKey};
use risingwave_meta::storage::{MemStore, MetaStore};
use risingwave_pb::common::WorkerNode;
use risingwave_pb::hummock::pin_version_response;
use risingwave_pb::meta::subscribe_response::{Info, Operation};
use risingwave_pb::meta::{MetaSnapshot, SubscribeResponse, SubscribeType};
use risingwave_storage::error::StorageResult;
use risingwave_storage::hummock::event_handler::HummockEvent;
use risingwave_storage::hummock::iterator::test_utils::mock_sstable_store;
use risingwave_storage::hummock::local_version::pinned_version::PinnedVersion;
use risingwave_storage::hummock::observer_manager::HummockObserverNode;
use risingwave_storage::hummock::store::state_store::LocalHummockStorage;
use risingwave_storage::hummock::test_utils::default_config_for_test;
use risingwave_storage::hummock::{HummockStorage, HummockStorageV1};
use risingwave_storage::monitor::StateStoreMetrics;
use risingwave_storage::storage_value::StorageValue;
use risingwave_storage::store::{
    EmptyFutureTrait, GetFutureTrait, IngestBatchFutureTrait, IterFutureTrait, NextFutureTrait,
    ReadOptions, StateStoreRead, StateStoreWrite, StaticSendSync, SyncFutureTrait, SyncResult,
    WriteOptions,
};
use risingwave_storage::{
    define_state_store_associated_type, define_state_store_read_associated_type,
    define_state_store_write_associated_type, StateStore, StateStoreIter,
};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

pub struct TestNotificationClient<S: MetaStore> {
    addr: HostAddr,
    notification_manager: NotificationManagerRef<S>,
    hummock_manager: HummockManagerRef<S>,
}

pub struct TestChannel<T>(UnboundedReceiver<std::result::Result<T, MessageStatus>>);

#[async_trait::async_trait]
impl<T: Send + 'static> Channel for TestChannel<T> {
    type Item = T;

    async fn message(&mut self) -> std::result::Result<Option<T>, MessageStatus> {
        match self.0.recv().await {
            None => Ok(None),
            Some(result) => result.map(|r| Some(r)),
        }
    }
}

impl<S: MetaStore> TestNotificationClient<S> {
    pub fn new(
        addr: HostAddr,
        notification_manager: NotificationManagerRef<S>,
        hummock_manager: HummockManagerRef<S>,
    ) -> Self {
        Self {
            addr,
            notification_manager,
            hummock_manager,
        }
    }
}

#[async_trait::async_trait]
impl<S: MetaStore> NotificationClient for TestNotificationClient<S> {
    type Channel = TestChannel<SubscribeResponse>;

    async fn subscribe(&self, subscribe_type: SubscribeType) -> Result<Self::Channel> {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

        let hummock_manager_guard = self.hummock_manager.get_read_guard().await;
        let meta_snapshot = MetaSnapshot {
            hummock_version: Some(hummock_manager_guard.current_version.clone()),
            ..Default::default()
        };
        tx.send(Ok(SubscribeResponse {
            status: None,
            operation: Operation::Snapshot as i32,
            info: Some(Info::Snapshot(meta_snapshot)),
            version: self.notification_manager.current_version().await,
        }))
        .unwrap();
        self.notification_manager
            .insert_sender(subscribe_type, WorkerKey(self.addr.to_protobuf()), tx)
            .await;
        Ok(TestChannel(rx))
    }
}

pub fn get_test_notification_client(
    env: MetaSrvEnv<MemStore>,
    hummock_manager_ref: Arc<HummockManager<MemStore>>,
    worker_node: WorkerNode,
) -> TestNotificationClient<MemStore> {
    TestNotificationClient::new(
        worker_node.get_host().unwrap().into(),
        env.notification_manager_ref(),
        hummock_manager_ref,
    )
}

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
        get_test_notification_client(env, hummock_manager_ref.clone(), worker_node.clone());
    let observer_manager = ObserverManager::new(
        notification_client,
        HummockObserverNode::new(Arc::new(FilterKeyExtractorManager::default()), tx.clone()),
    )
    .await;
    let _ = observer_manager.start().await.unwrap();
    let hummock_version = match rx.recv().await {
        Some(HummockEvent::VersionUpdate(pin_version_response::Payload::PinnedVersion(
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

/// Prefix the `key` with a dummy table id.
/// We use `0` because：
/// - This value is used in the code to identify unit tests and prevent some parameters that are not
///   easily constructible in tests from breaking the test.
/// - When calling state store interfaces, we normally pass `TableId::default()`, which is `0`.
pub fn prefixed_key<T: AsRef<[u8]>>(key: T) -> Bytes {
    let mut buf = Vec::new();
    buf.put_u32(0);
    buf.put_slice(key.as_ref());
    buf.into()
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

impl<L: StateStoreIter<Item: PartialEq + Debug>, G: StateStoreIter<Item = L::Item>> StateStoreIter
    for LocalGlobalStateStoreHolder<L, G>
{
    type Item = L::Item;

    type NextFuture<'a> = impl NextFutureTrait<'a, L::Item>;

    fn next(&mut self) -> Self::NextFuture<'_> {
        async {
            let local_result = self.local.next().await;
            let global_result = self.global.next().await;
            assert_result_eq(&local_result, &global_result);
            local_result
        }
    }
}

impl<L: StateStoreRead, G: StateStoreRead> StateStoreRead for LocalGlobalStateStoreHolder<L, G> {
    type Iter = LocalGlobalStateStoreHolder<L::Iter, G::Iter>;

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
            })
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

impl<G: Clone, L: Clone> Clone for LocalGlobalStateStoreHolder<G, L> {
    fn clone(&self) -> Self {
        Self {
            local: self.local.clone(),
            global: self.global.clone(),
        }
    }
}

impl<G: StateStore> StateStore for LocalGlobalStateStoreHolder<G::Local, G>
where
    <G as StateStore>::Local: Clone,
{
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
}

impl<G: StateStore> LocalGlobalStateStoreHolder<G::Local, G> {
    pub(crate) async fn new(state_store: G) -> Self {
        LocalGlobalStateStoreHolder {
            local: state_store.new_local(TEST_TABLE_ID).await,
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

impl HummockStateStoreTestTrait for HummockStorageV1 {
    fn get_pinned_version(&self) -> PinnedVersion {
        self.get_pinned_version()
    }
}

pub(crate) async fn with_hummock_storage_v1() -> (HummockStorageV1, Arc<MockHummockMetaClient>) {
    let sstable_store = mock_sstable_store();
    let hummock_options = Arc::new(default_config_for_test());
    let (env, hummock_manager_ref, _cluster_manager_ref, worker_node) =
        setup_compute_env(8080).await;
    let meta_client = Arc::new(MockHummockMetaClient::new(
        hummock_manager_ref.clone(),
        worker_node.id,
    ));

    let hummock_storage = HummockStorageV1::new(
        hummock_options,
        sstable_store,
        meta_client.clone(),
        get_test_notification_client(env, hummock_manager_ref, worker_node),
        Arc::new(StateStoreMetrics::unused()),
    )
    .await
    .unwrap();

    (hummock_storage, meta_client)
}

pub(crate) const TEST_TABLE_ID: TableId = TableId { table_id: 233 };

pub(crate) async fn with_hummock_storage_v2(
) -> (HummockV2MixedStateStore, Arc<MockHummockMetaClient>) {
    let sstable_store = mock_sstable_store();
    let hummock_options = Arc::new(default_config_for_test());
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
        get_test_notification_client(env, hummock_manager_ref, worker_node),
    )
    .await
    .unwrap();

    (
        HummockV2MixedStateStore::new(hummock_storage).await,
        meta_client,
    )
}

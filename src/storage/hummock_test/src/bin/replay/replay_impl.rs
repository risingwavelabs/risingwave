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

use std::ops::Bound;

use futures::stream::BoxStream;
use futures::{Stream, StreamExt};
use futures_async_stream::{for_await, try_stream};
use risingwave_common::error::Result as RwResult;
use risingwave_common::util::addr::HostAddr;
use risingwave_common_service::observer_manager::{Channel, NotificationClient};
use risingwave_hummock_sdk::HummockReadEpoch;
use risingwave_hummock_trace::{
    GlobalReplay, LocalReplay, LocalReplayRead, ReplayItem, ReplayRead, ReplayStateStore,
    ReplayWrite, Result, TraceError, TracedBytes, TracedNewLocalOptions, TracedReadOptions,
    TracedSubResp,
};
use risingwave_meta::manager::{MessageStatus, MetaSrvEnv, NotificationManagerRef, WorkerKey};
use risingwave_meta::storage::{MemStore, MetaStore};
use risingwave_pb::common::WorkerNode;
use risingwave_pb::meta::subscribe_response::{Info, Operation as RespOperation};
use risingwave_pb::meta::{SubscribeResponse, SubscribeType};
use risingwave_storage::hummock::store::state_store::LocalHummockStorage;
use risingwave_storage::hummock::HummockStorage;
use risingwave_storage::store::{
    LocalStateStore, StateStoreIterItemStream, StateStoreRead, SyncResult,
};
use risingwave_storage::{StateStore, StateStoreReadIterStream};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver};
pub(crate) struct GlobalReplayIter<S>
where
    S: StateStoreReadIterStream,
{
    inner: S,
}

impl<S> GlobalReplayIter<S>
where
    S: StateStoreReadIterStream,
{
    pub(crate) fn new(inner: S) -> Self {
        Self { inner }
    }

    pub(crate) fn into_stream(self) -> impl Stream<Item = Result<ReplayItem>> {
        self.inner.map(|item_res| {
            item_res
                .map(|(key, value)| (key.user_key.table_key.0.into(), value.into()))
                .map_err(|_| TraceError::IterFailed("iter failed to retrieve item".to_string()))
        })
    }
}

pub(crate) struct LocalReplayIter {
    inner: Vec<ReplayItem>,
}

impl LocalReplayIter {
    pub(crate) async fn new(stream: impl StateStoreIterItemStream) -> Self {
        let mut inner: Vec<_> = Vec::new();
        #[for_await]
        for value in stream {
            let value = value.unwrap();
            inner.push((value.0.user_key.table_key.0.into(), value.1.into()));
        }
        Self { inner }
    }

    #[try_stream(ok = ReplayItem, error = TraceError)]
    pub(crate) async fn into_stream(self) {
        for (key, value) in self.inner {
            yield (key, value)
        }
    }
}

pub(crate) struct GlobalReplayImpl {
    store: HummockStorage,
    notifier: NotificationManagerRef<MemStore>,
}

impl GlobalReplayImpl {
    pub(crate) fn new(store: HummockStorage, notifier: NotificationManagerRef<MemStore>) -> Self {
        Self { store, notifier }
    }
}

impl GlobalReplay for GlobalReplayImpl {}

#[async_trait::async_trait]
impl ReplayRead for GlobalReplayImpl {
    async fn iter(
        &self,
        key_range: (Bound<TracedBytes>, Bound<TracedBytes>),
        epoch: u64,
        read_options: TracedReadOptions,
    ) -> Result<BoxStream<'static, Result<ReplayItem>>> {
        let key_range = (
            key_range.0.map(TracedBytes::into),
            key_range.1.map(TracedBytes::into),
        );

        let iter = self
            .store
            .iter(key_range, epoch, read_options.into())
            .await
            .unwrap();
        let iter = iter.boxed();
        let stream = GlobalReplayIter::new(iter).into_stream().boxed();
        Ok(stream)
    }

    async fn get(
        &self,
        key: TracedBytes,
        epoch: u64,
        read_options: TracedReadOptions,
    ) -> Result<Option<TracedBytes>> {
        Ok(self
            .store
            .get(key.into(), epoch, read_options.into())
            .await
            .unwrap()
            .map(TracedBytes::from))
    }
}

#[async_trait::async_trait]
impl ReplayStateStore for GlobalReplayImpl {
    async fn sync(&self, id: u64) -> Result<usize> {
        let result: SyncResult = self
            .store
            .sync(id)
            .await
            .map_err(|e| TraceError::SyncFailed(format!("{e}")))?;
        Ok(result.sync_size)
    }

    fn seal_epoch(&self, epoch_id: u64, is_checkpoint: bool) {
        self.store.seal_epoch(epoch_id, is_checkpoint);
    }

    async fn notify_hummock(&self, info: Info, op: RespOperation, version: u64) -> Result<u64> {
        let prev_version_id = match &info {
            Info::HummockVersionDeltas(deltas) => deltas.version_deltas.last().map(|d| d.prev_id),
            _ => None,
        };

        self.notifier
            .notify_hummock_with_version(op, info, Some(version));

        // wait till version updated
        if let Some(prev_version_id) = prev_version_id {
            self.store.wait_version_update(prev_version_id).await;
        }
        Ok(version)
    }

    async fn new_local(&self, options: TracedNewLocalOptions) -> Box<dyn LocalReplay> {
        let local_storage = self.store.new_local(options.into()).await;
        Box::new(LocalReplayImpl(local_storage))
    }

    async fn try_wait_epoch(&self, epoch: HummockReadEpoch) -> Result<()> {
        self.store
            .try_wait_epoch(epoch)
            .await
            .map_err(|_| TraceError::TryWaitEpochFailed)?;
        Ok(())
    }

    fn validate_read_epoch(&self, epoch: HummockReadEpoch) -> Result<()> {
        self.store
            .validate_read_epoch(epoch)
            .map_err(|_| TraceError::ValidateReadEpochFailed)?;
        Ok(())
    }

    async fn clear_shared_buffer(&self) -> Result<()> {
        self.store
            .clear_shared_buffer()
            .await
            .map_err(|_| TraceError::ClearSharedBufferFailed)?;
        Ok(())
    }
}
pub(crate) struct LocalReplayImpl(LocalHummockStorage);

#[async_trait::async_trait]
impl LocalReplay for LocalReplayImpl {
    fn init(&mut self, epoch: u64) {
        self.0.init(epoch);
    }

    fn seal_current_epoch(&mut self, next_epoch: u64) {
        self.0.seal_current_epoch(next_epoch);
    }

    fn epoch(&self) -> u64 {
        self.0.epoch()
    }

    async fn flush(
        &mut self,
        delete_ranges: Vec<(Bound<TracedBytes>, Bound<TracedBytes>)>,
    ) -> Result<usize> {
        let delete_ranges = delete_ranges
            .into_iter()
            .map(|(start, end)| (start.map(TracedBytes::into), end.map(TracedBytes::into)))
            .collect();
        self.0
            .flush(delete_ranges)
            .await
            .map_err(|_| TraceError::FlushFailed)
    }

    fn is_dirty(&self) -> bool {
        self.0.is_dirty()
    }
}

#[async_trait::async_trait]
impl LocalReplayRead for LocalReplayImpl {
    async fn iter(
        &self,
        key_range: (Bound<TracedBytes>, Bound<TracedBytes>),
        read_options: TracedReadOptions,
    ) -> Result<BoxStream<'static, Result<ReplayItem>>> {
        let key_range = (key_range.0.map(|b| b.into()), key_range.1.map(|b| b.into()));

        let iter = LocalStateStore::iter(&self.0, key_range, read_options.into())
            .await
            .unwrap();

        let iter = iter.boxed();
        let stream = LocalReplayIter::new(iter).await.into_stream().boxed();
        Ok(stream)
    }

    async fn get(
        &self,
        key: TracedBytes,
        read_options: TracedReadOptions,
    ) -> Result<Option<TracedBytes>> {
        Ok(
            LocalStateStore::get(&self.0, key.into(), read_options.into())
                .await
                .unwrap()
                .map(TracedBytes::from),
        )
    }
}

#[async_trait::async_trait]
impl ReplayWrite for LocalReplayImpl {
    fn insert(
        &mut self,
        key: TracedBytes,
        new_val: TracedBytes,
        old_val: Option<TracedBytes>,
    ) -> Result<()> {
        LocalStateStore::insert(
            &mut self.0,
            key.into(),
            new_val.into(),
            old_val.map(|b| b.into()),
        )
        .unwrap();
        Ok(())
    }

    fn delete(&mut self, key: TracedBytes, old_val: TracedBytes) -> Result<()> {
        LocalStateStore::delete(&mut self.0, key.into(), old_val.into()).unwrap();
        Ok(())
    }
}

pub struct ReplayNotificationClient<S: MetaStore> {
    addr: HostAddr,
    notification_manager: NotificationManagerRef<S>,
    first_resp: Box<TracedSubResp>,
}

impl<S: MetaStore> ReplayNotificationClient<S> {
    pub fn new(
        addr: HostAddr,
        notification_manager: NotificationManagerRef<S>,
        first_resp: Box<TracedSubResp>,
    ) -> Self {
        Self {
            addr,
            notification_manager,
            first_resp,
        }
    }
}

#[async_trait::async_trait]
impl<S: MetaStore> NotificationClient for ReplayNotificationClient<S> {
    type Channel = ReplayChannel<SubscribeResponse>;

    async fn subscribe(&self, subscribe_type: SubscribeType) -> RwResult<Self::Channel> {
        let (tx, rx) = unbounded_channel();

        self.notification_manager
            .insert_sender(subscribe_type, WorkerKey(self.addr.to_protobuf()), tx)
            .await;

        // send the first snapshot message
        let op = self.first_resp.0.operation();
        let info = self.first_resp.0.info.clone();

        self.notification_manager
            .notify_hummock(op, info.unwrap())
            .await;

        Ok(ReplayChannel(rx))
    }
}

pub fn get_replay_notification_client(
    env: MetaSrvEnv<MemStore>,
    worker_node: WorkerNode,
    first_resp: Box<TracedSubResp>,
) -> ReplayNotificationClient<MemStore> {
    ReplayNotificationClient::new(
        worker_node.get_host().unwrap().into(),
        env.notification_manager_ref(),
        first_resp,
    )
}

pub struct ReplayChannel<T>(UnboundedReceiver<std::result::Result<T, MessageStatus>>);

#[async_trait::async_trait]
impl<T: Send + 'static> Channel for ReplayChannel<T> {
    type Item = T;

    async fn message(&mut self) -> std::result::Result<Option<T>, MessageStatus> {
        match self.0.recv().await {
            None => Ok(None),
            Some(result) => result.map(|r| Some(r)),
        }
    }
}

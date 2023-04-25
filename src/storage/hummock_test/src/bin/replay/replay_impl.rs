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

use std::ops::Bound;

use bytes::Bytes;
use risingwave_common::catalog::TableId;
use risingwave_common::error::Result as RwResult;
use risingwave_common::util::addr::HostAddr;
use risingwave_common_service::observer_manager::{Channel, NotificationClient};
use risingwave_hummock_sdk::key::FullKey;
use risingwave_hummock_trace::{
    GlobalReplay, LocalReplay, ReplayIter, ReplayRead, ReplayStateStore, ReplayWrite, Result,
    TraceError, TraceSubResp, TracedBytes, TracedReadOptions, TracedWriteOptions,
};
use risingwave_meta::manager::{
    MessageStatus, MetaSrvEnv, NotificationManagerRef, NotificationVersion, WorkerKey,
};
use risingwave_meta::storage::{MemStore, MetaStore};
use risingwave_pb::common::WorkerNode;
use risingwave_pb::meta::subscribe_response::{Info, Operation as RespOperation};
use risingwave_pb::meta::{SubscribeResponse, SubscribeType};
use risingwave_storage::hummock::store::state_store::LocalHummockStorage;
use risingwave_storage::hummock::HummockStorage;
use risingwave_storage::storage_value::StorageValue;
use risingwave_storage::store::{
    ReadOptions, StateStoreRead, StateStoreWrite, SyncResult, WriteOptions,
};
use risingwave_storage::{StateStore, StateStoreIter};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver};

use crate::dispatch_iter;
pub(crate) struct HummockReplayIter<I: StateStoreIter<Item = (FullKey<Vec<u8>>, Bytes)>>(I);

impl<I: StateStoreIter<Item = (FullKey<Vec<u8>>, Bytes)> + Send + Sync> HummockReplayIter<I> {
    fn new(iter: I) -> Self {
        Self(iter)
    }
}

#[async_trait::async_trait]
impl<I: StateStoreIter<Item = (FullKey<Vec<u8>>, Bytes)> + Send + Sync> ReplayIter
    for HummockReplayIter<I>
{
    async fn next(&mut self) -> Option<(TracedBytes, TracedBytes)> {
        let key_value: Option<(FullKey<Vec<u8>>, Bytes)> = self.0.next().await.unwrap();
        key_value.map(|(key, value)| {
            (
                TracedBytes::from(key.user_key.table_key.to_vec()),
                TracedBytes::from(value),
            )
        })
    }
}

pub(crate) struct GlobalReplayInterface {
    store: HummockStorage,
    notifier: NotificationManagerRef<MemStore>,
}

impl GlobalReplayInterface {
    pub(crate) fn new(store: HummockStorage, notifier: NotificationManagerRef<MemStore>) -> Self {
        Self { store, notifier }
    }
}
impl GlobalReplay for GlobalReplayInterface {}

#[async_trait::async_trait]
impl ReplayRead for GlobalReplayInterface {
    async fn iter(
        &self,
        key_range: (Bound<TracedBytes>, Bound<TracedBytes>),
        epoch: u64,
        read_options: TracedReadOptions,
    ) -> Result<Box<dyn ReplayIter>> {
        dispatch_iter!(self.store, key_range, epoch, read_options)
    }

    async fn get(
        &self,
        key: TracedBytes,
        epoch: u64,
        read_options: TracedReadOptions,
    ) -> Result<Option<TracedBytes>> {
        Ok(self
            .store
            .get(&key, epoch, from_trace_read_options(read_options))
            .await
            .unwrap()
            .map(TracedBytes::from))
    }
}

#[async_trait::async_trait]
impl ReplayStateStore for GlobalReplayInterface {
    async fn sync(&self, id: u64) -> Result<usize> {
        let result: SyncResult = self
            .store
            .sync(id)
            .await
            .map_err(|e| TraceError::SyncFailed(format!("{e}")))?;
        Ok(result.sync_size)
    }

    async fn seal_epoch(&self, epoch_id: u64, is_checkpoint: bool) {
        self.store.seal_epoch(epoch_id, is_checkpoint);
    }

    async fn notify_hummock(
        &self,
        info: Info,
        op: RespOperation,
        version: NotificationVersion,
    ) -> Result<u64> {
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

    async fn new_local(&self, table_id: u32) -> Box<dyn LocalReplay> {
        let table_id = TableId { table_id };
        let local_storage = self.store.new_local(table_id).await;
        Box::new(LocalReplayInterface(local_storage))
    }
}
pub(crate) struct LocalReplayInterface(LocalHummockStorage);

impl LocalReplay for LocalReplayInterface {}

#[async_trait::async_trait]
impl ReplayRead for LocalReplayInterface {
    async fn iter(
        &self,
        key_range: (Bound<TracedBytes>, Bound<TracedBytes>),
        epoch: u64,
        read_options: TracedReadOptions,
    ) -> Result<Box<dyn ReplayIter>> {
        dispatch_iter!(self.0, key_range, epoch, read_options)
    }

    async fn get(
        &self,
        key: TracedBytes,
        epoch: u64,
        read_options: TracedReadOptions,
    ) -> Result<Option<TracedBytes>> {
        Ok(self
            .0
            .get(&key, epoch, from_trace_read_options(read_options))
            .await
            .unwrap()
            .map(TracedBytes::from))
    }
}

#[async_trait::async_trait]
impl ReplayWrite for LocalReplayInterface {
    async fn ingest(
        &self,
        kv_pairs: Vec<(TracedBytes, Option<TracedBytes>)>,
        delete_ranges: Vec<(TracedBytes, TracedBytes)>,
        write_options: TracedWriteOptions,
    ) -> Result<usize> {
        let kv_pairs = kv_pairs
            .into_iter()
            .map(|(key, value)| {
                (
                    key.into_bytes(),
                    StorageValue {
                        user_value: value.map(TracedBytes::into_bytes),
                    },
                )
            })
            .collect();

        let delete_ranges = delete_ranges
            .into_iter()
            .map(|(left, right)| (left.into_bytes(), right.into_bytes()))
            .collect();

        let write_options = from_trace_write_options(write_options);

        let size = self
            .0
            .ingest_batch(kv_pairs, delete_ranges, write_options)
            .await
            .map_err(|e| TraceError::IngestFailed(format!("{e}")))?;

        Ok(size)
    }
}

pub struct ReplayNotificationClient<S: MetaStore> {
    addr: HostAddr,
    notification_manager: NotificationManagerRef<S>,
    first_resp: Box<TraceSubResp>,
}

impl<S: MetaStore> ReplayNotificationClient<S> {
    pub fn new(
        addr: HostAddr,
        notification_manager: NotificationManagerRef<S>,
        first_resp: Box<TraceSubResp>,
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
    first_resp: Box<TraceSubResp>,
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

fn from_trace_read_options(opt: TracedReadOptions) -> ReadOptions {
    ReadOptions {
        prefix_hint: opt.prefix_hint,
        ignore_range_tombstone: opt.ignore_range_tombstone,
        check_bloom_filter: opt.check_bloom_filter,
        retention_seconds: opt.retention_seconds,
        table_id: TableId {
            table_id: opt.table_id,
        },
    }
}

fn from_trace_write_options(opt: TracedWriteOptions) -> WriteOptions {
    WriteOptions {
        epoch: opt.epoch,
        table_id: TableId {
            table_id: opt.table_id,
        },
    }
}

#[macro_export]
macro_rules! dispatch_iter {
    ($storage:expr, $range:ident, $epoch:ident, $opts:ident) => {
        Ok(Box::new(HummockReplayIter::new(
            $storage
                .iter(
                    ($range.0.map(|b| b.to_vec()), $range.1.map(|b| b.to_vec())),
                    $epoch,
                    from_trace_read_options($opts),
                )
                .await
                .map_err(|e| TraceError::IterFailed(format!("{e}")))?,
        )))
    };
}

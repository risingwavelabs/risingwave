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
use std::sync::Arc;

use enum_as_inner::EnumAsInner;
use risingwave_common::config::StorageConfig;
use risingwave_common_service::observer_manager::RpcNotificationClient;
use risingwave_object_store::object::{
    parse_local_object_store, parse_remote_object_store, ObjectStoreImpl,
};

use crate::error::StorageResult;
use crate::hummock::hummock_meta_client::MonitoredHummockMetaClient;
use crate::hummock::{
    HummockStorage, HummockStorageV1, SstableStore, TieredCache, TieredCacheMetricsBuilder,
};
use crate::memory::MemoryStateStore;
use crate::monitor::{MonitoredStateStore as Monitored, ObjectStoreMetrics, StateStoreMetrics};
#[cfg(debug_assertions)]
use crate::store_impl::boxed_state_store::BoxDynamicDispatchedStateStore;
use crate::StateStore;

#[cfg(not(debug_assertions))]
pub type HummockStorageType = HummockStorage;
#[cfg(debug_assertions)]
pub type HummockStorageType = BoxDynamicDispatchedStateStore;

#[cfg(not(debug_assertions))]
pub type HummockStorageV1Type = HummockStorageV1;
#[cfg(debug_assertions)]
pub type HummockStorageV1Type = BoxDynamicDispatchedStateStore;

#[cfg(not(debug_assertions))]
pub type MemoryStateStoreType = MemoryStateStore;
#[cfg(debug_assertions)]
pub type MemoryStateStoreType = BoxDynamicDispatchedStateStore;

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
    HummockStateStore(Monitored<HummockStorageType>),
    HummockStateStoreV1(Monitored<HummockStorageV1Type>),
    /// In-memory B-Tree state store. Should only be used in unit and integration tests. If you
    /// want speed up e2e test, you should use Hummock in-memory mode instead. Also, this state
    /// store misses some critical implementation to ensure the correctness of persisting streaming
    /// state. (e.g., no read_epoch support, no async checkpoint)
    MemoryStateStore(Monitored<MemoryStateStoreType>),
}

impl StateStoreImpl {
    pub fn shared_in_memory_store(state_store_metrics: Arc<StateStoreMetrics>) -> Self {
        #[cfg(not(debug_assertions))]
        {
            Self::MemoryStateStore(MemoryStateStore::shared().monitored(state_store_metrics))
        }
        #[cfg(debug_assertions)]
        {
            Self::MemoryStateStore(
                (Box::new(MemoryStateStore::shared()) as BoxDynamicDispatchedStateStore)
                    .monitored(state_store_metrics),
            )
        }
    }

    pub fn for_test() -> Self {
        #[cfg(not(debug_assertions))]
        {
            Self::MemoryStateStore(
                MemoryStateStore::new().monitored(Arc::new(StateStoreMetrics::unused())),
            )
        }
        #[cfg(debug_assertions)]
        {
            Self::MemoryStateStore(
                (Box::new(MemoryStateStore::new()) as BoxDynamicDispatchedStateStore)
                    .monitored(Arc::new(StateStoreMetrics::unused())),
            )
        }
    }

    pub fn as_hummock(&self) -> Option<&HummockStorage> {
        #[cfg(not(debug_assertions))]
        {
            match self {
                StateStoreImpl::HummockStateStore(hummock) => Some(hummock.inner()),
                _ => None,
            }
        }
        #[cfg(debug_assertions)]
        {
            match self {
                StateStoreImpl::HummockStateStore(hummock) => {
                    Some(hummock.inner().as_hummock().expect("should be hummock"))
                }
                _ => None,
            }
        }
    }
}

impl Debug for StateStoreImpl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StateStoreImpl::HummockStateStore(_) => write!(f, "HummockStateStore"),
            StateStoreImpl::HummockStateStoreV1(_) => write!(f, "HummockStateStoreV1"),
            StateStoreImpl::MemoryStateStore(_) => write!(f, "MemoryStateStore"),
        }
    }
}

#[macro_export]
macro_rules! dispatch_state_store {
    ($impl:expr, $store:ident, $body:tt) => {{
        use $crate::store_impl::StateStoreImpl;

        match $impl {
            StateStoreImpl::MemoryStateStore($store) => {
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

            StateStoreImpl::HummockStateStore($store) => $body,

            StateStoreImpl::HummockStateStoreV1($store) => $body,
        }
    }};
}

impl StateStoreImpl {
    #[cfg_attr(not(target_os = "linux"), expect(unused_variables))]
    pub async fn new(
        s: &str,
        file_cache_dir: &str,
        config: Arc<StorageConfig>,
        hummock_meta_client: Arc<MonitoredHummockMetaClient>,
        state_store_stats: Arc<StateStoreMetrics>,
        object_store_metrics: Arc<ObjectStoreMetrics>,
        tiered_cache_metrics_builder: TieredCacheMetricsBuilder,
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
                capacity: config.file_cache.capacity_mb * 1024 * 1024,
                total_buffer_capacity: config.file_cache.total_buffer_capacity_mb * 1024 * 1024,
                cache_file_fallocate_unit: config.file_cache.cache_file_fallocate_unit_mb
                    * 1024
                    * 1024,
                cache_meta_fallocate_unit: config.file_cache.cache_meta_fallocate_unit_mb
                    * 1024
                    * 1024,
                cache_file_max_write_size: config.file_cache.cache_file_max_write_size_mb
                    * 1024
                    * 1024,
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
                    config.object_store_use_batch_delete,
                )
                .await;
                let object_store = if config.enable_local_spill {
                    let local_object_store = parse_local_object_store(
                        config.local_object_store.as_str(),
                        object_store_metrics.clone(),
                    );
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
                let notification_client =
                    RpcNotificationClient::new(hummock_meta_client.get_inner().clone());

                if !config.enable_state_store_v1 {
                    let inner = HummockStorage::new(
                        config.clone(),
                        sstable_store,
                        hummock_meta_client.clone(),
                        notification_client,
                        state_store_stats.clone(),
                    )
                    .await?;

                    #[cfg(not(debug_assertions))]
                    {
                        StateStoreImpl::HummockStateStore(inner.monitored(state_store_stats))
                    }
                    #[cfg(debug_assertions)]
                    {
                        StateStoreImpl::HummockStateStore(
                            (Box::new(inner) as BoxDynamicDispatchedStateStore)
                                .monitored(state_store_stats),
                        )
                    }
                } else {
                    let inner = HummockStorageV1::new(
                        config.clone(),
                        sstable_store,
                        hummock_meta_client.clone(),
                        notification_client,
                        state_store_stats.clone(),
                    )
                    .await?;

                    #[cfg(not(debug_assertions))]
                    {
                        StateStoreImpl::HummockStateStoreV1(inner.monitored(state_store_stats))
                    }
                    #[cfg(debug_assertions)]
                    {
                        StateStoreImpl::HummockStateStoreV1(
                            (Box::new(inner) as BoxDynamicDispatchedStateStore)
                                .monitored(state_store_stats),
                        )
                    }
                }
            }

            "in_memory" | "in-memory" => {
                tracing::warn!("In-memory state store should never be used in end-to-end benchmarks or production environment. Scaling and recovery are not supported.");
                StateStoreImpl::shared_in_memory_store(state_store_stats.clone())
            }

            other => unimplemented!("{} state store is not supported", other),
        };

        Ok(store)
    }
}

#[cfg(debug_assertions)]
pub mod boxed_state_store {
    use std::future::Future;
    use std::ops::{Bound, Deref, DerefMut};

    use bytes::Bytes;
    use risingwave_common::catalog::TableId;
    use risingwave_hummock_sdk::key::FullKey;
    use risingwave_hummock_sdk::HummockReadEpoch;

    use crate::error::StorageResult;
    use crate::hummock::{HummockStorage, HummockStorageV1};
    use crate::memory::MemoryStateStore;
    use crate::storage_value::StorageValue;
    use crate::store::{
        EmptyFutureTrait, GetFutureTrait, IngestBatchFutureTrait, IterFutureTrait, LocalStateStore,
        NextFutureTrait, ReadOptions, StateStoreRead, StateStoreWrite, StaticSendSync,
        SyncFutureTrait, SyncResult, WriteOptions,
    };
    use crate::{StateStore, StateStoreIter};

    // For StateStoreIter

    #[async_trait::async_trait]
    pub trait DynamicDispatchedStateStoreIter: StaticSendSync {
        async fn next(&mut self) -> StorageResult<Option<(FullKey<Vec<u8>>, Bytes)>>;
    }

    #[async_trait::async_trait]
    impl<I: StateStoreIter<Item = (FullKey<Vec<u8>>, Bytes)>> DynamicDispatchedStateStoreIter for I {
        async fn next(&mut self) -> StorageResult<Option<(FullKey<Vec<u8>>, Bytes)>> {
            self.next().await
        }
    }

    impl StateStoreIter for Box<dyn DynamicDispatchedStateStoreIter> {
        type Item = (FullKey<Vec<u8>>, Bytes);

        type NextFuture<'a> = impl NextFutureTrait<'a, Self::Item>;

        fn next(&mut self) -> Self::NextFuture<'_> {
            async { self.deref_mut().next().await }
        }
    }

    // For StateStoreRead

    #[async_trait::async_trait]
    pub trait DynamicDispatchedStateStoreRead: StaticSendSync {
        async fn get<'a>(
            &'a self,
            key: &'a [u8],
            epoch: u64,
            read_options: ReadOptions,
        ) -> StorageResult<Option<Bytes>>;

        async fn iter(
            &self,
            key_range: (Bound<Vec<u8>>, Bound<Vec<u8>>),
            epoch: u64,
            read_options: ReadOptions,
        ) -> StorageResult<Box<dyn DynamicDispatchedStateStoreIter>>;
    }

    #[async_trait::async_trait]
    impl<S: StateStoreRead> DynamicDispatchedStateStoreRead for S {
        async fn get<'a>(
            &'a self,
            key: &'a [u8],
            epoch: u64,
            read_options: ReadOptions,
        ) -> StorageResult<Option<Bytes>> {
            self.get(key, epoch, read_options).await
        }

        async fn iter(
            &self,
            key_range: (Bound<Vec<u8>>, Bound<Vec<u8>>),
            epoch: u64,
            read_options: ReadOptions,
        ) -> StorageResult<Box<dyn DynamicDispatchedStateStoreIter>> {
            Ok(Box::new(self.iter(key_range, epoch, read_options).await?))
        }
    }

    macro_rules! impl_state_store_read_for_box {
        ($box_type_name:ident) => {
            impl StateStoreRead for $box_type_name {
                type Iter = Box<dyn DynamicDispatchedStateStoreIter>;

                define_state_store_read_associated_type!();

                fn get<'a>(
                    &'a self,
                    key: &'a [u8],
                    epoch: u64,
                    read_options: ReadOptions,
                ) -> Self::GetFuture<'_> {
                    self.deref().get(key, epoch, read_options)
                }

                fn iter(
                    &self,
                    key_range: (Bound<Vec<u8>>, Bound<Vec<u8>>),
                    epoch: u64,
                    read_options: ReadOptions,
                ) -> Self::IterFuture<'_> {
                    self.deref().iter(key_range, epoch, read_options)
                }
            }
        };
    }

    // For StateStoreWrite

    #[async_trait::async_trait]
    pub trait DynamicDispatchedStateStoreWrite: StaticSendSync {
        async fn ingest_batch(
            &self,
            kv_pairs: Vec<(Bytes, StorageValue)>,
            delete_ranges: Vec<(Bytes, Bytes)>,
            write_options: WriteOptions,
        ) -> StorageResult<usize>;
    }

    #[async_trait::async_trait]
    impl<S: StateStoreWrite> DynamicDispatchedStateStoreWrite for S {
        async fn ingest_batch(
            &self,
            kv_pairs: Vec<(Bytes, StorageValue)>,
            delete_ranges: Vec<(Bytes, Bytes)>,
            write_options: WriteOptions,
        ) -> StorageResult<usize> {
            self.ingest_batch(kv_pairs, delete_ranges, write_options)
                .await
        }
    }

    macro_rules! impl_state_store_write_for_box {
        ($box_type_name:ident) => {
            impl StateStoreWrite for $box_type_name {
                define_state_store_write_associated_type!();

                fn ingest_batch(
                    &self,
                    kv_pairs: Vec<(Bytes, StorageValue)>,
                    delete_ranges: Vec<(Bytes, Bytes)>,
                    write_options: WriteOptions,
                ) -> Self::IngestBatchFuture<'_> {
                    self.deref()
                        .ingest_batch(kv_pairs, delete_ranges, write_options)
                }
            }
        };
    }

    // For LocalStateStore

    pub trait DynamicDispatchedLocalStateStore:
        DynamicDispatchedStateStoreRead + DynamicDispatchedStateStoreWrite
    {
    }
    impl<S: DynamicDispatchedStateStoreRead + DynamicDispatchedStateStoreWrite>
        DynamicDispatchedLocalStateStore for S
    {
    }
    pub type BoxDynamicDispatchedLocalStateStore = Box<dyn DynamicDispatchedLocalStateStore>;

    impl_state_store_read_for_box!(BoxDynamicDispatchedLocalStateStore);
    impl_state_store_write_for_box!(BoxDynamicDispatchedLocalStateStore);

    impl LocalStateStore for BoxDynamicDispatchedLocalStateStore {}

    // For global StateStore

    #[async_trait::async_trait]
    pub trait DynamicDispatchedStateStoreExt: StaticSendSync {
        async fn try_wait_epoch(&self, epoch: HummockReadEpoch) -> StorageResult<()>;

        async fn sync(&self, epoch: u64) -> StorageResult<SyncResult>;

        fn seal_epoch(&self, epoch: u64, is_checkpoint: bool);

        async fn clear_shared_buffer(&self) -> StorageResult<()>;

        async fn new_local(&self, table_id: TableId) -> BoxDynamicDispatchedLocalStateStore;
    }

    #[async_trait::async_trait]
    impl<S: StateStore> DynamicDispatchedStateStoreExt for S {
        async fn try_wait_epoch(&self, epoch: HummockReadEpoch) -> StorageResult<()> {
            self.try_wait_epoch(epoch).await
        }

        async fn sync(&self, epoch: u64) -> StorageResult<SyncResult> {
            self.sync(epoch).await
        }

        fn seal_epoch(&self, epoch: u64, is_checkpoint: bool) {
            self.seal_epoch(epoch, is_checkpoint);
        }

        async fn clear_shared_buffer(&self) -> StorageResult<()> {
            self.clear_shared_buffer().await
        }

        async fn new_local(&self, table_id: TableId) -> BoxDynamicDispatchedLocalStateStore {
            Box::new(self.new_local(table_id).await)
        }
    }

    pub type BoxDynamicDispatchedStateStore = Box<dyn DynamicDispatchedStateStore>;
    // With this trait, we can implement `Clone` for BoxDynamicDispatchedStateStore
    pub trait DynamicDispatchedStateStoreCloneBox {
        fn clone_box(&self) -> BoxDynamicDispatchedStateStore;
    }
    pub trait AsHummock {
        fn as_hummock(&self) -> Option<&HummockStorage>;
    }
    impl AsHummock for MemoryStateStore {
        fn as_hummock(&self) -> Option<&HummockStorage> {
            None
        }
    }
    impl AsHummock for HummockStorageV1 {
        fn as_hummock(&self) -> Option<&HummockStorage> {
            None
        }
    }
    impl AsHummock for HummockStorage {
        fn as_hummock(&self) -> Option<&HummockStorage> {
            Some(self)
        }
    }
    pub trait DynamicDispatchedStateStore:
        DynamicDispatchedStateStoreCloneBox
        + DynamicDispatchedStateStoreRead
        + DynamicDispatchedStateStoreExt
        + AsHummock
    {
    }
    impl<
            S: DynamicDispatchedStateStoreCloneBox
                + DynamicDispatchedStateStoreRead
                + DynamicDispatchedStateStoreExt
                + AsHummock,
        > DynamicDispatchedStateStore for S
    {
    }

    impl<S: StateStore + AsHummock> DynamicDispatchedStateStoreCloneBox for S {
        fn clone_box(&self) -> BoxDynamicDispatchedStateStore {
            Box::new(self.clone())
        }
    }

    impl Clone for BoxDynamicDispatchedStateStore {
        fn clone(&self) -> Self {
            self.clone_box()
        }
    }

    impl_state_store_read_for_box!(BoxDynamicDispatchedStateStore);

    impl StateStore for BoxDynamicDispatchedStateStore {
        type Local = BoxDynamicDispatchedLocalStateStore;

        type NewLocalFuture<'a> = impl Future<Output = Self::Local> + Send + 'a;

        define_state_store_associated_type!();

        fn try_wait_epoch(&self, epoch: HummockReadEpoch) -> Self::WaitEpochFuture<'_> {
            self.deref().try_wait_epoch(epoch)
        }

        fn sync(&self, epoch: u64) -> Self::SyncFuture<'_> {
            self.deref().sync(epoch)
        }

        fn clear_shared_buffer(&self) -> Self::ClearSharedBufferFuture<'_> {
            self.deref().clear_shared_buffer()
        }

        fn seal_epoch(&self, epoch: u64, is_checkpoint: bool) {
            self.deref().seal_epoch(epoch, is_checkpoint)
        }

        fn new_local(&self, table_id: TableId) -> Self::NewLocalFuture<'_> {
            self.deref().new_local(table_id)
        }
    }
}

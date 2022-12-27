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
use risingwave_common::config::RwConfig;
use risingwave_common_service::observer_manager::RpcNotificationClient;
use risingwave_hummock_sdk::filter_key_extractor::FilterKeyExtractorManagerRef;
use risingwave_object_store::object::{
    parse_local_object_store, parse_remote_object_store, ObjectStoreImpl,
};

use crate::error::StorageResult;
use crate::hummock::backup_reader::{parse_meta_snapshot_storage, BackupReader};
use crate::hummock::hummock_meta_client::MonitoredHummockMetaClient;
use crate::hummock::sstable_store::SstableStoreRef;
use crate::hummock::{
    HummockStorage, HummockStorageV1, MemoryLimiter, SstableIdManagerRef, SstableStore,
    TieredCache, TieredCacheMetricsBuilder,
};
use crate::memory::sled::SledStateStore;
use crate::memory::MemoryStateStore;
use crate::monitor::{MonitoredStateStore as Monitored, ObjectStoreMetrics, StateStoreMetrics};
use crate::StateStore;

pub type HummockStorageType = impl StateStore + AsHummockTrait;
pub type HummockStorageV1Type = impl StateStore + AsHummockTrait;
pub type MemoryStateStoreType = impl StateStore + AsHummockTrait;
pub type SledStateStoreType = impl StateStore + AsHummockTrait;

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
    SledStateStore(Monitored<SledStateStoreType>),
}

fn may_dynamic_dispatch(
    state_store: impl StateStore + AsHummockTrait,
) -> impl StateStore + AsHummockTrait {
    #[cfg(not(debug_assertions))]
    {
        state_store
    }
    #[cfg(debug_assertions)]
    {
        use crate::store_impl::boxed_state_store::BoxDynamicDispatchedStateStore;
        Box::new(state_store) as BoxDynamicDispatchedStateStore
    }
}

fn may_verify(state_store: impl StateStore + AsHummockTrait) -> impl StateStore + AsHummockTrait {
    #[cfg(not(debug_assertions))]
    {
        state_store
    }
    #[cfg(debug_assertions)]
    {
        use risingwave_common::util::env_var::env_var_is_true;
        use tracing::info;

        use crate::store_impl::verify::VerifyStateStore;

        let expected = if env_var_is_true("ENABLE_STATE_STORE_VERIFY") {
            info!("enable verify state store");
            Some(SledStateStore::new_temp())
        } else {
            info!("verify state store is not enabled");
            None
        };
        VerifyStateStore {
            actual: state_store,
            expected,
        }
    }
}

impl StateStoreImpl {
    fn in_memory(
        state_store: MemoryStateStore,
        state_store_metrics: Arc<StateStoreMetrics>,
    ) -> Self {
        // The specific type of MemoryStateStoreType in deducted here.
        Self::MemoryStateStore(may_dynamic_dispatch(state_store).monitored(state_store_metrics))
    }

    pub fn hummock(
        state_store: HummockStorage,
        state_store_metrics: Arc<StateStoreMetrics>,
    ) -> Self {
        // The specific type of HummockStateStoreType in deducted here.
        Self::HummockStateStore(
            may_dynamic_dispatch(may_verify(state_store)).monitored(state_store_metrics),
        )
    }

    pub fn hummock_v1(
        state_store: HummockStorageV1,
        state_store_metrics: Arc<StateStoreMetrics>,
    ) -> Self {
        // The specific type of HummockStateStoreV1Type in deducted here.
        Self::HummockStateStoreV1(
            may_dynamic_dispatch(may_verify(state_store)).monitored(state_store_metrics),
        )
    }

    pub fn sled(state_store: SledStateStore, state_store_metrics: Arc<StateStoreMetrics>) -> Self {
        Self::SledStateStore(may_dynamic_dispatch(state_store).monitored(state_store_metrics))
    }

    pub fn shared_in_memory_store(state_store_metrics: Arc<StateStoreMetrics>) -> Self {
        Self::in_memory(MemoryStateStore::shared(), state_store_metrics)
    }

    pub fn for_test() -> Self {
        Self::in_memory(
            MemoryStateStore::new(),
            Arc::new(StateStoreMetrics::unused()),
        )
    }

    pub fn as_hummock_trait(&self) -> Option<&dyn HummockTrait> {
        {
            match self {
                StateStoreImpl::HummockStateStore(hummock) => Some(
                    hummock
                        .inner()
                        .as_hummock_trait()
                        .expect("should be hummock"),
                ),
                StateStoreImpl::HummockStateStoreV1(hummock) => Some(
                    hummock
                        .inner()
                        .as_hummock_trait()
                        .expect("should be hummock"),
                ),
                _ => None,
            }
        }
    }

    pub fn as_hummock(&self) -> Option<&HummockStorage> {
        match self {
            StateStoreImpl::HummockStateStore(hummock) => Some(
                hummock
                    .inner()
                    .as_hummock_trait()
                    .expect("should be hummock")
                    .as_hummock()
                    .expect("should be hummock"),
            ),
            _ => None,
        }
    }
}

impl Debug for StateStoreImpl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StateStoreImpl::HummockStateStore(_) => write!(f, "HummockStateStore"),
            StateStoreImpl::HummockStateStoreV1(_) => write!(f, "HummockStateStoreV1"),
            StateStoreImpl::MemoryStateStore(_) => write!(f, "MemoryStateStore"),
            StateStoreImpl::SledStateStore(_) => write!(f, "SledStateStore"),
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

            StateStoreImpl::SledStateStore($store) => {
                // WARNING: don't change this. Enabling memory backend will cause monomorphization
                // explosion and thus slow compile time in release mode.
                #[cfg(debug_assertions)]
                {
                    $body
                }
                #[cfg(not(debug_assertions))]
                {
                    let _store = $store;
                    unimplemented!("sled state store should never be used in release mode");
                }
            }

            StateStoreImpl::HummockStateStore($store) => $body,

            StateStoreImpl::HummockStateStoreV1($store) => $body,
        }
    }};
}

#[cfg(debug_assertions)]
pub mod verify {
    use std::fmt::Debug;
    use std::future::Future;
    use std::ops::{Bound, Deref};

    use bytes::Bytes;
    use futures::{pin_mut, TryStreamExt};
    use futures_async_stream::try_stream;
    use risingwave_common::catalog::TableId;
    use risingwave_hummock_sdk::HummockReadEpoch;
    use tracing::log::warn;

    use crate::error::StorageError;
    use crate::storage_value::StorageValue;
    use crate::store::*;
    use crate::store_impl::{AsHummockTrait, HummockTrait};
    use crate::StateStore;

    fn assert_result_eq<Item: PartialEq + Debug, E>(
        first: &std::result::Result<Item, E>,
        second: &std::result::Result<Item, E>,
    ) {
        match (first, second) {
            (Ok(first), Ok(second)) => {
                if first != second {
                    warn!("result different: {:?} {:?}", first, second);
                }
                assert_eq!(first, second);
            }
            (Err(_), Err(_)) => {}
            _ => {
                warn!("one success and one failed");
                panic!("result not equal");
            }
        }
    }

    pub struct VerifyStateStore<A, E> {
        pub actual: A,
        pub expected: Option<E>,
    }

    impl<A: AsHummockTrait, E> AsHummockTrait for VerifyStateStore<A, E> {
        fn as_hummock_trait(&self) -> Option<&dyn HummockTrait> {
            self.actual.as_hummock_trait()
        }
    }

    impl<A: StateStoreRead, E: StateStoreRead> StateStoreRead for VerifyStateStore<A, E> {
        type IterStream = impl StateStoreReadIterStream;

        define_state_store_read_associated_type!();

        fn get<'a>(
            &'a self,
            key: &'a [u8],
            epoch: u64,
            read_options: ReadOptions,
        ) -> Self::GetFuture<'_> {
            async move {
                let actual = self.actual.get(key, epoch, read_options.clone()).await;
                if let Some(expected) = &self.expected {
                    let expected = expected.get(key, epoch, read_options).await;
                    assert_result_eq(&actual, &expected);
                }
                actual
            }
        }

        fn iter(
            &self,
            key_range: (Bound<Vec<u8>>, Bound<Vec<u8>>),
            epoch: u64,
            read_options: ReadOptions,
        ) -> Self::IterFuture<'_> {
            async move {
                let actual = self
                    .actual
                    .iter(key_range.clone(), epoch, read_options.clone())
                    .await?;
                let expected = if let Some(expected) = &self.expected {
                    Some(expected.iter(key_range, epoch, read_options).await?)
                } else {
                    None
                };

                Ok(verify_stream(actual, expected))
            }
        }
    }

    #[try_stream(ok = StateStoreIterItem, error = StorageError)]
    async fn verify_stream(
        actual: impl StateStoreReadIterStream,
        expected: Option<impl StateStoreReadIterStream>,
    ) {
        pin_mut!(actual);
        pin_mut!(expected);
        let mut expected = expected.as_pin_mut();

        loop {
            let actual = actual.try_next().await?;
            if let Some(expected) = expected.as_mut() {
                let expected = expected.try_next().await?;
                assert_eq!(actual, expected);
            }
            if let Some(actual) = actual {
                yield actual;
            } else {
                break;
            }
        }
    }

    impl<A: StateStoreWrite, E: StateStoreWrite> StateStoreWrite for VerifyStateStore<A, E> {
        define_state_store_write_associated_type!();

        fn ingest_batch(
            &self,
            kv_pairs: Vec<(Bytes, StorageValue)>,
            delete_ranges: Vec<(Bytes, Bytes)>,
            write_options: WriteOptions,
        ) -> Self::IngestBatchFuture<'_> {
            async move {
                let actual = self
                    .actual
                    .ingest_batch(
                        kv_pairs.clone(),
                        delete_ranges.clone(),
                        write_options.clone(),
                    )
                    .await;
                if let Some(expected) = &self.expected {
                    let expected = expected
                        .ingest_batch(kv_pairs, delete_ranges, write_options)
                        .await;
                    assert_eq!(actual.is_err(), expected.is_err());
                }
                actual
            }
        }
    }

    impl<A: Clone, E: Clone> Clone for VerifyStateStore<A, E> {
        fn clone(&self) -> Self {
            Self {
                actual: self.actual.clone(),
                expected: self.expected.clone(),
            }
        }
    }

    impl<A: LocalStateStore, E: LocalStateStore> LocalStateStore for VerifyStateStore<A, E> {}

    impl<A: StateStore, E: StateStore> StateStore for VerifyStateStore<A, E> {
        type Local = VerifyStateStore<A::Local, E::Local>;

        type NewLocalFuture<'a> = impl Future<Output = Self::Local> + Send + 'a;

        define_state_store_associated_type!();

        fn try_wait_epoch(&self, epoch: HummockReadEpoch) -> Self::WaitEpochFuture<'_> {
            self.actual.try_wait_epoch(epoch)
        }

        fn sync(&self, epoch: u64) -> Self::SyncFuture<'_> {
            async move {
                if let Some(expected) = &self.expected {
                    let _ = expected.sync(epoch).await;
                }
                self.actual.sync(epoch).await
            }
        }

        fn seal_epoch(&self, epoch: u64, is_checkpoint: bool) {
            self.actual.seal_epoch(epoch, is_checkpoint)
        }

        fn clear_shared_buffer(&self) -> Self::ClearSharedBufferFuture<'_> {
            async move { self.actual.clear_shared_buffer().await }
        }

        fn new_local(&self, table_id: TableId) -> Self::NewLocalFuture<'_> {
            async move {
                let expected = if let Some(expected) = &self.expected {
                    Some(expected.new_local(table_id).await)
                } else {
                    None
                };
                VerifyStateStore {
                    actual: self.actual.new_local(table_id).await,
                    expected,
                }
            }
        }
    }

    impl<A, E> Deref for VerifyStateStore<A, E> {
        type Target = A;

        fn deref(&self) -> &Self::Target {
            &self.actual
        }
    }
}

impl StateStoreImpl {
    #[cfg_attr(not(target_os = "linux"), expect(unused_variables))]
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        s: &str,
        file_cache_dir: &str,
        rw_config: &RwConfig,
        hummock_meta_client: Arc<MonitoredHummockMetaClient>,
        state_store_stats: Arc<StateStoreMetrics>,
        object_store_metrics: Arc<ObjectStoreMetrics>,
        tiered_cache_metrics_builder: TieredCacheMetricsBuilder,
        tracing: Arc<risingwave_tracing::RwTracingService>,
    ) -> StorageResult<Self> {
        let config = Arc::new(rw_config.storage.clone());
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
                    "Hummock",
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
                    let backup_store = parse_meta_snapshot_storage(rw_config).await?;
                    let backup_reader = BackupReader::new(backup_store);
                    let inner = HummockStorage::new(
                        config.clone(),
                        sstable_store,
                        backup_reader,
                        hummock_meta_client.clone(),
                        notification_client,
                        state_store_stats.clone(),
                        tracing,
                    )
                    .await?;

                    StateStoreImpl::hummock(inner, state_store_stats)
                } else {
                    let inner = HummockStorageV1::new(
                        config.clone(),
                        sstable_store,
                        hummock_meta_client.clone(),
                        notification_client,
                        state_store_stats.clone(),
                        tracing,
                    )
                    .await?;

                    StateStoreImpl::hummock_v1(inner, state_store_stats)
                }
            }

            "in_memory" | "in-memory" => {
                tracing::warn!("In-memory state store should never be used in end-to-end benchmarks or production environment. Scaling and recovery are not supported.");
                StateStoreImpl::shared_in_memory_store(state_store_stats.clone())
            }

            sled if sled.starts_with("sled://") => {
                tracing::warn!("sled state store should never be used in end-to-end benchmarks or production environment. Scaling and recovery are not supported.");
                let path = sled.strip_prefix("sled://").unwrap();
                StateStoreImpl::sled(SledStateStore::new(path), state_store_stats.clone())
            }

            other => unimplemented!("{} state store is not supported", other),
        };

        Ok(store)
    }
}

/// This trait is for aligning some common methods of hummock v1 and v2 for external use
pub trait HummockTrait {
    fn sstable_id_manager(&self) -> &SstableIdManagerRef;
    fn sstable_store(&self) -> SstableStoreRef;
    fn filter_key_extractor_manager(&self) -> &FilterKeyExtractorManagerRef;
    fn get_memory_limiter(&self) -> Arc<MemoryLimiter>;
    fn as_hummock(&self) -> Option<&HummockStorage>;
}

impl HummockTrait for HummockStorage {
    fn sstable_id_manager(&self) -> &SstableIdManagerRef {
        self.sstable_id_manager()
    }

    fn sstable_store(&self) -> SstableStoreRef {
        self.sstable_store()
    }

    fn filter_key_extractor_manager(&self) -> &FilterKeyExtractorManagerRef {
        self.filter_key_extractor_manager()
    }

    fn get_memory_limiter(&self) -> Arc<MemoryLimiter> {
        self.get_memory_limiter()
    }

    fn as_hummock(&self) -> Option<&HummockStorage> {
        Some(self)
    }
}

impl HummockTrait for HummockStorageV1 {
    fn sstable_id_manager(&self) -> &SstableIdManagerRef {
        self.sstable_id_manager()
    }

    fn sstable_store(&self) -> SstableStoreRef {
        self.sstable_store()
    }

    fn filter_key_extractor_manager(&self) -> &FilterKeyExtractorManagerRef {
        self.filter_key_extractor_manager()
    }

    fn get_memory_limiter(&self) -> Arc<MemoryLimiter> {
        self.get_memory_limiter()
    }

    fn as_hummock(&self) -> Option<&HummockStorage> {
        None
    }
}

pub trait AsHummockTrait {
    fn as_hummock_trait(&self) -> Option<&dyn HummockTrait>;
}

impl AsHummockTrait for HummockStorage {
    fn as_hummock_trait(&self) -> Option<&dyn HummockTrait> {
        Some(self)
    }
}

impl AsHummockTrait for HummockStorageV1 {
    fn as_hummock_trait(&self) -> Option<&dyn HummockTrait> {
        Some(self)
    }
}

impl AsHummockTrait for MemoryStateStore {
    fn as_hummock_trait(&self) -> Option<&dyn HummockTrait> {
        None
    }
}

impl AsHummockTrait for SledStateStore {
    fn as_hummock_trait(&self) -> Option<&dyn HummockTrait> {
        None
    }
}

#[cfg(debug_assertions)]
pub mod boxed_state_store {
    use std::future::Future;
    use std::ops::{Bound, Deref};

    use bytes::Bytes;
    use futures::stream::BoxStream;
    use risingwave_common::catalog::TableId;
    use risingwave_hummock_sdk::HummockReadEpoch;

    use crate::error::StorageResult;
    use crate::storage_value::StorageValue;
    use crate::store::*;
    use crate::store_impl::{AsHummockTrait, HummockTrait};
    use crate::StateStore;

    // For StateStoreRead

    pub type BoxStateStoreReadIterStream = BoxStream<'static, StorageResult<StateStoreIterItem>>;

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
        ) -> StorageResult<BoxStateStoreReadIterStream>;
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
        ) -> StorageResult<BoxStateStoreReadIterStream> {
            use futures::StreamExt;
            Ok(self.iter(key_range, epoch, read_options).await?.boxed())
        }
    }

    macro_rules! impl_state_store_read_for_box {
        ($box_type_name:ident) => {
            impl StateStoreRead for $box_type_name {
                type IterStream = BoxStateStoreReadIterStream;

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

    pub trait DynamicDispatchedStateStore:
        DynamicDispatchedStateStoreCloneBox
        + DynamicDispatchedStateStoreRead
        + DynamicDispatchedStateStoreExt
        + AsHummockTrait
    {
    }

    impl<
            S: DynamicDispatchedStateStoreCloneBox
                + DynamicDispatchedStateStoreRead
                + DynamicDispatchedStateStoreExt
                + AsHummockTrait,
        > DynamicDispatchedStateStore for S
    {
    }

    impl<S: StateStore + AsHummockTrait> DynamicDispatchedStateStoreCloneBox for S {
        fn clone_box(&self) -> BoxDynamicDispatchedStateStore {
            Box::new(self.clone())
        }
    }

    impl AsHummockTrait for BoxDynamicDispatchedStateStore {
        fn as_hummock_trait(&self) -> Option<&dyn HummockTrait> {
            self.deref().as_hummock_trait()
        }
    }

    impl Clone for BoxDynamicDispatchedStateStore {
        fn clone(&self) -> Self {
            self.deref().clone_box()
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

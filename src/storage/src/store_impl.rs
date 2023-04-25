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

use std::fmt::Debug;
use std::sync::Arc;

use enum_as_inner::EnumAsInner;
use risingwave_common_service::observer_manager::RpcNotificationClient;
use risingwave_hummock_sdk::filter_key_extractor::FilterKeyExtractorManagerRef;
use risingwave_hummock_sdk::opts::NewLocalOptions;
use risingwave_object_store::object::{
    parse_local_object_store, parse_remote_object_store, ObjectStoreImpl,
};

use crate::error::StorageResult;
use crate::hummock::backup_reader::BackupReaderRef;
use crate::hummock::hummock_meta_client::MonitoredHummockMetaClient;
use crate::hummock::sstable_store::SstableStoreRef;
use crate::hummock::{
    HummockStorage, MemoryLimiter, SstableObjectIdManagerRef, SstableStore, TieredCache,
    TieredCacheMetricsBuilder,
};
use crate::memory::sled::SledStateStore;
use crate::memory::MemoryStateStore;
use crate::monitor::{
    CompactorMetrics, HummockStateStoreMetrics, MonitoredStateStore as Monitored,
    MonitoredStorageMetrics, ObjectStoreMetrics,
};
use crate::opts::StorageOpts;
use crate::StateStore;

pub type HummockStorageType = impl StateStore + AsHummockTrait;
pub type MemoryStateStoreType = impl StateStore + AsHummockTrait;
pub type SledStateStoreType = impl StateStore + AsHummockTrait;

/// The type erased [`StateStore`].
#[derive(Clone, EnumAsInner)]
#[allow(clippy::enum_variant_names)]
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
        storage_metrics: Arc<MonitoredStorageMetrics>,
    ) -> Self {
        // The specific type of MemoryStateStoreType in deducted here.
        Self::MemoryStateStore(may_dynamic_dispatch(state_store).monitored(storage_metrics))
    }

    pub fn hummock(
        state_store: HummockStorage,
        storage_metrics: Arc<MonitoredStorageMetrics>,
    ) -> Self {
        // The specific type of HummockStateStoreType in deducted here.
        Self::HummockStateStore(
            may_dynamic_dispatch(may_verify(state_store)).monitored(storage_metrics),
        )
    }

    pub fn sled(
        state_store: SledStateStore,
        storage_metrics: Arc<MonitoredStorageMetrics>,
    ) -> Self {
        Self::SledStateStore(may_dynamic_dispatch(state_store).monitored(storage_metrics))
    }

    pub fn shared_in_memory_store(storage_metrics: Arc<MonitoredStorageMetrics>) -> Self {
        Self::in_memory(MemoryStateStore::shared(), storage_metrics)
    }

    pub fn for_test() -> Self {
        Self::in_memory(
            MemoryStateStore::new(),
            Arc::new(MonitoredStorageMetrics::unused()),
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
        }
    }};
}

#[cfg(debug_assertions)]
pub mod verify {
    use std::fmt::Debug;
    use std::future::Future;
    use std::ops::Deref;

    use bytes::Bytes;
    use futures::{pin_mut, TryStreamExt};
    use futures_async_stream::try_stream;
    use risingwave_hummock_sdk::opts::{NewLocalOptions, ReadOptions, WriteOptions};
    use risingwave_hummock_sdk::HummockReadEpoch;
    use tracing::log::warn;

    use crate::error::{StorageError, StorageResult};
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

        fn get(&self, key: Bytes, epoch: u64, read_options: ReadOptions) -> Self::GetFuture<'_> {
            async move {
                let actual = self
                    .actual
                    .get(key.clone(), epoch, read_options.clone())
                    .await;
                if let Some(expected) = &self.expected {
                    let expected = expected.get(key, epoch, read_options).await;
                    assert_result_eq(&actual, &expected);
                }
                actual
            }
        }

        fn iter(
            &self,
            key_range: IterKeyRange,
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
        actual: impl StateStoreIterItemStream,
        expected: Option<impl StateStoreIterItemStream>,
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

    impl<A: LocalStateStore, E: LocalStateStore> LocalStateStore for VerifyStateStore<A, E> {
        type FlushFuture<'a> = impl Future<Output = StorageResult<usize>> + 'a;
        type GetFuture<'a> = impl GetFutureTrait<'a>;
        type IterFuture<'a> = impl Future<Output = StorageResult<Self::IterStream<'a>>> + Send + 'a;
        type IterStream<'a> = impl StateStoreIterItemStream + 'a;

        define_local_state_store_associated_type!();

        // We don't verify `may_exist` across different state stores because
        // the return value of `may_exist` is implementation specific and may not
        // be consistent across different state store backends.
        fn may_exist(
            &self,
            key_range: IterKeyRange,
            read_options: ReadOptions,
        ) -> Self::MayExistFuture<'_> {
            self.actual.may_exist(key_range, read_options)
        }

        fn get(&self, key: Bytes, read_options: ReadOptions) -> Self::GetFuture<'_> {
            async move {
                let actual = self.actual.get(key.clone(), read_options.clone()).await;
                if let Some(expected) = &self.expected {
                    let expected = expected.get(key, read_options).await;
                    assert_result_eq(&actual, &expected);
                }
                actual
            }
        }

        fn iter(&self, key_range: IterKeyRange, read_options: ReadOptions) -> Self::IterFuture<'_> {
            async move {
                let actual = self
                    .actual
                    .iter(key_range.clone(), read_options.clone())
                    .await?;
                let expected = if let Some(expected) = &self.expected {
                    Some(expected.iter(key_range, read_options).await?)
                } else {
                    None
                };

                Ok(verify_stream(actual, expected))
            }
        }

        fn insert(
            &mut self,
            key: Bytes,
            new_val: Bytes,
            old_val: Option<Bytes>,
        ) -> StorageResult<()> {
            if let Some(expected) = &mut self.expected {
                expected.insert(key.clone(), new_val.clone(), old_val.clone())?;
            }
            self.actual.insert(key, new_val, old_val)?;

            Ok(())
        }

        fn delete(&mut self, key: Bytes, old_val: Bytes) -> StorageResult<()> {
            if let Some(expected) = &mut self.expected {
                expected.delete(key.clone(), old_val.clone())?;
            }
            self.actual.delete(key, old_val)?;
            Ok(())
        }

        fn flush(&mut self, delete_ranges: Vec<(Bytes, Bytes)>) -> Self::FlushFuture<'_> {
            async move {
                if let Some(expected) = &mut self.expected {
                    expected.flush(delete_ranges.clone()).await?;
                }
                self.actual.flush(delete_ranges).await
            }
        }

        fn init(&mut self, epoch: u64) {
            self.actual.init(epoch);
            if let Some(expected) = &mut self.expected {
                expected.init(epoch);
            }
        }

        fn seal_current_epoch(&mut self, next_epoch: u64) {
            self.actual.seal_current_epoch(next_epoch);
            if let Some(expected) = &mut self.expected {
                expected.seal_current_epoch(next_epoch);
            }
        }

        fn epoch(&self) -> u64 {
            let epoch = self.actual.epoch();
            if let Some(expected) = &self.expected {
                assert_eq!(epoch, expected.epoch());
            }
            epoch
        }

        fn is_dirty(&self) -> bool {
            let ret = self.actual.is_dirty();
            if let Some(expected) = &self.expected {
                assert_eq!(ret, expected.is_dirty());
            }
            ret
        }
    }

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

        fn new_local(&self, option: NewLocalOptions) -> Self::NewLocalFuture<'_> {
            async move {
                let expected = if let Some(expected) = &self.expected {
                    Some(expected.new_local(option.clone()).await)
                } else {
                    None
                };
                VerifyStateStore {
                    actual: self.actual.new_local(option).await,
                    expected,
                }
            }
        }

        fn validate_read_epoch(&self, epoch: HummockReadEpoch) -> StorageResult<()> {
            self.actual.validate_read_epoch(epoch)
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
        opts: Arc<StorageOpts>,
        hummock_meta_client: Arc<MonitoredHummockMetaClient>,
        state_store_metrics: Arc<HummockStateStoreMetrics>,
        object_store_metrics: Arc<ObjectStoreMetrics>,
        tiered_cache_metrics_builder: TieredCacheMetricsBuilder,
        tracing: Arc<risingwave_tracing::RwTracingService>,
        storage_metrics: Arc<MonitoredStorageMetrics>,
        compactor_metrics: Arc<CompactorMetrics>,
    ) -> StorageResult<Self> {
        #[cfg(not(target_os = "linux"))]
        let tiered_cache = TieredCache::none();

        #[cfg(target_os = "linux")]
        let tiered_cache = if opts.file_cache_dir.is_empty() {
            TieredCache::none()
        } else {
            use crate::hummock::file_cache::cache::FileCacheOptions;
            use crate::hummock::HummockError;

            let options = FileCacheOptions {
                dir: opts.file_cache_dir.to_string(),
                capacity: opts.file_cache_capacity_mb * 1024 * 1024,
                total_buffer_capacity: opts.file_cache_total_buffer_capacity_mb * 1024 * 1024,
                cache_file_fallocate_unit: opts.file_cache_file_fallocate_unit_mb * 1024 * 1024,
                cache_meta_fallocate_unit: opts.file_cache_meta_fallocate_unit_mb * 1024 * 1024,
                cache_file_max_write_size: opts.file_cache_file_max_write_size_mb * 1024 * 1024,
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
                    "Hummock",
                )
                .await;
                let object_store = if opts.enable_local_spill {
                    let local_object_store = parse_local_object_store(
                        opts.local_object_store.as_str(),
                        object_store_metrics.clone(),
                    );
                    ObjectStoreImpl::hybrid(local_object_store, remote_object_store)
                } else {
                    remote_object_store
                };

                let sstable_store = Arc::new(SstableStore::new(
                    Arc::new(object_store),
                    opts.data_directory.to_string(),
                    opts.block_cache_capacity_mb * (1 << 20),
                    opts.meta_cache_capacity_mb * (1 << 20),
                    opts.high_priority_ratio,
                    tiered_cache,
                ));
                let notification_client =
                    RpcNotificationClient::new(hummock_meta_client.get_inner().clone());
                let inner = HummockStorage::new(
                    opts.clone(),
                    sstable_store,
                    hummock_meta_client.clone(),
                    notification_client,
                    state_store_metrics.clone(),
                    tracing,
                    compactor_metrics.clone(),
                )
                .await?;

                StateStoreImpl::hummock(inner, storage_metrics)
            }

            "in_memory" | "in-memory" => {
                tracing::warn!("In-memory state store should never be used in end-to-end benchmarks or production environment. Scaling and recovery are not supported.");
                StateStoreImpl::shared_in_memory_store(storage_metrics.clone())
            }

            sled if sled.starts_with("sled://") => {
                tracing::warn!("sled state store should never be used in end-to-end benchmarks or production environment. Scaling and recovery are not supported.");
                let path = sled.strip_prefix("sled://").unwrap();
                StateStoreImpl::sled(SledStateStore::new(path), storage_metrics.clone())
            }

            other => unimplemented!("{} state store is not supported", other),
        };

        Ok(store)
    }
}

/// This trait is for aligning some common methods of `state_store_impl` for external use
pub trait HummockTrait {
    fn sstable_object_id_manager(&self) -> &SstableObjectIdManagerRef;
    fn sstable_store(&self) -> SstableStoreRef;
    fn filter_key_extractor_manager(&self) -> &FilterKeyExtractorManagerRef;
    fn get_memory_limiter(&self) -> Arc<MemoryLimiter>;
    fn backup_reader(&self) -> BackupReaderRef;
    fn as_hummock(&self) -> Option<&HummockStorage>;
}

impl HummockTrait for HummockStorage {
    fn sstable_object_id_manager(&self) -> &SstableObjectIdManagerRef {
        self.sstable_object_id_manager()
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

    fn backup_reader(&self) -> BackupReaderRef {
        self.backup_reader()
    }

    fn as_hummock(&self) -> Option<&HummockStorage> {
        Some(self)
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
    use std::ops::{Deref, DerefMut};

    use bytes::Bytes;
    use futures::stream::BoxStream;
    use futures::StreamExt;
    use risingwave_hummock_sdk::opts::{NewLocalOptions, ReadOptions};
    use risingwave_hummock_sdk::HummockReadEpoch;

    use crate::error::StorageResult;
    use crate::store::*;
    use crate::store_impl::{AsHummockTrait, HummockTrait};
    use crate::StateStore;

    // For StateStoreRead

    pub type BoxStateStoreReadIterStream = BoxStream<'static, StorageResult<StateStoreIterItem>>;

    #[async_trait::async_trait]
    pub trait DynamicDispatchedStateStoreRead: StaticSendSync {
        async fn get(
            &self,
            key: Bytes,
            epoch: u64,
            read_options: ReadOptions,
        ) -> StorageResult<Option<Bytes>>;

        async fn iter(
            &self,
            key_range: IterKeyRange,
            epoch: u64,
            read_options: ReadOptions,
        ) -> StorageResult<BoxStateStoreReadIterStream>;
    }

    #[async_trait::async_trait]
    impl<S: StateStoreRead> DynamicDispatchedStateStoreRead for S {
        async fn get(
            &self,
            key: Bytes,
            epoch: u64,
            read_options: ReadOptions,
        ) -> StorageResult<Option<Bytes>> {
            self.get(key, epoch, read_options).await
        }

        async fn iter(
            &self,
            key_range: IterKeyRange,
            epoch: u64,
            read_options: ReadOptions,
        ) -> StorageResult<BoxStateStoreReadIterStream> {
            Ok(self.iter(key_range, epoch, read_options).await?.boxed())
        }
    }

    // For LocalStateStore
    pub type BoxLocalStateStoreIterStream<'a> = BoxStream<'a, StorageResult<StateStoreIterItem>>;
    #[async_trait::async_trait]
    pub trait DynamicDispatchedLocalStateStore: StaticSendSync {
        async fn may_exist(
            &self,
            key_range: IterKeyRange,
            read_options: ReadOptions,
        ) -> StorageResult<bool>;

        async fn get(&self, key: Bytes, read_options: ReadOptions) -> StorageResult<Option<Bytes>>;

        async fn iter(
            &self,
            key_range: IterKeyRange,
            read_options: ReadOptions,
        ) -> StorageResult<BoxLocalStateStoreIterStream<'_>>;

        fn insert(
            &mut self,
            key: Bytes,
            new_val: Bytes,
            old_val: Option<Bytes>,
        ) -> StorageResult<()>;

        fn delete(&mut self, key: Bytes, old_val: Bytes) -> StorageResult<()>;

        async fn flush(&mut self, delete_ranges: Vec<(Bytes, Bytes)>) -> StorageResult<usize>;

        fn epoch(&self) -> u64;

        fn is_dirty(&self) -> bool;

        fn init(&mut self, epoch: u64);

        fn seal_current_epoch(&mut self, next_epoch: u64);
    }

    #[async_trait::async_trait]
    impl<S: LocalStateStore> DynamicDispatchedLocalStateStore for S {
        async fn may_exist(
            &self,
            key_range: IterKeyRange,
            read_options: ReadOptions,
        ) -> StorageResult<bool> {
            self.may_exist(key_range, read_options).await
        }

        async fn get(&self, key: Bytes, read_options: ReadOptions) -> StorageResult<Option<Bytes>> {
            self.get(key, read_options).await
        }

        async fn iter(
            &self,
            key_range: IterKeyRange,
            read_options: ReadOptions,
        ) -> StorageResult<BoxLocalStateStoreIterStream<'_>> {
            Ok(self.iter(key_range, read_options).await?.boxed())
        }

        fn insert(
            &mut self,
            key: Bytes,
            new_val: Bytes,
            old_val: Option<Bytes>,
        ) -> StorageResult<()> {
            self.insert(key, new_val, old_val)
        }

        fn delete(&mut self, key: Bytes, old_val: Bytes) -> StorageResult<()> {
            self.delete(key, old_val)
        }

        async fn flush(&mut self, delete_ranges: Vec<(Bytes, Bytes)>) -> StorageResult<usize> {
            self.flush(delete_ranges).await
        }

        fn epoch(&self) -> u64 {
            self.epoch()
        }

        fn is_dirty(&self) -> bool {
            self.is_dirty()
        }

        fn init(&mut self, epoch: u64) {
            self.init(epoch)
        }

        fn seal_current_epoch(&mut self, next_epoch: u64) {
            self.seal_current_epoch(next_epoch)
        }
    }

    pub type BoxDynamicDispatchedLocalStateStore = Box<dyn DynamicDispatchedLocalStateStore>;

    impl LocalStateStore for BoxDynamicDispatchedLocalStateStore {
        type IterStream<'a> = BoxLocalStateStoreIterStream<'a>;

        type FlushFuture<'a> = impl Future<Output = StorageResult<usize>> + 'a;
        type GetFuture<'a> = impl GetFutureTrait<'a>;
        type IterFuture<'a> = impl Future<Output = StorageResult<Self::IterStream<'a>>> + Send + 'a;

        define_local_state_store_associated_type!();

        fn may_exist(
            &self,
            key_range: IterKeyRange,
            read_options: ReadOptions,
        ) -> Self::MayExistFuture<'_> {
            self.deref().may_exist(key_range, read_options)
        }

        fn get(&self, key: Bytes, read_options: ReadOptions) -> Self::GetFuture<'_> {
            self.deref().get(key, read_options)
        }

        fn iter(&self, key_range: IterKeyRange, read_options: ReadOptions) -> Self::IterFuture<'_> {
            self.deref().iter(key_range, read_options)
        }

        fn insert(
            &mut self,
            key: Bytes,
            new_val: Bytes,
            old_val: Option<Bytes>,
        ) -> StorageResult<()> {
            self.deref_mut().insert(key, new_val, old_val)
        }

        fn delete(&mut self, key: Bytes, old_val: Bytes) -> StorageResult<()> {
            self.deref_mut().delete(key, old_val)
        }

        fn flush(&mut self, delete_ranges: Vec<(Bytes, Bytes)>) -> Self::FlushFuture<'_> {
            self.deref_mut().flush(delete_ranges)
        }

        fn epoch(&self) -> u64 {
            self.deref().epoch()
        }

        fn is_dirty(&self) -> bool {
            self.deref().is_dirty()
        }

        fn init(&mut self, epoch: u64) {
            self.deref_mut().init(epoch)
        }

        fn seal_current_epoch(&mut self, next_epoch: u64) {
            self.deref_mut().seal_current_epoch(next_epoch)
        }
    }

    // For global StateStore

    #[async_trait::async_trait]
    pub trait DynamicDispatchedStateStoreExt: StaticSendSync {
        async fn try_wait_epoch(&self, epoch: HummockReadEpoch) -> StorageResult<()>;

        async fn sync(&self, epoch: u64) -> StorageResult<SyncResult>;

        fn seal_epoch(&self, epoch: u64, is_checkpoint: bool);

        async fn clear_shared_buffer(&self) -> StorageResult<()>;

        async fn new_local(&self, option: NewLocalOptions) -> BoxDynamicDispatchedLocalStateStore;

        fn validate_read_epoch(&self, epoch: HummockReadEpoch) -> StorageResult<()>;
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

        async fn new_local(&self, option: NewLocalOptions) -> BoxDynamicDispatchedLocalStateStore {
            Box::new(self.new_local(option).await)
        }

        fn validate_read_epoch(&self, epoch: HummockReadEpoch) -> StorageResult<()> {
            self.validate_read_epoch(epoch)
        }
    }

    pub type BoxDynamicDispatchedStateStore = Box<dyn DynamicDispatchedStateStore>;

    impl StateStoreRead for BoxDynamicDispatchedStateStore {
        type IterStream = BoxStateStoreReadIterStream;

        define_state_store_read_associated_type!();

        fn get(&self, key: Bytes, epoch: u64, read_options: ReadOptions) -> Self::GetFuture<'_> {
            self.deref().get(key, epoch, read_options)
        }

        fn iter(
            &self,
            key_range: IterKeyRange,
            epoch: u64,
            read_options: ReadOptions,
        ) -> Self::IterFuture<'_> {
            self.deref().iter(key_range, epoch, read_options)
        }
    }

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

        fn new_local(&self, option: NewLocalOptions) -> Self::NewLocalFuture<'_> {
            self.deref().new_local(option)
        }

        fn validate_read_epoch(&self, epoch: HummockReadEpoch) -> StorageResult<()> {
            self.deref().validate_read_epoch(epoch)
        }
    }
}

// Copyright 2024 RisingWave Labs
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
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use enum_as_inner::EnumAsInner;
use risingwave_common::monitor::GLOBAL_METRICS_REGISTRY;
use risingwave_common_service::observer_manager::RpcNotificationClient;
use risingwave_object_store::object::build_remote_object_store;

use crate::error::StorageResult;
use crate::filter_key_extractor::{RemoteTableAccessor, RpcFilterKeyExtractorManager};
use crate::hummock::file_cache::preclude::*;
use crate::hummock::hummock_meta_client::MonitoredHummockMetaClient;
use crate::hummock::{
    set_foyer_metrics_registry, FileCache, FileCacheConfig, HummockError, HummockStorage,
    RecentFilter, SstableStore, SstableStoreConfig,
};
use crate::memory::sled::SledStateStore;
use crate::memory::MemoryStateStore;
use crate::monitor::{
    CompactorMetrics, HummockStateStoreMetrics, MonitoredStateStore as Monitored,
    MonitoredStorageMetrics, ObjectStoreMetrics,
};
use crate::opts::StorageOpts;
use crate::StateStore;

pub type HummockStorageType = impl StateStore + AsHummock;
pub type MemoryStateStoreType = impl StateStore + AsHummock;
pub type SledStateStoreType = impl StateStore + AsHummock;

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
    /// state. (e.g., no `read_epoch` support, no async checkpoint)
    MemoryStateStore(Monitored<MemoryStateStoreType>),
    SledStateStore(Monitored<SledStateStoreType>),
}

fn may_dynamic_dispatch(state_store: impl StateStore + AsHummock) -> impl StateStore + AsHummock {
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

fn may_verify(state_store: impl StateStore + AsHummock) -> impl StateStore + AsHummock {
    #[cfg(not(debug_assertions))]
    {
        state_store
    }
    #[cfg(debug_assertions)]
    {
        use std::marker::PhantomData;

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
            _phantom: PhantomData::<()>,
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

    pub fn as_hummock(&self) -> Option<&HummockStorage> {
        match self {
            StateStoreImpl::HummockStateStore(hummock) => {
                Some(hummock.inner().as_hummock().expect("should be hummock"))
            }
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

#[cfg(any(debug_assertions, test, feature = "test"))]
pub mod verify {
    use std::fmt::Debug;
    use std::future::Future;
    use std::marker::PhantomData;
    use std::ops::{Bound, Deref};
    use std::sync::Arc;

    use bytes::Bytes;
    use risingwave_common::buffer::Bitmap;
    use risingwave_hummock_sdk::key::{TableKey, TableKeyRange};
    use risingwave_hummock_sdk::{HummockReadEpoch, SyncResult};
    use tracing::log::warn;

    use crate::error::StorageResult;
    use crate::hummock::HummockStorage;
    use crate::storage_value::StorageValue;
    use crate::store::*;
    use crate::store_impl::AsHummock;
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

    pub struct VerifyStateStore<A, E, T = ()> {
        pub actual: A,
        pub expected: Option<E>,
        pub _phantom: PhantomData<T>,
    }

    impl<A: AsHummock, E> AsHummock for VerifyStateStore<A, E> {
        fn as_hummock(&self) -> Option<&HummockStorage> {
            self.actual.as_hummock()
        }
    }

    impl<A: StateStoreRead, E: StateStoreRead> StateStoreRead for VerifyStateStore<A, E> {
        type ChangeLogIter = impl StateStoreReadChangeLogIter;
        type Iter = impl StateStoreReadIter;

        async fn get(
            &self,
            key: TableKey<Bytes>,
            epoch: u64,
            read_options: ReadOptions,
        ) -> StorageResult<Option<Bytes>> {
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

        // TODO: may avoid manual async fn when the bug of rust compiler is fixed. Currently it will
        // fail to compile.
        #[allow(clippy::manual_async_fn)]
        fn iter(
            &self,
            key_range: TableKeyRange,
            epoch: u64,
            read_options: ReadOptions,
        ) -> impl Future<Output = StorageResult<Self::Iter>> + '_ {
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

                Ok(verify_iter::<StateStoreIterItem>(actual, expected))
            }
        }

        async fn iter_log(
            &self,
            epoch_range: (u64, u64),
            key_range: TableKeyRange,
            options: ReadLogOptions,
        ) -> StorageResult<Self::ChangeLogIter> {
            let actual = self
                .actual
                .iter_log(epoch_range, key_range.clone(), options.clone())
                .await?;
            let expected = if let Some(expected) = &self.expected {
                Some(expected.iter_log(epoch_range, key_range, options).await?)
            } else {
                None
            };

            Ok(verify_iter::<StateStoreReadLogItem>(actual, expected))
        }
    }

    impl<A: StateStoreIter<T>, E: StateStoreIter<T>, T: IterItem> StateStoreIter<T>
        for VerifyStateStore<A, E, T>
    where
        for<'a> T::ItemRef<'a>: PartialEq + Debug,
    {
        async fn try_next(&mut self) -> StorageResult<Option<T::ItemRef<'_>>> {
            let actual = self.actual.try_next().await?;
            if let Some(expected) = self.expected.as_mut() {
                let expected = expected.try_next().await?;
                assert_eq!(actual, expected);
            }
            Ok(actual)
        }
    }

    fn verify_iter<T: IterItem>(
        actual: impl StateStoreIter<T>,
        expected: Option<impl StateStoreIter<T>>,
    ) -> impl StateStoreIter<T>
    where
        for<'a> T::ItemRef<'a>: PartialEq + Debug,
    {
        VerifyStateStore {
            actual,
            expected,
            _phantom: PhantomData::<T>,
        }
    }

    impl<A: StateStoreWrite, E: StateStoreWrite> StateStoreWrite for VerifyStateStore<A, E> {
        fn ingest_batch(
            &self,
            kv_pairs: Vec<(TableKey<Bytes>, StorageValue)>,
            delete_ranges: Vec<(Bound<Bytes>, Bound<Bytes>)>,
            write_options: WriteOptions,
        ) -> StorageResult<usize> {
            let actual = self.actual.ingest_batch(
                kv_pairs.clone(),
                delete_ranges.clone(),
                write_options.clone(),
            );
            if let Some(expected) = &self.expected {
                let expected = expected.ingest_batch(kv_pairs, delete_ranges, write_options);
                assert_eq!(actual.is_err(), expected.is_err());
            }
            actual
        }
    }

    impl<A: Clone, E: Clone> Clone for VerifyStateStore<A, E> {
        fn clone(&self) -> Self {
            Self {
                actual: self.actual.clone(),
                expected: self.expected.clone(),
                _phantom: PhantomData,
            }
        }
    }

    impl<A: LocalStateStore, E: LocalStateStore> LocalStateStore for VerifyStateStore<A, E> {
        type Iter<'a> = impl StateStoreIter + 'a;

        // We don't verify `may_exist` across different state stores because
        // the return value of `may_exist` is implementation specific and may not
        // be consistent across different state store backends.
        fn may_exist(
            &self,
            key_range: TableKeyRange,
            read_options: ReadOptions,
        ) -> impl Future<Output = StorageResult<bool>> + Send + '_ {
            self.actual.may_exist(key_range, read_options)
        }

        async fn get(
            &self,
            key: TableKey<Bytes>,
            read_options: ReadOptions,
        ) -> StorageResult<Option<Bytes>> {
            let actual = self.actual.get(key.clone(), read_options.clone()).await;
            if let Some(expected) = &self.expected {
                let expected = expected.get(key, read_options).await;
                assert_result_eq(&actual, &expected);
            }
            actual
        }

        #[allow(clippy::manual_async_fn)]
        fn iter(
            &self,
            key_range: TableKeyRange,
            read_options: ReadOptions,
        ) -> impl Future<Output = StorageResult<Self::Iter<'_>>> + Send + '_ {
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

                Ok(verify_iter::<StateStoreIterItem>(actual, expected))
            }
        }

        fn insert(
            &mut self,
            key: TableKey<Bytes>,
            new_val: Bytes,
            old_val: Option<Bytes>,
        ) -> StorageResult<()> {
            if let Some(expected) = &mut self.expected {
                expected.insert(key.clone(), new_val.clone(), old_val.clone())?;
            }
            self.actual.insert(key, new_val, old_val)?;

            Ok(())
        }

        fn delete(&mut self, key: TableKey<Bytes>, old_val: Bytes) -> StorageResult<()> {
            if let Some(expected) = &mut self.expected {
                expected.delete(key.clone(), old_val.clone())?;
            }
            self.actual.delete(key, old_val)?;
            Ok(())
        }

        async fn flush(&mut self) -> StorageResult<usize> {
            if let Some(expected) = &mut self.expected {
                expected.flush().await?;
            }
            self.actual.flush().await
        }

        async fn try_flush(&mut self) -> StorageResult<()> {
            if let Some(expected) = &mut self.expected {
                expected.try_flush().await?;
            }
            self.actual.try_flush().await
        }

        async fn init(&mut self, options: InitOptions) -> StorageResult<()> {
            self.actual.init(options.clone()).await?;
            if let Some(expected) = &mut self.expected {
                expected.init(options).await?;
            }
            Ok(())
        }

        fn seal_current_epoch(&mut self, next_epoch: u64, opts: SealCurrentEpochOptions) {
            if let Some(expected) = &mut self.expected {
                expected.seal_current_epoch(next_epoch, opts.clone());
            }
            self.actual.seal_current_epoch(next_epoch, opts);
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

        fn update_vnode_bitmap(&mut self, vnodes: Arc<Bitmap>) -> Arc<Bitmap> {
            let ret = self.actual.update_vnode_bitmap(vnodes.clone());
            if let Some(expected) = &mut self.expected {
                assert_eq!(ret, expected.update_vnode_bitmap(vnodes));
            }
            ret
        }
    }

    impl<A: StateStore, E: StateStore> StateStore for VerifyStateStore<A, E> {
        type Local = VerifyStateStore<A::Local, E::Local>;

        fn try_wait_epoch(
            &self,
            epoch: HummockReadEpoch,
        ) -> impl Future<Output = StorageResult<()>> + Send + '_ {
            self.actual.try_wait_epoch(epoch)
        }

        async fn sync(&self, epoch: u64) -> StorageResult<SyncResult> {
            if let Some(expected) = &self.expected {
                let _ = expected.sync(epoch).await;
            }
            self.actual.sync(epoch).await
        }

        fn seal_epoch(&self, epoch: u64, is_checkpoint: bool) {
            self.actual.seal_epoch(epoch, is_checkpoint)
        }

        fn clear_shared_buffer(&self, prev_epoch: u64) -> impl Future<Output = ()> + Send + '_ {
            self.actual.clear_shared_buffer(prev_epoch)
        }

        async fn new_local(&self, option: NewLocalOptions) -> Self::Local {
            let expected = if let Some(expected) = &self.expected {
                Some(expected.new_local(option.clone()).await)
            } else {
                None
            };
            VerifyStateStore {
                actual: self.actual.new_local(option).await,
                expected,
                _phantom: PhantomData::<()>,
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
    #[cfg_attr(not(target_os = "linux"), allow(unused_variables))]
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        s: &str,
        opts: Arc<StorageOpts>,
        hummock_meta_client: Arc<MonitoredHummockMetaClient>,
        state_store_metrics: Arc<HummockStateStoreMetrics>,
        object_store_metrics: Arc<ObjectStoreMetrics>,
        storage_metrics: Arc<MonitoredStorageMetrics>,
        compactor_metrics: Arc<CompactorMetrics>,
        await_tree_config: Option<await_tree::Config>,
    ) -> StorageResult<Self> {
        set_foyer_metrics_registry(GLOBAL_METRICS_REGISTRY.clone());

        let (data_file_cache, recent_filter) = if opts.data_file_cache_dir.is_empty() {
            (FileCache::none(), None)
        } else {
            const MB: usize = 1024 * 1024;

            let config = FileCacheConfig {
                name: "data".to_string(),
                dir: PathBuf::from(opts.data_file_cache_dir.clone()),
                capacity: opts.data_file_cache_capacity_mb * MB,
                file_capacity: opts.data_file_cache_file_capacity_mb * MB,
                device_align: opts.data_file_cache_device_align,
                device_io_size: opts.data_file_cache_device_io_size,
                lfu_window_to_cache_size_ratio: opts.data_file_cache_lfu_window_to_cache_size_ratio,
                lfu_tiny_lru_capacity_ratio: opts.data_file_cache_lfu_tiny_lru_capacity_ratio,
                insert_rate_limit: opts.data_file_cache_insert_rate_limit_mb * MB,
                flushers: opts.data_file_cache_flushers,
                reclaimers: opts.data_file_cache_reclaimers,
                recover_concurrency: opts.data_file_cache_recover_concurrency,
                catalog_bits: opts.data_file_cache_catalog_bits,
                admissions: vec![],
                reinsertions: vec![],
                compression: opts
                    .data_file_cache_compression
                    .as_str()
                    .try_into()
                    .map_err(HummockError::file_cache)?,
            };
            let cache = FileCache::open(config)
                .await
                .map_err(HummockError::file_cache)?;
            let filter = Some(Arc::new(RecentFilter::new(
                opts.cache_refill_recent_filter_layers,
                Duration::from_millis(opts.cache_refill_recent_filter_rotate_interval_ms as u64),
            )));
            (cache, filter)
        };

        let meta_file_cache = if opts.meta_file_cache_dir.is_empty() {
            FileCache::none()
        } else {
            const MB: usize = 1024 * 1024;

            let config = FileCacheConfig {
                name: "meta".to_string(),
                dir: PathBuf::from(opts.meta_file_cache_dir.clone()),
                capacity: opts.meta_file_cache_capacity_mb * MB,
                file_capacity: opts.meta_file_cache_file_capacity_mb * MB,
                device_align: opts.meta_file_cache_device_align,
                device_io_size: opts.meta_file_cache_device_io_size,
                lfu_window_to_cache_size_ratio: opts.meta_file_cache_lfu_window_to_cache_size_ratio,
                lfu_tiny_lru_capacity_ratio: opts.meta_file_cache_lfu_tiny_lru_capacity_ratio,
                insert_rate_limit: opts.meta_file_cache_insert_rate_limit_mb * MB,
                flushers: opts.meta_file_cache_flushers,
                reclaimers: opts.meta_file_cache_reclaimers,
                recover_concurrency: opts.meta_file_cache_recover_concurrency,
                catalog_bits: opts.meta_file_cache_catalog_bits,
                admissions: vec![],
                reinsertions: vec![],
                compression: opts
                    .meta_file_cache_compression
                    .as_str()
                    .try_into()
                    .map_err(HummockError::file_cache)?,
            };
            FileCache::open(config)
                .await
                .map_err(HummockError::file_cache)?
        };

        let store = match s {
            hummock if hummock.starts_with("hummock+") => {
                let object_store = build_remote_object_store(
                    hummock.strip_prefix("hummock+").unwrap(),
                    object_store_metrics.clone(),
                    "Hummock",
                    Arc::new(opts.object_store_config.clone()),
                )
                .await;

                let sstable_store = Arc::new(SstableStore::new(SstableStoreConfig {
                    store: Arc::new(object_store),
                    path: opts.data_directory.to_string(),
                    block_cache_capacity: opts.block_cache_capacity_mb * (1 << 20),
                    block_cache_shard_num: opts.block_cache_shard_num,
                    block_cache_eviction: opts.block_cache_eviction_config.clone(),
                    meta_cache_capacity: opts.meta_cache_capacity_mb * (1 << 20),
                    meta_cache_shard_num: opts.meta_cache_shard_num,
                    meta_cache_eviction: opts.meta_cache_eviction_config.clone(),
                    prefetch_buffer_capacity: opts.prefetch_buffer_capacity_mb * (1 << 20),
                    max_prefetch_block_number: opts.max_prefetch_block_number,
                    data_file_cache,
                    meta_file_cache,
                    recent_filter,
                    state_store_metrics: state_store_metrics.clone(),
                }));
                let notification_client =
                    RpcNotificationClient::new(hummock_meta_client.get_inner().clone());
                let key_filter_manager = Arc::new(RpcFilterKeyExtractorManager::new(Box::new(
                    RemoteTableAccessor::new(hummock_meta_client.get_inner().clone()),
                )));
                let inner = HummockStorage::new(
                    opts.clone(),
                    sstable_store,
                    hummock_meta_client.clone(),
                    notification_client,
                    key_filter_manager,
                    state_store_metrics.clone(),
                    compactor_metrics.clone(),
                    await_tree_config,
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

pub trait AsHummock {
    fn as_hummock(&self) -> Option<&HummockStorage>;
}

impl AsHummock for HummockStorage {
    fn as_hummock(&self) -> Option<&HummockStorage> {
        Some(self)
    }
}

impl AsHummock for MemoryStateStore {
    fn as_hummock(&self) -> Option<&HummockStorage> {
        None
    }
}

impl AsHummock for SledStateStore {
    fn as_hummock(&self) -> Option<&HummockStorage> {
        None
    }
}

#[cfg(debug_assertions)]
pub mod boxed_state_store {
    use std::future::Future;
    use std::ops::{Deref, DerefMut};
    use std::sync::Arc;

    use bytes::Bytes;
    use dyn_clone::{clone_trait_object, DynClone};
    use risingwave_common::buffer::Bitmap;
    use risingwave_hummock_sdk::key::{TableKey, TableKeyRange};
    use risingwave_hummock_sdk::{HummockReadEpoch, SyncResult};

    use crate::error::StorageResult;
    use crate::hummock::HummockStorage;
    use crate::store::*;
    use crate::store_impl::AsHummock;
    use crate::StateStore;

    #[async_trait::async_trait]
    pub trait DynamicDispatchedStateStoreIter<T: IterItem>: Send {
        async fn try_next(&mut self) -> StorageResult<Option<T::ItemRef<'_>>>;
    }

    #[async_trait::async_trait]
    impl<T: IterItem, I: StateStoreIter<T>> DynamicDispatchedStateStoreIter<T> for I {
        async fn try_next(&mut self) -> StorageResult<Option<T::ItemRef<'_>>> {
            self.try_next().await
        }
    }

    pub type BoxStateStoreIter<'a, T> = Box<dyn DynamicDispatchedStateStoreIter<T> + 'a>;
    impl<'a, T: IterItem> StateStoreIter<T> for BoxStateStoreIter<'a, T> {
        fn try_next(
            &mut self,
        ) -> impl Future<Output = StorageResult<Option<T::ItemRef<'_>>>> + Send + '_ {
            self.deref_mut().try_next()
        }
    }

    // For StateStoreRead

    pub type BoxStateStoreReadIter = BoxStateStoreIter<'static, StateStoreIterItem>;
    pub type BoxStateStoreReadChangeLogIter = BoxStateStoreIter<'static, StateStoreReadLogItem>;

    #[async_trait::async_trait]
    pub trait DynamicDispatchedStateStoreRead: StaticSendSync {
        async fn get(
            &self,
            key: TableKey<Bytes>,
            epoch: u64,
            read_options: ReadOptions,
        ) -> StorageResult<Option<Bytes>>;

        async fn iter(
            &self,
            key_range: TableKeyRange,
            epoch: u64,
            read_options: ReadOptions,
        ) -> StorageResult<BoxStateStoreReadIter>;

        async fn iter_log(
            &self,
            epoch_range: (u64, u64),
            key_range: TableKeyRange,
            options: ReadLogOptions,
        ) -> StorageResult<BoxStateStoreReadChangeLogIter>;
    }

    #[async_trait::async_trait]
    impl<S: StateStoreRead> DynamicDispatchedStateStoreRead for S {
        async fn get(
            &self,
            key: TableKey<Bytes>,
            epoch: u64,
            read_options: ReadOptions,
        ) -> StorageResult<Option<Bytes>> {
            self.get(key, epoch, read_options).await
        }

        async fn iter(
            &self,
            key_range: TableKeyRange,
            epoch: u64,
            read_options: ReadOptions,
        ) -> StorageResult<BoxStateStoreReadIter> {
            Ok(Box::new(self.iter(key_range, epoch, read_options).await?))
        }

        async fn iter_log(
            &self,
            epoch_range: (u64, u64),
            key_range: TableKeyRange,
            options: ReadLogOptions,
        ) -> StorageResult<BoxStateStoreReadChangeLogIter> {
            Ok(Box::new(
                self.iter_log(epoch_range, key_range, options).await?,
            ))
        }
    }

    // For LocalStateStore
    pub type BoxLocalStateStoreIterStream<'a> = BoxStateStoreIter<'a, StateStoreIterItem>;
    #[async_trait::async_trait]
    pub trait DynamicDispatchedLocalStateStore: StaticSendSync {
        async fn may_exist(
            &self,
            key_range: TableKeyRange,
            read_options: ReadOptions,
        ) -> StorageResult<bool>;

        async fn get(
            &self,
            key: TableKey<Bytes>,
            read_options: ReadOptions,
        ) -> StorageResult<Option<Bytes>>;

        async fn iter(
            &self,
            key_range: TableKeyRange,
            read_options: ReadOptions,
        ) -> StorageResult<BoxLocalStateStoreIterStream<'_>>;

        fn insert(
            &mut self,
            key: TableKey<Bytes>,
            new_val: Bytes,
            old_val: Option<Bytes>,
        ) -> StorageResult<()>;

        fn delete(&mut self, key: TableKey<Bytes>, old_val: Bytes) -> StorageResult<()>;

        async fn flush(&mut self) -> StorageResult<usize>;

        async fn try_flush(&mut self) -> StorageResult<()>;

        fn epoch(&self) -> u64;

        fn is_dirty(&self) -> bool;

        async fn init(&mut self, epoch: InitOptions) -> StorageResult<()>;

        fn seal_current_epoch(&mut self, next_epoch: u64, opts: SealCurrentEpochOptions);

        fn update_vnode_bitmap(&mut self, vnodes: Arc<Bitmap>) -> Arc<Bitmap>;
    }

    #[async_trait::async_trait]
    impl<S: LocalStateStore> DynamicDispatchedLocalStateStore for S {
        async fn may_exist(
            &self,
            key_range: TableKeyRange,
            read_options: ReadOptions,
        ) -> StorageResult<bool> {
            self.may_exist(key_range, read_options).await
        }

        async fn get(
            &self,
            key: TableKey<Bytes>,
            read_options: ReadOptions,
        ) -> StorageResult<Option<Bytes>> {
            self.get(key, read_options).await
        }

        async fn iter(
            &self,
            key_range: TableKeyRange,
            read_options: ReadOptions,
        ) -> StorageResult<BoxLocalStateStoreIterStream<'_>> {
            Ok(Box::new(self.iter(key_range, read_options).await?))
        }

        fn insert(
            &mut self,
            key: TableKey<Bytes>,
            new_val: Bytes,
            old_val: Option<Bytes>,
        ) -> StorageResult<()> {
            self.insert(key, new_val, old_val)
        }

        fn delete(&mut self, key: TableKey<Bytes>, old_val: Bytes) -> StorageResult<()> {
            self.delete(key, old_val)
        }

        async fn flush(&mut self) -> StorageResult<usize> {
            self.flush().await
        }

        async fn try_flush(&mut self) -> StorageResult<()> {
            self.try_flush().await
        }

        fn epoch(&self) -> u64 {
            self.epoch()
        }

        fn is_dirty(&self) -> bool {
            self.is_dirty()
        }

        async fn init(&mut self, options: InitOptions) -> StorageResult<()> {
            self.init(options).await
        }

        fn seal_current_epoch(&mut self, next_epoch: u64, opts: SealCurrentEpochOptions) {
            self.seal_current_epoch(next_epoch, opts)
        }

        fn update_vnode_bitmap(&mut self, vnodes: Arc<Bitmap>) -> Arc<Bitmap> {
            self.update_vnode_bitmap(vnodes)
        }
    }

    pub type BoxDynamicDispatchedLocalStateStore = Box<dyn DynamicDispatchedLocalStateStore>;

    impl LocalStateStore for BoxDynamicDispatchedLocalStateStore {
        type Iter<'a> = BoxLocalStateStoreIterStream<'a>;

        fn may_exist(
            &self,
            key_range: TableKeyRange,
            read_options: ReadOptions,
        ) -> impl Future<Output = StorageResult<bool>> + Send + '_ {
            self.deref().may_exist(key_range, read_options)
        }

        fn get(
            &self,
            key: TableKey<Bytes>,
            read_options: ReadOptions,
        ) -> impl Future<Output = StorageResult<Option<Bytes>>> + Send + '_ {
            self.deref().get(key, read_options)
        }

        fn iter(
            &self,
            key_range: TableKeyRange,
            read_options: ReadOptions,
        ) -> impl Future<Output = StorageResult<Self::Iter<'_>>> + Send + '_ {
            self.deref().iter(key_range, read_options)
        }

        fn insert(
            &mut self,
            key: TableKey<Bytes>,
            new_val: Bytes,
            old_val: Option<Bytes>,
        ) -> StorageResult<()> {
            self.deref_mut().insert(key, new_val, old_val)
        }

        fn delete(&mut self, key: TableKey<Bytes>, old_val: Bytes) -> StorageResult<()> {
            self.deref_mut().delete(key, old_val)
        }

        fn flush(&mut self) -> impl Future<Output = StorageResult<usize>> + Send + '_ {
            self.deref_mut().flush()
        }

        fn try_flush(&mut self) -> impl Future<Output = StorageResult<()>> + Send + '_ {
            self.deref_mut().try_flush()
        }

        fn epoch(&self) -> u64 {
            self.deref().epoch()
        }

        fn is_dirty(&self) -> bool {
            self.deref().is_dirty()
        }

        fn init(
            &mut self,
            options: InitOptions,
        ) -> impl Future<Output = StorageResult<()>> + Send + '_ {
            self.deref_mut().init(options)
        }

        fn seal_current_epoch(&mut self, next_epoch: u64, opts: SealCurrentEpochOptions) {
            self.deref_mut().seal_current_epoch(next_epoch, opts)
        }

        fn update_vnode_bitmap(&mut self, vnodes: Arc<Bitmap>) -> Arc<Bitmap> {
            self.deref_mut().update_vnode_bitmap(vnodes)
        }
    }

    // For global StateStore

    #[async_trait::async_trait]
    pub trait DynamicDispatchedStateStoreExt: StaticSendSync {
        async fn try_wait_epoch(&self, epoch: HummockReadEpoch) -> StorageResult<()>;

        async fn sync(&self, epoch: u64) -> StorageResult<SyncResult>;

        fn seal_epoch(&self, epoch: u64, is_checkpoint: bool);

        async fn clear_shared_buffer(&self, prev_epoch: u64);

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

        async fn clear_shared_buffer(&self, prev_epoch: u64) {
            self.clear_shared_buffer(prev_epoch).await
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
        type ChangeLogIter = BoxStateStoreReadChangeLogIter;
        type Iter = BoxStateStoreReadIter;

        fn get(
            &self,
            key: TableKey<Bytes>,
            epoch: u64,
            read_options: ReadOptions,
        ) -> impl Future<Output = StorageResult<Option<Bytes>>> + Send + '_ {
            self.deref().get(key, epoch, read_options)
        }

        fn iter(
            &self,
            key_range: TableKeyRange,
            epoch: u64,
            read_options: ReadOptions,
        ) -> impl Future<Output = StorageResult<Self::Iter>> + '_ {
            self.deref().iter(key_range, epoch, read_options)
        }

        fn iter_log(
            &self,
            epoch_range: (u64, u64),
            key_range: TableKeyRange,
            options: ReadLogOptions,
        ) -> impl Future<Output = StorageResult<Self::ChangeLogIter>> + Send + '_ {
            self.deref().iter_log(epoch_range, key_range, options)
        }
    }

    pub trait DynamicDispatchedStateStore:
        DynClone + DynamicDispatchedStateStoreRead + DynamicDispatchedStateStoreExt + AsHummock
    {
    }

    clone_trait_object!(DynamicDispatchedStateStore);

    impl AsHummock for BoxDynamicDispatchedStateStore {
        fn as_hummock(&self) -> Option<&HummockStorage> {
            self.deref().as_hummock()
        }
    }

    impl<
            S: DynClone + DynamicDispatchedStateStoreRead + DynamicDispatchedStateStoreExt + AsHummock,
        > DynamicDispatchedStateStore for S
    {
    }

    impl StateStore for BoxDynamicDispatchedStateStore {
        type Local = BoxDynamicDispatchedLocalStateStore;

        fn try_wait_epoch(
            &self,
            epoch: HummockReadEpoch,
        ) -> impl Future<Output = StorageResult<()>> + Send + '_ {
            self.deref().try_wait_epoch(epoch)
        }

        fn sync(&self, epoch: u64) -> impl Future<Output = StorageResult<SyncResult>> + Send + '_ {
            self.deref().sync(epoch)
        }

        fn clear_shared_buffer(&self, prev_epoch: u64) -> impl Future<Output = ()> + Send + '_ {
            self.deref().clear_shared_buffer(prev_epoch)
        }

        fn seal_epoch(&self, epoch: u64, is_checkpoint: bool) {
            self.deref().seal_epoch(epoch, is_checkpoint)
        }

        fn new_local(
            &self,
            option: NewLocalOptions,
        ) -> impl Future<Output = Self::Local> + Send + '_ {
            self.deref().new_local(option)
        }

        fn validate_read_epoch(&self, epoch: HummockReadEpoch) -> StorageResult<()> {
            self.deref().validate_read_epoch(epoch)
        }
    }
}

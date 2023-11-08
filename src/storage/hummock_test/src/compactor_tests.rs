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

#[cfg(test)]
pub(crate) mod tests {

    use std::collections::{BTreeMap, BTreeSet, VecDeque};
    use std::ops::Bound;
    use std::sync::atomic::AtomicU32;
    use std::sync::Arc;

    use bytes::{BufMut, Bytes, BytesMut};
    use itertools::Itertools;
    use rand::{Rng, RngCore, SeedableRng};
    use risingwave_common::cache::CachePriority;
    use risingwave_common::catalog::TableId;
    use risingwave_common::constants::hummock::CompactionFilterFlag;
    use risingwave_common::util::epoch::Epoch;
    use risingwave_common_service::observer_manager::NotificationClient;
    use risingwave_hummock_sdk::can_concat;
    use risingwave_hummock_sdk::compaction_group::hummock_version_ext::HummockVersionExt;
    use risingwave_hummock_sdk::compaction_group::StaticCompactionGroupId;
    use risingwave_hummock_sdk::key::{next_key, FullKey, TableKey, TABLE_PREFIX_LEN};
    use risingwave_hummock_sdk::prost_key_range::KeyRangeExt;
    use risingwave_hummock_sdk::table_stats::to_prost_table_stats_map;
    use risingwave_meta::hummock::compaction::compaction_config::CompactionConfigBuilder;
    use risingwave_meta::hummock::compaction::selector::{
        default_compaction_selector, ManualCompactionOption,
    };
    use risingwave_meta::hummock::test_utils::{
        register_table_ids_to_compaction_group, setup_compute_env, setup_compute_env_with_config,
        unregister_table_ids_from_compaction_group,
    };
    use risingwave_meta::hummock::{HummockManagerRef, MockHummockMetaClient};
    use risingwave_pb::common::{HostAddress, WorkerType};
    use risingwave_pb::hummock::{CompactTask, HummockVersion, InputLevel, KeyRange, TableOption};
    use risingwave_pb::meta::add_worker_node_request::Property;
    use risingwave_rpc_client::HummockMetaClient;
    use risingwave_storage::filter_key_extractor::{
        FilterKeyExtractorImpl, FilterKeyExtractorManager, FixedLengthFilterKeyExtractor,
        FullKeyFilterKeyExtractor,
    };
    use risingwave_storage::hummock::compactor::compactor_runner::{compact, CompactorRunner};
    use risingwave_storage::hummock::compactor::fast_compactor_runner::CompactorRunner as FastCompactorRunner;
    use risingwave_storage::hummock::compactor::{
        CompactionExecutor, CompactorContext, DummyCompactionFilter, TaskProgress,
    };
    use risingwave_storage::hummock::iterator::test_utils::mock_sstable_store;
    use risingwave_storage::hummock::iterator::{ConcatIterator, UserIterator};
    use risingwave_storage::hummock::sstable_store::SstableStoreRef;
    use risingwave_storage::hummock::test_utils::gen_test_sstable_info;
    use risingwave_storage::hummock::value::HummockValue;
    use risingwave_storage::hummock::{
        CachePolicy, CompactionDeleteRanges, CompressionAlgorithm,
        HummockStorage as GlobalHummockStorage, HummockStorage, MemoryLimiter,
        SharedComapctorObjectIdManager, Sstable, SstableBuilderOptions, SstableIteratorReadOptions,
        SstableObjectIdManager,
    };
    use risingwave_storage::monitor::{CompactorMetrics, StoreLocalStatistic};
    use risingwave_storage::opts::StorageOpts;
    use risingwave_storage::storage_value::StorageValue;
    use risingwave_storage::store::*;

    use crate::get_notification_client_for_test;
    use crate::local_state_store_test_utils::LocalStateStoreTestExt;
    use crate::test_utils::{register_tables_with_id_for_test, TestIngestBatch};

    pub(crate) async fn get_hummock_storage(
        hummock_meta_client: Arc<dyn HummockMetaClient>,
        notification_client: impl NotificationClient,
        hummock_manager_ref: &HummockManagerRef,
        table_id: TableId,
    ) -> HummockStorage {
        let remote_dir = "hummock_001_test".to_string();
        let options = Arc::new(StorageOpts {
            sstable_size_mb: 1,
            block_size_kb: 1,
            bloom_false_positive: 0.1,
            data_directory: remote_dir.clone(),
            write_conflict_detection_enabled: true,
            ..Default::default()
        });
        let sstable_store = mock_sstable_store();

        let hummock = GlobalHummockStorage::for_test(
            options,
            sstable_store,
            hummock_meta_client.clone(),
            notification_client,
        )
        .await
        .unwrap();

        register_tables_with_id_for_test(
            hummock.filter_key_extractor_manager(),
            hummock_manager_ref,
            &[table_id.table_id()],
        )
        .await;

        hummock
    }

    async fn get_global_hummock_storage(
        hummock_meta_client: Arc<dyn HummockMetaClient>,
        notification_client: impl NotificationClient,
    ) -> GlobalHummockStorage {
        let remote_dir = "hummock_001_test".to_string();
        let options = Arc::new(StorageOpts {
            sstable_size_mb: 1,
            block_size_kb: 1,
            bloom_false_positive: 0.1,
            data_directory: remote_dir.clone(),
            write_conflict_detection_enabled: true,
            ..Default::default()
        });
        let sstable_store = mock_sstable_store();

        GlobalHummockStorage::for_test(
            options,
            sstable_store,
            hummock_meta_client.clone(),
            notification_client,
        )
        .await
        .unwrap()
    }

    async fn prepare_test_put_data(
        storage: &HummockStorage,
        hummock_meta_client: &Arc<dyn HummockMetaClient>,
        key: &Bytes,
        value_size: usize,
        epochs: Vec<u64>,
    ) {
        let mut local = storage.new_local(Default::default()).await;
        // 1. add sstables
        let val = b"0"[..].repeat(value_size);
        local.init_for_test(epochs[0]).await.unwrap();
        for (i, &epoch) in epochs.iter().enumerate() {
            let mut new_val = val.clone();
            new_val.extend_from_slice(&epoch.to_be_bytes());
            local
                .ingest_batch(
                    vec![(
                        TableKey(key.clone()),
                        StorageValue::new_put(Bytes::from(new_val)),
                    )],
                    vec![],
                    WriteOptions {
                        epoch,
                        table_id: Default::default(),
                    },
                )
                .await
                .unwrap();
            if i + 1 < epochs.len() {
                local.seal_current_epoch(epochs[i + 1]);
            } else {
                local.seal_current_epoch(u64::MAX);
            }
            let ssts = storage
                .seal_and_sync_epoch(epoch)
                .await
                .unwrap()
                .uncommitted_ssts;

            hummock_meta_client.commit_epoch(epoch, ssts).await.unwrap();
        }
    }

    fn get_compactor_context(storage: &HummockStorage) -> CompactorContext {
        get_compactor_context_impl(storage.storage_opts().clone(), storage.sstable_store())
    }

    fn get_compactor_context_impl(
        options: Arc<StorageOpts>,
        sstable_store: SstableStoreRef,
    ) -> CompactorContext {
        CompactorContext {
            storage_opts: options,
            sstable_store,
            compactor_metrics: Arc::new(CompactorMetrics::unused()),
            is_share_buffer_compact: false,
            compaction_executor: Arc::new(CompactionExecutor::new(Some(1))),
            memory_limiter: MemoryLimiter::unlimit(),
            task_progress_manager: Default::default(),
            await_tree_reg: None,
            running_task_count: Arc::new(AtomicU32::new(0)),
        }
    }

    #[tokio::test]
    async fn test_compaction_watermark() {
        let config = CompactionConfigBuilder::new()
            .level0_tier_compact_file_number(1)
            .level0_max_compact_file_number(130)
            .level0_overlapping_sub_level_compact_level_count(1)
            .build();
        let (env, hummock_manager_ref, _cluster_manager_ref, worker_node) =
            setup_compute_env_with_config(8080, config).await;
        let hummock_meta_client: Arc<dyn HummockMetaClient> = Arc::new(MockHummockMetaClient::new(
            hummock_manager_ref.clone(),
            worker_node.id,
        ));

        // 1. add sstables
        let mut key = BytesMut::default();
        key.put_u16(0);
        key.put_slice(b"same_key");
        let storage = get_hummock_storage(
            hummock_meta_client.clone(),
            get_notification_client_for_test(env, hummock_manager_ref.clone(), worker_node.clone()),
            &hummock_manager_ref,
            Default::default(),
        )
        .await;
        let rpc_filter_key_extractor_manager = match storage.filter_key_extractor_manager().clone()
        {
            FilterKeyExtractorManager::RpcFilterKeyExtractorManager(
                rpc_filter_key_extractor_manager,
            ) => rpc_filter_key_extractor_manager,
            FilterKeyExtractorManager::StaticFilterKeyExtractorManager(_) => unreachable!(),
        };
        let filter_key_extractor_manager = FilterKeyExtractorManager::RpcFilterKeyExtractorManager(
            rpc_filter_key_extractor_manager,
        );
        let compact_ctx = get_compactor_context(&storage);
        let sstable_object_id_manager = Arc::new(SstableObjectIdManager::new(
            hummock_meta_client.clone(),
            storage
                .storage_opts()
                .clone()
                .sstable_id_remote_fetch_number,
        ));
        let worker_node2 = hummock_manager_ref
            .cluster_manager
            .add_worker_node(
                WorkerType::ComputeNode,
                HostAddress::default(),
                Property::default(),
            )
            .await
            .unwrap();
        let _snapshot = hummock_manager_ref
            .pin_snapshot(worker_node2.id)
            .await
            .unwrap();
        let key = key.freeze();
        const SST_COUNT: u64 = 32;
        const TEST_WATERMARK: u64 = 8;
        prepare_test_put_data(
            &storage,
            &hummock_meta_client,
            &key,
            1 << 10,
            (1..SST_COUNT + 1).map(|v| (v * 1000) << 16).collect_vec(),
        )
        .await;
        // 2. get compact task
        while let Some(mut compact_task) = hummock_manager_ref
            .get_compact_task(
                StaticCompactionGroupId::StateDefault.into(),
                &mut default_compaction_selector(),
            )
            .await
            .unwrap()
        {
            let compaction_filter_flag = CompactionFilterFlag::TTL;
            compact_task.watermark = (TEST_WATERMARK * 1000) << 16;
            compact_task.compaction_filter_mask = compaction_filter_flag.bits();
            compact_task.table_options = BTreeMap::from([(
                0,
                TableOption {
                    retention_seconds: 64,
                },
            )]);
            compact_task.current_epoch_time = 0;

            let (_tx, rx) = tokio::sync::oneshot::channel();
            let (result_task, task_stats) = compact(
                compact_ctx.clone(),
                compact_task.clone(),
                rx,
                Box::new(sstable_object_id_manager.clone()),
                filter_key_extractor_manager.clone(),
            )
            .await;

            hummock_manager_ref
                .report_compact_task_for_test(
                    result_task.task_id,
                    Some(compact_task),
                    result_task.task_status(),
                    result_task.sorted_output_ssts,
                    Some(to_prost_table_stats_map(task_stats)),
                )
                .await
                .unwrap();
        }

        let mut val = b"0"[..].repeat(1 << 10);
        val.extend_from_slice(&((TEST_WATERMARK * 1000) << 16).to_be_bytes());

        let compactor_manager = hummock_manager_ref.compactor_manager_ref_for_test();
        let _recv = compactor_manager.add_compactor(worker_node.id);

        // 4. get the latest version and check
        let version = hummock_manager_ref.get_current_version().await;
        let group =
            version.get_compaction_group_levels(StaticCompactionGroupId::StateDefault.into());

        // base level
        let output_tables = group
            .levels
            .iter()
            .flat_map(|level| level.table_infos.clone())
            .chain(
                group
                    .l0
                    .as_ref()
                    .unwrap()
                    .sub_levels
                    .iter()
                    .flat_map(|level| level.table_infos.clone()),
            )
            .collect_vec();

        storage.wait_version(version).await;
        let mut table_key_count = 0;
        for output_sst in output_tables {
            let table = storage
                .sstable_store()
                .sstable(&output_sst, &mut StoreLocalStatistic::default())
                .await
                .unwrap();
            table_key_count += table.value().meta.key_count;
        }

        // we have removed these 31 keys before watermark 32.
        assert_eq!(table_key_count, (SST_COUNT - TEST_WATERMARK + 1) as u32);
        let read_epoch = (TEST_WATERMARK * 1000) << 16;

        let get_ret = storage
            .get(
                TableKey(key.clone()),
                read_epoch,
                ReadOptions {
                    cache_policy: CachePolicy::Fill(CachePriority::High),
                    ..Default::default()
                },
            )
            .await;
        let get_val = get_ret.unwrap().unwrap().to_vec();

        assert_eq!(get_val, val);
        let ret = storage
            .get(
                TableKey(key.clone()),
                ((TEST_WATERMARK - 1) * 1000) << 16,
                ReadOptions {
                    prefix_hint: Some(key.clone()),
                    cache_policy: CachePolicy::Fill(CachePriority::High),
                    ..Default::default()
                },
            )
            .await;
        assert!(ret.is_err());
    }

    #[tokio::test]
    async fn test_compaction_same_key_not_split() {
        let (env, hummock_manager_ref, _cluster_manager_ref, worker_node) =
            setup_compute_env(8080).await;
        let hummock_meta_client: Arc<dyn HummockMetaClient> = Arc::new(MockHummockMetaClient::new(
            hummock_manager_ref.clone(),
            worker_node.id,
        ));

        let storage = get_hummock_storage(
            hummock_meta_client.clone(),
            get_notification_client_for_test(env, hummock_manager_ref.clone(), worker_node.clone()),
            &hummock_manager_ref,
            Default::default(),
        )
        .await;

        let rpc_filter_key_extractor_manager = match storage.filter_key_extractor_manager().clone()
        {
            FilterKeyExtractorManager::RpcFilterKeyExtractorManager(
                rpc_filter_key_extractor_manager,
            ) => rpc_filter_key_extractor_manager,
            FilterKeyExtractorManager::StaticFilterKeyExtractorManager(_) => unreachable!(),
        };
        let filter_key_extractor_manager = FilterKeyExtractorManager::RpcFilterKeyExtractorManager(
            rpc_filter_key_extractor_manager,
        );
        let compact_ctx = get_compactor_context(&storage);
        let sstable_object_id_manager = Arc::new(SstableObjectIdManager::new(
            hummock_meta_client.clone(),
            storage
                .storage_opts()
                .clone()
                .sstable_id_remote_fetch_number,
        ));
        // 1. add sstables with 1MB value
        let mut key = BytesMut::default();
        key.put_u16(0);
        key.put_slice(b"same_key");
        let key = key.freeze();
        const SST_COUNT: u64 = 16;

        let mut val = b"0"[..].repeat(1 << 20);
        val.extend_from_slice(&SST_COUNT.to_be_bytes());
        prepare_test_put_data(
            &storage,
            &hummock_meta_client,
            &key,
            1 << 20,
            (1..SST_COUNT + 1).collect_vec(),
        )
        .await;

        // 2. get compact task

        // 3. compact
        while let Some(compact_task) = hummock_manager_ref
            .get_compact_task(
                StaticCompactionGroupId::StateDefault.into(),
                &mut default_compaction_selector(),
            )
            .await
            .unwrap()
        {
            // 3. compact
            let (_tx, rx) = tokio::sync::oneshot::channel();
            let (result_task, task_stats) = compact(
                compact_ctx.clone(),
                compact_task.clone(),
                rx,
                Box::new(sstable_object_id_manager.clone()),
                filter_key_extractor_manager.clone(),
            )
            .await;

            hummock_manager_ref
                .report_compact_task(
                    result_task.task_id,
                    result_task.task_status(),
                    result_task.sorted_output_ssts,
                    Some(to_prost_table_stats_map(task_stats)),
                )
                .await
                .unwrap();
        }

        // 4. get the latest version and check
        let version = hummock_manager_ref.get_current_version().await;
        let output_tables = version
            .get_compaction_group_levels(StaticCompactionGroupId::StateDefault.into())
            .levels
            .iter()
            .flat_map(|level| level.table_infos.clone())
            .collect_vec();
        for output_table in &output_tables {
            let table = storage
                .sstable_store()
                .sstable(output_table, &mut StoreLocalStatistic::default())
                .await
                .unwrap();
            let target_table_size = storage.storage_opts().sstable_size_mb * (1 << 20);
            assert!(
                table.value().meta.estimated_size > target_table_size,
                "table.meta.estimated_size {} <= target_table_size {}",
                table.value().meta.estimated_size,
                target_table_size
            );
        }
        // 5. storage get back the correct kv after compaction
        storage.wait_version(version).await;
        let get_val = storage
            .get(
                TableKey(key.clone()),
                SST_COUNT + 1,
                ReadOptions {
                    cache_policy: CachePolicy::Fill(CachePriority::High),
                    ..Default::default()
                },
            )
            .await
            .unwrap()
            .unwrap()
            .to_vec();
        assert_eq!(get_val, val);
    }

    pub(crate) async fn flush_and_commit(
        hummock_meta_client: &Arc<dyn HummockMetaClient>,
        storage: &HummockStorage,
        epoch: u64,
    ) {
        let ssts = storage
            .seal_and_sync_epoch(epoch)
            .await
            .unwrap()
            .uncommitted_ssts;
        hummock_meta_client.commit_epoch(epoch, ssts).await.unwrap();
    }

    async fn prepare_data(
        hummock_meta_client: Arc<dyn HummockMetaClient>,
        storage: &HummockStorage,
        existing_table_id: u32,
        keys_per_epoch: usize,
    ) {
        let kv_count: u16 = 128;
        let mut epoch: u64 = 1;

        let mut local = storage
            .new_local(NewLocalOptions::for_test(existing_table_id.into()))
            .await;

        // 1. add sstables
        let val = Bytes::from(b"0"[..].repeat(1 << 10)); // 1024 Byte value
        for idx in 0..kv_count {
            epoch += 1;

            if idx == 0 {
                local.init_for_test(epoch).await.unwrap();
            }

            for _ in 0..keys_per_epoch {
                let mut key = idx.to_be_bytes().to_vec();
                let ramdom_key = rand::thread_rng().gen::<[u8; 32]>();
                key.extend_from_slice(&ramdom_key);
                local
                    .insert(TableKey(Bytes::from(key)), val.clone(), None)
                    .unwrap();
            }
            local.flush(Vec::new()).await.unwrap();
            local.seal_current_epoch(epoch + 1);

            flush_and_commit(&hummock_meta_client, storage, epoch).await;
        }
    }

    pub fn prepare_compactor_and_filter(
        storage: &HummockStorage,
        existing_table_id: u32,
    ) -> (CompactorContext, FilterKeyExtractorManager) {
        let rpc_filter_key_extractor_manager = match storage.filter_key_extractor_manager().clone()
        {
            FilterKeyExtractorManager::RpcFilterKeyExtractorManager(
                rpc_filter_key_extractor_manager,
            ) => rpc_filter_key_extractor_manager,
            FilterKeyExtractorManager::StaticFilterKeyExtractorManager(_) => unreachable!(),
        };
        rpc_filter_key_extractor_manager.update(
            existing_table_id,
            Arc::new(FilterKeyExtractorImpl::FullKey(FullKeyFilterKeyExtractor)),
        );

        let filter_key_extractor_manager = FilterKeyExtractorManager::RpcFilterKeyExtractorManager(
            rpc_filter_key_extractor_manager,
        );

        (get_compactor_context(storage), filter_key_extractor_manager)
    }

    #[tokio::test]
    async fn test_compaction_drop_all_key() {
        let (env, hummock_manager_ref, _cluster_manager_ref, worker_node) =
            setup_compute_env(8080).await;
        let hummock_meta_client: Arc<dyn HummockMetaClient> = Arc::new(MockHummockMetaClient::new(
            hummock_manager_ref.clone(),
            worker_node.id,
        ));

        let existing_table_id: u32 = 1;
        let storage_existing_table_id = get_hummock_storage(
            hummock_meta_client.clone(),
            get_notification_client_for_test(env, hummock_manager_ref.clone(), worker_node),
            &hummock_manager_ref,
            TableId::from(existing_table_id),
        )
        .await;

        prepare_data(
            hummock_meta_client.clone(),
            &storage_existing_table_id,
            existing_table_id,
            1,
        )
        .await;

        // Mimic dropping table
        unregister_table_ids_from_compaction_group(&hummock_manager_ref, &[existing_table_id])
            .await;

        let manual_compcation_option = ManualCompactionOption {
            level: 0,
            ..Default::default()
        };
        // 2. get compact task and there should be none
        let compact_task = hummock_manager_ref
            .manual_get_compact_task(
                StaticCompactionGroupId::StateDefault.into(),
                manual_compcation_option,
            )
            .await
            .unwrap();

        assert!(compact_task.is_none());

        // 3. get the latest version and check
        let version = hummock_manager_ref.get_current_version().await;
        let output_level_info = version
            .get_compaction_group_levels(StaticCompactionGroupId::StateDefault.into())
            .levels
            .last()
            .unwrap();
        assert_eq!(0, output_level_info.total_file_size);

        // 5. get compact task
        let compact_task = hummock_manager_ref
            .get_compact_task(
                StaticCompactionGroupId::StateDefault.into(),
                &mut default_compaction_selector(),
            )
            .await
            .unwrap();

        assert!(compact_task.is_none());
    }

    #[tokio::test]
    async fn test_compaction_drop_key_by_existing_table_id() {
        let (env, hummock_manager_ref, _cluster_manager_ref, worker_node) =
            setup_compute_env(8080).await;
        let hummock_meta_client: Arc<dyn HummockMetaClient> = Arc::new(MockHummockMetaClient::new(
            hummock_manager_ref.clone(),
            worker_node.id,
        ));

        let global_storage = get_global_hummock_storage(
            hummock_meta_client.clone(),
            get_notification_client_for_test(env, hummock_manager_ref.clone(), worker_node.clone()),
        )
        .await;

        // register the local_storage to global_storage
        let mut storage_1 = global_storage
            .new_local(NewLocalOptions::for_test(TableId::from(1)))
            .await;
        let mut storage_2 = global_storage
            .new_local(NewLocalOptions::for_test(TableId::from(2)))
            .await;

        let rpc_filter_key_extractor_manager =
            match global_storage.filter_key_extractor_manager().clone() {
                FilterKeyExtractorManager::RpcFilterKeyExtractorManager(
                    rpc_filter_key_extractor_manager,
                ) => rpc_filter_key_extractor_manager,
                FilterKeyExtractorManager::StaticFilterKeyExtractorManager(_) => unreachable!(),
            };

        rpc_filter_key_extractor_manager.update(
            1,
            Arc::new(FilterKeyExtractorImpl::FullKey(FullKeyFilterKeyExtractor)),
        );

        rpc_filter_key_extractor_manager.update(
            2,
            Arc::new(FilterKeyExtractorImpl::FullKey(FullKeyFilterKeyExtractor)),
        );
        let filter_key_extractor_manager = FilterKeyExtractorManager::RpcFilterKeyExtractorManager(
            rpc_filter_key_extractor_manager,
        );
        let compact_ctx = get_compactor_context_impl(
            global_storage.storage_opts().clone(),
            global_storage.sstable_store(),
        );
        let sstable_object_id_manager = Arc::new(SstableObjectIdManager::new(
            hummock_meta_client.clone(),
            global_storage
                .storage_opts()
                .clone()
                .sstable_id_remote_fetch_number,
        ));
        // 1. add sstables
        let val = Bytes::from(b"0"[..].repeat(1 << 10)); // 1024 Byte value

        let drop_table_id = 1;
        let existing_table_ids = 2;
        let kv_count: usize = 128;
        let mut epoch: u64 = 1;
        register_table_ids_to_compaction_group(
            &hummock_manager_ref,
            &[drop_table_id, existing_table_ids],
            StaticCompactionGroupId::StateDefault.into(),
        )
        .await;
        for index in 0..kv_count {
            epoch += 1;
            let next_epoch = epoch + 1;
            if index == 0 {
                storage_1.init_for_test(epoch).await.unwrap();
                storage_2.init_for_test(epoch).await.unwrap();
            }

            let (storage, other) = if index % 2 == 0 {
                (&mut storage_1, &mut storage_2)
            } else {
                (&mut storage_2, &mut storage_1)
            };

            let mut prefix = BytesMut::default();
            let random_key = rand::thread_rng().gen::<[u8; 32]>();
            prefix.put_u16(1);
            prefix.put_slice(random_key.as_slice());

            storage
                .insert(TableKey(prefix.freeze()), val.clone(), None)
                .unwrap();
            storage.flush(Vec::new()).await.unwrap();
            storage.seal_current_epoch(next_epoch);
            other.seal_current_epoch(next_epoch);

            let ssts = global_storage
                .seal_and_sync_epoch(epoch)
                .await
                .unwrap()
                .uncommitted_ssts;
            hummock_meta_client.commit_epoch(epoch, ssts).await.unwrap();
        }

        // Mimic dropping table
        unregister_table_ids_from_compaction_group(&hummock_manager_ref, &[drop_table_id]).await;

        let manual_compcation_option = ManualCompactionOption {
            level: 0,
            ..Default::default()
        };
        // 2. get compact task
        let mut compact_task = hummock_manager_ref
            .manual_get_compact_task(
                StaticCompactionGroupId::StateDefault.into(),
                manual_compcation_option,
            )
            .await
            .unwrap()
            .unwrap();
        let compaction_filter_flag = CompactionFilterFlag::STATE_CLEAN | CompactionFilterFlag::TTL;
        compact_task.compaction_filter_mask = compaction_filter_flag.bits();

        // assert compact_task
        assert_eq!(
            compact_task
                .input_ssts
                .iter()
                .filter(|level| level.level_idx != compact_task.target_level)
                .map(|level| level.table_infos.len())
                .sum::<usize>(),
            kv_count
        );

        // 4. compact
        let (_tx, rx) = tokio::sync::oneshot::channel();
        let (result_task, task_stats) = compact(
            compact_ctx,
            compact_task.clone(),
            rx,
            Box::new(sstable_object_id_manager.clone()),
            filter_key_extractor_manager,
        )
        .await;

        hummock_manager_ref
            .report_compact_task(
                result_task.task_id,
                result_task.task_status(),
                result_task.sorted_output_ssts,
                Some(to_prost_table_stats_map(task_stats)),
            )
            .await
            .unwrap();

        // 5. get the latest version and check
        let version: HummockVersion = hummock_manager_ref.get_current_version().await;
        let mut tables_from_version = vec![];
        version.level_iter(StaticCompactionGroupId::StateDefault.into(), |level| {
            tables_from_version.extend(level.table_infos.iter().cloned());
            true
        });

        let mut key_count = 0;
        for table in tables_from_version {
            key_count += global_storage
                .sstable_store()
                .sstable(&table, &mut StoreLocalStatistic::default())
                .await
                .unwrap()
                .value()
                .meta
                .key_count;
        }
        assert_eq!((kv_count / 2) as u32, key_count);

        // 6. get compact task and there should be none
        let compact_task = hummock_manager_ref
            .get_compact_task(
                StaticCompactionGroupId::StateDefault.into(),
                &mut default_compaction_selector(),
            )
            .await
            .unwrap();
        assert!(compact_task.is_none());

        epoch += 1;
        // to update version for hummock_storage
        global_storage.wait_version(version).await;

        // 7. scan kv to check key table_id
        let scan_result = global_storage
            .scan(
                (Bound::Unbounded, Bound::Unbounded),
                epoch,
                None,
                ReadOptions {
                    table_id: TableId::from(existing_table_ids),
                    prefetch_options: PrefetchOptions::new_for_exhaust_iter(),
                    cache_policy: CachePolicy::Fill(CachePriority::High),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let mut scan_count = 0;
        for (k, _) in scan_result {
            let table_id = k.user_key.table_id.table_id();
            assert_eq!(table_id, existing_table_ids);
            scan_count += 1;
        }
        assert_eq!(key_count, scan_count);
    }

    #[tokio::test]
    async fn test_compaction_drop_key_by_retention_seconds() {
        let (env, hummock_manager_ref, _cluster_manager_ref, worker_node) =
            setup_compute_env(8080).await;
        let hummock_meta_client: Arc<dyn HummockMetaClient> = Arc::new(MockHummockMetaClient::new(
            hummock_manager_ref.clone(),
            worker_node.id,
        ));

        let existing_table_id = 2;
        let storage = get_hummock_storage(
            hummock_meta_client.clone(),
            get_notification_client_for_test(env, hummock_manager_ref.clone(), worker_node.clone()),
            &hummock_manager_ref,
            TableId::from(existing_table_id),
        )
        .await;

        let rpc_filter_key_extractor_manager = match storage.filter_key_extractor_manager().clone()
        {
            FilterKeyExtractorManager::RpcFilterKeyExtractorManager(
                rpc_filter_key_extractor_manager,
            ) => rpc_filter_key_extractor_manager,
            FilterKeyExtractorManager::StaticFilterKeyExtractorManager(_) => unreachable!(),
        };

        let compact_ctx = get_compactor_context(&storage);
        let sstable_object_id_manager = Arc::new(SstableObjectIdManager::new(
            hummock_meta_client.clone(),
            storage
                .storage_opts()
                .clone()
                .sstable_id_remote_fetch_number,
        ));
        rpc_filter_key_extractor_manager.update(
            2,
            Arc::new(FilterKeyExtractorImpl::FullKey(FullKeyFilterKeyExtractor)),
        );
        let filter_key_extractor_manager = FilterKeyExtractorManager::RpcFilterKeyExtractorManager(
            rpc_filter_key_extractor_manager,
        );
        // 1. add sstables
        let val = Bytes::from(b"0"[..].to_vec()); // 1 Byte value

        let kv_count = 11;
        // let base_epoch = Epoch(0);
        let base_epoch = Epoch::now();
        let mut epoch: u64 = base_epoch.0;
        let millisec_interval_epoch: u64 = (1 << 16) * 100;
        let mut epoch_set = BTreeSet::new();

        let mut local = storage
            .new_local(NewLocalOptions::for_test(existing_table_id.into()))
            .await;
        for i in 0..kv_count {
            epoch += millisec_interval_epoch;
            let next_epoch = epoch + millisec_interval_epoch;
            if i == 0 {
                local.init_for_test(epoch).await.unwrap();
            }
            epoch_set.insert(epoch);
            let mut prefix = BytesMut::default();
            let random_key = rand::thread_rng().gen::<[u8; 32]>();
            prefix.put_u16(1);
            prefix.put_slice(random_key.as_slice());

            local
                .insert(TableKey(prefix.freeze()), val.clone(), None)
                .unwrap();
            local.flush(Vec::new()).await.unwrap();
            local.seal_current_epoch(next_epoch);

            let ssts = storage
                .seal_and_sync_epoch(epoch)
                .await
                .unwrap()
                .uncommitted_ssts;
            hummock_meta_client.commit_epoch(epoch, ssts).await.unwrap();
        }

        let manual_compcation_option = ManualCompactionOption {
            level: 0,
            ..Default::default()
        };
        // 2. get compact task
        let mut compact_task = hummock_manager_ref
            .manual_get_compact_task(
                StaticCompactionGroupId::StateDefault.into(),
                manual_compcation_option,
            )
            .await
            .unwrap()
            .unwrap();

        let compaction_filter_flag = CompactionFilterFlag::STATE_CLEAN | CompactionFilterFlag::TTL;
        compact_task.compaction_filter_mask = compaction_filter_flag.bits();
        let retention_seconds_expire_second = 1;
        compact_task.table_options = BTreeMap::from_iter([(
            existing_table_id,
            TableOption {
                retention_seconds: retention_seconds_expire_second,
            },
        )]);
        compact_task.current_epoch_time = epoch;

        // assert compact_task
        assert_eq!(
            compact_task
                .input_ssts
                .iter()
                .map(|level| level.table_infos.len())
                .sum::<usize>(),
            kv_count,
        );

        // 3. compact
        let (_tx, rx) = tokio::sync::oneshot::channel();
        let (result_task, task_stats) = compact(
            compact_ctx,
            compact_task.clone(),
            rx,
            Box::new(sstable_object_id_manager.clone()),
            filter_key_extractor_manager,
        )
        .await;

        hummock_manager_ref
            .report_compact_task(
                result_task.task_id,
                result_task.task_status(),
                result_task.sorted_output_ssts,
                Some(to_prost_table_stats_map(task_stats)),
            )
            .await
            .unwrap();

        // 4. get the latest version and check
        let version: HummockVersion = hummock_manager_ref.get_current_version().await;
        let mut tables_from_version = vec![];
        version.level_iter(StaticCompactionGroupId::StateDefault.into(), |level| {
            tables_from_version.extend(level.table_infos.iter().cloned());
            true
        });

        let mut key_count = 0;
        for table in tables_from_version {
            key_count += storage
                .sstable_store()
                .sstable(&table, &mut StoreLocalStatistic::default())
                .await
                .unwrap()
                .value()
                .meta
                .key_count;
        }
        let expect_count = kv_count as u32 - retention_seconds_expire_second + 1;
        assert_eq!(expect_count, key_count); // retention_seconds will clean the key (which epoch < epoch - retention_seconds)

        // 5. get compact task and there should be none
        let compact_task = hummock_manager_ref
            .get_compact_task(
                StaticCompactionGroupId::StateDefault.into(),
                &mut default_compaction_selector(),
            )
            .await
            .unwrap();
        assert!(compact_task.is_none());

        epoch += 1;
        // to update version for hummock_storage
        storage.wait_version(version).await;

        // 6. scan kv to check key table_id
        let scan_result = storage
            .scan(
                (Bound::Unbounded, Bound::Unbounded),
                epoch,
                None,
                ReadOptions {
                    table_id: TableId::from(existing_table_id),
                    prefetch_options: PrefetchOptions::new_for_exhaust_iter(),
                    cache_policy: CachePolicy::Fill(CachePriority::High),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let mut scan_count = 0;
        for (k, _) in scan_result {
            let table_id = k.user_key.table_id.table_id();
            assert_eq!(table_id, existing_table_id);
            scan_count += 1;
        }
        assert_eq!(key_count, scan_count);
    }

    #[tokio::test]
    async fn test_compaction_with_filter_key_extractor() {
        let (env, hummock_manager_ref, _cluster_manager_ref, worker_node) =
            setup_compute_env(8080).await;
        let hummock_meta_client: Arc<dyn HummockMetaClient> = Arc::new(MockHummockMetaClient::new(
            hummock_manager_ref.clone(),
            worker_node.id,
        ));

        let existing_table_id = 2;
        let mut key = BytesMut::default();
        key.put_u16(1);
        key.put_slice(b"key_prefix");
        let key_prefix = key.freeze();
        let storage = get_hummock_storage(
            hummock_meta_client.clone(),
            get_notification_client_for_test(env, hummock_manager_ref.clone(), worker_node.clone()),
            &hummock_manager_ref,
            TableId::from(existing_table_id),
        )
        .await;

        let rpc_filter_key_extractor_manager = match storage.filter_key_extractor_manager().clone()
        {
            FilterKeyExtractorManager::RpcFilterKeyExtractorManager(
                rpc_filter_key_extractor_manager,
            ) => rpc_filter_key_extractor_manager,
            FilterKeyExtractorManager::StaticFilterKeyExtractorManager(_) => unreachable!(),
        };

        rpc_filter_key_extractor_manager.update(
            existing_table_id,
            Arc::new(FilterKeyExtractorImpl::FixedLength(
                FixedLengthFilterKeyExtractor::new(TABLE_PREFIX_LEN + key_prefix.len()),
            )),
        );
        let filter_key_extractor_manager = FilterKeyExtractorManager::RpcFilterKeyExtractorManager(
            rpc_filter_key_extractor_manager,
        );
        let compact_ctx = get_compactor_context(&storage);
        let sstable_object_id_manager = Arc::new(SstableObjectIdManager::new(
            hummock_meta_client.clone(),
            storage
                .storage_opts()
                .clone()
                .sstable_id_remote_fetch_number,
        ));
        // 1. add sstables
        let val = Bytes::from(b"0"[..].to_vec()); // 1 Byte value
        let kv_count = 11;
        // let base_epoch = Epoch(0);
        let base_epoch = Epoch::now();
        let mut epoch: u64 = base_epoch.0;
        let millisec_interval_epoch: u64 = (1 << 16) * 100;
        let mut epoch_set = BTreeSet::new();

        let mut local = storage
            .new_local(NewLocalOptions::for_test(existing_table_id.into()))
            .await;
        for i in 0..kv_count {
            epoch += millisec_interval_epoch;
            if i == 0 {
                local.init_for_test(epoch).await.unwrap();
            }
            let next_epoch = epoch + millisec_interval_epoch;
            epoch_set.insert(epoch);

            let ramdom_key = [key_prefix.as_ref(), &rand::thread_rng().gen::<[u8; 32]>()].concat();
            local
                .insert(TableKey(Bytes::from(ramdom_key)), val.clone(), None)
                .unwrap();
            local.flush(Vec::new()).await.unwrap();
            local.seal_current_epoch(next_epoch);
            let ssts = storage
                .seal_and_sync_epoch(epoch)
                .await
                .unwrap()
                .uncommitted_ssts;
            hummock_meta_client.commit_epoch(epoch, ssts).await.unwrap();
        }

        let manual_compcation_option = ManualCompactionOption {
            level: 0,
            ..Default::default()
        };
        // 2. get compact task
        let mut compact_task = hummock_manager_ref
            .manual_get_compact_task(
                StaticCompactionGroupId::StateDefault.into(),
                manual_compcation_option,
            )
            .await
            .unwrap()
            .unwrap();

        assert_eq!(
            compact_task
                .input_ssts
                .iter()
                .map(|level| level.table_infos.len())
                .sum::<usize>(),
            kv_count,
        );

        let compaction_filter_flag = CompactionFilterFlag::STATE_CLEAN | CompactionFilterFlag::TTL;
        compact_task.compaction_filter_mask = compaction_filter_flag.bits();
        compact_task.current_epoch_time = epoch;

        // 3. compact
        let (_tx, rx) = tokio::sync::oneshot::channel();
        let (result_task, task_stats) = compact(
            compact_ctx,
            compact_task.clone(),
            rx,
            Box::new(sstable_object_id_manager.clone()),
            filter_key_extractor_manager,
        )
        .await;

        hummock_manager_ref
            .report_compact_task(
                result_task.task_id,
                result_task.task_status(),
                result_task.sorted_output_ssts,
                Some(to_prost_table_stats_map(task_stats)),
            )
            .await
            .unwrap();

        // 4. get the latest version and check
        let version: HummockVersion = hummock_manager_ref.get_current_version().await;
        let tables_from_version: Vec<_> = version
            .get_compaction_group_levels(StaticCompactionGroupId::StateDefault.into())
            .levels
            .iter()
            .flat_map(|level| level.table_infos.iter())
            .collect::<Vec<_>>();

        let mut key_count = 0;
        for table in tables_from_version {
            key_count += storage
                .sstable_store()
                .sstable(table, &mut StoreLocalStatistic::default())
                .await
                .unwrap()
                .value()
                .meta
                .key_count;
        }
        let expect_count = kv_count as u32;
        assert_eq!(expect_count, key_count); // ttl will clean the key (which epoch < epoch - ttl)

        // 5. get compact task and there should be none
        let compact_task = hummock_manager_ref
            .get_compact_task(
                StaticCompactionGroupId::StateDefault.into(),
                &mut default_compaction_selector(),
            )
            .await
            .unwrap();
        assert!(compact_task.is_none());

        epoch += 1;
        // to update version for hummock_storage
        storage.wait_version(version).await;

        // 6. scan kv to check key table_id
        let bloom_filter_key = [
            existing_table_id.to_be_bytes().to_vec(),
            key_prefix.to_vec(),
        ]
        .concat();
        let start_bound_key = TableKey(key_prefix);
        let end_bound_key = TableKey(Bytes::from(next_key(start_bound_key.as_ref())));
        let scan_result = storage
            .scan(
                (
                    Bound::Included(start_bound_key),
                    Bound::Excluded(end_bound_key),
                ),
                epoch,
                None,
                ReadOptions {
                    prefix_hint: Some(Bytes::from(bloom_filter_key)),
                    table_id: TableId::from(existing_table_id),
                    prefetch_options: PrefetchOptions::new_for_exhaust_iter(),
                    cache_policy: CachePolicy::Fill(CachePriority::High),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        let mut scan_count = 0;
        for (k, _) in scan_result {
            let table_id = k.user_key.table_id.table_id();
            assert_eq!(table_id, existing_table_id);
            scan_count += 1;
        }
        assert_eq!(key_count, scan_count);
    }

    #[tokio::test]
    async fn test_compaction_delete_range() {
        let (env, hummock_manager_ref, _cluster_manager_ref, worker_node) =
            setup_compute_env(8080).await;
        let hummock_meta_client: Arc<dyn HummockMetaClient> = Arc::new(MockHummockMetaClient::new(
            hummock_manager_ref.clone(),
            worker_node.id,
        ));
        let existing_table_id: u32 = 1;
        let storage = get_hummock_storage(
            hummock_meta_client.clone(),
            get_notification_client_for_test(env, hummock_manager_ref.clone(), worker_node.clone()),
            &hummock_manager_ref,
            TableId::from(existing_table_id),
        )
        .await;
        let (compact_ctx, filter_key_extractor_manager) =
            prepare_compactor_and_filter(&storage, existing_table_id);
        let sstable_object_id_manager = Arc::new(SstableObjectIdManager::new(
            hummock_meta_client.clone(),
            storage
                .storage_opts()
                .clone()
                .sstable_id_remote_fetch_number,
        ));
        prepare_data(hummock_meta_client.clone(), &storage, existing_table_id, 2).await;
        let mut local = storage
            .new_local(NewLocalOptions::for_test(existing_table_id.into()))
            .await;
        local.init_for_test(130).await.unwrap();
        let prefix_key_range = |k: u16| {
            let key = k.to_be_bytes();
            (
                Bound::Included(Bytes::copy_from_slice(key.as_slice())),
                Bound::Excluded(Bytes::copy_from_slice(next_key(key.as_slice()).as_slice())),
            )
        };
        local
            .flush(vec![prefix_key_range(1u16), prefix_key_range(2u16)])
            .await
            .unwrap();
        local.seal_current_epoch(u64::MAX);

        flush_and_commit(&hummock_meta_client, &storage, 130).await;

        let manual_compcation_option = ManualCompactionOption {
            level: 0,
            ..Default::default()
        };
        // 2. get compact task
        let mut compact_task = hummock_manager_ref
            .manual_get_compact_task(
                StaticCompactionGroupId::StateDefault.into(),
                manual_compcation_option,
            )
            .await
            .unwrap()
            .unwrap();

        let compaction_filter_flag = CompactionFilterFlag::STATE_CLEAN;
        compact_task.compaction_filter_mask = compaction_filter_flag.bits();
        // assert compact_task
        assert_eq!(
            compact_task
                .input_ssts
                .iter()
                .map(|level| level.table_infos.len())
                .sum::<usize>(),
            129
        );

        // 3. compact
        let (_tx, rx) = tokio::sync::oneshot::channel();
        let (result_task, task_stats) = compact(
            compact_ctx,
            compact_task.clone(),
            rx,
            Box::new(sstable_object_id_manager.clone()),
            filter_key_extractor_manager,
        )
        .await;

        hummock_manager_ref
            .report_compact_task(
                result_task.task_id,
                result_task.task_status(),
                result_task.sorted_output_ssts,
                Some(to_prost_table_stats_map(task_stats)),
            )
            .await
            .unwrap();

        // 4. get the latest version and check
        let version = hummock_manager_ref.get_current_version().await;
        let output_level_info = version
            .get_compaction_group_levels(StaticCompactionGroupId::StateDefault.into())
            .levels
            .last()
            .unwrap();
        assert_eq!(1, output_level_info.table_infos.len());
        assert_eq!(252, output_level_info.table_infos[0].total_key_count);
    }

    type KeyValue = (FullKey<Vec<u8>>, HummockValue<Vec<u8>>);

    async fn test_fast_compact_impl(
        data1: Vec<KeyValue>,
        data2: Vec<KeyValue>,
        data3: Vec<KeyValue>,
        data4: Vec<KeyValue>,
    ) {
        let (env, hummock_manager_ref, _cluster_manager_ref, worker_node) =
            setup_compute_env(8080).await;
        let hummock_meta_client: Arc<dyn HummockMetaClient> = Arc::new(MockHummockMetaClient::new(
            hummock_manager_ref.clone(),
            worker_node.id,
        ));
        let existing_table_id: u32 = 1;
        let storage = get_hummock_storage(
            hummock_meta_client.clone(),
            get_notification_client_for_test(env, hummock_manager_ref.clone(), worker_node.clone()),
            &hummock_manager_ref,
            TableId::from(existing_table_id),
        )
        .await;
        hummock_manager_ref.get_new_sst_ids(10).await.unwrap();
        let (compact_ctx, _) = prepare_compactor_and_filter(&storage, existing_table_id);

        let sstable_store = compact_ctx.sstable_store.clone();
        let capacity = 256 * 1024;
        let mut options = SstableBuilderOptions {
            capacity,
            block_capacity: 2048,
            restart_interval: 16,
            bloom_false_positive: 0.1,
            ..Default::default()
        };
        let sst1 = gen_test_sstable_info(options.clone(), 1, data1, sstable_store.clone()).await;
        let sst2 = gen_test_sstable_info(options.clone(), 2, data2, sstable_store.clone()).await;
        options.compression_algorithm = CompressionAlgorithm::Lz4;
        let sst3 = gen_test_sstable_info(options.clone(), 3, data3, sstable_store.clone()).await;
        let sst4 = gen_test_sstable_info(options, 4, data4, sstable_store.clone()).await;
        let read_options = Arc::new(SstableIteratorReadOptions::default());

        let task = CompactTask {
            input_ssts: vec![
                InputLevel {
                    level_idx: 5,
                    level_type: 1,
                    table_infos: vec![sst1, sst2],
                },
                InputLevel {
                    level_idx: 6,
                    level_type: 1,
                    table_infos: vec![sst3, sst4],
                },
            ],
            existing_table_ids: vec![1],
            task_id: 1,
            watermark: 1000,
            splits: vec![KeyRange::inf()],
            target_level: 6,
            base_level: 4,
            target_file_size: capacity as u64,
            compression_algorithm: 1,
            gc_delete_keys: true,
            ..Default::default()
        };
        let deg = Arc::new(CompactionDeleteRanges::default());
        let multi_filter_key_extractor =
            Arc::new(FilterKeyExtractorImpl::FullKey(FullKeyFilterKeyExtractor));
        let compaction_filter = DummyCompactionFilter {};
        let slow_compact_runner = CompactorRunner::new(
            0,
            compact_ctx.clone(),
            task.clone(),
            Box::new(SharedComapctorObjectIdManager::for_test(
                VecDeque::from_iter([5, 6, 7, 8, 9]),
            )),
        );
        let fast_compact_runner = FastCompactorRunner::new(
            compact_ctx.clone(),
            task.clone(),
            multi_filter_key_extractor.clone(),
            Box::new(SharedComapctorObjectIdManager::for_test(
                VecDeque::from_iter([10, 11, 12, 13, 14]),
            )),
            Arc::new(TaskProgress::default()),
        );
        let (_, ret1, _) = slow_compact_runner
            .run(
                compaction_filter,
                multi_filter_key_extractor,
                deg,
                Arc::new(TaskProgress::default()),
            )
            .await
            .unwrap();
        let ret = ret1.into_iter().map(|sst| sst.sst_info).collect_vec();
        let (ssts, _) = fast_compact_runner.run().await.unwrap();
        let fast_ret = ssts.into_iter().map(|sst| sst.sst_info).collect_vec();
        println!("ssts: {} vs {}", fast_ret.len(), ret.len());
        let mut fast_tables = Vec::with_capacity(fast_ret.len());
        let mut normal_tables = Vec::with_capacity(ret.len());
        let mut stats = StoreLocalStatistic::default();
        for sst_info in &fast_ret {
            fast_tables.push(
                compact_ctx
                    .sstable_store
                    .sstable(sst_info, &mut stats)
                    .await
                    .unwrap(),
            );
        }

        for sst_info in &ret {
            normal_tables.push(
                compact_ctx
                    .sstable_store
                    .sstable(sst_info, &mut stats)
                    .await
                    .unwrap(),
            );
        }
        println!(
            "fast sstables {}.file size={}",
            fast_ret[0].object_id, fast_ret[0].file_size,
        );
        assert!(can_concat(&ret));
        assert!(can_concat(&fast_ret));

        let mut normal_iter = UserIterator::for_test(
            ConcatIterator::new(ret, compact_ctx.sstable_store.clone(), read_options.clone()),
            (Bound::Unbounded, Bound::Unbounded),
        );
        let mut fast_iter = UserIterator::for_test(
            ConcatIterator::new(
                fast_ret,
                compact_ctx.sstable_store.clone(),
                read_options.clone(),
            ),
            (Bound::Unbounded, Bound::Unbounded),
        );

        normal_iter.rewind().await.unwrap();
        fast_iter.rewind().await.unwrap();
        let mut count = 0;
        while normal_iter.is_valid() {
            assert_eq!(
                normal_iter.key(),
                fast_iter.key(),
                "not equal in {}, len: {} {} vs {}",
                count,
                normal_iter.key().user_key.table_key.as_ref().len(),
                u64::from_be_bytes(
                    normal_iter.key().user_key.table_key.as_ref()[0..8]
                        .try_into()
                        .unwrap()
                ),
                u64::from_be_bytes(
                    fast_iter.key().user_key.table_key.as_ref()[0..8]
                        .try_into()
                        .unwrap()
                ),
            );
            let hash = Sstable::hash_for_bloom_filter(
                fast_iter.key().user_key.encode().as_slice(),
                fast_iter.key().user_key.table_id.table_id,
            );
            assert_eq!(normal_iter.value(), fast_iter.value());
            let key_ref = fast_iter.key().user_key.as_ref();
            assert!(normal_tables.iter().any(|table| {
                table
                    .value()
                    .may_match_hash(&(Bound::Included(key_ref), Bound::Included(key_ref)), hash)
            }));
            assert!(fast_tables.iter().any(|table| {
                table
                    .value()
                    .may_match_hash(&(Bound::Included(key_ref), Bound::Included(key_ref)), hash)
            }));
            normal_iter.next().await.unwrap();
            fast_iter.next().await.unwrap();
            count += 1;
        }
    }

    #[tokio::test]
    async fn test_fast_compact() {
        const KEY_COUNT: usize = 20000;
        let mut last_k: u64 = 0;
        let mut rng = rand::rngs::StdRng::seed_from_u64(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        );
        let mut data1 = Vec::with_capacity(KEY_COUNT / 2);
        let mut data = Vec::with_capacity(KEY_COUNT);
        let mut last_epoch = 400;
        for _ in 0..KEY_COUNT {
            let rand_v = rng.next_u32() % 100;
            let (k, epoch) = if rand_v == 0 {
                (last_k + 3000, 400)
            } else if rand_v < 5 {
                (last_k, last_epoch - 1)
            } else {
                (last_k + 1, 400)
            };
            let key = k.to_be_bytes().to_vec();
            let key = FullKey::new(TableId::new(1), TableKey(key), epoch);
            let rand_v = rng.next_u32() % 10;
            let v = if rand_v == 1 {
                HummockValue::delete()
            } else {
                HummockValue::put(format!("sst1-{}", epoch).into_bytes())
            };
            if last_k != k && data1.is_empty() && data.len() >= KEY_COUNT / 2 {
                std::mem::swap(&mut data, &mut data1);
            }
            data.push((key, v));
            last_k = k;
            last_epoch = epoch;
        }
        let data2 = data;
        let mut data3 = Vec::with_capacity(KEY_COUNT);
        let mut data = Vec::with_capacity(KEY_COUNT);
        let mut last_k: u64 = 0;
        let max_epoch = std::cmp::min(300, last_epoch - 1);
        last_epoch = max_epoch;

        for _ in 0..KEY_COUNT * 2 {
            let rand_v = rng.next_u32() % 100;
            let (k, epoch) = if rand_v == 0 {
                (last_k + 1000, max_epoch)
            } else if rand_v < 5 {
                (last_k, last_epoch - 1)
            } else {
                (last_k + 1, max_epoch)
            };
            let key = k.to_be_bytes().to_vec();
            let key = FullKey::new(TableId::new(1), TableKey(key), epoch);
            let v = HummockValue::put(format!("sst2-{}", epoch).into_bytes());
            if last_k != k && data3.is_empty() && data.len() >= KEY_COUNT {
                std::mem::swap(&mut data, &mut data3);
            }
            data.push((key, v));
            last_k = k;
            last_epoch = epoch;
        }
        let data4 = data;
        test_fast_compact_impl(data1, data2, data3, data4).await;
    }
}

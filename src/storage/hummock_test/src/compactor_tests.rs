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

    use std::collections::{BTreeSet, HashMap};
    use std::ops::Bound;
    use std::sync::Arc;

    use bytes::Bytes;
    use itertools::Itertools;
    use rand::Rng;
    use risingwave_common::catalog::TableId;
    use risingwave_common::constants::hummock::CompactionFilterFlag;
    use risingwave_common::util::epoch::Epoch;
    use risingwave_common_service::observer_manager::NotificationClient;
    use risingwave_hummock_sdk::compact::CompactorRuntimeConfig;
    use risingwave_hummock_sdk::compaction_group::hummock_version_ext::HummockVersionExt;
    use risingwave_hummock_sdk::compaction_group::StaticCompactionGroupId;
    use risingwave_hummock_sdk::filter_key_extractor::{
        FilterKeyExtractorImpl, FilterKeyExtractorManagerRef, FixedLengthFilterKeyExtractor,
        FullKeyFilterKeyExtractor,
    };
    use risingwave_hummock_sdk::key::{next_key, TABLE_PREFIX_LEN};
    use risingwave_meta::hummock::compaction::{default_level_selector, ManualCompactionOption};
    use risingwave_meta::hummock::test_utils::{
        register_table_ids_to_compaction_group, setup_compute_env,
        unregister_table_ids_from_compaction_group,
    };
    use risingwave_meta::hummock::{HummockManagerRef, MockHummockMetaClient};
    use risingwave_meta::storage::MetaStore;
    use risingwave_pb::hummock::{HummockVersion, TableOption};
    use risingwave_rpc_client::HummockMetaClient;
    use risingwave_storage::hummock::compactor::{CompactionExecutor, Compactor, CompactorContext};
    use risingwave_storage::hummock::iterator::test_utils::mock_sstable_store;
    use risingwave_storage::hummock::sstable_store::SstableStoreRef;
    use risingwave_storage::hummock::{
        HummockStorage as GlobalHummockStorage, HummockStorage, MemoryLimiter, SstableIdManager,
    };
    use risingwave_storage::monitor::{CompactorMetrics, StoreLocalStatistic};
    use risingwave_storage::opts::StorageOpts;
    use risingwave_storage::storage_value::StorageValue;
    use risingwave_storage::store::*;

    use crate::get_notification_client_for_test;
    use crate::test_utils::{register_tables_with_id_for_test, TestIngestBatch};

    pub(crate) async fn get_hummock_storage<S: MetaStore>(
        hummock_meta_client: Arc<dyn HummockMetaClient>,
        notification_client: impl NotificationClient,
        hummock_manager_ref: &HummockManagerRef<S>,
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
        local.init(epochs[0]);
        for (i, &epoch) in epochs.iter().enumerate() {
            let mut new_val = val.clone();
            new_val.extend_from_slice(&epoch.to_be_bytes());
            local
                .ingest_batch(
                    vec![(key.clone(), StorageValue::new_put(Bytes::from(new_val)))],
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

    fn get_compactor_context_with_filter_key_extractor_manager(
        storage: &HummockStorage,
        hummock_meta_client: &Arc<dyn HummockMetaClient>,
        filter_key_extractor_manager: FilterKeyExtractorManagerRef,
    ) -> CompactorContext {
        get_compactor_context_with_filter_key_extractor_manager_impl(
            storage.storage_opts().clone(),
            storage.sstable_store(),
            hummock_meta_client,
            filter_key_extractor_manager,
        )
    }

    fn get_compactor_context_with_filter_key_extractor_manager_impl(
        options: Arc<StorageOpts>,
        sstable_store: SstableStoreRef,
        hummock_meta_client: &Arc<dyn HummockMetaClient>,
        filter_key_extractor_manager: FilterKeyExtractorManagerRef,
    ) -> CompactorContext {
        CompactorContext {
            storage_opts: options.clone(),
            sstable_store,
            hummock_meta_client: hummock_meta_client.clone(),
            compactor_metrics: Arc::new(CompactorMetrics::unused()),
            is_share_buffer_compact: false,
            compaction_executor: Arc::new(CompactionExecutor::new(Some(1))),
            read_memory_limiter: MemoryLimiter::unlimit(),
            filter_key_extractor_manager,
            sstable_id_manager: Arc::new(SstableIdManager::new(
                hummock_meta_client.clone(),
                options.sstable_id_remote_fetch_number,
            )),
            task_progress_manager: Default::default(),
            compactor_runtime_config: Arc::new(tokio::sync::Mutex::new(
                CompactorRuntimeConfig::default(),
            )),
        }
    }

    #[tokio::test]
    async fn test_compaction_watermark() {
        let (env, hummock_manager_ref, _cluster_manager_ref, worker_node) =
            setup_compute_env(8080).await;
        let hummock_meta_client: Arc<dyn HummockMetaClient> = Arc::new(MockHummockMetaClient::new(
            hummock_manager_ref.clone(),
            worker_node.id,
        ));

        // 1. add sstables
        let key = Bytes::from(0u64.to_be_bytes().to_vec());

        let storage = get_hummock_storage(
            hummock_meta_client.clone(),
            get_notification_client_for_test(env, hummock_manager_ref.clone(), worker_node.clone()),
            &hummock_manager_ref,
            Default::default(),
        )
        .await;
        let compact_ctx = get_compactor_context_with_filter_key_extractor_manager(
            &storage,
            &hummock_meta_client,
            storage.filter_key_extractor_manager().clone(),
        );

        prepare_test_put_data(
            &storage,
            &hummock_meta_client,
            &key,
            1 << 10,
            (1..129).map(|v| (v * 1000) << 16).collect_vec(),
        )
        .await;

        // 2. get compact task
        let mut compact_task = hummock_manager_ref
            .get_compact_task(
                StaticCompactionGroupId::StateDefault.into(),
                &mut default_level_selector(),
            )
            .await
            .unwrap()
            .unwrap();
        let compaction_filter_flag = CompactionFilterFlag::TTL;
        compact_task.watermark = (32 * 1000) << 16;
        compact_task.compaction_filter_mask = compaction_filter_flag.bits();
        compact_task.table_options = HashMap::from([(
            0,
            TableOption {
                retention_seconds: 64,
            },
        )]);
        compact_task.current_epoch_time = 0;
        let mut val = b"0"[..].repeat(1 << 10);
        val.extend_from_slice(&compact_task.watermark.to_be_bytes());

        let compactor_manager = hummock_manager_ref.compactor_manager_ref_for_test();
        compactor_manager.add_compactor(worker_node.id, u64::MAX);
        let compactor = hummock_manager_ref.get_idle_compactor().await.unwrap();
        hummock_manager_ref
            .assign_compaction_task(&compact_task, compactor.context_id())
            .await
            .unwrap();
        assert_eq!(compactor.context_id(), worker_node.id);

        // assert compact_task
        assert_eq!(compact_task.input_ssts.len(), 128);

        // 3. compact
        let (_tx, rx) = tokio::sync::oneshot::channel();
        Compactor::compact(Arc::new(compact_ctx), compact_task.clone(), rx).await;

        // 4. get the latest version and check
        let version = hummock_manager_ref.get_current_version().await;
        let output_table = version
            .get_compaction_group_levels(StaticCompactionGroupId::StateDefault.into())
            .l0
            .as_ref()
            .unwrap()
            .sub_levels
            .last()
            .unwrap()
            .table_infos
            .first()
            .unwrap()
            .clone();
        storage.wait_version(version).await;
        let table = storage
            .sstable_store()
            .sstable(&output_table, &mut StoreLocalStatistic::default())
            .await
            .unwrap();

        // we have removed these 31 keys before watermark 32.
        assert_eq!(table.value().meta.key_count, 97);

        let get_val = storage
            .get(
                key.clone(),
                (32 * 1000) << 16,
                ReadOptions {
                    ignore_range_tombstone: false,

                    prefix_hint: None,
                    table_id: Default::default(),
                    retention_seconds: None,
                    read_version_from_backup: false,
                },
            )
            .await
            .unwrap()
            .unwrap()
            .to_vec();

        assert_eq!(get_val, val);
        let ret = storage
            .get(
                key.clone(),
                (31 * 1000) << 16,
                ReadOptions {
                    ignore_range_tombstone: false,
                    prefix_hint: Some(key.clone()),
                    table_id: Default::default(),
                    retention_seconds: None,
                    read_version_from_backup: false,
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
        let compact_ctx = get_compactor_context_with_filter_key_extractor_manager(
            &storage,
            &hummock_meta_client,
            storage.filter_key_extractor_manager().clone(),
        );

        // 1. add sstables with 1MB value
        let key = Bytes::from(&b"same_key"[..]);
        let mut val = b"0"[..].repeat(1 << 20);
        val.extend_from_slice(&128u64.to_be_bytes());
        prepare_test_put_data(
            &storage,
            &hummock_meta_client,
            &key,
            1 << 20,
            (1..129).collect_vec(),
        )
        .await;

        // 2. get compact task
        let mut compact_task = hummock_manager_ref
            .get_compact_task(
                StaticCompactionGroupId::StateDefault.into(),
                &mut default_level_selector(),
            )
            .await
            .unwrap()
            .unwrap();
        let compaction_filter_flag = CompactionFilterFlag::NONE;
        compact_task.compaction_filter_mask = compaction_filter_flag.bits();
        compact_task.current_epoch_time = 0;

        let compactor_manager = hummock_manager_ref.compactor_manager_ref_for_test();
        compactor_manager.add_compactor(worker_node.id, u64::MAX);
        let compactor = hummock_manager_ref.get_idle_compactor().await.unwrap();
        hummock_manager_ref
            .assign_compaction_task(&compact_task, compactor.context_id())
            .await
            .unwrap();
        assert_eq!(compactor.context_id(), worker_node.id);

        // assert compact_task
        assert_eq!(
            compact_task
                .input_ssts
                .iter()
                .map(|level| level.table_infos.len())
                .sum::<usize>(),
            128
        );
        compact_task.target_level = 6;

        // 3. compact
        let (_tx, rx) = tokio::sync::oneshot::channel();
        Compactor::compact(Arc::new(compact_ctx), compact_task.clone(), rx).await;

        // 4. get the latest version and check
        let version = hummock_manager_ref.get_current_version().await;
        let output_table = version
            .get_compaction_group_levels(StaticCompactionGroupId::StateDefault.into())
            .levels
            .last()
            .unwrap()
            .table_infos
            .first()
            .unwrap();
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

        // 5. storage get back the correct kv after compaction
        storage.wait_version(version).await;
        let get_val = storage
            .get(
                key.clone(),
                129,
                ReadOptions {
                    ignore_range_tombstone: false,

                    prefix_hint: None,
                    table_id: Default::default(),
                    retention_seconds: None,
                    read_version_from_backup: false,
                },
            )
            .await
            .unwrap()
            .unwrap()
            .to_vec();
        assert_eq!(get_val, val);

        // 6. get compact task and there should be none
        let compact_task = hummock_manager_ref
            .get_compact_task(
                StaticCompactionGroupId::StateDefault.into(),
                &mut default_level_selector(),
            )
            .await
            .unwrap();

        assert!(compact_task.is_none());
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
        let kv_count: u8 = 128;
        let mut epoch: u64 = 1;

        let mut local = storage
            .new_local(NewLocalOptions::for_test(existing_table_id.into()))
            .await;

        // 1. add sstables
        let val = Bytes::from(b"0"[..].repeat(1 << 10)); // 1024 Byte value
        for idx in 0..kv_count {
            epoch += 1;

            if idx == 0 {
                local.init(epoch);
            }

            for _ in 0..keys_per_epoch {
                let mut key = vec![idx];
                let ramdom_key = rand::thread_rng().gen::<[u8; 32]>();
                key.extend_from_slice(&ramdom_key);
                local.insert(Bytes::from(key), val.clone(), None).unwrap();
            }
            local.flush(Vec::new()).await.unwrap();
            local.seal_current_epoch(epoch + 1);

            flush_and_commit(&hummock_meta_client, storage, epoch).await;
        }
    }

    pub(crate) fn prepare_compactor_and_filter(
        storage: &HummockStorage,
        hummock_meta_client: &Arc<dyn HummockMetaClient>,
        existing_table_id: u32,
    ) -> CompactorContext {
        let filter_key_extractor_manager = storage.filter_key_extractor_manager().clone();
        filter_key_extractor_manager.update(
            existing_table_id,
            Arc::new(FilterKeyExtractorImpl::FullKey(
                FullKeyFilterKeyExtractor::default(),
            )),
        );

        get_compactor_context_with_filter_key_extractor_manager(
            storage,
            hummock_meta_client,
            filter_key_extractor_manager,
        )
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

        let compact_ctx_existing_table_id = prepare_compactor_and_filter(
            &storage_existing_table_id,
            &hummock_meta_client,
            existing_table_id,
        );

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

        // 2. get compact task
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
                .map(|level| level.table_infos.len())
                .sum::<usize>(),
            128
        );

        // 3. compact
        let (_tx, rx) = tokio::sync::oneshot::channel();
        Compactor::compact(
            Arc::new(compact_ctx_existing_table_id),
            compact_task.clone(),
            rx,
        )
        .await;

        // 4. get the latest version and check
        let version = hummock_manager_ref.get_current_version().await;
        let output_level_info = version
            .get_compaction_group_levels(StaticCompactionGroupId::StateDefault.into())
            .levels
            .last()
            .unwrap();
        assert_eq!(0, output_level_info.total_file_size);

        // 5. get compact task and there should be none
        let compact_task = hummock_manager_ref
            .get_compact_task(
                StaticCompactionGroupId::StateDefault.into(),
                &mut default_level_selector(),
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

        let filter_key_extractor_manager = global_storage.filter_key_extractor_manager().clone();
        filter_key_extractor_manager.update(
            1,
            Arc::new(FilterKeyExtractorImpl::FullKey(
                FullKeyFilterKeyExtractor::default(),
            )),
        );

        filter_key_extractor_manager.update(
            2,
            Arc::new(FilterKeyExtractorImpl::FullKey(
                FullKeyFilterKeyExtractor::default(),
            )),
        );

        let compact_ctx = get_compactor_context_with_filter_key_extractor_manager_impl(
            global_storage.storage_opts().clone(),
            global_storage.sstable_store(),
            &hummock_meta_client,
            filter_key_extractor_manager.clone(),
        );

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
                storage_1.init(epoch);
                storage_2.init(epoch);
            }

            let (storage, other) = if index % 2 == 0 {
                (&mut storage_1, &mut storage_2)
            } else {
                (&mut storage_2, &mut storage_1)
            };

            let random_key = rand::thread_rng().gen::<[u8; 32]>();
            storage
                .insert(
                    Bytes::copy_from_slice(random_key.as_slice()),
                    val.clone(),
                    None,
                )
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
        compact_task.existing_table_ids.push(2);
        let compaction_filter_flag = CompactionFilterFlag::STATE_CLEAN | CompactionFilterFlag::TTL;
        compact_task.compaction_filter_mask = compaction_filter_flag.bits();

        // 3. pick compactor and assign
        let compactor_manager = hummock_manager_ref.compactor_manager_ref_for_test();
        compactor_manager.add_compactor(worker_node.id, u64::MAX);
        let compactor = hummock_manager_ref.get_idle_compactor().await.unwrap();
        hummock_manager_ref
            .assign_compaction_task(&compact_task, compactor.context_id())
            .await
            .unwrap();
        assert_eq!(compactor.context_id(), worker_node.id);
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
        Compactor::compact(Arc::new(compact_ctx), compact_task.clone(), rx).await;

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
                &mut default_level_selector(),
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
                    ignore_range_tombstone: false,

                    prefix_hint: None,
                    table_id: TableId::from(existing_table_ids),
                    retention_seconds: None,
                    read_version_from_backup: false,
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
        let filter_key_extractor_manager = storage.filter_key_extractor_manager().clone();
        let compact_ctx = get_compactor_context_with_filter_key_extractor_manager(
            &storage,
            &hummock_meta_client,
            filter_key_extractor_manager.clone(),
        );
        filter_key_extractor_manager.update(
            2,
            Arc::new(FilterKeyExtractorImpl::FullKey(
                FullKeyFilterKeyExtractor::default(),
            )),
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
                local.init(epoch);
            }
            epoch_set.insert(epoch);

            let ramdom_key = rand::thread_rng().gen::<[u8; 32]>();
            local
                .insert(
                    Bytes::copy_from_slice(ramdom_key.as_slice()),
                    val.clone(),
                    None,
                )
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

        compact_task.existing_table_ids.push(existing_table_id);
        let compaction_filter_flag = CompactionFilterFlag::STATE_CLEAN | CompactionFilterFlag::TTL;
        compact_task.compaction_filter_mask = compaction_filter_flag.bits();
        let retention_seconds_expire_second = 1;
        compact_task.table_options = HashMap::from_iter([(
            existing_table_id,
            TableOption {
                retention_seconds: retention_seconds_expire_second,
            },
        )]);
        compact_task.current_epoch_time = epoch;

        let compactor_manager = hummock_manager_ref.compactor_manager_ref_for_test();
        compactor_manager.add_compactor(worker_node.id, u64::MAX);
        let compactor = hummock_manager_ref.get_idle_compactor().await.unwrap();
        hummock_manager_ref
            .assign_compaction_task(&compact_task, compactor.context_id())
            .await
            .unwrap();
        assert_eq!(compactor.context_id(), worker_node.id);

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
        Compactor::compact(Arc::new(compact_ctx), compact_task.clone(), rx).await;

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
        let expect_count = kv_count as u32 - retention_seconds_expire_second;
        assert_eq!(expect_count, key_count); // retention_seconds will clean the key (which epoch < epoch - retention_seconds)

        // 5. get compact task and there should be none
        let compact_task = hummock_manager_ref
            .get_compact_task(
                StaticCompactionGroupId::StateDefault.into(),
                &mut default_level_selector(),
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
                    ignore_range_tombstone: false,

                    prefix_hint: None,
                    table_id: TableId::from(existing_table_id),
                    retention_seconds: None,
                    read_version_from_backup: false,
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
        let key_prefix = "key_prefix".as_bytes();

        let storage = get_hummock_storage(
            hummock_meta_client.clone(),
            get_notification_client_for_test(env, hummock_manager_ref.clone(), worker_node.clone()),
            &hummock_manager_ref,
            TableId::from(existing_table_id),
        )
        .await;

        let filter_key_extractor_manager = storage.filter_key_extractor_manager().clone();
        filter_key_extractor_manager.update(
            existing_table_id,
            Arc::new(FilterKeyExtractorImpl::FixedLength(
                FixedLengthFilterKeyExtractor::new(TABLE_PREFIX_LEN + key_prefix.len()),
            )),
        );

        let compact_ctx = get_compactor_context_with_filter_key_extractor_manager(
            &storage,
            &hummock_meta_client,
            filter_key_extractor_manager.clone(),
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
            if i == 0 {
                local.init(epoch);
            }
            let next_epoch = epoch + millisec_interval_epoch;
            epoch_set.insert(epoch);

            let ramdom_key = [key_prefix, &rand::thread_rng().gen::<[u8; 32]>()].concat();
            local
                .insert(Bytes::from(ramdom_key), val.clone(), None)
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

        compact_task.existing_table_ids.push(existing_table_id);
        let compaction_filter_flag = CompactionFilterFlag::STATE_CLEAN | CompactionFilterFlag::TTL;
        compact_task.compaction_filter_mask = compaction_filter_flag.bits();
        // compact_task.table_options =
        //     HashMap::from_iter([(existing_table_id, TableOption { ttl: 0 })]);
        compact_task.current_epoch_time = epoch;

        let compactor_manager = hummock_manager_ref.compactor_manager_ref_for_test();
        compactor_manager.add_compactor(worker_node.id, u64::MAX);
        let compactor = hummock_manager_ref.get_idle_compactor().await.unwrap();
        hummock_manager_ref
            .assign_compaction_task(&compact_task, compactor.context_id())
            .await
            .unwrap();
        assert_eq!(compactor.context_id(), worker_node.id);

        // 3. compact
        let (_tx, rx) = tokio::sync::oneshot::channel();
        Compactor::compact(Arc::new(compact_ctx), compact_task.clone(), rx).await;

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
                &mut default_level_selector(),
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
        let start_bound_key = Bytes::from(key_prefix.to_vec());
        let end_bound_key = Bytes::from(next_key(start_bound_key.as_ref()));
        let scan_result = storage
            .scan(
                (
                    Bound::Included(start_bound_key),
                    Bound::Excluded(end_bound_key),
                ),
                epoch,
                None,
                ReadOptions {
                    ignore_range_tombstone: false,
                    prefix_hint: Some(Bytes::from(bloom_filter_key)),
                    table_id: TableId::from(existing_table_id),
                    retention_seconds: None,
                    read_version_from_backup: false,
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
        let compact_ctx =
            prepare_compactor_and_filter(&storage, &hummock_meta_client, existing_table_id);

        prepare_data(hummock_meta_client.clone(), &storage, existing_table_id, 2).await;
        let mut local = storage
            .new_local(NewLocalOptions::for_test(existing_table_id.into()))
            .await;
        local.init(130);
        let prefix_key_range = |key: [u8; 1]| {
            (
                Bytes::copy_from_slice(key.as_slice()),
                Bytes::copy_from_slice(next_key(key.as_slice()).as_slice()),
            )
        };
        local
            .flush(vec![prefix_key_range([1u8]), prefix_key_range([2u8])])
            .await
            .unwrap();
        local.seal_current_epoch(u64::MAX);

        flush_and_commit(&hummock_meta_client, &storage, 130).await;
        let compactor_manager = hummock_manager_ref.compactor_manager_ref_for_test();
        compactor_manager.add_compactor(worker_node.id, u64::MAX);

        // 2. get compact task
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
        let compactor = hummock_manager_ref.get_idle_compactor().await.unwrap();
        hummock_manager_ref
            .assign_compaction_task(&compact_task, compactor.context_id())
            .await
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
        Compactor::compact(Arc::new(compact_ctx), compact_task.clone(), rx).await;

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
}

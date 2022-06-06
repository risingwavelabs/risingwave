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

#[cfg(test)]
mod tests {

    use std::sync::Arc;

    use bytes::Bytes;
    use risingwave_common::config::StorageConfig;
    use risingwave_meta::hummock::test_utils::setup_compute_env;
    use risingwave_meta::hummock::MockHummockMetaClient;
    use risingwave_rpc_client::HummockMetaClient;

    use crate::hummock::compactor::{get_remote_sstable_id_generator, Compactor, CompactorContext};
    use crate::hummock::iterator::test_utils::mock_sstable_store;
    use crate::hummock::HummockStorage;
    use crate::monitor::{StateStoreMetrics, StoreLocalStatistic};
    use crate::storage_value::StorageValue;
    use crate::StateStore;

    // with compression disabled.
    async fn get_hummock_storage(
        hummock_meta_client: Arc<dyn HummockMetaClient>,
    ) -> HummockStorage {
        let remote_dir = "hummock_001_test".to_string();
        let options = Arc::new(StorageConfig {
            sstable_size_mb: 1,
            block_size_kb: 1,
            bloom_false_positive: 0.1,
            data_directory: remote_dir.clone(),
            write_conflict_detection_enabled: true,
            enable_compression: false,
            ..Default::default()
        });
        let sstable_store = mock_sstable_store();
        let storage = HummockStorage::with_default_stats(
            options.clone(),
            sstable_store,
            hummock_meta_client.clone(),
            Arc::new(StateStoreMetrics::unused()),
        )
        .await
        .unwrap();
        storage
    }

    #[tokio::test]
    #[ignore]
    async fn test_compaction_basic() {
        todo!()
    }

    #[tokio::test]
    async fn test_compaction_same_key_not_split() {
        let (_env, hummock_manager_ref, _cluster_manager_ref, worker_node) =
            setup_compute_env(8080).await;
        let hummock_meta_client = Arc::new(MockHummockMetaClient::new(
            hummock_manager_ref.clone(),
            worker_node.id,
        ));
        let storage = get_hummock_storage(hummock_meta_client.clone()).await;
        let compact_ctx = CompactorContext {
            options: storage.options().clone(),
            sstable_store: storage.sstable_store(),
            hummock_meta_client: hummock_meta_client.clone(),
            stats: Arc::new(StateStoreMetrics::unused()),
            is_share_buffer_compact: false,
            sstable_id_generator: get_remote_sstable_id_generator(hummock_meta_client.clone()),
            compaction_executor: None,
        };

        // 1. add sstables
        let key = Bytes::from(&b"same_key"[..]);
        let val = Bytes::from(b"0"[..].repeat(4 << 20)); // 4MB value
        let kv_count = 128;
        let mut epoch: u64 = 1;
        for _ in 0..kv_count {
            epoch += 1;
            storage
                .ingest_batch(
                    vec![(key.clone(), StorageValue::new_default_put(val.clone()))],
                    epoch,
                )
                .await
                .unwrap();
            storage.sync(Some(epoch)).await.unwrap();
            hummock_meta_client
                .commit_epoch(
                    epoch,
                    storage.local_version_manager.get_uncommitted_ssts(epoch),
                )
                .await
                .unwrap();
        }

        // 2. get compact task
        let compact_task = hummock_manager_ref
            .get_compact_task()
            .await
            .unwrap()
            .unwrap();
        hummock_manager_ref
            .assign_compaction_task(&compact_task, worker_node.id, async { true })
            .await
            .unwrap();

        // assert compact_task
        assert_eq!(
            compact_task.input_ssts.first().unwrap().table_infos.len(),
            kv_count
        );

        // 3. compact
        Compactor::compact(Arc::new(compact_ctx), compact_task.clone()).await;

        // 4. get the latest version and check
        let version = hummock_manager_ref.get_current_version().await;
        let output_table_id = version
            .get_levels()
            .last()
            .unwrap()
            .table_infos
            .first()
            .unwrap()
            .id;
        let table = storage
            .sstable_store()
            .sstable(output_table_id, &mut StoreLocalStatistic::default())
            .await
            .unwrap();
        let target_table_size = storage.options().sstable_size_mb * (1 << 20);
        assert!(
            table.value().meta.estimated_size > target_table_size,
            "table.meta.estimated_size {} <= target_table_size {}",
            table.value().meta.estimated_size,
            target_table_size
        );

        // 5. storage get back the correct kv after compaction
        storage
            .local_version_manager()
            .try_update_pinned_version(version);
        let get_val = storage.get(&key, epoch).await.unwrap().unwrap();
        assert_eq!(get_val, val);

        // 6. get compact task and there should be none
        let compact_task = hummock_manager_ref.get_compact_task().await.unwrap();

        assert!(compact_task.is_none());
    }
}

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

use std::iter::once;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use risingwave_common::config::StorageConfig;
use risingwave_pb::common::{HostAddress, WorkerType};
use risingwave_pb::hummock::checksum::Algorithm as ChecksumAlg;
use risingwave_storage::hummock::compactor::{Compactor, SubCompactContext};
use risingwave_storage::hummock::local_version_manager::LocalVersionManager;
use risingwave_storage::hummock::value::HummockValue;
use risingwave_storage::hummock::{HummockStorage, SstableStore};
use risingwave_storage::monitor::StateStoreMetrics;
use risingwave_storage::object::InMemObjectStore;

use crate::cluster::StoredClusterManager;
use crate::hummock::mock_hummock_meta_client::MockHummockMetaClient;
use crate::hummock::HummockManager;
use crate::manager::{MetaSrvEnv, NotificationManager};
use crate::rpc::metrics::MetaMetrics;
use crate::storage::MemStore;

async fn get_hummock_meta_client() -> MockHummockMetaClient {
    let env = MetaSrvEnv::for_test().await;
    let hummock_manager = Arc::new(
        HummockManager::new(env.clone(), Arc::new(MetaMetrics::new()))
            .await
            .unwrap(),
    );
    let notification_manager = Arc::new(NotificationManager::new(env.epoch_generator_ref()));
    let cluster_manager =
        StoredClusterManager::new(env, notification_manager, Duration::from_secs(3600))
            .await
            .unwrap();
    let fake_host_address = HostAddress {
        host: "127.0.0.1".to_string(),
        port: 80,
    };
    let (worker_node, _) = cluster_manager
        .add_worker_node(fake_host_address.clone(), WorkerType::ComputeNode)
        .await
        .unwrap();
    cluster_manager
        .activate_worker_node(fake_host_address)
        .await
        .unwrap();
    MockHummockMetaClient::new(hummock_manager, worker_node.id)
}

async fn get_hummock_storage() -> (HummockStorage, Arc<HummockManager<MemStore>>) {
    let remote_dir = "hummock_001_test".to_string();
    let options = Arc::new(StorageConfig {
        sstable_size: 64,
        block_size: 1 << 10,
        bloom_false_positive: 0.1,
        data_directory: remote_dir.clone(),
        checksum_algo: ChecksumAlg::XxHash64,
        async_checkpoint_enabled: true,
        write_conflict_detection_enabled: true,
    });
    let hummock_meta_client = Arc::new(get_hummock_meta_client().await);
    let obj_client = Arc::new(InMemObjectStore::new());
    let sstable_store = Arc::new(SstableStore::new(
        obj_client.clone(),
        remote_dir,
        Arc::new(StateStoreMetrics::unused()),
    ));
    let local_version_manager = Arc::new(LocalVersionManager::new(sstable_store.clone()));
    let storage = HummockStorage::with_default_stats(
        options.clone(),
        sstable_store,
        local_version_manager.clone(),
        hummock_meta_client.clone(),
        Arc::new(StateStoreMetrics::unused()),
    )
    .await
    .unwrap();
    (storage, hummock_meta_client.hummock_manager_ref())
}

#[tokio::test]
#[ignore]
async fn test_compaction_basic() {
    todo!()
}

#[tokio::test]
#[ignore]
// TODO(soundOfDestiny): re-enable the test case
async fn test_compaction_same_key_not_split() {
    let (storage, hummock_storage_ref) = get_hummock_storage().await;
    let sub_compact_context = SubCompactContext {
        options: storage.options().clone(),
        local_version_manager: storage.local_version_manager().clone(),
        sstable_store: storage.sstable_store(),
        hummock_meta_client: storage.hummock_meta_client().clone(),
        stats: Arc::new(StateStoreMetrics::unused()),
        is_share_buffer_compact: false,
    };

    // 1. add sstables
    let kv_count = 128;
    let epoch: u64 = 1;
    for _ in 0..kv_count {
        storage
            .write_batch(
                once((
                    Bytes::from(&b"same_key"[..]),
                    HummockValue::Put(Bytes::from(&b"value"[..])),
                )),
                epoch,
            )
            .await
            .unwrap();
        storage
            .shared_buffer_manager()
            .sync(Some(epoch))
            .await
            .unwrap();
    }

    // 2. commit epoch
    storage
        .hummock_meta_client()
        .commit_epoch(epoch)
        .await
        .unwrap();

    // 3. get compact task
    let mut compact_task = hummock_storage_ref
        .get_compact_task()
        .await
        .unwrap()
        .unwrap();

    // assert compact_task
    assert_eq!(
        compact_task
            .input_ssts
            .first()
            .unwrap()
            .level
            .as_ref()
            .unwrap()
            .table_ids
            .len(),
        kv_count
    );

    // 4. compact
    Compactor::run_compact(&sub_compact_context, &mut compact_task)
        .await
        .unwrap();

    let output_table_count = compact_task.sorted_output_ssts.len();
    // should not split into multiple tables
    assert_eq!(output_table_count, 1);

    let table = compact_task.sorted_output_ssts.get(0).unwrap();
    let table = storage
        .local_version_manager()
        .pick_few_tables(&[table.id])
        .await
        .unwrap()
        .first()
        .cloned()
        .unwrap();
    // assert that output table reaches the target size
    let target_table_size = storage.options().sstable_size;
    assert!(table.meta.estimated_size > target_table_size);

    // 5. get compact task
    let compact_task = hummock_storage_ref.get_compact_task().await.unwrap();

    assert!(compact_task.is_none());
}

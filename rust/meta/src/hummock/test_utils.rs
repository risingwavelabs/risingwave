use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use itertools::Itertools;
use risingwave_pb::common::{HostAddress, WorkerNode, WorkerType};
use risingwave_pb::hummock::{HummockVersion, KeyRange, SstableInfo, SstableMeta};
use risingwave_storage::hummock::key::key_with_epoch;
use risingwave_storage::hummock::value::HummockValue;
use risingwave_storage::hummock::{
    HummockEpoch, HummockSSTableId, SSTableBuilder, SSTableBuilderOptions,
};
use tokio::sync::mpsc;

use crate::cluster::StoredClusterManager;
use crate::hummock::HummockManager;
use crate::manager::{MetaSrvEnv, NotificationManager};
use crate::rpc::metrics::MetaMetrics;
use crate::storage::MemStore;

pub fn generate_test_tables(
    epoch: u64,
    table_id: &mut u64,
) -> (Vec<SstableInfo>, Vec<(SstableMeta, Bytes)>) {
    // Tables to add
    let opt = SSTableBuilderOptions {
        bloom_false_positive: 0.1,
        block_size: 4096,
        table_capacity: 0,
        checksum_algo: risingwave_pb::hummock::checksum::Algorithm::XxHash64,
    };

    let mut sst_info = vec![];
    let mut sst_data = vec![];
    for i in 0..2 {
        let mut b = SSTableBuilder::new(opt.clone());
        let kv_pairs = vec![
            (i, HummockValue::Put(b"test".as_slice())),
            (i * 10, HummockValue::Put(b"test".as_slice())),
        ];
        for kv in kv_pairs {
            b.add(&iterator_test_key_of_epoch(*table_id, kv.0, epoch), kv.1);
        }
        let (data, meta) = b.finish();
        sst_data.push((meta.clone(), data));
        sst_info.push(SstableInfo {
            id: *table_id,
            key_range: Some(KeyRange {
                left: meta.smallest_key,
                right: meta.largest_key,
                inf: false,
            }),
        });
        (*table_id) += 1;
    }
    (sst_info, sst_data)
}

/// Generate keys like `001_key_test_00002` with timestamp `epoch`.
pub fn iterator_test_key_of_epoch(table: u64, idx: usize, ts: HummockEpoch) -> Vec<u8> {
    // key format: {prefix_index}_version
    key_with_epoch(
        format!("{:03}_key_test_{:05}", table, idx)
            .as_bytes()
            .to_vec(),
        ts,
    )
}

pub fn get_sorted_sstable_ids(sstables: &[SstableInfo]) -> Vec<HummockSSTableId> {
    sstables.iter().map(|table| table.id).sorted().collect_vec()
}

pub fn get_sorted_committed_sstable_ids(hummock_version: &HummockVersion) -> Vec<HummockSSTableId> {
    hummock_version
        .levels
        .iter()
        .flat_map(|level| level.table_ids.clone())
        .sorted()
        .collect_vec()
}

pub async fn setup_compute_env(
    port: i32,
) -> (
    MetaSrvEnv<MemStore>,
    Arc<HummockManager<MemStore>>,
    Arc<StoredClusterManager<MemStore>>,
    WorkerNode,
) {
    let env = MetaSrvEnv::for_test().await;
    let hummock_manager = Arc::new(
        HummockManager::new(env.clone(), Arc::new(MetaMetrics::new()))
            .await
            .unwrap(),
    );
    let (_, delete_worker_receiver) = mpsc::unbounded_channel();
    let cluster_manager = Arc::new(
        StoredClusterManager::new(
            env.clone(),
            Some(hummock_manager.clone()),
            Arc::new(NotificationManager::new(delete_worker_receiver)),
            Duration::from_secs(1),
        )
        .await
        .unwrap(),
    );
    let fake_host_address = HostAddress {
        host: "127.0.0.1".to_string(),
        port,
    };
    let (worker_node, _) = cluster_manager
        .add_worker_node(fake_host_address, WorkerType::ComputeNode)
        .await
        .unwrap();
    (env, hummock_manager, cluster_manager, worker_node)
}

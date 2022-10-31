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
mod replay;

use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use std::sync::Arc;

use clap::Parser;
use replay::HummockInterface;
use risingwave_common::config::StorageConfig;
use risingwave_hummock_test::test_utils::get_replay_notification_client;
use risingwave_hummock_trace::{
    HummockReplay, Operation, Record, Replayable, Result, TraceReader, TraceReaderImpl,
};
use risingwave_meta::hummock::test_utils::setup_compute_env;
use risingwave_meta::hummock::MockHummockMetaClient;
use risingwave_object_store::object::parse_remote_object_store;
use risingwave_storage::hummock::{HummockStorage, SstableStore, TieredCache};
use risingwave_storage::monitor::{ObjectStoreMetrics, StateStoreMetrics};
#[derive(Parser, Debug)]
struct Args {
    #[arg(short, long)]
    path: String,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 16)]
async fn main() {
    let opts = Args::parse();
    let path = Path::new(&opts.path);
    run_replay(path).await.unwrap();
}

async fn run_replay(path: &Path) -> Result<()> {
    let f = BufReader::new(File::open(path)?);
    let mut reader = TraceReaderImpl::new_bincode(f)?;
    let r = reader.read().unwrap();
    let replay_interface = create_replay_hummock(r).await.unwrap();

    let (mut replayer, handle) = HummockReplay::new(reader, replay_interface);

    replayer.run().unwrap();

    handle.await.expect("fail to wait replaying thread");
    Ok(())
}

async fn create_replay_hummock(r: Record) -> Result<Box<dyn Replayable>> {
    let config = StorageConfig {
        sstable_size_mb: 32,
        block_size_kb: 64,
        bloom_false_positive: 0.1,
        share_buffers_sync_parallelism: 2,
        share_buffer_compaction_worker_threads_number: 1,
        shared_buffer_capacity_mb: 64,
        data_directory: "hummock_001".to_string(),
        write_conflict_detection_enabled: true,
        block_cache_capacity_mb: 64,
        meta_cache_capacity_mb: 64,
        disable_remote_compactor: false,
        enable_local_spill: false,
        local_object_store: "minio://hummockadmin:hummockadmin@127.0.0.1:9301/hummock001"
            .to_string(),
        // local_object_store: "memory".to_string(),
        share_buffer_upload_concurrency: 1,
        compactor_memory_limit_mb: 64,
        sstable_id_remote_fetch_number: 1,
        ..Default::default()
    };

    let config = Arc::new(config);
    let state_store_stats = Arc::new(StateStoreMetrics::unused());
    let object_store_stats = Arc::new(ObjectStoreMetrics::unused());
    let object_store =
        parse_remote_object_store(&config.local_object_store, object_store_stats).await;

    let sstable_store = {
        let tiered_cache = TieredCache::none();
        Arc::new(SstableStore::new(
            Arc::new(object_store),
            config.data_directory.to_string(),
            config.block_cache_capacity_mb * (1 << 20),
            config.meta_cache_capacity_mb * (1 << 20),
            tiered_cache,
        ))
    };

    let (hummock_meta_client, notification_client, notifier) = {
        let (env, hummock_manager_ref, _cluster_manager_ref, worker_node) =
            setup_compute_env(8080).await;
        let notifier = env.notification_manager_ref().clone();

        let notification_client = match r.2 {
            Operation::MetaMessage(resp) => {
                get_replay_notification_client(env, worker_node.clone(), resp)
            }
            _ => unreachable!(),
        };

        (
            Arc::new(MockHummockMetaClient::new(
                hummock_manager_ref,
                worker_node.id,
            )),
            notification_client,
            notifier,
        )
    };

    let future = HummockStorage::new(
        config,
        sstable_store,
        hummock_meta_client.clone(),
        notification_client,
        state_store_stats,
    );

    let storage = future
        .await
        .expect("fail to create a HummockStorage object");

    let replay_interface = HummockInterface::new(storage, notifier);

    Ok(Box::new(replay_interface))
}

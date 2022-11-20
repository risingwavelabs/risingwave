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
mod replay_impl;

use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use std::sync::Arc;

use clap::Parser;
use replay_impl::{get_replay_notification_client, HummockInterface, Replay};
use risingwave_common::config::{load_config, StorageConfig};
use risingwave_hummock_trace::{
    HummockReplay, Operation, Record, Replayable, Result, TraceReader, TraceReaderImpl,
};
use risingwave_meta::hummock::test_utils::setup_compute_env;
use risingwave_meta::hummock::MockHummockMetaClient;
use risingwave_object_store::object::parse_remote_object_store;
use risingwave_storage::hummock::{HummockStorage, SstableStore, TieredCache};
use risingwave_storage::monitor::{ObjectStoreMetrics, StateStoreMetrics};
use serde::{Deserialize, Serialize};
#[derive(Parser, Debug)]
struct Args {
    #[arg(short, long)]
    path: String,
    // path to config file
    #[arg(short, long, default_value = "src/config/risingwave.toml")]
    config: String,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let args = Args::parse();
    run_replay(args).await.unwrap();
}

async fn run_replay(args: Args) -> Result<()> {
    let path = Path::new(&args.path);
    let f = BufReader::new(File::open(path)?);
    let mut reader = TraceReaderImpl::new_bincode(f)?;
    // first record is the snapshot
    let r = reader.read().unwrap();
    let replay_interface = create_replay_hummock(r, &args).await.unwrap();
    let mut replayer = HummockReplay::new(reader, replay_interface);
    replayer.run().await.unwrap();

    Ok(())
}

async fn create_replay_hummock(r: Record, args: &Args) -> Result<Box<dyn Replayable>> {
    let config: ReplayConfig = load_config(&args.config).expect("failed to read config file");
    let config = Arc::new(config.storage);

    let state_store_stats = Arc::new(StateStoreMetrics::unused());
    let object_store_stats = Arc::new(ObjectStoreMetrics::unused());
    let object_store =
        parse_remote_object_store(&config.local_object_store, object_store_stats, false).await;

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
        let notifier = env.notification_manager_ref();
        let Record(_, _, op) = r;
        let notification_client = match op {
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

    let storage = HummockStorage::new(
        config,
        sstable_store,
        hummock_meta_client.clone(),
        notification_client,
        state_store_stats,
    )
    .await
    .expect("fail to create a HummockStorage object");
    let replay_interface = HummockInterface::new(storage, notifier);

    Ok(Box::new(Replay::Global(replay_interface)))
}

#[derive(Serialize, Deserialize, Default)]
struct ReplayConfig {
    storage: StorageConfig,
}

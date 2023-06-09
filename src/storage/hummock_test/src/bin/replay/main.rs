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

#![feature(bound_map)]
#![feature(generators)]
#![feature(stmt_expr_attributes)]
#![feature(proc_macro_hygiene)]

#[macro_use]
mod replay_impl;

use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use std::sync::Arc;

use clap::Parser;
use replay_impl::{get_replay_notification_client, GlobalReplayInterface};
use risingwave_common::config::{
    extract_storage_memory_config, load_config, StorageConfig, NO_OVERRIDE,
};
use risingwave_common::system_param::reader::SystemParamsReader;
use risingwave_hummock_trace::{
    GlobalReplay, HummockReplay, Operation, Record, Result, TraceReader, TraceReaderImpl, USE_TRACE,
};
use risingwave_meta::hummock::test_utils::setup_compute_env;
use risingwave_meta::hummock::MockHummockMetaClient;
use risingwave_object_store::object::parse_remote_object_store;
use risingwave_storage::filter_key_extractor::{
    FakeRemoteTableAccessor, FilterKeyExtractorManager,
};
use risingwave_storage::hummock::{HummockStorage, SstableStore, TieredCache};
use risingwave_storage::monitor::{CompactorMetrics, HummockStateStoreMetrics, ObjectStoreMetrics};
use risingwave_storage::opts::StorageOpts;
use serde::{Deserialize, Serialize};

const SST_OFFSET: u64 = 2147383647;

#[derive(Parser, Debug)]
struct Args {
    #[arg(short, long)]
    path: String,

    // path to config file
    #[arg(short, long, default_value = "src/config/risingwave.user.toml")]
    config: String,

    #[arg(short, long)]
    object_storage: String,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let args = Args::parse();
    // disable runtime tracing when replaying
    std::env::set_var(USE_TRACE, "false");
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

async fn create_replay_hummock(r: Record, args: &Args) -> Result<impl GlobalReplay> {
    let config = load_config(&args.config, NO_OVERRIDE);
    let storage_memory_config = extract_storage_memory_config(&config);
    let system = config.system.clone();
    let system_params_reader = SystemParamsReader::from(system.into_init_system_params());

    let storage_opts = Arc::new(StorageOpts::from((
        &config,
        &system_params_reader,
        &storage_memory_config,
    )));

    let state_store_stats = Arc::new(HummockStateStoreMetrics::unused());
    let object_store_stats = Arc::new(ObjectStoreMetrics::unused());

    let compactor_metrics = Arc::new(CompactorMetrics::unused());

    let object_store =
        parse_remote_object_store(&args.object_storage, object_store_stats, "Hummock").await;

    let sstable_store = {
        let tiered_cache = TieredCache::none();
        Arc::new(SstableStore::new(
            Arc::new(object_store),
            storage_opts.data_directory.to_string(),
            storage_opts.block_cache_capacity_mb * (1 << 20),
            storage_opts.meta_cache_capacity_mb * (1 << 20),
            storage_opts.high_priority_ratio,
            tiered_cache,
        ))
    };

    let (hummock_meta_client, notification_client, notifier) = {
        let (env, hummock_manager_ref, _cluster_manager_ref, worker_node) =
            setup_compute_env(8080).await;
        let notifier = env.notification_manager_ref();

        let notification_client = match r.operation {
            Operation::MetaMessage(resp) => {
                get_replay_notification_client(env, worker_node.clone(), resp)
            }
            _ => unreachable!(),
        };

        (
            Arc::new(MockHummockMetaClient::with_sst_offset(
                hummock_manager_ref,
                worker_node.id,
                SST_OFFSET,
            )),
            notification_client,
            notifier,
        )
    };

    let key_filter_manager = Arc::new(FilterKeyExtractorManager::new(Box::new(
        FakeRemoteTableAccessor {},
    )));

    let storage = HummockStorage::new(
        storage_opts,
        sstable_store,
        hummock_meta_client.clone(),
        notification_client,
        key_filter_manager,
        state_store_stats,
        Arc::new(risingwave_tracing::RwTracingService::disabled()),
        compactor_metrics,
    )
    .await
    .expect("fail to create a HummockStorage object");
    let replay_interface = GlobalReplayInterface::new(storage, notifier);

    Ok(replay_interface)
}

#[derive(Serialize, Deserialize, Default)]
struct ReplayConfig {
    storage: StorageConfig,
}

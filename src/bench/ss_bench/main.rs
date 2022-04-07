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

use std::sync::Arc;

mod operations;
mod utils;

use clap::Parser;
use operations::*;
use risingwave_common::config::StorageConfig;
use risingwave_meta::hummock::test_utils::setup_compute_env;
use risingwave_meta::hummock::MockHummockMetaClient;
use risingwave_storage::monitor::StateStoreMetrics;
use risingwave_storage::{dispatch_state_store, StateStoreImpl};

use crate::utils::display_stats::print_statistics;

#[derive(Parser, Debug)]
pub(crate) struct Opts {
    // ----- backend type  -----
    #[clap(long, default_value = "in-memory")]
    store: String,

    // ----- Hummock -----
    #[clap(long, default_value_t = 256)]
    table_size_mb: u32,

    #[clap(long, default_value_t = 64)]
    block_size_kb: u32,

    #[clap(long, default_value_t = 0.1)]
    bloom_false_positive: f64,

    // ----- benchmarks -----
    #[clap(long)]
    benchmarks: String,

    #[clap(long, default_value_t = 1)]
    concurrency_num: u32,

    // ----- operation number -----
    #[clap(long, default_value_t = 1000000)]
    num: i64,

    #[clap(long, default_value_t = -1)]
    deletes: i64,

    #[clap(long, default_value_t = -1)]
    reads: i64,

    #[clap(long, default_value_t = -1)]
    scans: i64,

    #[clap(long, default_value_t = -1)]
    writes: i64,

    // ----- single batch -----
    #[clap(long, default_value_t = 100)]
    batch_size: u32,

    #[clap(long, default_value_t = 16)]
    key_size: u32,

    #[clap(long, default_value_t = 5)]
    key_prefix_size: u32,

    #[clap(long, default_value_t = 10)]
    keys_per_prefix: u32,

    #[clap(long, default_value_t = 100)]
    value_size: u32,

    #[clap(long, default_value_t = 0)]
    seed: u64,

    // ----- flag -----
    #[clap(long)]
    statistics: bool,

    #[clap(long)]
    calibrate_histogram: bool,
}

fn preprocess_options(opts: &mut Opts) {
    if opts.reads < 0 {
        opts.reads = opts.num;
    }
    if opts.scans < 0 {
        opts.scans = opts.num;
    }
    if opts.deletes < 0 {
        opts.deletes = opts.num;
    }
    if opts.writes < 0 {
        opts.writes = opts.num;
    }
}

/// This is used to benchmark the state store performance.
/// For usage, see `README.md`
#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let mut opts = Opts::parse();
    let state_store_stats = Arc::new(StateStoreMetrics::unused());

    println!("Configurations before preprocess:\n {:?}", &opts);
    preprocess_options(&mut opts);
    println!("Configurations after preprocess:\n {:?}", &opts);

    let config = Arc::new(StorageConfig {
        bloom_false_positive: opts.bloom_false_positive,
        sstable_size: opts.table_size_mb * (1 << 20),
        block_size: opts.block_size_kb * (1 << 10),
        share_buffers_sync_parallelism: 2,
        data_directory: "hummock_001".to_string(),
        async_checkpoint_enabled: true,
        write_conflict_detection_enabled: false,
        block_cache_capacity: 256 << 20,
        meta_cache_capacity: 64 << 20,
    });

    let (_env, hummock_manager_ref, _cluster_manager_ref, worker_node) =
        setup_compute_env(8080).await;
    let mock_hummock_meta_client = Arc::new(MockHummockMetaClient::new(
        hummock_manager_ref.clone(),
        worker_node.id,
    ));
    let state_store = StateStoreImpl::new(
        &opts.store,
        config,
        mock_hummock_meta_client.clone(),
        state_store_stats.clone(),
    )
    .await
    .expect("Failed to get state_store");

    dispatch_state_store!(state_store, store, {
        Operations::run(store, mock_hummock_meta_client, &opts).await
    });

    if opts.statistics {
        print_statistics(&state_store_stats);
    }
}

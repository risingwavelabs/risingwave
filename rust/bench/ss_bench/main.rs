use std::sync::Arc;

mod operations;
mod utils;

use clap::Parser;
use operations::*;
use risingwave_common::config::StorageConfig;
use risingwave_pb::common::WorkerType;
use risingwave_rpc_client::MetaClient;
use risingwave_storage::monitor::DEFAULT_STATE_STORE_STATS;
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

    #[clap(long, default_value = "crc32c")]
    checksum_algo: String,

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
/// For usage, see: https://github.com/singularity-data/risingwave-dev/blob/main/docs/developer/benchmark_tool/state_store.md
#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let mut opts = Opts::parse();
    let stats = DEFAULT_STATE_STORE_STATS.clone();

    println!("Configurations before preprocess:\n {:?}", &opts);
    preprocess_options(&mut opts);
    println!("Configurations after preprocess:\n {:?}", &opts);

    let config = Arc::new(StorageConfig {
        bloom_false_positive: opts.bloom_false_positive,
        checksum_algo: opts.checksum_algo.clone(),
        sstable_size: opts.table_size_mb * (1 << 20),
        block_size: opts.block_size_kb * (1 << 10),
        data_directory: "hummock_001".to_string(),
    });

    let meta_address = "http://127.0.0.1:5690";
    let mut hummock_meta_client = MetaClient::new(meta_address).await.unwrap();
    let meta_address = "127.0.0.1:5690".parse().unwrap();
    hummock_meta_client
        .register(meta_address, WorkerType::ComputeNode)
        .await
        .unwrap();

    let state_store =
        match StateStoreImpl::new(&opts.store, config, hummock_meta_client, stats.clone()).await {
            Ok(state_store_impl) => state_store_impl,
            Err(_) => {
                eprintln!("Failed to get state_store");
                return;
            }
        };

    dispatch_state_store!(state_store, store, { Operations::run(store, &opts).await });

    if opts.statistics {
        print_statistics(&stats);
    }
}

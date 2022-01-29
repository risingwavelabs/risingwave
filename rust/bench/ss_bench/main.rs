use std::sync::Arc;

mod benchmarks;
mod utils;

use benchmarks::*;
use clap::Parser;
use risingwave_common::error::{Result, RwError};
use risingwave_pb::hummock::checksum::Algorithm as ChecksumAlg;
use risingwave_storage::hummock::local_version_manager::LocalVersionManager;
use risingwave_storage::hummock::mock::{MockHummockMetaClient, MockHummockMetaService};
use risingwave_storage::hummock::version_manager::VersionManager;
use risingwave_storage::hummock::{HummockOptions, HummockStateStore, HummockStorage};
use risingwave_storage::memory::MemoryStateStore;
use risingwave_storage::object::{ConnectionInfo, S3ObjectStore};
use risingwave_storage::rocksdb_local::RocksDBStateStore;
use risingwave_storage::tikv::TikvStateStore;
use risingwave_storage::StateStore;

#[allow(dead_code)]
enum WorkloadType {
    WriteBatch = 0,
    GetRandom = 1,
    GetSeq = 2,
    PrefixScanRandom = 3,
    DeleteRandom = 4,
    DeleteSeq = 5,
}

#[derive(Parser, Debug)]
pub(crate) struct Opts {
    // ----- backend type  -----
    #[clap(long, default_value = "in-memory")]
    store: String,

    // ----- benchmarks -----
    #[clap(long)]
    benchmarks: String,

    #[clap(long, default_value_t = 1000)]
    iterations: u32,

    #[clap(long, default_value_t = 1)]
    concurrency_num: u32,

    // ----- Hummock -----
    #[clap(long, default_value_t = 64)]
    block_size_kb: u32,

    #[clap(long, default_value_t = 256)]
    table_size_mb: u32,

    #[clap(long, default_value_t = 0.1)]
    bloom_false_positive: f64,

    #[clap(long, default_value = "crc32c")]
    checksum_algo: String,

    // ----- data -----
    #[clap(long, default_value_t = 10)]
    key_size: u32,

    #[clap(long, default_value_t = 5)]
    key_prefix_size: u32,

    #[clap(long, default_value_t = 10)]
    key_prefix_frequency: u32,

    #[clap(long, default_value_t = 10)]
    value_size: u32,

    #[clap(long, default_value_t = 1000)]
    kvs_per_batch: u32,
}

fn get_checksum_algo(algo: &str) -> ChecksumAlg {
    match algo {
        "crc32c" => ChecksumAlg::Crc32c,
        "xxhash64" => ChecksumAlg::XxHash64,
        other => unimplemented!("checksum algorithm \"{}\" is not supported", other),
    }
}

#[derive(Clone)]
pub(crate) enum StateStoreImpl {
    Hummock(HummockStateStore),
    Memory(MemoryStateStore),
    RocksDB(RocksDBStateStore),
    Tikv(TikvStateStore),
}

async fn get_state_store_impl(opts: &Opts) -> Result<StateStoreImpl> {
    let instance = match opts.store.as_ref() {
        "in-memory" | "in_memory" => StateStoreImpl::Memory(MemoryStateStore::new()),
        tikv if tikv.starts_with("tikv") => StateStoreImpl::Tikv(TikvStateStore::new(vec![tikv
            .strip_prefix("tikv://")
            .unwrap()
            .to_string()])),
        minio if minio.starts_with("hummock+minio://") => {
            let object_client = Arc::new(S3ObjectStore::new_with_minio(
                minio.strip_prefix("hummock+").unwrap(),
            ));
            let remote_dir = "hummock_001";
            StateStoreImpl::Hummock(HummockStateStore::new(
                HummockStorage::new(
                    object_client.clone(),
                    HummockOptions {
                        sstable_size: opts.table_size_mb * (1 << 20),
                        block_size: opts.block_size_kb * (1 << 10),
                        bloom_false_positive: opts.bloom_false_positive,
                        remote_dir: remote_dir.to_string(),
                        checksum_algo: get_checksum_algo(opts.checksum_algo.as_ref()),
                    },
                    Arc::new(VersionManager::new()),
                    Arc::new(LocalVersionManager::new(object_client, remote_dir)),
                    Arc::new(MockHummockMetaClient::new(Arc::new(
                        MockHummockMetaService::new(),
                    ))),
                )
                .await
                .map_err(RwError::from)?,
            ))
        }
        s3 if s3.starts_with("hummock+s3://") => {
            let s3_test_conn_info = ConnectionInfo::new();
            let s3_store = Arc::new(S3ObjectStore::new(
                s3_test_conn_info,
                s3.strip_prefix("hummock+s3://").unwrap().to_string(),
            ));
            let remote_dir = "hummock_001";
            StateStoreImpl::Hummock(HummockStateStore::new(
                HummockStorage::new(
                    s3_store.clone(),
                    HummockOptions {
                        sstable_size: opts.table_size_mb * (1 << 20),
                        block_size: opts.block_size_kb * (1 << 10),
                        bloom_false_positive: opts.bloom_false_positive,
                        remote_dir: remote_dir.to_string(),
                        checksum_algo: get_checksum_algo(opts.checksum_algo.as_ref()),
                    },
                    Arc::new(VersionManager::new()),
                    Arc::new(LocalVersionManager::new(s3_store, remote_dir)),
                    Arc::new(MockHummockMetaClient::new(Arc::new(
                        MockHummockMetaService::new(),
                    ))),
                )
                .await
                .map_err(RwError::from)?,
            ))
        }
        rocksdb if rocksdb.starts_with("rocksdb_local://") => StateStoreImpl::RocksDB(
            RocksDBStateStore::new(rocksdb.strip_prefix("rocksdb_local://").unwrap()),
        ),
        other => unimplemented!("state store \"{}\" is not supported", other),
    };
    Ok(instance)
}

async fn run_benchmarks(store: impl StateStore, opts: &Opts) {
    for benchmark in opts.benchmarks.split(',') {
        match benchmark {
            "writebatch" => write_batch::run(&store, opts).await,
            "getrandom" => get_random::run(&store, opts).await,
            "getseq" => get_seq::run(&store, opts).await,
            "prefixscanrandom" => prefix_scan_random::run(&store, opts).await,
            other => unimplemented!("operation \"{}\" is not supported.", other),
        }
    }
}

/// This is used to benchmark the state store performance.
/// For usage, see: https://github.com/singularity-data/risingwave-dev/blob/main/docs/developer/benchmark_tool/state_store.md
#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let opts = Opts::parse();

    println!("Input configurations: {:?}", &opts);

    let state_store = match get_state_store_impl(&opts).await {
        Ok(state_store_impl) => state_store_impl,
        Err(_) => {
            eprintln!("Failed to get state_store");
            return;
        }
    };

    match state_store {
        StateStoreImpl::Hummock(store) => run_benchmarks(store, &opts).await,
        StateStoreImpl::Memory(store) => run_benchmarks(store, &opts).await,
        StateStoreImpl::RocksDB(store) => run_benchmarks(store, &opts).await,
        StateStoreImpl::Tikv(store) => run_benchmarks(store, &opts).await,
    };
}

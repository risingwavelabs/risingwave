mod workload_generator;
mod writebatch;

use std::sync::Arc;

use clap::Parser;
use risingwave_pb::hummock::checksum::Algorithm as ChecksumAlg;
use risingwave_storage::hummock::{HummockOptions, HummockStateStore, HummockStorage};
use risingwave_storage::memory::MemoryStateStore;
use risingwave_storage::object::{ConnectionInfo, S3ObjectStore};

#[derive(Parser, Debug)]
pub struct Opts {
    // ----- backend type parameters  -----
    #[clap(long, default_value = "in-memory")]
    store: String,

    // ----- operation type parameters  -----
    #[clap(long)]
    op: String,

    // ----- Hummock parameters  -----
    #[clap(long, default_value_t = 64)]
    block_size_kb: u32,

    #[clap(long, default_value_t = 256)]
    table_size_mb: u32,

    #[clap(long, default_value_t = 0.1)]
    bloom_false_positive: f64,

    #[clap(long, default_value = "crc32c")]
    checksum_algo: String,

    // ----- data parameters  -----
    #[clap(long, default_value_t = 10)]
    key_size: u32,

    #[clap(long, default_value_t = 10)]
    value_size: u32,

    #[clap(long, default_value_t = 1000)]
    kvs_per_batch: u32,

    #[clap(long, default_value_t = 10)]
    duration: u32,
}

fn get_checksum_algo(algo: &str) -> ChecksumAlg {
    match algo {
        "crc32c" => ChecksumAlg::Crc32c,
        "xxhash64" => ChecksumAlg::XxHash64,
        other => unimplemented!("checksum algorithm \"{}\" is not supported", other),
    }
}

#[derive(Clone)]
pub enum StateStoreImpl {
    HummockStateStore(HummockStateStore),
    MemoryStateStore(MemoryStateStore),
}

fn get_state_store_impl(opts: &Opts) -> StateStoreImpl {
    match opts.store.as_ref() {
        "in-memory" | "in_memory" => StateStoreImpl::MemoryStateStore(MemoryStateStore::new()),
        minio if minio.starts_with("hummock+minio://") => {
            StateStoreImpl::HummockStateStore(HummockStateStore::new(HummockStorage::new(
                Arc::new(S3ObjectStore::new_with_minio(
                    minio.strip_prefix("hummock+").unwrap(),
                )),
                HummockOptions {
                    table_size: opts.table_size_mb * (1 << 20),
                    block_size: opts.block_size_kb * (1 << 10),
                    bloom_false_positive: opts.bloom_false_positive,
                    remote_dir: "hummock_001".to_string(),
                    checksum_algo: get_checksum_algo(opts.checksum_algo.as_ref()),
                    stats_enabled: false,
                },
            )))
        }
        s3 if s3.starts_with("hummock+s3://") => {
            let s3_test_conn_info = ConnectionInfo::new();
            let s3_store = S3ObjectStore::new(
                s3_test_conn_info,
                s3.strip_prefix("hummock+s3://").unwrap().to_string(),
            );
            StateStoreImpl::HummockStateStore(HummockStateStore::new(HummockStorage::new(
                Arc::new(s3_store),
                HummockOptions {
                    table_size: opts.table_size_mb * (1 << 20),
                    block_size: opts.block_size_kb * (1 << 10),
                    bloom_false_positive: opts.bloom_false_positive,
                    remote_dir: "hummock_001".to_string(),
                    checksum_algo: get_checksum_algo(opts.checksum_algo.as_ref()),
                    stats_enabled: true,
                },
            )))
        }
        other => unimplemented!("state store \"{}\" is not supported", other),
    }
}

/// This is used to bench the state store performance.
///
/// USAGE:
/// ss-bench [OPTIONS] --op <OP>
///
/// OPTIONS:
///         --block-size-kb <BLOCK_SIZE_KB>                  [default: 64]
///         --bloom-false-positive <BLOOM_FALSE_POSITIVE>    [default: 0.1]
///         --checksum-algo <CHECKSUM_ALGO>                  [default: crc32c]
///         --duration <DURATION>                            [default: 10]
////     -h, --help                                           Print help information
///         --key-size <KEY_SIZE>                            [default: 10]
///         --kvs-per-batch <KVS_PER_BATCH>                  [default: 1000]
///         --op <OP>
///         --store <STORE>                                  [default: in-memory]
///         --table-size-mb <TABLE_SIZE_MB>                  [default: 256]
///         --value-size <VALUE_SIZE>                        [default: 10]
#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let opts = Opts::parse();

    println!("input options: {:?}", &opts);

    let store = get_state_store_impl(&opts);

    match opts.op.as_ref() {
        "writebatch" => writebatch::run(store, &opts).await,
        other => unimplemented!("operation \"{}\" is not supported.", other),
    }
}

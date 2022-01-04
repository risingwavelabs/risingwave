use std::time::Instant;

use risingwave_storage::StateStore;

use crate::workload_generator::gen_workload;
use crate::{Opts, StateStoreImpl};

pub async fn run(store: StateStoreImpl, opts: &Opts) {
    let batch = gen_workload(opts);

    let start = Instant::now();
    match store {
        StateStoreImpl::HummockStateStore(hummock_store) => {
            hummock_store.ingest_batch(batch, 0).await.unwrap();
        }
        StateStoreImpl::MemoryStateStore(memory_store) => {
            memory_store.ingest_batch(batch, 0).await.unwrap();
        }
    }

    let time_nano = start.elapsed().as_nanos();
    let time = match time_nano {
        nano_sec if time_nano < 1000 => format! {"{} nano sec", nano_sec},
        micro_sec if time_nano < 1_000_000 => format! {"{} micro sec", micro_sec/1000},
        milli_sec if time_nano < 1_000_000_000 => format! {"{} milli sec", milli_sec/1_000_000},
        sec => format! {"{} sec", sec/1_000_000_000},
    };

    println!(
        "Finished {} KV get operations in {}",
        opts.kvs_per_batch, time
    );
}

use std::time::Instant;

use bytes::Bytes;
use futures::future;
use itertools::Itertools;
use rand::distributions::Uniform;
use rand::prelude::{Distribution, StdRng};
use rand::SeedableRng;
use risingwave_storage::StateStore;

use crate::utils::latency_stat::LatencyStat;
use crate::utils::workload::{get_epoch, Workload};
use crate::{Opts, WorkloadType};

pub(crate) async fn run(store: &impl StateStore, opts: &Opts) {
    let workload = Workload::new(opts, WorkloadType::PrefixScanRandom, None);
    store
        .ingest_batch(workload.batch.clone(), get_epoch())
        .await
        .unwrap();

    // generate queried prefixes
    let mut rng = StdRng::seed_from_u64(233);
    let range = Uniform::from(0..workload.prefixes.len());
    let prefix_num = (opts.iterations - opts.iterations % opts.concurrency_num) as usize;
    let mut scan_prefixes = (0..prefix_num)
        .into_iter()
        .map(|_| workload.prefixes[range.sample(&mut rng)].clone())
        .collect_vec();

    // partitioned these prefixes for each concurrency
    let mut grouped_prefixes = vec![vec![]; opts.concurrency_num as usize];
    for (i, prefix) in scan_prefixes.drain(..).enumerate() {
        grouped_prefixes[i % opts.concurrency_num as usize].push(prefix);
    }

    // actual prefix scan process
    let prefix_scan = |prefixes: Vec<Bytes>| async {
        let mut latencies = vec![];
        for prefix in prefixes {
            let start = Instant::now();
            store.scan(&prefix, None).await.unwrap();
            let time_nano = start.elapsed().as_nanos();
            latencies.push(time_nano);
        }
        latencies
    };
    let total_start = Instant::now();
    let futures = grouped_prefixes
        .drain(..)
        .map(|prefixes| prefix_scan(prefixes))
        .collect_vec();
    let latencies_list: Vec<Vec<u128>> = future::join_all(futures).await;
    let total_time_nano = total_start.elapsed().as_nanos();

    // calculate metrics
    let mut latencies = vec![];
    for list in latencies_list {
        for latency in list {
            latencies.push(latency);
        }
    }
    let stat = LatencyStat::new(latencies);
    let qps = prefix_num as u128 * 1_000_000_000 / total_time_nano as u128;

    println!(
        "
    Prefix scan
      {}
      QPS: {}",
        stat, qps
    );
}

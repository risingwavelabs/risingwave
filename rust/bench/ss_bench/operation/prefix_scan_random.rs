use std::time::Instant;

use rand::distributions::Uniform;
use rand::prelude::{Distribution, StdRng};
use rand::SeedableRng;
use risingwave_storage::StateStore;

use crate::utils::latency_stat::LatencyStat;
use crate::utils::workload::{get_epoch, Workload};
use crate::Opts;

pub(crate) async fn run(store: &impl StateStore, opts: &Opts) {
    let workload = Workload::new_sorted_workload(opts, Some(0));
    store
        .ingest_batch(workload.batch.clone(), get_epoch())
        .await
        .unwrap();
    let mut rng = StdRng::seed_from_u64(233);
    let range = Uniform::from(0..workload.prefixes.len());
    let scan_prefixes = (0..opts.iterations)
        .into_iter()
        .map(|_| workload.prefixes[range.sample(&mut rng)].clone());

    // ----- calculate QPS -----
    let start = Instant::now();
    for prefix in scan_prefixes {
        store.scan(&prefix, None).await.unwrap();
    }
    let time_nano = start.elapsed().as_nanos();
    let ops = workload.prefixes.len() as u128 * 1_000_000_000 / time_nano as u128;

    // delete data
    Workload::del_batch(store, workload.batch).await;

    // ----- calculate latencies -----
    // To avoid overheads of frequent time measurements in QPS calculation, we calculte latencies
    // separately.
    // To avoid cache, we re-ingest data.
    let workload = Workload::new_sorted_workload(opts, Some(1));
    store
        .ingest_batch(workload.batch, get_epoch())
        .await
        .unwrap();
    let scan_prefixes = (0..opts.iterations)
        .into_iter()
        .map(|_| workload.prefixes[range.sample(&mut rng)].clone());

    let mut latencies = Vec::with_capacity(workload.prefixes.len());
    for prefix in scan_prefixes {
        let start = Instant::now();

        store.scan(&prefix, None).await.unwrap();

        let time_nano = start.elapsed().as_nanos();
        latencies.push(time_nano);
    }

    let stat = LatencyStat::new(latencies);

    println!(
        "
    Prefix scan
      latency:
        min: {},
        mean: {},
        p50: {},
        p90: {},
        p99: {},
        max: {},
        std_dev: {:.3};
      OPS: {}",
        stat.min, stat.mean, stat.p50, stat.p90, stat.p99, stat.max, stat.std_dev, ops
    );
}

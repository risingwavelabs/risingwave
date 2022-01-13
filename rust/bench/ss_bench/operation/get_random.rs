use std::time::Instant;

use rand::distributions::Uniform;
use rand::prelude::{Distribution, StdRng};
use rand::SeedableRng;
use risingwave_storage::StateStore;

use crate::utils::latency_stat::LatencyStat;
use crate::utils::workload::{get_epoch, Workload};
use crate::Opts;

pub(crate) async fn run(store: &impl StateStore, opts: &Opts) {
    // ----- calculate QPS -----
    let batch = Workload::new_sorted_workload(opts, Some(0)).batch;
    let mut rng = StdRng::seed_from_u64(233);
    let range = Uniform::from(0..opts.kvs_per_batch as usize);

    // generate queried point get key
    let get_keys: Vec<_> = (0..opts.iterations)
        .into_iter()
        .map(|_| batch[range.sample(&mut rng)].0.clone())
        .collect();

    store
        .ingest_batch(batch.clone(), get_epoch())
        .await
        .unwrap();

    let start = Instant::now();
    for key in &get_keys {
        store.get(key).await.unwrap();
    }
    let time_nano = start.elapsed().as_nanos();
    let qps = opts.kvs_per_batch as u128 * 1_000_000_000 / time_nano as u128;

    // delete all the data
    Workload::del_batch(store, batch).await;

    // ----- calculate latencies -----
    // To avoid overheads of frequent time measurements in QPS calculation, we calculte latencies
    // separately.
    // To avoid cache, we re-ingest data.
    let batch = Workload::new_sorted_workload(opts, Some(1)).batch;
    let get_keys: Vec<_> = (0..opts.iterations)
        .into_iter()
        .map(|_| batch[range.sample(&mut rng)].0.clone())
        .collect();

    store.ingest_batch(batch, get_epoch()).await.unwrap();
    let mut latencies = Vec::with_capacity(opts.iterations as usize);
    for key in &get_keys {
        let start = Instant::now();

        store.get(key).await.unwrap();

        let time_nano = start.elapsed().as_nanos();
        latencies.push(time_nano);
    }

    let stat = LatencyStat::new(latencies);

    println!(
    "Point get latency:\n\tmean: {};\n\tp50: {};\n\tp90: {};\n\tp99: {};\n\tstd dev: {:.3}\nQPS: {}",
    stat.mean, stat.p50, stat.p90, stat.p99, stat.std_dev, qps
  );
}

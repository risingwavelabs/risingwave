use std::time::Instant;

use risingwave_storage::StateStore;

use crate::utils::latency_stat::LatencyStat;
use crate::utils::workload::{get_epoch, Workload};
use crate::Opts;

pub(crate) async fn run(store: &impl StateStore, opts: &Opts) {
    let batches: Vec<_> = (0..opts.iterations)
        .into_iter()
        .map(|i| Workload::new_sorted_workload(opts, Some(i as u64)).batch)
        .collect();

    let mut latencies = Vec::with_capacity(opts.iterations as usize);

    // bench for multi iterations
    for batch in batches {
        let batch_clone = batch.clone();

        // count time
        let start = Instant::now();
        store.ingest_batch(batch, get_epoch()).await.unwrap();
        let time_nano = start.elapsed().as_nanos();
        latencies.push(time_nano);

        // clear content
        Workload::del_batch(store, batch_clone).await;
    }

    // calculate operation per second
    let latency_sum = latencies.iter().sum::<u128>();
    let ops = opts.kvs_per_batch as u128 * 1_000_000_000 * opts.iterations as u128 / latency_sum;

    let stat = LatencyStat::new(latencies);

    println!(
    "Batch ingestion latency:\n\tmean: {};\n\tp50: {};\n\tp90: {};\n\tp99: {};\n\tstd dev: {:.3}\nKV ingestion OPS: {}",
    stat.mean, stat.p50, stat.p90, stat.p99, stat.std_dev, ops
  );
}

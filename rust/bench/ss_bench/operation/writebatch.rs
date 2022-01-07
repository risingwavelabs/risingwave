use std::time::Instant;

use bytes::Bytes;
use risingwave_storage::StateStore;

use crate::operation::utils::LatencyStat;
use crate::workload_generator::gen_workload;
use crate::Opts;

pub async fn run(store: impl StateStore, opts: &Opts) {
    let batches: Vec<_> = (0..opts.iterations)
        .into_iter()
        .map(|_| gen_workload(opts))
        .collect();

    let mut latencies = Vec::with_capacity(opts.iterations as usize);

    // bench for multi iterations
    let epoch: u64 = 0;
    for batch in batches {
        let del_batch: Vec<(Bytes, Option<Bytes>)> =
            batch.iter().map(|(k, _)| (k.clone(), None)).collect();

        // count time
        let start = Instant::now();
        store.ingest_batch(batch, epoch).await.unwrap();
        let time_nano = start.elapsed().as_nanos();
        latencies.push(time_nano);

        // clear content
        store.ingest_batch(del_batch, epoch).await.unwrap();
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

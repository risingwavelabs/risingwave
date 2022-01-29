use std::time::Instant;

use bytes::Bytes;
use futures::future;
use itertools::Itertools;
use risingwave_storage::StateStore;

use crate::utils::latency_stat::LatencyStat;
use crate::utils::workload::{get_epoch, Workload};
use crate::{Opts, WorkloadType};

pub(crate) async fn run(store: &impl StateStore, opts: &Opts) {
  // TODO(sun ting): create an opetion for batch number. opts.iterations is ambiguous here.
  let batch_num = (opts.iterations - opts.iterations % opts.concurrency_num) as usize;
  let mut batches = (0..batch_num)
    .into_iter()
    .map(|i| Workload::new(opts, WorkloadType::WriteBatch, Some(i as u64)).batch)
    .collect_vec();

  // partitioned these batches for each concurrency
  let mut grouped_batches = vec![vec![]; opts.concurrency_num as usize];
  for (i, batch) in batches.drain(..).enumerate() {
    grouped_batches[i % opts.concurrency_num as usize].push(batch);
  }

  // actual batch ingestion process
  let ingest_batch = |batches: Vec<Vec<(Bytes, Option<Bytes>)>>| async {
    let mut latencies = vec![];
    for batch in batches {
      let start = Instant::now();
      store.ingest_batch(batch, get_epoch()).await.unwrap();
      let time_nano = start.elapsed().as_nanos();
      latencies.push(time_nano);
    }
    latencies
  };

  let total_start = Instant::now();
  let futures = grouped_batches
    .drain(..)
    .map(|batches| ingest_batch(batches))
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
  // calculate operation per second
  let ops = opts.kvs_per_batch as u128 * 1_000_000_000 * batch_num as u128 / total_time_nano;

  println!(
    "
    Batch ingestion
      {}
      KV ingestion OPS: {}",
    stat, ops
  );
}

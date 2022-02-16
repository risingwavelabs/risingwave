use std::time::Instant;

use bytes::Bytes;
use futures::future;
use itertools::Itertools;
use risingwave_storage::StateStore;

use super::OperationRunner;
use crate::utils::latency_stat::LatencyStat;
use crate::utils::workload::{get_epoch, Workload};
use crate::{Opts, WorkloadType};

impl OperationRunner {
    pub(crate) async fn write_batch(&self, store: &impl StateStore, opts: &Opts) {
        let mut batches = (0..opts.write_batches)
            .into_iter()
            .map(|i| Workload::new(opts, WorkloadType::WriteBatch, Some(i as u64)).batch)
            .collect_vec();
        let batches_len = batches.len();

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
        let ops = opts.batch_size as u128 * 1_000_000_000 * batches_len as u128 / total_time_nano;

        println!(
            "
    writebatch
      {}
      KV ingestion OPS: {}",
            stat, ops
        );
    }
}

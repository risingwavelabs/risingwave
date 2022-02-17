use std::time::Instant;

use async_std::task;
use itertools::Itertools;
use risingwave_storage::StateStore;

use super::Operations;
use crate::utils::latency_stat::LatencyStat;
use crate::utils::workload::{get_epoch, Workload};
use crate::{Opts, WorkloadType};

impl Operations {
    pub(crate) async fn write_batch(&mut self, store: &impl StateStore, opts: &Opts) {
        let mut batches = (0..opts.write_batches)
            .into_iter()
            .map(|i| {
                let (prefixes, keys, values) =
                    Workload::gen(opts, WorkloadType::WriteBatch, Some(i as u64));

                // add new prefixes and keys to global prefixes and keys
                self.merge_prefixes(prefixes);
                self.merge_keys(keys.clone());

                Workload::make_batch(keys, values)
            })
            .collect_vec();
        let batches_len = batches.len();

        // partitioned these batches for each concurrency
        let mut grouped_batches = vec![vec![]; opts.concurrency_num as usize];
        for (i, batch) in batches.drain(..).enumerate() {
            grouped_batches[i % opts.concurrency_num as usize].push(batch);
        }

        let mut args = grouped_batches
            .into_iter()
            .map(|batches| (batches, store.clone()))
            .collect_vec();

        let total_start = Instant::now();

        let futures = args
            .drain(..)
            .map(|(batches, store)| async move {
                let mut latencies: Vec<u128> = vec![];
                for batch in batches {
                    let start = Instant::now();
                    store.ingest_batch(batch, get_epoch()).await.unwrap();
                    let time_nano = start.elapsed().as_nanos();
                    latencies.push(time_nano);
                }
                latencies
            })
            .collect_vec();

        let handles = futures.into_iter().map(task::spawn).collect_vec();

        let latencies_list = futures::future::join_all(handles).await;

        let total_time_nano = total_start.elapsed().as_nanos();

        // calculate metrics
        let latencies = latencies_list.into_iter().flatten().collect();
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

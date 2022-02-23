use std::mem::size_of_val;
use std::time::Instant;

use itertools::Itertools;
use risingwave_storage::StateStore;

use super::{Batch, Operations, PerfMetrics};
use crate::utils::latency_stat::LatencyStat;
use crate::utils::workload::{get_epoch, Workload};
use crate::Opts;

impl Operations {
    pub(crate) async fn write_batch(&mut self, store: &impl StateStore, opts: &Opts) {
        let (prefixes, keys) = Workload::new_random_keys(opts, opts.writes as u64);
        let values = Workload::new_values(opts,  opts.writes as u64);

        // add new prefixes and keys to global prefixes and keys
        self.track_prefixes(prefixes);
        self.track_keys(keys.clone());

        let batches = Workload::make_batches(opts, keys, values);

        let perf = self.run_batches(store, opts, batches).await;

        println!(
            "
    writebatch
      {}
      KV ingestion OPS: {}  {} bytes/sec",
            perf.stat, perf.qps, perf.bytes_pre_sec
        );
    }

    pub(crate) async fn delete_random(&mut self, store: &impl StateStore, opts: &Opts) {
        let (prefixes, keys) = Workload::new_random_keys(opts, opts.deletes as u64);
        let values = vec![None; opts.deletes as usize];

        self.untrack_keys(keys.clone());
        self.untrack_prefixes(prefixes);

        let batches = Workload::make_batches(opts, keys, values);

        let perf = self.run_batches(store, opts, batches).await;

        println!(
            "
    deleterandom
      {}
      KV ingestion OPS: {}  {} bytes/sec",
            perf.stat, perf.qps, perf.bytes_pre_sec
        );
    }

    async fn run_batches(
        &mut self,
        store: &impl StateStore,
        opts: &Opts,
        mut batches: Vec<Batch>,
    ) -> PerfMetrics {
        let batches_len = batches.len();
        let size = size_of_val(&batches);

        // partitioned these batches for each concurrency
        let mut grouped_batches = vec![vec![]; opts.concurrency_num as usize];
        for (i, batch) in batches.drain(..).enumerate() {
            grouped_batches[i % opts.concurrency_num as usize].push(batch);
        }

        let mut args = grouped_batches
            .into_iter()
            .map(|batches| (batches, store.clone()))
            .collect_vec();

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

        let total_start = Instant::now();

        let handles = futures.into_iter().map(tokio::spawn).collect_vec();
        let latencies_list = futures::future::join_all(handles).await;

        let total_time_nano = total_start.elapsed().as_nanos();

        // calculate metrics
        let latencies: Vec<u128> = latencies_list
            .into_iter()
            .flat_map(|res| res.unwrap())
            .collect_vec();
        let stat = LatencyStat::new(latencies);
        // calculate operation per second
        let ops = opts.batch_size as u128 * 1_000_000_000 * batches_len as u128 / total_time_nano;
        let bytes_pre_sec = size as u128 * 1_000_000_000 / total_time_nano;

        PerfMetrics {
            stat,
            qps: ops,
            bytes_pre_sec,
        }
    }
}

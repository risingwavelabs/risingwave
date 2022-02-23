use std::mem::size_of_val;
use std::time::Instant;

use bytes::Buf;
use itertools::Itertools;
use rand::distributions::Uniform;
use rand::prelude::Distribution;
use risingwave_storage::hummock::key::next_key;
use risingwave_storage::StateStore;

use super::Operations;
use crate::utils::latency_stat::LatencyStat;
use crate::utils::workload::Workload;
use crate::Opts;

impl Operations {
    pub(crate) async fn prefix_scan_random(&mut self, store: &impl StateStore, opts: &Opts) {
        // generate queried prefixes
        let mut scan_prefixes = match self.prefixes.is_empty() {
            // if prefixes is empty, use default prefix: ["a"*key_prefix_size]
            true => Workload::new_random_keys(opts, opts.reads as u64, &mut self.rng).0,
            false => {
                let dist = Uniform::from(0..self.prefixes.len());
                (0..opts.reads as usize)
                    .into_iter()
                    .map(|_| self.prefixes[dist.sample(&mut self.rng)].clone())
                    .collect_vec()
            }
        };

        // partitioned these prefixes for each concurrency
        let mut grouped_prefixes = vec![vec![]; opts.concurrency_num as usize];
        for (i, prefix) in scan_prefixes.drain(..).enumerate() {
            grouped_prefixes[i % opts.concurrency_num as usize].push(prefix);
        }

        let mut args = grouped_prefixes
            .into_iter()
            .map(|prefixes| (prefixes, store.clone()))
            .collect_vec();

        let futures = args
            .drain(..)
            .map(|(prefixes, store)| async move {
                let mut latencies = vec![];
                let mut sizes = vec![];
                // actual prefix scan process
                for prefix in prefixes {
                    let start = Instant::now();
                    let kv_pairs = store
                        .scan(
                            prefix.chunk().to_vec()..next_key(prefix.chunk()),
                            None,
                            u64::MAX,
                        )
                        .await
                        .unwrap();
                    let size = size_of_val(&kv_pairs);
                    let time_nano = start.elapsed().as_nanos();
                    latencies.push(time_nano);
                    sizes.push(size);
                }
                (latencies, sizes)
            })
            .collect_vec();

        let total_start = Instant::now();

        let handles = futures.into_iter().map(tokio::spawn).collect_vec();
        let results = futures::future::join_all(handles).await;

        let total_time_nano = total_start.elapsed().as_nanos();

        // calculate metrics
        let mut total_latencies = vec![];
        let mut total_sizes: usize = 0;
        let _ = results
            .into_iter()
            .map(|res| {
                let (latencies, sizes) = res.unwrap();
                total_latencies.extend(latencies);
                total_sizes += sizes.iter().sum::<usize>();
            })
            .collect_vec();
        let stat = LatencyStat::new(total_latencies);
        let qps = opts.reads as u128 * 1_000_000_000 / total_time_nano as u128;
        let bytes_pre_sec = total_sizes as u128 * 1_000_000_000 / total_time_nano as u128;

        println!(
            "
    prefixscanrandom
      {}
      QPS: {}  {} bytes/sec",
            stat, qps, bytes_pre_sec
        );
    }
}

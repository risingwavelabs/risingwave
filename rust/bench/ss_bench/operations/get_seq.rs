use std::time::Instant;

use itertools::Itertools;
use risingwave_storage::StateStore;

use super::Operations;
use crate::utils::latency_stat::LatencyStat;
use crate::utils::workload::Workload;
use crate::Opts;

impl Operations {
    pub(crate) async fn get_seq(&self, store: &impl StateStore, opts: &Opts) {
        // generate queried point get key
        let mut get_keys = match self.keys.is_empty() {
            true => Workload::new_sequential_keys(opts).1,
            false => {
                assert!(
                    opts.reads as usize <= self.keys.len(),
                    "getseq cannot read more data than KV pairs in the state store"
                );

                (0..opts.reads as usize)
                    .into_iter()
                    .map(|i| self.keys[i].clone())
                    .collect_vec()
            }
        };

        // partitioned these keys for each concurrency
        let mut grouped_keys = vec![vec![]; opts.concurrency_num as usize];
        for (i, key) in get_keys.drain(..).enumerate() {
            grouped_keys[i % opts.concurrency_num as usize].push(key);
        }

        let mut args = grouped_keys
            .into_iter()
            .map(|keys| (keys, store.clone()))
            .collect_vec();
        let futures = args
            .drain(..)
            .map(|(keys, store)| async move {
                let mut latencies: Vec<u128> = vec![];
                let mut sizes: Vec<usize> = vec![];
                for key in keys {
                    let start = Instant::now();
                    let size = match store.get(&key, u64::MAX).await.unwrap() {
                        Some(v) => v.len(),
                        None => 0,
                    };
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
        let mut total_size: usize = 0;
        let _ = results
            .into_iter()
            .map(|res| {
                let (latencies, sizes) = res.unwrap();
                total_latencies.extend(latencies.into_iter());
                total_size += sizes.iter().sum::<usize>();
            })
            .collect::<Vec<_>>();
        let stat = LatencyStat::new(total_latencies);
        let qps = opts.reads as u128 * 1_000_000_000 / total_time_nano as u128;
        let bytes_pre_sec = total_size as u128 * 1_000_000_000 / total_time_nano as u128;

        println!(
            "
    getseq
    {}
    QPS: {}  {} bytes/sec",
            stat, qps, bytes_pre_sec
        );
    }
}

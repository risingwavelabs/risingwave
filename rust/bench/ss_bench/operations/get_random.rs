use std::time::Instant;

use itertools::Itertools;
use rand::distributions::Uniform;
use rand::prelude::{Distribution, StdRng};
use rand::SeedableRng;
use risingwave_storage::StateStore;

use super::Operations;
use crate::utils::latency_stat::LatencyStat;
use crate::utils::workload::Workload;
use crate::Opts;

impl Operations {
    pub(crate) async fn get_random(&self, store: &impl StateStore, opts: &Opts) {
        // generate queried point get key
        let mut get_keys = match self.keys.is_empty() {
            true => Workload::new_random_keys(opts, 233).1,
            false => {
                let mut rng = StdRng::seed_from_u64(233);
                let dist = Uniform::from(0..self.keys.len());
                (0..opts.reads)
                    .into_iter()
                    .map(|_| self.keys[dist.sample(&mut rng)].clone())
                    .collect_vec()
            }
        };

        // partitioned these keys for each concurrency
        let mut grouped_keys = vec![vec![]; opts.concurrency_num as usize];
        for (i, key) in get_keys.drain(..).enumerate() {
            grouped_keys[i % opts.concurrency_num as usize].push(key);
        }

        let total_start = Instant::now();

        let handles = grouped_keys
            .drain(..)
            .map(|keys| {
                // actual point get process
                let store = store.clone();
                std::thread::spawn(|| async move {
                    let mut latencies: Vec<u128> = vec![];
                    for key in keys {
                        let start = Instant::now();
                        store.get(&key, u64::MAX).await.unwrap();
                        let time_nano = start.elapsed().as_nanos();
                        latencies.push(time_nano);
                    }
                    latencies
                })
            })
            .collect_vec();

        let mut latencies: Vec<u128> = Vec::new();
        for handle in handles {
            let latency = handle.join().unwrap().await;
            latencies.extend(latency);
        }

        let total_time_nano = total_start.elapsed().as_nanos();

        // calculate metrics
        let stat = LatencyStat::new(latencies);
        let qps = opts.reads as u128 * 1_000_000_000 / total_time_nano as u128;

        println!(
            "
    getrandom
      {}
      QPS: {}",
            stat, qps
        );
    }
}

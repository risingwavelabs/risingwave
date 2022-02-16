use std::time::Instant;

use bytes::Bytes;
use futures::future;
use itertools::Itertools;
use rand::distributions::Uniform;
use rand::prelude::{Distribution, StdRng};
use rand::SeedableRng;
use risingwave_storage::StateStore;

use super::OperationRunner;
use crate::utils::latency_stat::LatencyStat;
use crate::utils::workload::{get_epoch, Workload};
use crate::{Opts, WorkloadType};

impl OperationRunner {
    pub(crate) async fn get_random(&self, store: &impl StateStore, opts: &Opts) {
        let (_prefixes, keys, values) = Workload::new(opts, WorkloadType::GetRandom, None);
        let batch = Workload::make_batch(keys, values);
        store
            .ingest_batch(batch.clone(), get_epoch())
            .await
            .unwrap();

        // generate queried point get key
        let mut rng = StdRng::seed_from_u64(233);
        let dist = Uniform::from(0..opts.batch_size as usize);
        let mut get_keys = (0..opts.reads)
            .into_iter()
            .map(|_| batch[dist.sample(&mut rng)].0.clone())
            .collect_vec();
        let get_keys_len = get_keys.len();

        // partitioned these keys for each concurrency
        let mut grouped_keys = vec![vec![]; opts.concurrency_num as usize];
        for (i, key) in get_keys.drain(..).enumerate() {
            grouped_keys[i % opts.concurrency_num as usize].push(key);
        }

        // actual point get process
        let epoch = u64::MAX;
        let get = |keys: Vec<Bytes>| async {
            let mut latencies = vec![];
            for key in keys {
                let start = Instant::now();
                store.get(&key, epoch).await.unwrap();
                let time_nano = start.elapsed().as_nanos();
                latencies.push(time_nano);
            }
            latencies
        };
        let total_start = Instant::now();
        let futures = grouped_keys.drain(..).map(|keys| get(keys)).collect_vec();
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
        let qps = get_keys_len as u128 * 1_000_000_000 / total_time_nano as u128;

        println!(
            "
    getrandom
      {}
      QPS: {}",
            stat, qps
        );
    }
}

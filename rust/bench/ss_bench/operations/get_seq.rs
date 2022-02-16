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
    pub(crate) async fn get_seq(&self, store: &impl StateStore, opts: &Opts) {
        let (_prefixes, keys, values) = Workload::new(opts, WorkloadType::GetSeq, None);
        let batch = Workload::make_batch(keys, values);
        store
            .ingest_batch(batch.clone(), get_epoch())
            .await
            .unwrap();

        // generate queried point get key
        // FIXME(Sun Ting): use global data to generate queries
        let mut get_keys = (0..opts.reads as usize)
            .into_iter()
            .map(|i| batch[i].0.clone())
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
    getseq
      {}
      QPS: {}",
            stat, qps
        );
    }
}

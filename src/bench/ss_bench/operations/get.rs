// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::time::Instant;

use bytes::Bytes;
use itertools::Itertools;
use rand::distributions::Uniform;
use rand::prelude::Distribution;
use risingwave_storage::store::ReadOptions;
use risingwave_storage::StateStore;

use super::{Operations, PerfMetrics};
use crate::utils::latency_stat::LatencyStat;
use crate::utils::workload::Workload;
use crate::Opts;

impl Operations {
    pub(crate) async fn get_random(&mut self, store: &impl StateStore, opts: &Opts) {
        // generate queried point get key
        let get_keys = match self.keys.is_empty() {
            true => Workload::new_random_keys(opts, opts.reads as u64, &mut self.rng).1,
            false => {
                let dist = Uniform::from(0..self.keys.len());
                (0..opts.reads)
                    .into_iter()
                    .map(|_| self.keys[dist.sample(&mut self.rng)].clone())
                    .collect_vec()
            }
        };

        let perf = Operations::run_get(store, opts, get_keys).await;

        println!(
            "
    getrandom
      {}
      QPS: {}  {} bytes/sec",
            perf.stat, perf.qps, perf.bytes_pre_sec
        );
    }

    pub(crate) async fn get_seq(&self, store: &impl StateStore, opts: &Opts) {
        // generate queried point get key
        let get_keys = match self.keys.is_empty() {
            true => Workload::new_sequential_keys(opts, opts.reads as u64).1,
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

        let perf = Operations::run_get(store, opts, get_keys).await;

        println!(
            "
    getseq
      {}
      QPS: {}  {} bytes/sec",
            perf.stat, perf.qps, perf.bytes_pre_sec
        );
    }

    async fn run_get(
        store: &impl StateStore,
        opts: &Opts,
        mut get_keys: Vec<Bytes>,
    ) -> PerfMetrics {
        // partitioned these keys for each concurrency
        let mut grouped_keys: Vec<Vec<Bytes>> = vec![vec![]; opts.concurrency_num as usize];
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
                    let val_size = match store
                        .get(
                            &key,
                            ReadOptions {
                                epoch: u64::MAX,
                                table_id: None,
                                retention_seconds: None,
                            },
                        )
                        .await
                        .unwrap()
                    {
                        Some(v) => v.len(),
                        None => 0,
                    };
                    // account sizes from both key and value
                    let size = key.len() + val_size;
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
            .collect_vec();
        let stat = LatencyStat::new(total_latencies);
        let qps = opts.reads as u128 * 1_000_000_000 / total_time_nano as u128;
        let bytes_pre_sec = total_size as u128 * 1_000_000_000 / total_time_nano as u128;

        PerfMetrics {
            stat,
            qps,
            bytes_pre_sec,
        }
    }
}

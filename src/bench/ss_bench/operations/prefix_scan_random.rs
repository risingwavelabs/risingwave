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

use bytes::Buf;
use itertools::Itertools;
use rand::distributions::Uniform;
use rand::prelude::Distribution;
use risingwave_hummock_sdk::key::next_key;
use risingwave_storage::store::ReadOptions;
use risingwave_storage::StateStore;

use super::Operations;
use crate::utils::latency_stat::LatencyStat;
use crate::utils::workload::Workload;
use crate::Opts;

impl Operations {
    pub(crate) async fn prefix_scan_random(&mut self, store: &impl StateStore, opts: &Opts) {
        // generate queried prefixes
        let mut scan_prefixes = match self.prefixes.is_empty() {
            true => Workload::new_random_prefixes(opts, opts.scans as u64, &mut self.rng),
            false => {
                let dist = Uniform::from(0..self.prefixes.len());
                (0..opts.scans as usize)
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
                            ReadOptions {
                                epoch: u64::MAX,
                                table_id: None,
                                retention_seconds: None,
                            },
                        )
                        .await
                        .unwrap();
                    let size = kv_pairs
                        .iter()
                        .map(|(k, v)| k.len() + v.len())
                        .sum::<usize>();
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
        let qps = opts.scans as u128 * 1_000_000_000 / total_time_nano as u128;
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

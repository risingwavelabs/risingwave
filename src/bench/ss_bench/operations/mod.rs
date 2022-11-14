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

use std::collections::BTreeSet;
use std::sync::Arc;

use bytes::Bytes;
use rand::prelude::StdRng;
use rand::SeedableRng;
use risingwave_meta::hummock::MockHummockMetaClient;
use risingwave_storage::hummock::compactor::CompactorContext;
use risingwave_storage::hummock::local_version_manager::LocalVersionManager;
use risingwave_storage::StateStore;

use crate::utils::display_stats::*;
use crate::utils::latency_stat::LatencyStat;
use crate::Opts;
pub(crate) mod get;
pub(crate) mod prefix_scan_random;
pub(crate) mod write_batch;

pub(crate) struct Operations {
    pub(crate) keys: Vec<Bytes>,
    pub(crate) prefixes: Vec<Bytes>,
    pub meta_client: Arc<MockHummockMetaClient>,

    // TODO(Sun Ting): exploit specified (no need to support encryption) rng to speed up
    rng: StdRng,
}

type Batch = Vec<(Bytes, Option<Bytes>)>;

pub(crate) struct PerfMetrics {
    stat: LatencyStat,
    qps: u128,
    bytes_pre_sec: u128,
}

impl Operations {
    /// Run operations in the `--benchmarks` option
    pub(crate) async fn run(
        store: impl StateStore,
        meta_service: Arc<MockHummockMetaClient>,
        context: Option<(Arc<CompactorContext>, LocalVersionManagerRef)>,
        opts: &Opts,
    ) {
        let mut stat_display = DisplayStats::default();

        let mut runner = Operations {
            keys: vec![],
            prefixes: vec![],
            rng: StdRng::seed_from_u64(opts.seed),
            meta_client: meta_service,
        };

        for operation in opts.benchmarks.split(',') {
            // (Sun Ting) TODO: remove statistics print for each operation
            // after new performance display is ready
            match operation {
                "writebatch" => runner.write_batch(&store, opts, context.clone()).await,
                "deleterandom" => runner.delete_random(&store, opts).await,
                "getrandom" => runner.get_random(&store, opts).await,
                "getseq" => runner.get_seq(&store, opts).await,
                "prefixscanrandom" => runner.prefix_scan_random(&store, opts).await,
                other => unimplemented!("operation \"{}\" is not supported.", other),
            }

            stat_display.update_stat();

            // display metrics from state store metric system
            if opts.calibrate_histogram {
                match operation {
                    "writebatch" => stat_display.display_write_batch(),
                    "deleterandom" => stat_display.display_delete_random(),
                    // (Sun Ting) TODO: implement other performance displays
                    "getrandom" => {}
                    "getseq" => {}
                    "prefixscanrandom" => {}
                    other => unimplemented!("operation \"{}\" is not supported.", other),
                }
            }
        }
    }

    /// Track new prefixes
    fn track_prefixes(&mut self, mut other: Vec<Bytes>) {
        self.prefixes.append(&mut other);
        self.prefixes.sort();
        self.prefixes.dedup_by(|k1, k2| k1 == k2);
    }

    /// Track new keys
    fn track_keys(&mut self, mut other: Vec<Bytes>) {
        self.keys.append(&mut other);
        self.keys.sort();
        self.keys.dedup_by(|k1, k2| k1 == k2);
    }

    /// Untrack deleted keys
    #[allow(clippy::mutable_key_type)]
    fn untrack_keys(&mut self, other: &[Bytes]) {
        let untrack_set = other.iter().collect::<BTreeSet<_>>();
        self.keys.retain(|k| !untrack_set.contains(k));
    }

    /// Untrack prefixes of deleted keys
    // TODO(Ting Sun): decide whether and how to implement untrack_prefixes
    #[allow(dead_code)]
    fn untrack_prefixes(&mut self, mut _other: &[Bytes]) {}
}

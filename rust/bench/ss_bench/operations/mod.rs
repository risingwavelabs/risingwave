use std::collections::BTreeSet;

use bytes::Bytes;
use prometheus::Registry;
use rand::prelude::StdRng;
use rand::SeedableRng;
use risingwave_storage::monitor::StateStoreStats;
use risingwave_storage::StateStore;

use crate::utils::diff_statistics::*;
use crate::utils::latency_stat::LatencyStat;
use crate::Opts;
pub(crate) mod get;
pub(crate) mod prefix_scan_random;
pub(crate) mod write_batch;

pub(crate) struct Operations {
    pub(crate) keys: Vec<Bytes>,
    pub(crate) prefixes: Vec<Bytes>,

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
    pub(crate) async fn run(store: impl StateStore, opts: &Opts) {
        let reg = Registry::new();
        let stat = StateStoreStats::new(&reg);
        let mut stat_diff = StatDiff {
            prev_stat: stat.clone(),
            cur_stat: stat,
        };

        let mut runner = Operations {
            keys: vec![],
            prefixes: vec![],
            rng: StdRng::seed_from_u64(opts.seed),
        };

        for operation in opts.benchmarks.split(',') {
            // (Sun Ting) TODO: remove statistics print for each operation
            // after new performance display is ready
            match operation {
                "writebatch" => runner.write_batch(&store, opts).await,
                "deleterandom" => runner.delete_random(&store, opts).await,
                "getrandom" => runner.get_random(&store, opts).await,
                "getseq" => runner.get_seq(&store, opts).await,
                "prefixscanrandom" => runner.prefix_scan_random(&store, opts).await,
                other => unimplemented!("operation \"{}\" is not supported.", other),
            }

            stat_diff.update_stat();

            // display metrics
            match operation {
                "writebatch" => stat_diff.display_write_batch(),
                // (Sun Ting) TODO: implement other performance displays
                "deleterandom" => {}
                "getrandom" => {}
                "getseq" => {}
                "prefixscanrandom" => {}
                other => unimplemented!("operation \"{}\" is not supported.", other),
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

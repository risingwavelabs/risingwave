use bytes::Bytes;
use risingwave_storage::StateStore;

use crate::utils::latency_stat::LatencyStat;
use crate::Opts;

pub(crate) mod get;
pub(crate) mod prefix_scan_random;
pub(crate) mod write_batch;

#[derive(Clone, Default)]
pub(crate) struct Operations {
    pub(crate) keys: Vec<Bytes>,
    pub(crate) prefixes: Vec<Bytes>,
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
        let mut runner = Operations::default();

        for operation in opts.benchmarks.split(',') {
            match operation {
                "writebatch" => runner.write_batch(&store, opts).await,
                "deleterandom" => runner.delete_random(&store, opts).await,
                "getrandom" => runner.get_random(&store, opts).await,
                "getseq" => runner.get_seq(&store, opts).await,
                "prefixscanrandom" => runner.prefix_scan_random(&store, opts).await,
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
    fn untrack_keys(&mut self, mut other: Vec<Bytes>) {
        // TODO(Ting Sun): consider use Set as the data structure for keys and prefixes
        for keys in other.drain(..) {
            self.keys.retain(|k| k != &keys);
        }
    }

    /// Untrack prefixes of deleted keys
    // TODO(Ting Sun): decide whether and how to implement untrack_prefixes
    fn untrack_prefixes(&mut self, mut _other: Vec<Bytes>) {}
}

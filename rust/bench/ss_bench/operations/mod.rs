use bytes::Bytes;
use risingwave_storage::StateStore;

use crate::Opts;

pub(crate) mod get_random;
pub(crate) mod get_seq;
pub(crate) mod prefix_scan_random;
pub(crate) mod write_batch;

#[derive(Clone, Default)]
pub(crate) struct OperationRunner {
    pub(crate) keys: Vec<Bytes>,
    pub(crate) prefixes: Vec<Bytes>,
}

impl OperationRunner {
    pub(crate) async fn run_operations(store: impl StateStore, opts: &Opts) {
        let mut runner = OperationRunner::default();

        for operation in opts.benchmarks.split(',') {
            match operation {
                "writebatch" => runner.write_batch(&store, opts).await,
                "getrandom" => runner.get_random(&store, opts).await,
                "getseq" => runner.get_seq(&store, opts).await,
                "prefixscanrandom" => runner.prefix_scan_random(&store, opts).await,
                other => unimplemented!("operation \"{}\" is not supported.", other),
            }
        }
    }

    fn merge_prefixes(&mut self, mut other: Vec<Bytes>) {
        self.prefixes.append(&mut other);
        self.prefixes.sort_by(|k1, k2| k1.cmp(k2));
        self.prefixes.dedup_by(|k1, k2| k1 == k2);
    }

    fn merge_keys(&mut self, mut other: Vec<Bytes>) {
        self.keys.append(&mut other);
        self.keys.sort_by(|k1, k2| k1.cmp(k2));
        self.keys.dedup_by(|k1, k2| k1 == k2);
    }
}

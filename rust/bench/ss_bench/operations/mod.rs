use risingwave_storage::StateStore;

use crate::utils::workload::Workload;
use crate::Opts;

pub(crate) mod get_random;
pub(crate) mod get_seq;
pub(crate) mod prefix_scan_random;
pub(crate) mod write_batch;

pub struct OperationManager {
    pub workload: Workload,
}

impl OperationManager {
    pub(crate) fn new() -> Self {
        OperationManager {
            workload: Workload::default(),
        }
    }

    pub(crate) async fn run_operations(&self, store: impl StateStore, opts: &Opts) {
        for operation in opts.benchmarks.split(',') {
            match operation {
                "writebatch" => self.write_batch(&store, opts).await,
                "getrandom" => self.get_random(&store, opts).await,
                "getseq" => self.get_seq(&store, opts).await,
                "prefixscanrandom" => self.prefix_scan_random(&store, opts).await,
                other => unimplemented!("operation \"{}\" is not supported.", other),
            }
        }
    }
}

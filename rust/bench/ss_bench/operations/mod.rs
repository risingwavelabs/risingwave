use risingwave_storage::StateStore;

use crate::utils::workload::Workload;
use crate::Opts;

pub(crate) mod get_random;
pub(crate) mod get_seq;
pub(crate) mod prefix_scan_random;
pub(crate) mod write_batch;

pub struct OperationRunner {
    pub workload: Workload,
}

impl OperationRunner {
    pub(crate) async fn run_operations(store: impl StateStore, opts: &Opts) {
        let runner = OperationRunner {
            workload: Workload::default(),
        };

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
}

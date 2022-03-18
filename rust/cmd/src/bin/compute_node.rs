#![cfg_attr(coverage, feature(no_coverage))]

use tikv_jemallocator::Jemalloc;

#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

#[cfg_attr(coverage, no_coverage)]
#[cfg(not(feature = "all-in-one"))]
#[tokio::main]
async fn main() {
    use clap::StructOpt;

    let opts = risingwave_compute::ComputeNodeOpts::parse();

    risingwave_logging::oneshot_common();
    risingwave_logging::init_risingwave_logger(opts.enable_jaeger_tracing, false);

    risingwave_compute::start(opts).await
}

#[cfg(feature = "all-in-one")]
fn main() {
    panic!("compute-node binary cannot be used in all-in-one mode")
}

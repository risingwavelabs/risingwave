use clap::StructOpt;
use tikv_jemallocator::Jemalloc;

#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

#[cfg(not(tarpaulin_include))]
#[tokio::main]
async fn main() {
    let opts = risingwave_compute::ComputeNodeOpts::parse();

    risingwave_logging::oneshot_common();
    risingwave_logging::init_risingwave_logger(opts.enable_jaeger_tracing, false);

    risingwave_compute::start(opts).await
}

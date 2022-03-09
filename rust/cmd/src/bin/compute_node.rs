use tikv_jemallocator::Jemalloc;

#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

#[cfg(not(tarpaulin_include))]
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
    panic!("meta-node binary cannot be used in all-in-one mode")
}

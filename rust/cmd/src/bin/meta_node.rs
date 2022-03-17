use tikv_jemallocator::Jemalloc;

#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

#[cfg(not(tarpaulin_include))]
#[cfg(not(feature = "all-in-one"))]
#[tokio::main]
async fn main() {
    use clap::StructOpt;

    let opts = risingwave_meta::MetaNodeOpts::parse();

    risingwave_logging::oneshot_common();
    risingwave_logging::init_risingwave_logger(false, false);

    risingwave_meta::start(opts).await
}

#[cfg(feature = "all-in-one")]
fn main() {
    panic!("meta-node binary cannot be used in all-in-one mode")
}

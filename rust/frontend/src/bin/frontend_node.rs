use clap::StructOpt;
use tikv_jemallocator::Jemalloc;

#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

#[cfg(not(tarpaulin_include))]
#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let opts = risingwave_frontend::FrontendOpts::parse();

    risingwave_logging::oneshot_common();
    risingwave_logging::init_risingwave_logger(false);

    risingwave_frontend::start(opts).await
}

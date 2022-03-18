#![cfg_attr(coverage, feature(no_coverage))]

use tikv_jemallocator::Jemalloc;

#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

#[cfg_attr(coverage, no_coverage)]
#[tokio::main]
async fn main() {
    risingwave_logging::oneshot_common();
    risingwave_logging::init_risingwave_logger(false, true);

    risingwave_ctl::start().await
}

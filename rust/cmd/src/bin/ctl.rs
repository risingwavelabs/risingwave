use tikv_jemallocator::Jemalloc;

#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

#[cfg(not(tarpaulin_include))]
#[tokio::main]
async fn main() {
    risingwave_logging::oneshot_common();
    risingwave_logging::init_risingwave_logger(false, true);

    risingwave_ctl::start().await
}

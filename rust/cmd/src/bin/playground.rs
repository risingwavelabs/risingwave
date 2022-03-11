use tikv_jemallocator::Jemalloc;

#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

#[cfg(not(tarpaulin_include))]
#[cfg(not(feature = "all-in-one"))]
#[tokio::main]
async fn main() {
    use clap::StructOpt;
    use risingwave_compute::ComputeNodeOpts;
    use risingwave_frontend::FrontendOpts;
    use risingwave_meta::MetaNodeOpts;
    use tokio::signal;

    risingwave_logging::oneshot_common();
    risingwave_logging::init_risingwave_logger(false, false);

    let meta_opts = MetaNodeOpts::parse_from(["--backend mem"]);
    let compute_opts = ComputeNodeOpts::parse_from(["--state_store in-memory"]);
    let frontend_opts = FrontendOpts::parse_from([""]);

    let _meta_handle = tokio::spawn(async move { risingwave_meta::start(meta_opts).await });
    let _compute_handle =
        tokio::spawn(async move { risingwave_compute::start(compute_opts).await });
    let _frontend_handle =
        tokio::spawn(async move { risingwave_frontend::start(frontend_opts).await });

    // TODO: should we join all handles?
    // Currently, not all services can be shutdown gracefully, just quit on Ctrl-C now.
    signal::ctrl_c().await.unwrap();
    println!("Exit");
}

#[cfg(feature = "all-in-one")]
fn main() {
    panic!("playground binary cannot be used in all-in-one mode")
}

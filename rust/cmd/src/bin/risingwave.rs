use tikv_jemallocator::Jemalloc;

#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

#[cfg(feature = "all-in-one")]
#[cfg(not(tarpaulin_include))]
#[tokio::main]
async fn main() {
    use std::collections::HashMap;
    use std::env;
    use std::future::Future;
    use std::pin::Pin;

    use clap::StructOpt;

    /// Get the launch target of this all-in-one binary
    fn get_target() -> String {
        env::var("RW_NODE").unwrap_or_else(|_| {
            let x = env::args().nth(0).expect("cannot find argv[0]").to_string();
            let x = x.rsplit('/').nth(0).expect("cannot find binary name");
            x.to_string()
        })
    }

    let target = get_target();

    let mut fns: HashMap<&str, Box<dyn Future<Output = ()>>> = HashMap::new();

    fns.insert(
        "compute",
        Box::new(async move {
            eprintln!("launching compute node");

            let opts = risingwave_compute::ComputeNodeOpts::parse();

            risingwave_logging::oneshot_common();
            risingwave_logging::init_risingwave_logger(opts.enable_jaeger_tracing, false);

            risingwave_compute::start(opts).await
        }),
    );

    fns.insert(
        "meta",
        Box::new(async move {
            eprintln!("launching meta node");

            let opts = risingwave_meta::MetaNodeOpts::parse();

            risingwave_logging::oneshot_common();
            risingwave_logging::init_risingwave_logger(false, false);

            risingwave_meta::start(opts).await
        }),
    );

    fns.insert(
        "frontend",
        Box::new(async move {
            eprintln!("launching frontend node");

            let opts = risingwave_frontend::FrontendOpts::parse();

            risingwave_logging::oneshot_common();
            risingwave_logging::init_risingwave_logger(false, false);

            risingwave_frontend::start(opts).await
        }),
    );

    fns.insert(
        "ctl",
        Box::new(async move {
            eprintln!("launching risectl");

            risingwave_logging::oneshot_common();
            risingwave_logging::init_risingwave_logger(false, true);

            risingwave_ctl::start().await
        }),
    );

    match fns.remove(target.as_str()) {
        Some(func) => {
            let func: Pin<Box<dyn Future<Output = ()>>> = func.into();
            func.await
        }
        None => {
            panic!("unknown target: {}\nplease set `RW_NODE` env variable or create a symbol link to `risingwave` binary with either {:?}", target, fns.keys().collect::<Vec<_>>());
        }
    }
}

#[cfg(not(feature = "all-in-one"))]
#[cfg(not(tarpaulin_include))]
fn main() {
    panic!("please enable `all-in-one` flag when cargo build to use all-in-one binary");
}

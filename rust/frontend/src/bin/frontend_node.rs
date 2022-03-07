use tracing::Level;
use tracing_subscriber::filter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::prelude::*;

#[cfg(not(tarpaulin_include))]
#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    use std::sync::Arc;

    use clap::StructOpt;
    use pgwire::pg_server::pg_serve;
    use risingwave_frontend::session::SessionManagerImpl;
    use risingwave_frontend::FrontendOpts;

    let opts: FrontendOpts = FrontendOpts::parse();

    let fmt_layer = {
        // Configure log output to stdout
        let fmt_layer = tracing_subscriber::fmt::layer().compact().with_ansi(false);
        let filter = filter::Targets::new()
            // Only enable WARN and ERROR for 3rd-party crates
            .with_target("aws_endpoint", Level::WARN)
            .with_target("hyper", Level::WARN)
            .with_target("h2", Level::WARN)
            .with_target("tower", Level::WARN)
            .with_target("isahc", Level::WARN);

        // Configure RisingWave's own crates to log at TRACE level, uncomment the following line if
        // needed.

        // let filter = configure_risingwave_targets(filter);

        // Enable DEBUG level for all other crates
        // TODO: remove this in release mode
        let filter = filter.with_default(Level::DEBUG);

        fmt_layer.with_filter(filter)
    };

    tracing_subscriber::registry().with(fmt_layer).init();

    let session_mgr = Arc::new(SessionManagerImpl::new(&opts).await.unwrap());
    pg_serve(&opts.host, session_mgr).await.unwrap();
}

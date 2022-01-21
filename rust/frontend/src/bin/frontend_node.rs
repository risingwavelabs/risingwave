#[cfg(not(tarpaulin_include))]
#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    use std::sync::Arc;

    use clap::StructOpt;
    use frontend::session::RwSessionManager;
    use frontend::FrontendOpts;
    use pgwire::pg_server::pg_serve;

    let opts: FrontendOpts = FrontendOpts::parse();
    log4rs::init_file(&opts.log4rs_config, Default::default()).unwrap();

    let session_mgr = Arc::new(RwSessionManager::new(opts.clone()).await.unwrap());
    pg_serve(&opts.host, session_mgr).await.unwrap();
}

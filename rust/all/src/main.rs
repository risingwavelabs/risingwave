use std::sync::Arc;

use clap::Parser;
use frontend::session::SessionManagerImpl;
use frontend::FrontendOpts;
use pgwire::pg_server::pg_serve;
use risingwave::server::compute_node_serve;
use risingwave::ComputeNodeOpts;
use risingwave_meta::rpc::server::{rpc_serve, MetaStoreBackend};

#[tokio::main]
async fn main() {
    rpc_serve(
        "127.0.0.1".parse().unwrap(),
        None,
        None,
        MetaStoreBackend::Mem,
    )
    .await.unwrap();
    compute_node_serve("127.0.0.1".parse().unwrap(), ComputeNodeOpts::parse()).await;
    let session_mgr = Arc::new(
        SessionManagerImpl::new(&FrontendOpts::parse())
            .await
            .unwrap(),
    );
    pg_serve("127.0.0.1", session_mgr).await.unwrap();
}

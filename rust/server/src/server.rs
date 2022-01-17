use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Request, Response, Server};
use prometheus::{Encoder, TextEncoder};
use risingwave_batch::rpc::service::task_service::TaskServiceImpl;
use risingwave_batch::task::{BatchTaskEnv, TaskManager};
use risingwave_common::config::ComputeNodeConfig;
use risingwave_meta::rpc::meta_client::MetaClient;
use risingwave_pb::stream_service::stream_service_server::StreamServiceServer;
use risingwave_pb::task_service::exchange_service_server::ExchangeServiceServer;
use risingwave_pb::task_service::task_service_server::TaskServiceServer;
use risingwave_source::MemSourceManager;
use risingwave_storage::table::SimpleTableManager;
use risingwave_storage::StateStoreImpl;
use risingwave_stream::task::{StreamManager, StreamTaskEnv};
use tokio::sync::mpsc::UnboundedSender;
use tokio::task::JoinHandle;

use crate::rpc::service::exchange_service::ExchangeServiceImpl;
use crate::rpc::service::stream_service::StreamServiceImpl;
use crate::ComputeNodeOpts;

fn load_config(opts: &ComputeNodeOpts) -> ComputeNodeConfig {
    if opts.config_path.is_empty() {
        return ComputeNodeConfig::default();
    }
    let config_path = PathBuf::from(opts.config_path.to_owned());
    ComputeNodeConfig::init(config_path).unwrap()
}

/// Bootstraps the compute-node.
pub async fn compute_node_serve(
    addr: SocketAddr,
    opts: ComputeNodeOpts,
) -> (JoinHandle<()>, UnboundedSender<()>) {
    let meta_client = MetaClient::new(&opts.meta_address).await.unwrap();
    let worker_id = meta_client.register(addr).await.unwrap();
    let _config = load_config(&opts); // TODO: _config will be used by streaming env & batch env.

    let state_store: StateStoreImpl = opts.state_store.parse().unwrap();
    let table_mgr = Arc::new(SimpleTableManager::new(state_store.clone()));
    let task_mgr = Arc::new(TaskManager::new());
    let stream_mgr = Arc::new(StreamManager::new(addr, state_store));
    let source_mgr = Arc::new(MemSourceManager::new());

    // FIXME: We should trigger barrier from meta service. Currently, we use a timer to
    // trigger barrier periodically.
    let stream_mgr1 = stream_mgr.clone();
    tokio::spawn(async move {
        let mut epoch = 0u64;
        loop {
            stream_mgr1.checkpoint(epoch).unwrap();
            epoch += 1;
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    });

    let batch_env = BatchTaskEnv::new(
        table_mgr.clone(),
        source_mgr.clone(),
        task_mgr.clone(),
        addr,
    );
    let stream_env = StreamTaskEnv::new(table_mgr, source_mgr, addr, worker_id);
    let task_srv = TaskServiceImpl::new(task_mgr.clone(), batch_env);
    let exchange_srv = ExchangeServiceImpl::new(task_mgr, stream_mgr.clone());
    let stream_srv = StreamServiceImpl::new(stream_mgr, stream_env);

    let (shutdown_send, mut shutdown_recv) = tokio::sync::mpsc::unbounded_channel();
    let join_handle = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(TaskServiceServer::new(task_srv))
            .add_service(ExchangeServiceServer::new(exchange_srv))
            .add_service(StreamServiceServer::new(stream_srv))
            .serve_with_shutdown(addr, async move {
                tokio::select! {
                  _ = tokio::signal::ctrl_c() => {},
                  _ = shutdown_recv.recv() => {},
                }
            })
            .await
            .unwrap();
    });

    // Boot metrics service.
    if opts.metrics_level > 0 {
        MetricsManager::boot_metrics_service(opts.prometheus_listener_addr.clone());
    }

    (join_handle, shutdown_send)
}

pub struct MetricsManager {}

impl MetricsManager {
    pub fn boot_metrics_service(listen_addr: String) {
        tokio::spawn(async move {
            info!(
                "Prometheus listener for Prometheus is set up on http://{}",
                listen_addr
            );

            let listen_socket_addr: SocketAddr = listen_addr.parse().unwrap();
            let serve_future =
                Server::bind(&listen_socket_addr).serve(make_service_fn(|_| async {
                    Ok::<_, hyper::Error>(service_fn(MetricsManager::metrics_service))
                }));

            if let Err(err) = serve_future.await {
                eprintln!("server error: {}", err);
            }
        });
    }

    async fn metrics_service(_req: Request<Body>) -> Result<Response<Body>, hyper::Error> {
        let encoder = TextEncoder::new();
        let mut buffer = vec![];
        let mf = prometheus::gather();
        encoder.encode(&mf, &mut buffer).unwrap();
        let response = Response::builder()
            .header(hyper::header::CONTENT_TYPE, encoder.format_type())
            .body(Body::from(buffer))
            .unwrap();

        Ok(response)
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;

    use clap::StructOpt;
    use risingwave_common::util::addr::get_host_port;
    use risingwave_meta::test_utils::LocalMeta;
    use tokio::sync::mpsc::UnboundedSender;
    use tokio::task::JoinHandle;

    use crate::server::compute_node_serve;
    use crate::ComputeNodeOpts;

    async fn start_compute_node() -> (JoinHandle<()>, UnboundedSender<()>) {
        let args: [OsString; 0] = []; // No argument.
        let opts = ComputeNodeOpts::parse_from(args);
        let addr = get_host_port(opts.host.as_str()).unwrap();
        compute_node_serve(addr, opts).await
    }

    #[tokio::test]
    async fn test_server_shutdown() {
        let sled_root = tempfile::tempdir().unwrap();
        let meta = LocalMeta::start(sled_root).await;
        let (join, shutdown) = start_compute_node().await;
        shutdown.send(()).unwrap();
        join.await.unwrap();
        meta.stop().await;
    }
}

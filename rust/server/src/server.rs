use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Request, Response, Server};
use prometheus::{Encoder, TextEncoder};
use risingwave_batch::rpc::service::task_service::TaskServiceImpl;
use risingwave_batch::task::{BatchTaskEnv, TaskManager};
use risingwave_common::config::ComputeNodeConfig;
use risingwave_pb::stream_service::stream_service_server::StreamServiceServer;
use risingwave_pb::task_service::exchange_service_server::ExchangeServiceServer;
use risingwave_pb::task_service::task_service_server::TaskServiceServer;
use risingwave_rpc_client::MetaClient;
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
    let config = load_config(&opts);

    let meta_client = MetaClient::new(&opts.meta_address).await.unwrap();
    let state_store = StateStoreImpl::from_str(&opts.state_store, meta_client.clone())
        .await
        .unwrap();
    let table_mgr = Arc::new(SimpleTableManager::new(state_store.clone()));
    let task_mgr = Arc::new(TaskManager::new());
    let stream_mgr = Arc::new(StreamManager::new(addr, state_store));
    let source_mgr = Arc::new(MemSourceManager::new());

    let batch_config = Arc::new(config.batch.clone());
    let batch_env = BatchTaskEnv::new(
        table_mgr.clone(),
        source_mgr.clone(),
        task_mgr.clone(),
        addr,
        batch_config,
    );
    let stream_env = StreamTaskEnv::new(table_mgr, source_mgr, addr);
    let task_srv = TaskServiceImpl::new(task_mgr.clone(), batch_env);
    let exchange_srv = ExchangeServiceImpl::new(task_mgr, stream_mgr.clone());
    let stream_srv = StreamServiceImpl::new(stream_mgr, stream_env.clone());

    // Start gRPC services.
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

    // After all services booted, register self to meta service
    let worker_id = meta_client.register(addr).await.unwrap();
    stream_env.set_worker_id(worker_id);

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
    use risingwave_meta::test_utils::LocalMeta;
    use tokio::sync::mpsc::UnboundedSender;
    use tokio::task::JoinHandle;

    use crate::server::compute_node_serve;
    use crate::ComputeNodeOpts;

    async fn start_compute_node() -> (JoinHandle<()>, UnboundedSender<()>) {
        let args: [OsString; 0] = []; // No argument.
        let mut opts = ComputeNodeOpts::parse_from(args);
        opts.meta_address = format!("http://{}", LocalMeta::meta_addr());
        let addr = opts.host.parse().unwrap();
        compute_node_serve(addr, opts).await
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn test_server_shutdown() {
        let meta = LocalMeta::start_in_tempdir().await;
        let (join, shutdown) = start_compute_node().await;
        shutdown.send(()).unwrap();
        join.await.unwrap();
        meta.stop().await;
    }
}

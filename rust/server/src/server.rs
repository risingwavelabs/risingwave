use std::net::SocketAddr;
use std::sync::Arc;

use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Request, Response, Server};
use prometheus::{Encoder, TextEncoder};
use risingwave_batch::rpc::service::task_service::TaskServiceImpl;
use risingwave_batch::task::{BatchTaskEnv, TaskManager};
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

pub fn rpc_serve(
    addr: SocketAddr,
    state_store: StateStoreImpl,
    prometheus_listener_addr: &str,
    metrics_level: u32,
) -> (JoinHandle<()>, UnboundedSender<()>) {
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
            stream_mgr1.send_barrier(epoch);
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
    let stream_env = StreamTaskEnv::new(table_mgr, source_mgr, addr);
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
    if metrics_level > 0 {
        MetricsManager::boot_metrics_service(prometheus_listener_addr.to_string());
    }

    (join_handle, shutdown_send)
}

pub struct MetricsManager {}

impl MetricsManager {
    pub fn boot_metrics_service(listen_addr: String) {
        tokio::spawn(async move {
            println!(
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
    use risingwave_common::util::addr::get_host_port;
    use risingwave_storage::memory::MemoryStateStore;
    use risingwave_storage::StateStoreImpl;

    use crate::server::rpc_serve;

    #[tokio::test]
    async fn test_server_shutdown() {
        let addr = get_host_port("127.0.0.1:5688").unwrap();
        let prometheus_addr = "127.0.0.1:1222";
        let store = StateStoreImpl::MemoryStateStore(MemoryStateStore::new());
        let (join_handle, shutdown_send) = rpc_serve(addr, store, prometheus_addr, 1);
        shutdown_send.send(()).unwrap();
        join_handle.await.unwrap();
    }
}

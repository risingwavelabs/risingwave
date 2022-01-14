use risingwave_common::util::addr::get_host_port;
use tokio::sync::mpsc::UnboundedSender;
use tokio::task::JoinHandle;

use crate::rpc::server::MetaStoreBackend;

pub struct LocalMeta {
    join_handle: JoinHandle<()>,
    shutdown_sender: UnboundedSender<()>,
}

impl LocalMeta {
    /// Start a local meta node in the background.
    pub async fn start() -> Self {
        let dashboard_addr = get_host_port("127.0.0.1:5691").unwrap();
        let addr = get_host_port("127.0.0.1:5690").unwrap();
        let (join_handle, shutdown_sender) = crate::rpc::server::rpc_serve(
            addr,
            Some(dashboard_addr),
            None,
            MetaStoreBackend::Sled(tempfile::tempdir().unwrap().into_path()),
        )
        .await;
        Self {
            join_handle,
            shutdown_sender,
        }
    }

    pub async fn stop(self) {
        self.shutdown_sender.send(()).unwrap();
        self.join_handle.await.unwrap();
    }
}

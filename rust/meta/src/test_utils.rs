use risingwave_rpc_client::MetaClient;
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
        let addr = Self::meta_addr().parse().unwrap();
        let (join_handle, shutdown_sender) =
            crate::rpc::server::rpc_serve(addr, None, MetaStoreBackend::Mem).await;
        Self {
            join_handle,
            shutdown_sender,
        }
    }

    pub async fn stop(self) {
        self.shutdown_sender.send(()).unwrap();
        self.join_handle.await.unwrap();
    }

    pub async fn create_client() -> MetaClient {
        MetaClient::new(&format!("http://{}", Self::meta_addr()))
            .await
            .unwrap()
    }

    pub fn meta_addr() -> &'static str {
        "127.0.0.1:56901"
    }
}

use risingwave_common::util::addr::get_host_port;
use risingwave_rpc_client::MetaClient;
use tokio::sync::mpsc::UnboundedSender;
use tokio::task::JoinHandle;

use crate::rpc::server::MetaStoreBackend;

pub struct LocalMeta {
    join_handle: JoinHandle<()>,
    shutdown_sender: UnboundedSender<()>,
}

impl LocalMeta {
    pub async fn start_in_tempdir() -> Self {
        let sled_root = tempfile::tempdir().unwrap();
        Self::start(sled_root).await
    }

    /// Start a local meta node in the background.
    pub async fn start(sled_root: impl AsRef<std::path::Path>) -> Self {
        let addr = get_host_port(Self::meta_addr()).unwrap();
        let (join_handle, shutdown_sender) = crate::rpc::server::rpc_serve(
            addr,
            None,
            None,
            MetaStoreBackend::Sled(sled_root.as_ref().to_path_buf()),
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

    pub async fn create_client() -> MetaClient {
        MetaClient::new(&format!("http://{}", Self::meta_addr()))
            .await
            .unwrap()
    }

    pub fn meta_addr() -> &'static str {
        "127.0.0.1:56901"
    }
}

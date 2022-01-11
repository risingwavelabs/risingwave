use std::sync::Arc;
use std::time::Duration;

use risingwave_common::error::{Result, ToRwResult};
use risingwave_pb::hummock::hummock_manager_service_client::HummockManagerServiceClient;
use risingwave_pb::hummock::{
    CreateHummockContextRequest, HummockContext, InvalidateHummockContextRequest,
    RefreshHummockContextRequest,
};
use tokio::sync::broadcast;
use tokio::sync::broadcast::{Receiver, Sender};
use tokio::task::JoinHandle;
use tonic::transport::Channel;

use crate::hummock::HummockError;

// TODO make configurable
/// maximum number for `HummockClient` to retry connection creation.
const CONNECT_RETRY_MAX: isize = isize::MAX;
/// time interval for `HummockClient` to retry connection creation, at milliseconds.
const CONNECT_RETRY_INTERVAL: u64 = 300;
/// connect timeout, at milliseconds
const CONNECT_TIMEOUT: u64 = 1000;

pub struct HummockClient {
    hummock_context: Arc<HummockContext>,
    // TODO add retry when sending rpc and reconnect rpc_client
    rpc_client: HummockManagerServiceClient<Channel>,
    notify_shutdown: Sender<()>,
}

impl HummockClient {
    pub fn hummock_context(&self) -> Arc<HummockContext> {
        self.hummock_context.clone()
    }
    pub fn rpc_client(&self) -> HummockManagerServiceClient<Channel> {
        self.rpc_client.clone()
    }
    pub fn notify_shutdown(&self) -> &Sender<()> {
        &self.notify_shutdown
    }

    async fn new_rpc_client(endpoint: &str) -> Result<HummockManagerServiceClient<Channel>> {
        for _ in 0..CONNECT_RETRY_MAX {
            match tonic::transport::Endpoint::new(endpoint.to_owned())
                .unwrap()
                .connect_timeout(Duration::from_millis(CONNECT_TIMEOUT))
                .connect()
                .await
            {
                Ok(channel) => {
                    return Ok(HummockManagerServiceClient::new(channel));
                }
                Err(_) => {
                    tokio::time::sleep(Duration::from_millis(CONNECT_RETRY_INTERVAL)).await;
                }
            }
        }
        Err(HummockError::create_rpc_client_error().into())
    }

    pub async fn new(endpoint: &str) -> Result<Self> {
        let mut rpc_client = HummockClient::new_rpc_client(endpoint).await?;
        let hummock_context = rpc_client
            .create_hummock_context(CreateHummockContextRequest {})
            .await
            .to_rw_result()?
            .into_inner()
            .hummock_context
            .unwrap();

        let (tx, _) = broadcast::channel(1);
        let instance = Self {
            hummock_context: Arc::new(hummock_context),
            rpc_client,
            notify_shutdown: tx,
        };

        Ok(instance)
    }

    pub async fn start_hummock_context_refresher(&self) -> JoinHandle<Result<()>> {
        tokio::spawn(HummockClient::context_refresher_loop(
            self.rpc_client(),
            self.hummock_context(),
            self.notify_shutdown().subscribe(),
        ))
    }

    async fn context_refresher_loop(
        mut client: HummockManagerServiceClient<Channel>,
        hummock_context: Arc<HummockContext>,
        mut shutdown_rx: Receiver<()>,
    ) -> Result<()> {
        let mut ttl = hummock_context.ttl;
        loop {
            let tick = async {
                let mut interval = tokio::time::interval(Duration::from_millis(
                    num_integer::Integer::div_ceil(&ttl, &3),
                ));
                interval.tick().await;
                interval.tick().await;
            };
            tokio::select! {
              biased;

              _ = tick => {
                let result = client
                  .refresh_hummock_context(RefreshHummockContextRequest {
                    context_identifier: hummock_context.identifier,
                  })
                  .await;
                if let Ok(new_ttl) = result {
                  ttl = new_ttl.into_inner().ttl;
                }
              }
              _ = shutdown_rx.recv() => {
                let _ = client
                  .invalidate_hummock_context(InvalidateHummockContextRequest {
                    context_identifier: hummock_context.identifier,
                  })
                  .await;
                return Ok(());
              }
            }
        }
    }
}

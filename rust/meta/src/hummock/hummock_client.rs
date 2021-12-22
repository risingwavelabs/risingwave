use std::sync::Arc;
use std::time::Duration;

use num_integer::Integer;
use risingwave_common::error::{Result, ToRwResult};
use risingwave_pb::hummock::hummock_manager_service_client::HummockManagerServiceClient;
use risingwave_pb::hummock::{
    CreateHummockContextRequest, HummockContext, InvalidateHummockContextRequest,
    RefreshHummockContextRequest,
};
use tokio::sync::broadcast;
use tokio::sync::broadcast::{Receiver, Sender};
use tonic::transport::Channel;

pub struct HummockClient {
    pub hummock_context: Arc<HummockContext>,
    pub client: HummockManagerServiceClient<Channel>,
    shutdown_tx: Sender<()>,
}

impl HummockClient {
    pub async fn new(endpoint: &str, context_group: &str) -> Result<Self> {
        let mut client = HummockManagerServiceClient::connect(endpoint.to_owned())
            .await
            .to_rw_result()?;
        let hummock_context = client
            .create_hummock_context(CreateHummockContextRequest {
                group: context_group.to_owned(),
            })
            .await
            .to_rw_result()?
            .into_inner()
            .hummock_context
            .unwrap();

        let (tx, rx) = broadcast::channel(16);
        let instance = Self {
            hummock_context: Arc::new(hummock_context),
            client,
            shutdown_tx: tx,
        };

        let client = HummockManagerServiceClient::connect(endpoint.to_owned())
            .await
            .to_rw_result()?;
        tokio::spawn(Self::start_context_refresher(
            client,
            instance.hummock_context.clone(),
            rx,
        ));

        Ok(instance)
    }

    async fn start_context_refresher(
        mut client: HummockManagerServiceClient<Channel>,
        hummock_context: Arc<HummockContext>,
        mut shutdown_rx: Receiver<()>,
    ) {
        let mut ttl = hummock_context.ttl;
        loop {
            let tick = async {
                let mut interval = tokio::time::interval(Duration::from_millis(ttl.div_ceil(&3)));
                interval.tick().await;
                interval.tick().await;
            };
            tokio::select! {
              _ = tick => {
                let result = client
                  .refresh_hummock_context(RefreshHummockContextRequest {
                    context_identifier: hummock_context.identifier,
                  })
                  .await;
                if let Ok(value) = result {
                  ttl = value.into_inner().ttl;
                }
              }
              _ = shutdown_rx.recv() => {
                let _ = client
                  .invalidate_hummock_context(InvalidateHummockContextRequest {
                    context_identifier: hummock_context.identifier,
                  })
                  .await;
                break;
              }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use risingwave_common::error::Result;
    use risingwave_common::util::addr::get_host_port;
    use risingwave_pb::hummock::InvalidateHummockContextRequest;

    use super::HummockClient;
    use crate::hummock;
    use crate::rpc::server::rpc_serve;

    #[tokio::test]
    async fn test_create_hummock_client() -> Result<()> {
        let hummock_config = hummock::Config {
            context_ttl: 1000,
            context_check_interval: 300,
        };
        let address = "127.0.0.1:9528";
        let (join_handle, shutdown_send) = rpc_serve(
            get_host_port(address).unwrap(),
            Some(hummock_config.clone()),
        )
        .await;
        // Till the server is up
        tokio::time::sleep(Duration::from_secs(1)).await;

        let endpoint = format!("http://{}", address);
        let context_group = "context_group_1";
        let context_group_3 = "context_group_3";
        let hummock_client = HummockClient::new(endpoint.as_str(), context_group).await;
        assert!(hummock_client.is_ok());

        let hummock_client_2 = HummockClient::new(endpoint.as_str(), context_group).await;
        assert!(hummock_client_2.is_err());

        let hummock_client_3 = HummockClient::new(endpoint.as_str(), context_group_3).await;
        assert!(hummock_client_3.is_ok());

        tokio::time::sleep(Duration::from_millis(hummock_config.context_ttl * 2)).await;

        let mut hummock_client_3 = hummock_client_3.unwrap();
        let result = hummock_client_3
            .client
            .invalidate_hummock_context(InvalidateHummockContextRequest {
                context_identifier: hummock_client_3.hummock_context.identifier,
            })
            .await;
        assert!(result.is_ok());

        let result = hummock_client_3
            .client
            .invalidate_hummock_context(InvalidateHummockContextRequest {
                context_identifier: hummock_client_3.hummock_context.identifier,
            })
            .await;
        assert!(result.is_err());

        shutdown_send.send(()).unwrap();
        join_handle.await.unwrap();

        Ok(())
    }
}

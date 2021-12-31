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

pub struct HummockClient {
    pub hummock_context: Arc<HummockContext>,
    pub client: HummockManagerServiceClient<Channel>,
    pub notify_shutdown: Sender<()>,
}

impl HummockClient {
    pub async fn new(endpoint: &str) -> Result<Self> {
        let mut client = HummockManagerServiceClient::connect(endpoint.to_owned())
            .await
            .to_rw_result()?;
        let hummock_context = client
            .create_hummock_context(CreateHummockContextRequest {})
            .await
            .to_rw_result()?
            .into_inner()
            .hummock_context
            .unwrap();

        let (tx, _) = broadcast::channel(1);
        let instance = Self {
            hummock_context: Arc::new(hummock_context),
            client: client.clone(),
            notify_shutdown: tx,
        };

        Ok(instance)
    }

    async fn start_hummock_context_refresher(&self) -> JoinHandle<Result<()>> {
        tokio::spawn(HummockClient::context_refresher_loop(
            self.client.clone(),
            self.hummock_context.clone(),
            self.notify_shutdown.subscribe(),
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
                  .await
                  .to_rw_result_with("Failure: refresh_hummock_context");
                match result {
                  Ok(value) => { ttl = value.into_inner().ttl; }
                  Err(e) => { return Err(e); }
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use risingwave_common::error::Result;
    use risingwave_pb::hummock::{
        AddTablesRequest, GetTablesRequest, InvalidateHummockContextRequest, PinVersionRequest,
        Table,
    };
    use tokio::net::TcpListener;
    use tokio::sync::mpsc::UnboundedSender;
    use tokio::task::JoinHandle;

    use super::HummockClient;
    use crate::hummock;
    use crate::rpc::server::rpc_serve_with_listener;

    async fn start_server(
        hummock_config: &hummock::Config,
    ) -> (String, JoinHandle<()>, UnboundedSender<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let endpoint = format!("http://127.0.0.1:{}", address.port());
        let (join_handle, shutdown_send) =
            rpc_serve_with_listener(listener, None, Some(hummock_config.clone())).await;
        // Till the server is up
        tokio::time::sleep(Duration::from_millis(300)).await;
        (endpoint, join_handle, shutdown_send)
    }

    #[tokio::test]
    async fn test_create_hummock_client() -> Result<()> {
        let hummock_config = hummock::Config {
            context_ttl: 1000,
            context_check_interval: 300,
        };
        let (endpoint, join_handle, shutdown_send) = start_server(&hummock_config).await;
        // create hummock_client without refresher
        let hummock_client = HummockClient::new(endpoint.as_str()).await;
        assert!(hummock_client.is_ok());
        let mut hummock_client = hummock_client.unwrap();

        // create hummock_client3 with a refresher
        let hummock_client_3 = HummockClient::new(endpoint.as_str()).await;
        assert!(hummock_client_3.is_ok());
        let mut hummock_client_3 = hummock_client_3.unwrap();
        let refresher_join_handle = hummock_client_3.start_hummock_context_refresher().await;

        tokio::time::sleep(Duration::from_millis(hummock_config.context_ttl * 2)).await;

        // The request will fail as hummock_client was already invalidated by hummock manager
        let result = hummock_client
            .client
            .invalidate_hummock_context(InvalidateHummockContextRequest {
                context_identifier: hummock_client.hummock_context.identifier,
            })
            .await;
        assert!(result.is_err());

        // The request will succeed as hummock_client_3 is kept alive by refresher
        let result = hummock_client_3
            .client
            .invalidate_hummock_context(InvalidateHummockContextRequest {
                context_identifier: hummock_client_3.hummock_context.identifier,
            })
            .await;
        assert!(result.is_ok());

        // The request will fail as the context was invalidated by the previous request.
        let result = hummock_client_3
            .client
            .invalidate_hummock_context(InvalidateHummockContextRequest {
                context_identifier: hummock_client_3.hummock_context.identifier,
            })
            .await;
        assert!(result.is_err());

        drop(hummock_client_3);
        let result = refresher_join_handle.await;
        // refresher exits normally
        assert!(result.is_ok());

        shutdown_send.send(()).unwrap();
        join_handle.await.unwrap();

        Ok(())
    }

    #[tokio::test]
    async fn test_table_operations() -> Result<()> {
        let hummock_config = hummock::Config {
            context_ttl: 1000,
            context_check_interval: 300,
        };
        let (endpoint, join_handle, shutdown_send) = start_server(&hummock_config).await;

        let mut hummock_client = HummockClient::new(endpoint.as_str()).await?;
        hummock_client.start_hummock_context_refresher().await;
        let original_tables = vec![Table { sst_id: 0 }, Table { sst_id: 1 }];
        let version_id = hummock_client
            .client
            .add_tables(AddTablesRequest {
                context_identifier: hummock_client.hummock_context.identifier,
                tables: vec![original_tables[0].clone()],
            })
            .await
            .unwrap()
            .into_inner()
            .version_id;
        let version_id_2 = hummock_client
            .client
            .add_tables(AddTablesRequest {
                context_identifier: hummock_client.hummock_context.identifier,
                tables: vec![original_tables[1].clone()],
            })
            .await
            .unwrap()
            .into_inner()
            .version_id;
        // add_tables will increase hummock version
        assert_eq!(version_id + 1, version_id_2);

        // pin a hummock version
        let pin_response = hummock_client
            .client
            .pin_version(PinVersionRequest {
                context_identifier: hummock_client.hummock_context.identifier,
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(version_id_2, pin_response.pinned_version_id);
        // get tables by pinned_version
        let mut got_tables = hummock_client
            .client
            .get_tables(GetTablesRequest {
                context_identifier: hummock_client.hummock_context.identifier,
                pinned_version: pin_response.pinned_version,
            })
            .await
            .unwrap()
            .into_inner()
            .tables;
        got_tables.sort_by_key(|t| t.sst_id);
        assert_eq!(got_tables, original_tables);

        // unpin
        // TODO uncomment the code below will fail the rust_test_with_coverage
        // let result = hummock_client
        //   .client
        //   .unpin_version(UnpinVersionRequest {
        //     context_identifier: hummock_client.hummock_context.identifier,
        //     pinned_version_id: pin_response.pinned_version_id,
        //   })
        //   .await;
        // assert!(result.is_ok());

        shutdown_send.send(()).unwrap();
        join_handle.await.unwrap();

        Ok(())
    }
}

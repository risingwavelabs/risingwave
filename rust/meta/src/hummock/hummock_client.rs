use std::sync::Arc;
use std::time::Duration;

use risingwave_common::error::{Result, ToRwResult};
use risingwave_pb::hummock::hummock_manager_service_client::HummockManagerServiceClient;
use risingwave_pb::hummock::{
    CreateHummockContextRequest, HummockContext, InvalidateHummockContextRequest,
    RefreshHummockContextRequest,
};
use risingwave_storage::hummock::HummockError;
use tokio::sync::broadcast;
use tokio::sync::broadcast::{Receiver, Sender};
use tokio::task::JoinHandle;
use tonic::transport::Channel;

// TODO make configurable
/// maximum number for `HummockClient` to retry connection creation.
const CONNECT_RETRY_MAX: isize = isize::MAX;
/// time interval for `HummockClient` to retry connection creation, at milliseconds.
const CONNECT_RETRY_INTERVAL: u64 = 300;

pub struct HummockClient {
    hummock_context: Arc<HummockContext>,
    // TODO add retry when sending rpc
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
            match HummockManagerServiceClient::connect(endpoint.to_owned()).await {
                Ok(client) => {
                    return Ok(client);
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

    async fn start_hummock_context_refresher(&self) -> JoinHandle<Result<()>> {
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use risingwave_common::error::{Result, ToRwResult};
    use risingwave_pb::hummock::{
        AddTablesRequest, GetTablesRequest, InvalidateHummockContextRequest, PinSnapshotRequest,
        PinVersionRequest, RefreshHummockContextRequest, Table, UnpinVersionRequest,
    };
    use risingwave_storage::hummock::value::HummockValue;
    use risingwave_storage::hummock::{TableBuilder, TableBuilderOptions};
    use tokio::net::TcpListener;
    use tokio::sync::mpsc::UnboundedSender;
    use tokio::task::JoinHandle;

    use super::HummockClient;
    use crate::hummock;
    use crate::hummock::tests::iterator_test_key_of_ts;
    use crate::rpc::server::{rpc_serve_with_listener, MetaStoreBackend};

    async fn start_server(
        hummock_config: &hummock::Config,
        port: Option<u32>,
    ) -> (String, JoinHandle<()>, UnboundedSender<()>) {
        let listener = TcpListener::bind(format!("127.0.0.1:{}", port.unwrap_or(0)))
            .await
            .unwrap();
        let address = listener.local_addr().unwrap();
        let endpoint = format!("http://127.0.0.1:{}", address.port());
        let (join_handle, shutdown_send) = rpc_serve_with_listener(
            listener,
            None,
            Some(hummock_config.clone()),
            MetaStoreBackend::Sled(tempfile::tempdir().unwrap().into_path()),
        )
        .await;
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
        let (endpoint, join_handle, shutdown_send) = start_server(&hummock_config, None).await;
        // create hummock_client without refresher
        let hummock_client = HummockClient::new(endpoint.as_str()).await;
        assert!(hummock_client.is_ok());
        let hummock_client = hummock_client.unwrap();

        // create hummock_client3 with a refresher
        let hummock_client_3 = HummockClient::new(endpoint.as_str()).await;
        assert!(hummock_client_3.is_ok());
        let hummock_client_3 = hummock_client_3.unwrap();
        let refresher_join_handle = hummock_client_3.start_hummock_context_refresher().await;

        tokio::time::sleep(Duration::from_millis(hummock_config.context_ttl * 2)).await;

        // The request will fail as hummock_client was already invalidated by hummock manager
        let result = hummock_client
            .rpc_client()
            .invalidate_hummock_context(InvalidateHummockContextRequest {
                context_identifier: hummock_client.hummock_context().identifier,
            })
            .await;
        assert!(result.is_err());

        // The request will succeed as hummock_client_3 is kept alive by refresher
        let result = hummock_client_3
            .rpc_client()
            .invalidate_hummock_context(InvalidateHummockContextRequest {
                context_identifier: hummock_client_3.hummock_context.identifier,
            })
            .await;
        assert!(result.is_ok());

        // The request will fail as the context was invalidated by the previous request.
        let result = hummock_client_3
            .rpc_client()
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

    async fn generate_test_tables(hummock_client: &HummockClient) -> Result<Vec<Table>> {
        // Tables to add
        let opt = TableBuilderOptions {
            bloom_false_positive: 0.1,
            block_size: 4096,
            table_capacity: 0,
            checksum_algo: risingwave_pb::hummock::checksum::Algorithm::XxHash64,
        };

        let mut tables = vec![];
        for i in 0..2 {
            let table_id = i as u64;
            let mut b = TableBuilder::new(opt.clone());
            let kv_pairs = vec![
                (i, HummockValue::Put(b"test".to_vec())),
                (i * 10, HummockValue::Put(b"test".to_vec())),
            ];
            let snapshot = hummock_client
                .rpc_client()
                .pin_snapshot(PinSnapshotRequest {
                    context_identifier: hummock_client.hummock_context().identifier,
                })
                .await
                .to_rw_result()?
                .into_inner()
                .snapshot
                .unwrap();
            // let snapshot = hummock_manager.pin_snapshot( context.identifier).await?;
            for kv in kv_pairs {
                b.add(&iterator_test_key_of_ts(table_id, kv.0, snapshot.ts), kv.1);
            }
            let (_data, meta) = b.finish();
            tables.push(Table {
                id: table_id,
                meta: Some(meta),
            })
        }
        Ok(tables)
    }

    #[tokio::test]
    async fn test_table_operations() -> Result<()> {
        let hummock_config = hummock::Config {
            context_ttl: 1000,
            context_check_interval: 300,
        };
        let (endpoint, join_handle, shutdown_send) = start_server(&hummock_config, None).await;

        let hummock_client = HummockClient::new(endpoint.as_str()).await?;
        hummock_client.start_hummock_context_refresher().await;
        let original_tables = generate_test_tables(&hummock_client).await?;
        let version_id = hummock_client
            .rpc_client()
            .add_tables(AddTablesRequest {
                context_identifier: hummock_client.hummock_context().identifier,
                tables: vec![original_tables[0].clone()],
            })
            .await
            .unwrap()
            .into_inner()
            .version_id;
        let version_id_2 = hummock_client
            .rpc_client()
            .add_tables(AddTablesRequest {
                context_identifier: hummock_client.hummock_context().identifier,
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
            .rpc_client()
            .pin_version(PinVersionRequest {
                context_identifier: hummock_client.hummock_context().identifier,
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(version_id_2, pin_response.pinned_version_id);
        // get tables by pinned_version
        let mut got_tables = hummock_client
            .rpc_client()
            .get_tables(GetTablesRequest {
                context_identifier: hummock_client.hummock_context().identifier,
                pinned_version: pin_response.pinned_version,
            })
            .await
            .unwrap()
            .into_inner()
            .tables;
        got_tables.sort_by_key(|t| t.id);
        assert_eq!(got_tables, original_tables);

        // unpin
        let result = hummock_client
            .rpc_client()
            .unpin_version(UnpinVersionRequest {
                context_identifier: hummock_client.hummock_context().identifier,
                pinned_version_id: pin_response.pinned_version_id,
            })
            .await;
        assert!(result.is_ok());

        shutdown_send.send(()).unwrap();
        join_handle.await.unwrap();

        Ok(())
    }

    #[tokio::test]
    async fn test_retry_connect() -> Result<()> {
        let hummock_config = hummock::Config {
            context_ttl: 100000,
            context_check_interval: 300,
        };

        // pick a port
        let port = {
            let listener = TcpListener::bind(format!("127.0.0.1:{}", 0)).await.unwrap();
            let address = listener.local_addr().unwrap();
            address.port() as u32
        };
        let server_join_handle = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(2)).await;
            // There is still a chance the port was already used and then failed this test. But I
            // think it is acceptable.
            let (_, join_handle, shutdown_send) = start_server(&hummock_config, Some(port)).await;
            tokio::time::sleep(Duration::from_secs(2)).await;
            shutdown_send.send(()).unwrap();
            join_handle.await.unwrap();
        });

        let result = tokio::time::timeout(Duration::from_secs(5), async move {
            HummockClient::new(&format!("http://127.0.0.1:{}", port)).await
        })
        .await;
        // not timeout
        assert!(result.is_ok());

        let hummock_client = result.unwrap()?;
        let result = hummock_client
            .rpc_client()
            .refresh_hummock_context(RefreshHummockContextRequest {
                context_identifier: hummock_client.hummock_context().identifier,
            })
            .await;
        assert!(result.is_ok());

        let result = server_join_handle.await;
        assert!(result.is_ok());
        Ok(())
    }
}

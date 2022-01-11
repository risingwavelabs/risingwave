use std::time::Duration;

use risingwave_common::error::Result;
use risingwave_pb::hummock::{
    AbortEpochRequest, AddTablesRequest, CommitEpochRequest, GetTablesRequest, HummockVersion,
    InvalidateHummockContextRequest, PinVersionRequest, PinVersionResponse,
    RefreshHummockContextRequest, Table, UnpinVersionRequest,
};
use risingwave_storage::hummock::value::HummockValue;
use risingwave_storage::hummock::{TableBuilder, TableBuilderOptions};
use tokio::net::TcpListener;
use tokio::sync::mpsc::UnboundedSender;
use tokio::task::JoinHandle;

use super::HummockClient;
use crate::hummock;
use crate::hummock::hummock_manager_tests::iterator_test_key_of_ts;
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
#[ignore]
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
            context_identifier: hummock_client_3.hummock_context().identifier,
        })
        .await;
    assert!(result.is_ok());

    // The request will fail as the context was invalidated by the previous request.
    let result = hummock_client_3
        .rpc_client()
        .invalidate_hummock_context(InvalidateHummockContextRequest {
            context_identifier: hummock_client_3.hummock_context().identifier,
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

fn generate_test_tables(epoch: u64, table_id: &mut u64) -> Vec<Table> {
    // Tables to add
    let opt = TableBuilderOptions {
        bloom_false_positive: 0.1,
        block_size: 4096,
        table_capacity: 0,
        checksum_algo: risingwave_pb::hummock::checksum::Algorithm::XxHash64,
    };

    let mut tables = vec![];
    for i in 0..2 {
        let mut b = TableBuilder::new(opt.clone());
        let kv_pairs = vec![
            (i, HummockValue::Put(b"test".to_vec())),
            (i * 10, HummockValue::Put(b"test".to_vec())),
        ];
        for kv in kv_pairs {
            b.add(&iterator_test_key_of_ts(*table_id, kv.0, epoch), kv.1);
        }
        let (_data, meta) = b.finish();
        tables.push(Table {
            id: *table_id,
            meta: Some(meta),
        });
        (*table_id) += 1;
    }
    tables
}

async fn add_tables(client: &HummockClient, tables: Vec<Table>, epoch: u64) -> u64 {
    client
        .rpc_client()
        .add_tables(AddTablesRequest {
            context_identifier: client.hummock_context().identifier,
            tables,
            epoch,
        })
        .await
        .unwrap()
        .into_inner()
        .version_id
}

async fn get_tables(client: &HummockClient, version: HummockVersion) -> Vec<Table> {
    let mut got_tables = client
        .rpc_client()
        .get_tables(GetTablesRequest {
            context_identifier: client.hummock_context().identifier,
            pinned_version: Some(version),
        })
        .await
        .unwrap()
        .into_inner()
        .tables;
    got_tables.sort_by_key(|t| t.id);
    got_tables
}

async fn pin_version(client: &HummockClient) -> (u64, HummockVersion) {
    let PinVersionResponse {
        pinned_version_id,
        pinned_version,
        ..
    } = client
        .rpc_client()
        .pin_version(PinVersionRequest {
            context_identifier: client.hummock_context().identifier,
        })
        .await
        .unwrap()
        .into_inner();
    (pinned_version_id, pinned_version.unwrap())
}

async fn unpin_version(client: &HummockClient, pinned_version_id: u64) {
    let result = client
        .rpc_client()
        .unpin_version(UnpinVersionRequest {
            context_identifier: client.hummock_context().identifier,
            pinned_version_id,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(result.status.is_none());
}

async fn commit_epoch(client: &HummockClient, epoch: u64) {
    let result = client
        .rpc_client()
        .commit_epoch(CommitEpochRequest {
            context_identifier: client.hummock_context().identifier,
            epoch,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(result.status.is_none());
}

async fn abort_epoch(client: &HummockClient, epoch: u64) {
    let result = client
        .rpc_client()
        .abort_epoch(AbortEpochRequest {
            context_identifier: client.hummock_context().identifier,
            epoch,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(result.status.is_none());
}

#[tokio::test]
#[ignore]
async fn test_table_operations() -> Result<()> {
    let hummock_config = hummock::Config {
        context_ttl: 1000,
        context_check_interval: 300,
    };
    let (endpoint, join_handle, shutdown_send) = start_server(&hummock_config, None).await;

    let mut epoch: u64 = 1;
    let mut table_id = 1;
    let hummock_client = HummockClient::new(endpoint.as_str()).await?;
    hummock_client.start_hummock_context_refresher().await;
    let original_tables = generate_test_tables(epoch, &mut table_id);
    let version_id = add_tables(&hummock_client, vec![original_tables[0].clone()], epoch).await;
    // commit this epoch
    commit_epoch(&hummock_client, epoch).await;

    epoch += 1;
    let version_id_2 = add_tables(&hummock_client, vec![original_tables[1].clone()], epoch).await;
    // commit_epoch and add_tables will both increase hummock version
    // add_tables will increase hummock version
    assert_eq!(version_id + 2, version_id_2);
    // commit this epoch
    commit_epoch(&hummock_client, epoch).await;

    // pin a hummock version
    let (pinned_version_id, pinned_version) = pin_version(&hummock_client).await;
    assert_eq!(version_id_2 + 1, pinned_version_id);
    // get tables by pinned_version
    let got_tables = get_tables(&hummock_client, pinned_version).await;
    assert_eq!(got_tables, original_tables);

    // unpin
    unpin_version(&hummock_client, pinned_version_id).await;

    shutdown_send.send(()).unwrap();
    join_handle.await.unwrap();

    Ok(())
}

async fn test_hummock_transaction() -> Result<()> {
    let hummock_config = hummock::Config {
        context_ttl: 1000,
        context_check_interval: 300,
    };
    let (endpoint, join_handle, shutdown_send) = start_server(&hummock_config, None).await;
    let hummock_client = HummockClient::new(endpoint.as_str()).await?;
    hummock_client.start_hummock_context_refresher().await;
    let mut table_id = 1;
    let mut committed_tables = vec![];

    // Add and commit tables in epoch1.
    // BEFORE:  umcommitted_epochs = [], committed_epochs = []
    // RUNNING: umcommitted_epochs = [epoch1], committed_epochs = []
    // AFTER:   umcommitted_epochs = [], committed_epochs = [epoch1]
    let epoch1: u64 = 1;
    {
        // Add tables in epoch1
        let tables_in_epoch1 = generate_test_tables(epoch1, &mut table_id);
        add_tables(&hummock_client, tables_in_epoch1.clone(), epoch1).await;

        // Get tables before committing epoch1. No tables should be returned.
        let (pinned_version_id, mut pinned_version) = pin_version(&hummock_client).await;
        let uncommitted_epoch = pinned_version.uncommitted_epochs.first_mut().unwrap();
        assert_eq!(epoch1, uncommitted_epoch.epoch);
        assert_eq!(pinned_version.max_committed_epoch, hummock::INVALID_EPOCH);
        uncommitted_epoch.table_ids.sort_unstable();
        let table_ids_in_epoch1: Vec<u64> = tables_in_epoch1.iter().map(|t| t.id).collect();
        assert_eq!(table_ids_in_epoch1, uncommitted_epoch.table_ids);
        let got_tables = get_tables(&hummock_client, pinned_version).await;
        assert!(got_tables.is_empty());
        unpin_version(&hummock_client, pinned_version_id).await;

        // Commit epoch1
        commit_epoch(&hummock_client, epoch1).await;
        committed_tables.extend(tables_in_epoch1.clone());

        // Get tables after committing epoch1. All tables committed in epoch1 should be returned
        let (pinned_version_id, pinned_version) = pin_version(&hummock_client).await;
        assert!(pinned_version.uncommitted_epochs.is_empty());
        assert_eq!(pinned_version.max_committed_epoch, epoch1);
        let got_tables = get_tables(&hummock_client, pinned_version).await;
        assert_eq!(committed_tables, got_tables);
        unpin_version(&hummock_client, pinned_version_id).await;
    }

    // Add and commit tables in epoch2.
    // BEFORE:  umcommitted_epochs = [], committed_epochs = [epoch1]
    // RUNNING: umcommitted_epochs = [epoch2], committed_epochs = [epoch1]
    // AFTER:   umcommitted_epochs = [], committed_epochs = [epoch1, epoch2]
    let epoch2 = epoch1 + 1;
    {
        // Add tables in epoch2
        let tables_in_epoch2 = generate_test_tables(epoch2, &mut table_id);
        add_tables(&hummock_client, tables_in_epoch2.clone(), epoch2).await;

        // Get tables before committing epoch2. tables_in_epoch1 should be returned and
        // tables_in_epoch2 should be invisible.
        let (pinned_version_id, mut pinned_version) = pin_version(&hummock_client).await;
        let uncommitted_epoch = pinned_version.uncommitted_epochs.first_mut().unwrap();
        assert_eq!(epoch2, uncommitted_epoch.epoch);
        uncommitted_epoch.table_ids.sort_unstable();
        let table_ids_in_epoch2: Vec<u64> = tables_in_epoch2.iter().map(|t| t.id).collect();
        assert_eq!(table_ids_in_epoch2, uncommitted_epoch.table_ids);
        assert_eq!(pinned_version.max_committed_epoch, epoch1);
        let got_tables = get_tables(&hummock_client, pinned_version).await;
        assert_eq!(committed_tables, got_tables);
        unpin_version(&hummock_client, pinned_version_id).await;

        // Commit epoch2
        commit_epoch(&hummock_client, epoch1).await;
        committed_tables.extend(tables_in_epoch2);

        // Get tables after committing epoch2. tables_in_epoch1 and tables_in_epoch2 should be
        // returned
        let (pinned_version_id, pinned_version) = pin_version(&hummock_client).await;
        assert!(pinned_version.uncommitted_epochs.is_empty());
        assert_eq!(pinned_version.max_committed_epoch, epoch2);
        let got_tables = get_tables(&hummock_client, pinned_version).await;
        assert_eq!(committed_tables, got_tables);
        unpin_version(&hummock_client, pinned_version_id).await;
    }

    // Add tables in epoch3 and epoch4. Abort epoch3, commit epoch4.
    // BEFORE:  umcommitted_epochs = [], committed_epochs = [epoch1, epoch2]
    // RUNNING: umcommitted_epochs = [epoch3, epoch4], committed_epochs = [epoch1, epoch2]
    // AFTER:   umcommitted_epochs = [], committed_epochs = [epoch1, epoch2, epoch4]
    let epoch3 = epoch2 + 1;
    let epoch4 = epoch3 + 1;
    {
        // Add tables in epoch3 and epoch4
        let tables_in_epoch3: Vec<Table> = generate_test_tables(epoch3, &mut table_id);
        add_tables(&hummock_client, tables_in_epoch3.clone(), epoch3).await;
        let tables_in_epoch4: Vec<Table> = generate_test_tables(epoch4, &mut table_id);
        add_tables(&hummock_client, tables_in_epoch4.clone(), epoch4).await;

        // Get tables before committing epoch3 and epoch4. tables_in_epoch1 and tables_in_epoch2
        // should be returned
        let (pinned_version_id, mut pinned_version) = pin_version(&hummock_client).await;
        let uncommitted_epoch3 = pinned_version
            .uncommitted_epochs
            .iter_mut()
            .find(|e| e.epoch == epoch3)
            .unwrap();
        uncommitted_epoch3.table_ids.sort_unstable();
        let table_ids_in_epoch3: Vec<u64> = tables_in_epoch3.iter().map(|t| t.id).collect();
        assert_eq!(table_ids_in_epoch3, uncommitted_epoch3.table_ids);
        let uncommitted_epoch4 = pinned_version
            .uncommitted_epochs
            .iter_mut()
            .find(|e| e.epoch == epoch4)
            .unwrap();
        uncommitted_epoch4.table_ids.sort_unstable();
        let table_ids_in_epoch4: Vec<u64> = tables_in_epoch4.iter().map(|t| t.id).collect();
        assert_eq!(table_ids_in_epoch4, uncommitted_epoch4.table_ids);
        assert_eq!(pinned_version.max_committed_epoch, epoch2);
        let got_tables = get_tables(&hummock_client, pinned_version).await;
        assert_eq!(committed_tables, got_tables);
        unpin_version(&hummock_client, pinned_version_id).await;

        // Abort epoch3
        abort_epoch(&hummock_client, epoch3).await;

        // Get tables after aborting epoch3. tables_in_epoch1 and tables_in_epoch2 should be
        // returned
        let (pinned_version_id, mut pinned_version) = pin_version(&hummock_client).await;
        assert!(pinned_version
            .uncommitted_epochs
            .iter_mut()
            .all(|e| e.epoch != epoch3));
        let uncommitted_epoch4 = pinned_version
            .uncommitted_epochs
            .iter_mut()
            .find(|e| e.epoch == epoch4)
            .unwrap();
        uncommitted_epoch4.table_ids.sort_unstable();
        assert_eq!(table_ids_in_epoch4, uncommitted_epoch4.table_ids);
        assert_eq!(pinned_version.max_committed_epoch, epoch2);
        let got_tables = get_tables(&hummock_client, pinned_version).await;
        assert_eq!(committed_tables, got_tables);
        unpin_version(&hummock_client, pinned_version_id).await;

        // Commit epoch4
        commit_epoch(&hummock_client, epoch3).await;
        committed_tables.extend(tables_in_epoch4);

        // Get tables after committing epoch4. tables_in_epoch1, tables_in_epoch2, tables_in_epoch4
        // should be returned.
        let (pinned_version_id, pinned_version) = pin_version(&hummock_client).await;
        assert!(pinned_version.uncommitted_epochs.is_empty());
        assert_eq!(pinned_version.max_committed_epoch, epoch4);
        let got_tables = get_tables(&hummock_client, pinned_version).await;
        assert_eq!(committed_tables, got_tables);
        unpin_version(&hummock_client, pinned_version_id).await;
    }

    shutdown_send.send(()).unwrap();
    join_handle.await.unwrap();

    Ok(())
}

#[tokio::test]
#[ignore]
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
        // There is still a chance the port was already used and then failed this test. But I think
        // it is acceptable.
        let (_, join_handle, shutdown_send) = start_server(&hummock_config, Some(port)).await;
        tokio::time::sleep(Duration::from_secs(2)).await;
        shutdown_send.send(()).unwrap();
        join_handle.await.unwrap();
    });

    let result = tokio::time::timeout(Duration::from_secs(10), async move {
        HummockClient::new(&format!("http://127.0.0.1:{}", port)).await
    })
    .await;
    // shouldn't timeout
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

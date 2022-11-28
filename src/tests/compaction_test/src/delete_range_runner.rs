use std::collections::BTreeMap;
use std::future::Future;
use std::net::SocketAddr;
use std::ops::Bound;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use itertools::Itertools;
use risingwave_common::catalog::TableId;
use risingwave_common::config::load_config;
use risingwave_common::util::addr::HostAddr;
use risingwave_hummock_sdk::HummockEpoch;
use risingwave_pb::catalog::Table as ProstTable;
use risingwave_pb::common::WorkerType;
use risingwave_pb::hummock::CompactionGroup;
use risingwave_rpc_client::MetaClient;
use risingwave_storage::hummock::store::state_store::LocalHummockStorage;
use risingwave_storage::storage_value::StorageValue;
use risingwave_storage::store::{ReadOptions, StateStoreRead, StateStoreWrite, WriteOptions};
use risingwave_storage::{StateStore, StateStoreIter};

use crate::runner::{create_hummock_store_with_metrics, start_compactor_thread, start_meta_node};
use crate::{CompactionTestOpts, TestToolConfig};

pub fn start_delete_range(opts: CompactionTestOpts) -> Pin<Box<dyn Future<Output = ()> + Send>> {
    // WARNING: don't change the function signature. Making it `async fn` will cause
    // slow compile in release mode.
    Box::pin(async move {
        tracing::info!("Compaction delete-range test start with options {:?}", opts);
        let prefix = opts.state_store.strip_prefix("hummock+");
        match prefix {
            Some(s) => {
                assert!(
                    s.starts_with("s3://") || s.starts_with("minio://"),
                    "Only support S3 and MinIO object store"
                );
            }
            None => {
                panic!("Invalid state store");
            }
        }
        let listen_address = opts.host.parse().unwrap();
        tracing::info!("Server Listening at {}", listen_address);

        let client_address = opts
            .client_address
            .as_ref()
            .unwrap_or_else(|| {
                tracing::warn!("Client address is not specified, defaulting to host address");
                &opts.host
            })
            .parse()
            .unwrap();

        let ret = compaction_test_main(listen_address, client_address, opts).await;

        match ret {
            Ok(_) => {
                tracing::info!("Success");
            }
            Err(e) => {
                tracing::error!("Failure {}", e);
            }
        }
    })
}

pub async fn compaction_test_main(
    _listen_addr: SocketAddr,
    client_addr: HostAddr,
    opts: CompactionTestOpts,
) -> anyhow::Result<()> {
    let meta_listen_addr = opts
        .meta_address
        .strip_prefix("http://")
        .unwrap()
        .to_owned();

    let _meta_handle = tokio::spawn(start_meta_node(
        meta_listen_addr.clone(),
        opts.config_path.clone(),
    ));

    // Wait for meta starts
    tokio::time::sleep(Duration::from_secs(1)).await;
    tracing::info!("Started embedded Meta");

    let (compactor_thrd, compactor_shutdown_tx) = start_compactor_thread(
        opts.meta_address.clone(),
        client_addr.to_string(),
        opts.state_store.clone(),
        opts.config_path.clone(),
    );

    let meta_client =
        MetaClient::register_new(&opts.meta_address, WorkerType::RiseCtl, &client_addr, 0).await?;
    meta_client.activate(&client_addr).await.unwrap();
    let delete_key_table = ProstTable {
        id: 1,
        schema_id: 1,
        database_id: 1,
        name: "delete-key-table".to_string(),
        columns: vec![],
        pk: vec![],
        dependent_relations: vec![],
        is_index: false,
        distribution_key: vec![],
        stream_key: vec![],
        appendonly: false,
        owner: 0,
        properties: Default::default(),
        fragment_id: 0,
        vnode_col_idx: None,
        value_indices: vec![],
        definition: "".to_string(),
        handle_pk_conflict: false,
        optional_associated_source_id: None,
    };
    let mut delete_range_table = delete_key_table.clone();
    delete_range_table.id = 2;
    delete_range_table.name = "delete-range-table".to_string();
    let group1 = CompactionGroup {
        id: 0,
        parent_id: 0,
        member_table_ids: vec![1],
        compaction_config: None,
        table_id_to_options: Default::default(),
    };
    let group2 = CompactionGroup {
        id: 0,
        parent_id: 0,
        member_table_ids: vec![2],
        compaction_config: None,
        table_id_to_options: Default::default(),
    };
    meta_client
        .init_metadata_for_replay(
            vec![delete_key_table, delete_range_table],
            vec![group1, group2],
        )
        .await?;
    start_test_thread(opts, meta_client).await;
    compactor_shutdown_tx.send(()).unwrap();
    compactor_thrd.join().unwrap();
    Ok(())
}

async fn start_test_thread(opts: CompactionTestOpts, meta_client: MetaClient) {
    let mut config: TestToolConfig = load_config(&opts.config_path).unwrap();
    config.storage.enable_state_store_v1 = false;
    let storage_config = Arc::new(config.storage.clone());
    let hummock = create_hummock_store_with_metrics(&meta_client, storage_config.clone(), &opts)
        .await
        .unwrap();
    let storage = hummock.inner().new_local(TableId::new(1)).await;
    let _ = NormalState {
        storage,
        cache: BTreeMap::default(),
        epoch: 1,
    };
}

struct NormalState {
    storage: LocalHummockStorage,
    cache: BTreeMap<Bytes, StorageValue>,
    epoch: u64,
}

struct DeleteRangeState {
    storage: LocalHummockStorage,
    cache: BTreeMap<Bytes, StorageValue>,
    epoch: u64,
}

#[async_trait::async_trait]
trait CheckState {
    async fn delete_range(&mut self, left: &[u8], right: &[u8]);
    async fn get(&self, key: &[u8]) -> Option<Bytes>;
    async fn scan(&self, left: &[u8], right: &[u8]) -> Vec<(Bytes, Bytes)>;
    fn insert(&mut self, key: &[u8], val: &[u8]);
    async fn commit(&mut self, epoch: u64) -> Result<(), String>;
}

#[async_trait::async_trait]
impl CheckState for NormalState {
    async fn delete_range(&mut self, left: &[u8], right: &[u8]) {
        self.cache
            .retain(|key, _| key.as_ref().lt(left) || key.as_ref().ge(right));
        let mut iter = self
            .storage
            .iter(
                (
                    Bound::Included(left.to_vec()),
                    Bound::Excluded(right.to_vec()),
                ),
                self.epoch,
                ReadOptions {
                    prefix_hint: None,
                    ignore_range_tombstone: true,
                    check_bloom_filter: false,
                    retention_seconds: None,
                    table_id: Default::default(),
                },
            )
            .await
            .unwrap();
        while let Some((full_key, _)) = iter.next().await.unwrap() {
            self.cache.insert(
                Bytes::from(full_key.user_key.encode()),
                StorageValue::new_delete(),
            );
        }
    }

    fn insert(&mut self, key: &[u8], val: &[u8]) {
        self.cache.insert(
            Bytes::copy_from_slice(key),
            StorageValue::new_put(val.to_vec()),
        );
    }

    async fn get(&self, key: &[u8]) -> Option<Bytes> {
        if let Some(val) = self.cache.get(key) {
            return val.user_value.clone();
        }
        self.storage
            .get(
                key,
                self.epoch,
                ReadOptions {
                    prefix_hint: None,
                    ignore_range_tombstone: true,
                    check_bloom_filter: true,
                    retention_seconds: None,
                    table_id: Default::default(),
                },
            )
            .await
            .unwrap()
    }

    async fn scan(&self, left: &[u8], right: &[u8]) -> Vec<(Bytes, Bytes)> {
        let mut iter = self
            .storage
            .iter(
                (
                    Bound::Included(left.to_vec()),
                    Bound::Excluded(right.to_vec()),
                ),
                self.epoch,
                ReadOptions {
                    prefix_hint: None,
                    ignore_range_tombstone: true,
                    check_bloom_filter: false,
                    retention_seconds: None,
                    table_id: Default::default(),
                },
            )
            .await
            .unwrap();
        let mut ret = vec![];
        while let Some((full_key, val)) = iter.next().await.unwrap() {
            ret.push((Bytes::from(full_key.user_key.encode()), val));
        }
        ret
    }

    async fn commit(&mut self, epoch: u64) -> Result<(), String> {
        let data = std::mem::take(&mut self.cache).into_iter().collect_vec();
        self.storage
            .ingest_batch(
                data,
                vec![],
                WriteOptions {
                    epoch,
                    table_id: TableId::new(1),
                },
            )
            .await
            .map_err(|e| format!("{:?}", e))?;
        Ok(())
    }
}

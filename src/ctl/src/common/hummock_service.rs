// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::env;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail, Result};
use risingwave_object_store::object::parse_remote_object_store;
use risingwave_rpc_client::MetaClient;
use risingwave_storage::hummock::hummock_meta_client::MonitoredHummockMetaClient;
use risingwave_storage::hummock::{
    HummockStorage, SstableStore, TieredCache, TieredCacheMetricsBuilder,
};
use risingwave_storage::monitor::{
    CompactorMetrics, HummockMetrics, HummockStateStoreMetrics, MonitoredStateStore,
    MonitoredStorageMetrics, ObjectStoreMetrics,
};
use risingwave_storage::opts::StorageOpts;
use risingwave_storage::{StateStore, StateStoreImpl};
use tokio::sync::oneshot::Sender;
use tokio::task::JoinHandle;

pub struct HummockServiceOpts {
    pub hummock_url: String,
    pub data_dir: Option<String>,

    heartbeat_handle: Option<JoinHandle<()>>,
    heartbeat_shutdown_sender: Option<Sender<()>>,
}

#[derive(Clone)]
pub struct Metrics {
    pub hummock_metrics: Arc<HummockMetrics>,
    pub state_store_metrics: Arc<HummockStateStoreMetrics>,
    pub object_store_metrics: Arc<ObjectStoreMetrics>,
    pub storage_metrics: Arc<MonitoredStorageMetrics>,
    pub compactor_metrics: Arc<CompactorMetrics>,
}

impl HummockServiceOpts {
    /// Recover hummock service options from env variable
    ///
    /// Currently, we will read these variables for meta:
    ///
    /// * `RW_HUMMOCK_URL`: hummock store address
    pub fn from_env(data_dir: Option<String>) -> Result<Self> {
        let hummock_url = match env::var("RW_HUMMOCK_URL") {
            Ok(url) => {
                if !url.starts_with("hummock+") {
                    return Err(anyhow!(
                        "only url starting with 'hummock+' is supported in risectl"
                    ));
                }
                tracing::info!("using Hummock URL from `RW_HUMMOCK_URL`: {}", url);
                url
            }
            Err(_) => {
                const MESSAGE: &str = "env variable `RW_HUMMOCK_URL` not found.

For `./risedev d` use cases, please do the following.
* start the cluster with shared storage:
  - consider adding `use: minio` in the risedev config,
  - or directly use `./risedev d for-ctl` to start the cluster.
* use `./risedev ctl` to use risectl.

For `./risedev apply-compose-deploy` users,
* `RW_HUMMOCK_URL` will be printed out when deploying. Please copy the bash exports to your console.
";
                bail!(MESSAGE);
            }
        };
        Ok(Self {
            hummock_url,
            data_dir,
            heartbeat_handle: None,
            heartbeat_shutdown_sender: None,
        })
    }

    fn get_storage_opts(&self) -> StorageOpts {
        let mut opts = StorageOpts {
            share_buffer_compaction_worker_threads_number: 0,
            ..Default::default()
        };

        if let Some(dir) = &self.data_dir {
            opts.data_directory = dir.clone();
        }

        opts
    }

    pub async fn create_hummock_store_with_metrics(
        &mut self,
        meta_client: &MetaClient,
    ) -> Result<(MonitoredStateStore<HummockStorage>, Metrics)> {
        let (heartbeat_handle, heartbeat_shutdown_sender) = MetaClient::start_heartbeat_loop(
            meta_client.clone(),
            Duration::from_millis(1000),
            vec![],
        );
        self.heartbeat_handle = Some(heartbeat_handle);
        self.heartbeat_shutdown_sender = Some(heartbeat_shutdown_sender);

        // FIXME: allow specify custom config
        let opts = self.get_storage_opts();

        tracing::info!("using StorageOpts: {:#?}", opts);

        let metrics = Metrics {
            hummock_metrics: Arc::new(HummockMetrics::unused()),
            state_store_metrics: Arc::new(HummockStateStoreMetrics::unused()),
            object_store_metrics: Arc::new(ObjectStoreMetrics::unused()),
            storage_metrics: Arc::new(MonitoredStorageMetrics::unused()),
            compactor_metrics: Arc::new(CompactorMetrics::unused()),
        };

        let state_store_impl = StateStoreImpl::new(
            &self.hummock_url,
            Arc::new(opts),
            Arc::new(MonitoredHummockMetaClient::new(
                meta_client.clone(),
                metrics.hummock_metrics.clone(),
            )),
            metrics.state_store_metrics.clone(),
            metrics.object_store_metrics.clone(),
            TieredCacheMetricsBuilder::unused(),
            metrics.storage_metrics.clone(),
            metrics.compactor_metrics.clone(),
        )
        .await?;

        if let Some(hummock_state_store) = state_store_impl.as_hummock() {
            Ok((
                hummock_state_store
                    .clone()
                    .monitored(metrics.storage_metrics.clone()),
                metrics,
            ))
        } else {
            Err(anyhow!("only Hummock state store is supported in risectl"))
        }
    }

    pub async fn create_sstable_store(&self) -> Result<Arc<SstableStore>> {
        let object_store = parse_remote_object_store(
            self.hummock_url.strip_prefix("hummock+").unwrap(),
            Arc::new(ObjectStoreMetrics::unused()),
            "Hummock",
        )
        .await;

        let opts = self.get_storage_opts();

        Ok(Arc::new(SstableStore::new(
            Arc::new(object_store),
            opts.data_directory,
            opts.block_cache_capacity_mb * (1 << 20),
            opts.meta_cache_capacity_mb * (1 << 20),
            0,
            TieredCache::none(),
        )))
    }
}

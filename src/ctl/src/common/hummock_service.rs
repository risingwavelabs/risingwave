// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::env;
use std::sync::Arc;

use anyhow::{anyhow, bail, Result};
use risingwave_common::config::StorageConfig;
use risingwave_rpc_client::MetaClient;
use risingwave_storage::hummock::hummock_meta_client::MonitoredHummockMetaClient;
use risingwave_storage::hummock::HummockStorage;
use risingwave_storage::monitor::{
    HummockMetrics, MonitoredStateStore, ObjectStoreMetrics, StateStoreMetrics,
};
use risingwave_storage::StateStoreImpl;

use super::MetaServiceOpts;

pub struct HummockServiceOpts {
    pub meta_opts: MetaServiceOpts,
    pub hummock_url: String,
}

pub struct Metrics {
    pub hummock_metrics: Arc<HummockMetrics>,
    pub state_store_metrics: Arc<StateStoreMetrics>,
    pub object_store_metrics: Arc<ObjectStoreMetrics>,
}

impl HummockServiceOpts {
    /// Recover hummock service options from env variable
    ///
    /// Currently, we will read these variables for meta:
    ///
    /// * `RW_HUMMOCK_URL`: meta service address
    pub fn from_env() -> Result<Self> {
        let meta_opts = MetaServiceOpts::from_env()?;

        let hummock_url = match env::var("RW_HUMMOCK_URL") {
            Ok(url) => {
                tracing::info!("using Hummock URL from `RW_HUMMOCK_URL`: {}", url);
                url
            }
            Err(_) => {
                bail!("env variable `RW_HUMMOCK_URL` not found, please do one of the following:\n* use `./risedev ctl` to start risectl.\n* `source .risingwave/config/risectl-env` or `source ~/risingwave-deploy/risectl-env` before running risectl.\n* manually set `RW_HUMMOCK_URL` in env variable.\nPlease also remember to add `use: minio` to risedev config.");
            }
        };
        Ok(Self {
            meta_opts,
            hummock_url,
        })
    }

    pub async fn create_hummock_store_with_metrics(
        &self,
    ) -> Result<(MetaClient, MonitoredStateStore<HummockStorage>, Metrics)> {
        let meta_client = self.meta_opts.create_meta_client().await?;

        // FIXME: allow specify custom config
        let mut config = StorageConfig::default();
        config.share_buffer_compaction_worker_threads_number = 0;

        tracing::info!("using Hummock config: {:#?}", config);

        let metrics = Metrics {
            hummock_metrics: Arc::new(HummockMetrics::unused()),
            state_store_metrics: Arc::new(StateStoreMetrics::unused()),
            object_store_metrics: Arc::new(ObjectStoreMetrics::unused()),
        };

        let state_store_impl = StateStoreImpl::new(
            &self.hummock_url,
            Arc::new(config),
            Arc::new(MonitoredHummockMetaClient::new(
                meta_client.clone(),
                metrics.hummock_metrics.clone(),
            )),
            metrics.state_store_metrics.clone(),
            metrics.object_store_metrics.clone(),
        )
        .await?;

        if let StateStoreImpl::HummockStateStore(hummock_state_store) = state_store_impl {
            Ok((meta_client, hummock_state_store, metrics))
        } else {
            Err(anyhow!("only Hummock state store is supported in risectl"))
        }
    }

    pub async fn create_hummock_store(
        &self,
    ) -> Result<(MetaClient, MonitoredStateStore<HummockStorage>)> {
        let (meta_client, hummock_client, _) = self.create_hummock_store_with_metrics().await?;
        Ok((meta_client, hummock_client))
    }
}

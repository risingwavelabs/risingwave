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

use risingwave_rpc_client::MetaClient;
use risingwave_storage::hummock::HummockStorage;
use risingwave_storage::monitor::MonitoredStateStore;
use tokio::sync::OnceCell;

use crate::common::hummock_service::{HummockServiceOpts, Metrics};
use crate::common::meta_service::MetaServiceOpts;

#[derive(Default)]
pub struct CtlContext {
    meta_client: OnceCell<MetaClient>,
    hummock: OnceCell<(MonitoredStateStore<HummockStorage>, Metrics)>,
}

impl CtlContext {
    pub async fn meta_client(&self) -> anyhow::Result<MetaClient> {
        self.meta_client
            .get_or_try_init(|| async {
                let meta_opts = MetaServiceOpts::from_env()?;
                meta_opts.create_meta_client().await
            })
            .await
            .cloned()
    }

    pub async fn hummock_store(&self, hummock_opts: HummockServiceOpts) -> anyhow::Result<MonitoredStateStore<HummockStorage>> {
        let (hummock, _) = self
            .hummock_store_with_metrics(hummock_opts)
            .await?;
        Ok(hummock)
    }

    pub async fn hummock_store_with_metrics(
        &self,
        mut hummock_opts: HummockServiceOpts
    ) -> anyhow::Result<(MonitoredStateStore<HummockStorage>, Metrics)> {
        let meta_client = self.meta_client().await?;
        self.hummock
            .get_or_try_init(|| async {
                hummock_opts
                    .create_hummock_store_with_metrics(&meta_client)
                    .await
            })
            .await
            .cloned()
    }

    pub async fn try_close(mut self) {
        tracing::info!("clean up context");
        if let Some(meta_client) = self.meta_client.take() {
            if let Err(e) = meta_client
                .unregister(meta_client.host_addr().clone())
                .await
            {
                tracing::warn!(
                    "failed to unregister ctl worker {}: {}",
                    meta_client.worker_id(),
                    e
                );
            }
        }
    }
}

// Copyright 2023 Singularity Data
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

use crate::common::hummock_service::{HummockServiceOpts, Metrics};
use crate::common::meta_service::MetaServiceOpts;

#[derive(Default)]
pub struct CtlContext {
    meta_client: tokio::sync::Mutex<CtlContextInner>,
}

#[derive(Default)]
struct CtlContextInner {
    meta_client: Option<MetaClient>,
    hummock: Option<(MonitoredStateStore<HummockStorage>, Metrics)>,
}

impl CtlContext {
    pub async fn get_meta_client(&self) -> anyhow::Result<MetaClient> {
        let mut guard = self.meta_client.lock().await;
        guard.get_meta_client().await
    }

    pub async fn get_hummock_store(&self) -> anyhow::Result<MonitoredStateStore<HummockStorage>> {
        let (hummock, _) = self.get_hummock_store_with_metrics().await?;
        Ok(hummock)
    }

    pub async fn get_hummock_store_with_metrics(
        &self,
    ) -> anyhow::Result<(MonitoredStateStore<HummockStorage>, Metrics)> {
        let mut guard = self.meta_client.lock().await;
        guard.get_hummock_store_with_metrics().await
    }

    pub async fn try_close(self) {
        tracing::info!("clean up context");
        let mut guard = self.meta_client.lock().await;
        if let Some(meta_client) = guard.meta_client.take() {
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

impl CtlContextInner {
    async fn get_meta_client(&mut self) -> anyhow::Result<MetaClient> {
        let meta_client = match &self.meta_client {
            None => {
                let meta_opts = MetaServiceOpts::from_env()?;
                let meta_client = meta_opts.create_meta_client().await?;
                self.meta_client = Some(meta_client.clone());
                meta_client
            }
            Some(meta_client) => meta_client.clone(),
        };
        Ok(meta_client)
    }

    async fn get_hummock_store_with_metrics(
        &mut self,
    ) -> anyhow::Result<(MonitoredStateStore<HummockStorage>, Metrics)> {
        let (hummock, metrics) = match &self.hummock {
            None => {
                let meta_client = self.get_meta_client().await?;
                let mut hummock_opts = HummockServiceOpts::from_env()?;
                let (hummock, metrics) = hummock_opts
                    .create_hummock_store_with_metrics(&meta_client)
                    .await?;
                self.hummock = Some((hummock.clone(), metrics.clone()));
                (hummock, metrics)
            }
            Some((hummock, metrics)) => (hummock.clone(), metrics.clone()),
        };
        Ok((hummock, metrics))
    }
}

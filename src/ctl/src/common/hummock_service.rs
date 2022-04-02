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

use anyhow::{anyhow, Result};
use risingwave_common::config::StorageConfig;
use risingwave_storage::hummock::HummockStorage;
use risingwave_storage::hummock::hummock_meta_client::MonitoredHummockMetaClient;
use risingwave_storage::monitor::{HummockMetrics, MonitoredStateStore, StateStoreMetrics};
use risingwave_storage::StateStoreImpl;

use super::MetaServiceOpts;

pub struct HummockServiceOpts {
    pub meta_opts: MetaServiceOpts,
    pub hummock_url: String,
}

impl HummockServiceOpts {
    /// Recover hummock service options from env variable
    ///
    /// Currently, we will read these variables for meta:
    ///
    /// * `RW_HUMMOCK_URL`: meta service address
    pub fn from_env() -> Result<Self> {
        let meta_opts = MetaServiceOpts::from_env()?;
        let hummock_url = env::var("RW_HUMMOCK_URL").unwrap_or_else(|_| {
            const DEFAULT_ADDR: &str = "hummock+minio://hummock:12345678@127.0.0.1:9301/hummock001";
            tracing::warn!(
                "`RW_HUMMOCK_URL` not found, using default hummock URL {}",
                DEFAULT_ADDR
            );
            DEFAULT_ADDR.to_string()
        });
        Ok(Self {
            meta_opts,
            hummock_url,
        })
    }

    pub async fn create_hummock_store(&self) -> Result<MonitoredStateStore<HummockStorage>> {
        let meta_client = self.meta_opts.create_meta_client().await?;

        // FIXME: allow specify custom config
        let config = StorageConfig::default();

        tracing::info!("using Hummock config: {:#?}", config);

        let state_store_impl = StateStoreImpl::new(
            &self.hummock_url,
            Arc::new(config),
            Arc::new(MonitoredHummockMetaClient::new(
                meta_client,
                Arc::new(HummockMetrics::unused()),
            )),
            Arc::new(StateStoreMetrics::unused()),
        )
        .await?;

        if let StateStoreImpl::HummockStateStore(hummock_state_store) = state_store_impl {
            Ok(hummock_state_store)
        } else {
            Err(anyhow!("only Hummock state store is supported in risectl"))
        }
    }
}

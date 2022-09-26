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

use risingwave_pb::hummock::WriteLimiterThreshold;

use crate::hummock::error::Result;
use crate::hummock::HummockManager;
use crate::model::{MetadataModel, MetadataModelResult};
use crate::storage::MetaStore;

/// Configuration that is persisted to meta store, and can be updated during runtime.
#[derive(Clone, Debug)]
pub struct HummockConfig {
    pub write_limiter_threshold: WriteLimiterThreshold,
}

impl HummockConfig {
    pub async fn new<S: MetaStore>(meta_store: &S) -> Result<Self> {
        let write_limiter_threshold = WriteLimiterThreshold::list(meta_store)
            .await?
            .pop()
            .unwrap_or(WriteLimiterThreshold {
                max_sub_level_number: 1000,
                max_delay_sec: 60,
                per_file_delay_sec: 0.1,
            });
        let config = Self {
            write_limiter_threshold,
        };
        tracing::info!("Hummock config:\n{:#?}", config);
        Ok(config)
    }
}

impl<S> HummockManager<S>
where
    S: MetaStore,
{
    pub async fn get_config(&self) -> HummockConfig {
        self.config.read().await.clone()
    }

    pub async fn set_write_limiter_threshold(
        &self,
        threshold: WriteLimiterThreshold,
    ) -> Result<()> {
        let mut guard = self.config.write().await;
        threshold.insert(self.env.meta_store()).await?;
        tracing::info!(
            "Set write_limiter_threshold: {:#?}\nPrevious value: {:#?}",
            threshold,
            guard.write_limiter_threshold
        );
        guard.write_limiter_threshold = threshold;
        Ok(())
    }
}

/// Column family name for hummock config.
const HUMMOCK_CONFIG_CF_NAME: &str = "cf/hummock_config";

const WRITE_LIMITER_THRESHOLD_KEY: &str = "write_limiter_threshold";
impl MetadataModel for WriteLimiterThreshold {
    type KeyType = String;
    type ProstType = WriteLimiterThreshold;

    fn cf_name() -> String {
        String::from(HUMMOCK_CONFIG_CF_NAME)
    }

    fn to_protobuf(&self) -> Self::ProstType {
        self.clone()
    }

    fn from_protobuf(prost: Self::ProstType) -> Self {
        prost
    }

    fn key(&self) -> MetadataModelResult<Self::KeyType> {
        Ok(String::from(WRITE_LIMITER_THRESHOLD_KEY))
    }
}

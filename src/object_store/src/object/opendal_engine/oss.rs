// Copyright 2025 RisingWave Labs
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

use std::sync::Arc;

use opendal::Operator;
use opendal::layers::LoggingLayer;
use opendal::services::Oss;
use risingwave_common::config::ObjectStoreConfig;

use super::{MediaType, OpendalObjectStore};
use crate::object::ObjectResult;
use crate::object::object_metrics::ObjectStoreMetrics;

impl OpendalObjectStore {
    /// create opendal oss engine.
    pub fn new_oss_engine(
        bucket: String,
        root: String,
        config: Arc<ObjectStoreConfig>,
        metrics: Arc<ObjectStoreMetrics>,
    ) -> ObjectResult<Self> {
        // Create oss backend builder.
        let mut builder = Oss::default().bucket(&bucket).root(&root);

        let endpoint = std::env::var("OSS_ENDPOINT")
            .unwrap_or_else(|_| panic!("OSS_ENDPOINT not found from environment variables"));
        let access_key_id = std::env::var("OSS_ACCESS_KEY_ID")
            .unwrap_or_else(|_| panic!("OSS_ACCESS_KEY_ID not found from environment variables"));
        let access_key_secret = std::env::var("OSS_ACCESS_KEY_SECRET").unwrap_or_else(|_| {
            panic!("OSS_ACCESS_KEY_SECRET not found from environment variables")
        });

        builder = builder
            .endpoint(&endpoint)
            .access_key_id(&access_key_id)
            .access_key_secret(&access_key_secret);

        let op: Operator = Operator::new(builder)?
            .layer(LoggingLayer::default())
            .finish();
        Ok(Self {
            op,
            media_type: MediaType::Oss,
            config,
            metrics,
        })
    }
}

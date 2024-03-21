// Copyright 2024 RisingWave Labs
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

use std::time::Duration;

use opendal::layers::{LoggingLayer, RetryLayer};
use opendal::services::S3;
use opendal::Operator;
use risingwave_common::config::ObjectStoreConfig;

use super::{EngineType, OpendalObjectStore};
use crate::object::ObjectResult;

impl OpendalObjectStore {
    /// create opendal s3 engine.
    pub fn new_s3_engine(
        bucket: String,
        object_store_config: ObjectStoreConfig,
    ) -> ObjectResult<Self> {
        // Create s3 builder.
        let mut builder = S3::default();
        builder.bucket(&bucket);
        // For AWS S3, there is no need to set an endpoint; for other S3 compatible object stores, it is necessary to set this field.
        if let Ok(endpoint_url) = std::env::var("RW_S3_ENDPOINT") {
            builder.endpoint(&endpoint_url);
        }

        if std::env::var("RW_IS_FORCE_PATH_STYLE").is_err() {
            builder.enable_virtual_host_style();
        }

        let op: Operator = Operator::new(builder)?
            .layer(LoggingLayer::default())
            .layer(
                RetryLayer::new()
                    .with_min_delay(Duration::from_millis(
                        object_store_config.s3.object_store_req_retry_interval_ms,
                    ))
                    .with_max_delay(Duration::from_millis(
                        object_store_config.s3.object_store_req_retry_max_delay_ms,
                    ))
                    .with_max_times(object_store_config.s3.object_store_req_retry_max_attempts)
                    .with_factor(1.0)
                    .with_jitter(),
            )
            .finish();
        Ok(Self {
            op,
            engine_type: EngineType::S3,
        })
    }
}

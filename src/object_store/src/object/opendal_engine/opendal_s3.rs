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

use opendal::layers::{LoggingLayer, RetryLayer};
use opendal::services::S3;
use opendal::Operator;

use super::{EngineType, OpendalObjectStore};
use crate::object::ObjectResult;

impl OpendalObjectStore {
    /// create opendal s3 engine.
    pub fn new_s3_engine(bucket: String, root: String) -> ObjectResult<Self> {
        // Create s3 builder.
        let mut builder = S3::default();
        builder.bucket(&bucket);
        builder.root(&root);

        // For AWS S3, there is no need to set an endpoint; for other S3 compatible object stores, it is necessary to set this field.
        if let Ok(endpoint_url) = std::env::var("RW_S3_ENDPOINT") {
            builder.endpoint(&endpoint_url);
        }

        if let Ok(region) = std::env::var("AWS_REGION") {
            builder.region(&region);
        } else {
            tracing::error!("aws s3 region is not set, bucket {}", bucket);
        }

        if let Ok(access) = std::env::var("AWS_ACCESS_KEY_ID") {
            builder.access_key_id(&access);
        } else {
            tracing::error!("access key id of aws s3 is not set, bucket {}", bucket);
        }

        if let Ok(secret) = std::env::var("AWS_SECRET_ACCESS_KEY") {
            builder.secret_access_key(&secret);
        } else {
            tracing::error!("secret access key of aws s3 is not set, bucket {}", bucket);
        }

        if std::env::var("RW_IS_FORCE_PATH_STYLE").is_err() {
            builder.enable_virtual_host_style();
        }

        builder.disable_config_load();
        let op: Operator = Operator::new(builder)?
            .layer(LoggingLayer::default())
            .layer(RetryLayer::default())
            .finish();
        Ok(Self {
            op,
            engine_type: EngineType::OpendalS3,
        })
    }
}

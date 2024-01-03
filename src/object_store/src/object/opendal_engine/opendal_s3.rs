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

use opendal::{layers::{LoggingLayer, RetryLayer}, services::S3};
use opendal::services::Gcs;
use opendal::Operator;

use super::{EngineType, OpendalObjectStore};
use crate::object::ObjectResult;

impl OpendalObjectStore {
    /// create opendal gcs engine.
    pub fn new_s3_engine(bucket: String, root: String) -> ObjectResult<Self> {
        // Create gcs backend builder.
        let mut builder = Gcs::default();

        builder.bucket(&bucket);

        builder.root(&root);

        // Create s3 builder.
        let mut builder = S3::default();
        builder.bucket(&bucket);
        builder.region(&s3_properties.region_name);

        if let Some(endpoint_url) = s3_properties.endpoint_url {
            builder.endpoint(&endpoint_url);
        }

        if let Some(access) = s3_properties.access {
            builder.access_key_id(&access);
        } else {
            tracing::error!(
                "access key id of aws s3 is not set, bucket {}",
                s3_properties.bucket_name
            );
        }

        if let Some(secret) = s3_properties.secret {
            builder.secret_access_key(&secret);
        } else {
            tracing::error!(
                "secret access key of aws s3 is not set, bucket {}",
                s3_properties.bucket_name
            );
        }

        builder.enable_virtual_host_style();

        if let Some(assume_role) = assume_role {
            builder.role_arn(&assume_role);
        }

        builder.disable_config_load();
        let op: Operator = Operator::new(builder)?
            .layer(LoggingLayer::default())
            .layer(RetryLayer::default())
            .finish();
        Ok(Self {
            op,
            engine_type: EngineType::Gcs,
        })
    }
}

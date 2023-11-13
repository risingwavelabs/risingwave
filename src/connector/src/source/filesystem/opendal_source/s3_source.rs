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

use std::marker::PhantomData;

use opendal::layers::{LoggingLayer, RetryLayer};
use opendal::services::S3;
use opendal::Operator;

use super::opendal_enumerator::{EngineType, OpendalEnumerator};
use super::OpenDALProperties;
use crate::source::filesystem::S3Properties;

impl<C: OpenDALProperties> OpendalEnumerator<C>
where
    C: Sized + Send + Clone + PartialEq + 'static + Sync,
{
    /// create opendal gcs engine.
    pub fn new_s3_source(s3_properties: S3Properties) -> anyhow::Result<Self> {
        // Create gcs backend builder.
        let mut builder = S3::default();

        builder.bucket(&s3_properties.bucket_name);

        builder.region(&s3_properties.region_name);

        if let Some(endpoint_url) = s3_properties.endpoint_url {
            builder.endpoint(&endpoint_url);
        }

        if let Some(access) = s3_properties.access {
            builder.access_key_id(&access);
        }

        if let Some(secret) = s3_properties.secret {
            builder.secret_access_key(&secret);
        }

        let op: Operator = Operator::new(builder)?
            .layer(LoggingLayer::default())
            .layer(RetryLayer::default())
            .finish();
        Ok(Self {
            op,
            engine_type: EngineType::S3,
            marker: PhantomData,
        })
    }
}

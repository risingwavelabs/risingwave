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
use opendal::services::Gcs;
use opendal::Operator;

use super::opendal_enumerator::{EngineType, OpendalEnumerator};
use super::{GcsProperties, OpenDALProperties};

impl<C: OpenDALProperties> OpendalEnumerator<C>
where
    C: Send + Clone + PartialEq + 'static + Sync,
{
    /// create opendal gcs engine.
    pub fn new_gcs_source(gcs_properties: GcsProperties) -> anyhow::Result<Self> {
        // Create gcs backend builder.
        let mut builder = Gcs::default();

        builder.bucket(&gcs_properties.bucket_name);

        // if credential env is set, use it. Otherwise, ADC will be used.
        let cred = std::env::var("GOOGLE_APPLICATION_CREDENTIALS");
        if let Ok(cred) = cred {
            builder.credential(&cred);
        }
        let op: Operator = Operator::new(builder)?
            .layer(LoggingLayer::default())
            .layer(RetryLayer::default())
            .finish();
        Ok(Self {
            op,
            engine_type: EngineType::Gcs,
            marker: PhantomData,
        })
    }
}

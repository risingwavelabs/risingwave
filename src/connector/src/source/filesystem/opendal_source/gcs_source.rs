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

use anyhow::Context;
use opendal::layers::{LoggingLayer, RetryLayer};
use opendal::services::Gcs;
use opendal::Operator;

use super::opendal_enumerator::OpendalEnumerator;
use super::{GcsProperties, OpendalSource};
use crate::source::filesystem::s3::enumerator::get_prefix;

impl<Src: OpendalSource> OpendalEnumerator<Src> {
    /// create opendal gcs source.
    pub fn new_gcs_source(gcs_properties: GcsProperties) -> anyhow::Result<Self> {
        // Create gcs builder.
        let mut builder = Gcs::default();

        builder.bucket(&gcs_properties.bucket_name);

        // if credential env is set, use it. Otherwise, ADC will be used.
        let cred = gcs_properties.credential;
        if let Some(cred) = cred {
            builder.credential(&cred);
        }

        if let Some(service_account) = gcs_properties.service_account {
            builder.service_account(&service_account);
        }
        let op: Operator = Operator::new(builder)?
            .layer(LoggingLayer::default())
            .layer(RetryLayer::default())
            .finish();

        let (prefix, matcher) = if let Some(pattern) = gcs_properties.match_pattern.as_ref() {
            let prefix = get_prefix(pattern);
            let matcher = glob::Pattern::new(pattern)
                .with_context(|| format!("Invalid match_pattern: {}", pattern))?;
            (Some(prefix), Some(matcher))
        } else {
            (None, None)
        };
        Ok(Self {
            op,
            prefix,
            matcher,
            marker: PhantomData,
        })
    }
}

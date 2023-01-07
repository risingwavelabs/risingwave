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

use std::collections::HashMap;

use async_trait::async_trait;
use aws_sdk_s3::client::Client;
use itertools::Itertools;

use crate::aws_utils::{default_conn_config, s3_client, AwsConfigV2};
use crate::source::filesystem::file_common::FsSplit;
use crate::source::filesystem::s3::S3Properties;
use crate::source::SplitEnumerator;

#[derive(Debug, Clone)]
pub struct S3SplitEnumerator {
    bucket_name: String,
    client: Client,
}

#[async_trait]
impl SplitEnumerator for S3SplitEnumerator {
    type Properties = S3Properties;
    type Split = FsSplit;

    async fn new(properties: Self::Properties) -> anyhow::Result<Self> {
        let config = AwsConfigV2::from(HashMap::from(properties.clone()));
        let sdk_config = config.load_config(None).await;
        let s3_client = s3_client(&sdk_config, Some(default_conn_config()));
        Ok(S3SplitEnumerator {
            bucket_name: properties.bucket_name,
            client: s3_client,
        })
    }

    async fn list_splits(&mut self) -> anyhow::Result<Vec<Self::Split>> {
        let list_obj_out = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket_name)
            .send()
            .await?;

        let objects = list_obj_out.contents();
        let splits = objects
            .map(|objs| {
                objs.iter()
                    .map(|obj| {
                        let obj_name = obj.key().unwrap().to_string();
                        FsSplit::new(obj_name, 0, obj.size() as usize)
                    })
                    .collect_vec()
            })
            .unwrap_or_else(Vec::default);
        Ok(splits)
    }
}

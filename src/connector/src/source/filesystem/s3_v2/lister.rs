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

use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use aws_sdk_s3::error::DisplayErrorContext;
use aws_sdk_s3::Client;
use futures_async_stream::try_stream;
use itertools::Itertools;
use risingwave_common::error::RwError;
use risingwave_common::types::Timestamp;

use crate::aws_auth::AwsAuthProps;
use crate::aws_utils::{default_conn_config, s3_client};
use crate::source::filesystem::file_common::{FsPage, FsSplit};
use crate::source::filesystem::S3Properties;
use crate::source::{BoxTryStream, SourceLister};

/// Get the prefix from a glob
fn get_prefix(glob: &str) -> String {
    let mut escaped = false;
    let mut escaped_filter = false;
    glob.chars()
        .take_while(|c| match (c, &escaped) {
            ('*', false) => false,
            ('[', false) => false,
            ('{', false) => false,
            ('\\', false) => {
                escaped = true;
                true
            }
            (_, false) => true,
            (_, true) => {
                escaped = false;
                true
            }
        })
        .filter(|c| match (c, &escaped_filter) {
            (_, true) => {
                escaped_filter = false;
                true
            }
            ('\\', false) => {
                escaped_filter = true;
                false
            }
            (_, _) => true,
        })
        .collect()
}

pub struct S3SourceLister {
    client: Client,
    // prefix is used to reduce the number of objects to be listed
    prefix: Option<String>,
    matcher: Option<glob::Pattern>,
    bucket_name: String,
}

impl S3SourceLister {
    #[try_stream(boxed, ok = Vec<FsPage>, error = RwError)]
    async fn paginate_inner(self) {
        'round: loop { // start a new round
            let mut next_continuation_token = None;
            'truncated: loop { // loop to paginate
                let mut req = self
                    .client
                    .list_objects_v2()
                    .bucket(&self.bucket_name)
                    .set_prefix(self.prefix.clone());
                if let Some(continuation_token) = next_continuation_token.take() {
                    req = req.continuation_token(continuation_token);
                }
                let mut res = req
                    .send()
                    .await
                    .map_err(|e| anyhow!(DisplayErrorContext(e)))?;

                yield res
                    .contents
                    .take()
                    .unwrap_or_default()
                    .iter()
                    .filter(|obj| obj.key().is_some())
                    .filter(|obj| {
                        self.matcher
                            .as_ref()
                            .map(|m| m.matches(obj.key().unwrap()))
                            .unwrap_or(true)
                    })
                    .into_iter()
                    .map(|obj| {
                        let aws_ts = obj.last_modified().unwrap();
                        FsPage::new(
                            obj.key().unwrap().to_owned(),
                            obj.size() as usize,
                            Timestamp::from_timestamp_uncheck(aws_ts.secs(), aws_ts.subsec_nanos()),
                        )
                    })
                    .collect_vec();

                if res.is_truncated() {
                    next_continuation_token = Some(res.next_continuation_token.unwrap())
                } else {
                    break 'truncated;
                }
            }
        }
    }
}

#[async_trait]
impl SourceLister for S3SourceLister {
    type Properties = S3Properties;
    type Split = FsSplit;

    async fn new(properties: Self::Properties) -> Result<Self> {
        let config = AwsAuthProps::from(&properties);
        let sdk_config = config.build_config().await?;
        let s3_client = s3_client(&sdk_config, Some(default_conn_config()));
        let (prefix, matcher) = if let Some(pattern) = properties.match_pattern.as_ref() {
            let prefix = get_prefix(pattern);
            let matcher = glob::Pattern::new(pattern)
                .with_context(|| format!("Invalid match_pattern: {}", pattern))?;
            (Some(prefix), Some(matcher))
        } else {
            (None, None)
        };

        Ok(Self {
            bucket_name: properties.bucket_name,
            matcher,
            prefix,
            client: s3_client,
        })
    }

    fn paginate(self) -> BoxTryStream<Vec<FsPage>> {
        self.paginate_inner()
    }
}

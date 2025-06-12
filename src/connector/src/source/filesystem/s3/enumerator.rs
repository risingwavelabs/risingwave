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

use anyhow::Context;
use async_trait::async_trait;
use aws_sdk_s3::client::Client;
use itertools::Itertools;

use crate::aws_utils::{default_conn_config, s3_client};
use crate::connector_common::AwsAuthProps;
use crate::error::ConnectorResult;
use crate::source::filesystem::file_common::LegacyFsSplit;
use crate::source::filesystem::s3::LegacyS3Properties;
use crate::source::{SourceEnumeratorContextRef, SplitEnumerator};

/// Get the prefix from a glob
pub fn get_prefix(glob: &str) -> String {
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

#[derive(Debug, Clone)]
pub struct LegacyS3SplitEnumerator {
    pub(crate) bucket_name: String,
    // prefix is used to reduce the number of objects to be listed
    pub(crate) prefix: Option<String>,
    pub(crate) matcher: Option<glob::Pattern>,
    pub(crate) client: Client,

    // token get the next page, used when the current page is truncated
    pub(crate) next_continuation_token: Option<String>,
}

#[async_trait]
impl SplitEnumerator for LegacyS3SplitEnumerator {
    type Properties = LegacyS3Properties;
    type Split = LegacyFsSplit;

    async fn new(
        properties: Self::Properties,
        _context: SourceEnumeratorContextRef,
    ) -> crate::error::ConnectorResult<Self> {
        let config = AwsAuthProps::from(&properties);
        let sdk_config = config.build_config().await?;
        let s3_client = s3_client(&sdk_config, Some(default_conn_config()));
        let properties = properties.common;
        let (prefix, matcher) = if let Some(pattern) = properties.match_pattern.as_ref() {
            let prefix = get_prefix(pattern);
            let matcher = glob::Pattern::new(pattern)
                .with_context(|| format!("Invalid match_pattern: {}", pattern))?;
            (Some(prefix), Some(matcher))
        } else {
            (None, None)
        };

        Ok(LegacyS3SplitEnumerator {
            bucket_name: properties.bucket_name,
            matcher,
            prefix,
            client: s3_client,
            next_continuation_token: None,
        })
    }

    async fn list_splits(&mut self) -> crate::error::ConnectorResult<Vec<Self::Split>> {
        // fetch one page as validation, no need to get all pages
        let (_, _) = self.get_next_page::<LegacyFsSplit>().await?;

        Ok(vec![LegacyFsSplit {
            name: "empty_split".to_owned(),
            offset: 0,
            size: 0,
        }])
    }
}

#[async_trait]
pub trait FsListInner: Sized {
    // fixme: better to implement as an Iterator, but the last page still have some contents
    async fn get_next_page<T: for<'a> From<&'a aws_sdk_s3::types::Object>>(
        &mut self,
    ) -> ConnectorResult<(Vec<T>, bool)>;
}

#[async_trait]
impl FsListInner for LegacyS3SplitEnumerator {
    async fn get_next_page<T: for<'a> From<&'a aws_sdk_s3::types::Object>>(
        &mut self,
    ) -> ConnectorResult<(Vec<T>, bool)> {
        let mut has_finished = false;
        let mut req = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket_name)
            .set_prefix(self.prefix.clone());
        if let Some(continuation_token) = self.next_continuation_token.take() {
            req = req.continuation_token(continuation_token);
        }
        let mut res = req
            .send()
            .await
            .with_context(|| format!("failed to list objects in bucket `{}`", self.bucket_name))?;
        if res.is_truncated().unwrap_or_default() {
            self.next_continuation_token
                .clone_from(&res.next_continuation_token);
        } else {
            has_finished = true;
            self.next_continuation_token = None;
        }
        let objects = res.contents.take().unwrap_or_default();
        let matched_objs: Vec<T> = objects
            .iter()
            .filter(|obj| obj.key().is_some())
            .filter(|obj| {
                self.matcher
                    .as_ref()
                    .map(|m| m.matches(obj.key().unwrap()))
                    .unwrap_or(true)
            })
            .map(T::from)
            .collect_vec();
        Ok((matched_objs, has_finished))
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;

    #[test]
    fn test_get_prefix() {
        assert_eq!(&get_prefix("a/"), "a/");
        assert_eq!(&get_prefix("a/**"), "a/");
        assert_eq!(&get_prefix("[ab]*"), "");
        assert_eq!(&get_prefix("a/{a,b}*"), "a/");
        assert_eq!(&get_prefix(r"a/\{a,b}"), "a/{a,b}");
        assert_eq!(&get_prefix(r"a/\[ab]"), "a/[ab]");
    }

    use super::*;
    use crate::source::SourceEnumeratorContext;
    use crate::source::filesystem::s3::S3PropertiesCommon;
    #[tokio::test]
    #[ignore]
    async fn test_s3_split_enumerator() {
        let props = S3PropertiesCommon {
            region_name: "ap-southeast-1".to_owned(),
            bucket_name: "mingchao-s3-source".to_owned(),
            match_pattern: Some("happy[0-9].csv".to_owned()),
            access: None,
            secret: None,
            endpoint_url: None,
            enable_config_load: None,
        };
        let mut enumerator =
            LegacyS3SplitEnumerator::new(props.into(), SourceEnumeratorContext::dummy().into())
                .await
                .unwrap();
        let splits = enumerator.list_splits().await.unwrap();
        let names = splits.into_iter().map(|split| split.name).collect_vec();
        assert_eq!(names.len(), 2);
        assert!(names.contains(&"happy1.csv".to_owned()));
        assert!(names.contains(&"happy2.csv".to_owned()));
    }
}

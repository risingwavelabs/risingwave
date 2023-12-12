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

use anyhow::anyhow;
use async_trait::async_trait;
use aws_sdk_s3::error::DisplayErrorContext;
use aws_sdk_s3::types::Object;
use itertools::Itertools;

use crate::source::filesystem::{FsPageItem, S3SplitEnumerator};
use crate::source::{FsFilterCtrlCtx, FsListInner};

#[async_trait]
impl FsListInner for S3SplitEnumerator {
    async fn get_next_page<T: for<'a> From<&'a Object>>(
        &mut self,
    ) -> anyhow::Result<(Vec<T>, bool)> {
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
            .map_err(|e| anyhow!(DisplayErrorContext(e)))?;
        if res.is_truncated().unwrap_or_default() {
            self.next_continuation_token = res.next_continuation_token.clone();
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

    fn filter_policy(&self, _ctx: &FsFilterCtrlCtx, _page_num: usize, _item: &FsPageItem) -> bool {
        true
    }
}

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
use crate::source::filesystem::file_common::FsPage;
use crate::source::filesystem::{FsPageItem, S3Properties, S3SplitEnumerator};
use crate::source::{BoxTryStream, FsFilterCtrlCtx, FsListInner, FsSourceList};

#[async_trait]
impl FsSourceList for S3SplitEnumerator {
    fn paginate(self) -> BoxTryStream<FsPage> {
        self.paginate_inner()
    }

    fn ctrl_policy(_ctx: FsFilterCtrlCtx, _page_num: usize, _item: &FsPageItem) -> bool {
        true
    }
}

impl S3SplitEnumerator {
    #[try_stream(boxed, ok = FsPage, error = RwError)]
    async fn paginate_inner(mut self) {
        loop {
            let ctx: FsFilterCtrlCtx;
            let mut page_num = 0;
            'inner: loop {
                let (fs_page, has_finished) = self.get_next_page::<FsPageItem>().await?;
                let matched_items = fs_page
                    .into_iter()
                    .filter(|item| Self::ctrl_policy(&ctx, page_num, item))
                    .collect_vec();
                page_num += 1;
                yield matched_items;
                if has_finished {
                    break 'inner;
                }
            }
            self.next_continuation_token = None;
        }
    }
}

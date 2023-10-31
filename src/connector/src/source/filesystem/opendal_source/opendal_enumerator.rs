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

use async_nats::jetstream::object_store::ObjectMetadata;
use opendal::{Operator, Lister, Metakey};
use futures::{stream::{self, BoxStream}, StreamExt};

use crate::source::FsListInner;
pub struct OpendalSource {
    pub(crate) op: Operator,
    pub(crate) engine_type: EngineType,
}

impl OpendalSource{
    async fn list(&self, prefix: &str) ->  anyhow::Result<ObjectMetadataIter> {
        let object_lister = self
            .op
            .lister_with(prefix)
            .delimiter("")
            .metakey(Metakey::ContentLength | Metakey::ContentType)
            .await?;

        let stream = stream::unfold(object_lister, |mut object_lister| async move {
            match object_lister.next().await {
                Some(Ok(object)) => {
                    let key = object.path().to_string();
                    let om = object.metadata();
                    let last_modified = match om.last_modified() {
                        Some(t) => t.timestamp() as f64,
                        None => 0_f64,
                    };
                    let total_size = om.content_length() as usize;
                    let metadata = ObjectListMetadata {
                        key,
                        last_modified,
                        total_size,
                    };
                    Some((Ok(metadata), object_lister))
                }
                Some(Err(err)) => Some((Err(err.into()), object_lister)),
                None => None,
            }
        });

        Ok(stream.boxed())
    }
}

pub type ObjectMetadataIter = BoxStream<'static,  anyhow::Result<ObjectListMetadata>>;

#[derive(Clone)]
pub enum EngineType {

    Gcs,
}


#[derive(Debug, Clone, PartialEq)]
pub struct ObjectListMetadata {
    // Full path
    pub key: String,
    // Seconds since unix epoch.
    pub last_modified: f64,
    pub total_size: usize,
}

#[async_trait]
impl FsListInner for OpendalSource {
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
        if res.is_truncated() {
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

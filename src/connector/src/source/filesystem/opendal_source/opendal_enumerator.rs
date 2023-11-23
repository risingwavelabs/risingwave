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

use async_trait::async_trait;
use chrono::NaiveDateTime;
use futures::stream::{self, BoxStream};
use futures::StreamExt;
use opendal::{Metakey, Operator};
use risingwave_common::types::Timestamp;

use super::OpenDalProperties;
use crate::source::filesystem::{FsPageItem, OpendalFsSplit};
use crate::source::{SourceEnumeratorContextRef, SplitEnumerator};

#[derive(Debug, Clone)]
pub struct OpendalEnumerator<C: OpenDalProperties>
where
    C: Send + Clone + Sized + PartialEq + 'static + Sync,
{
    pub(crate) op: Operator,
    pub(crate) engine_type: EngineType,
    // prefix is used to reduce the number of objects to be listed
    pub(crate) prefix: Option<String>,
    pub(crate) matcher: Option<glob::Pattern>,
    pub(crate) marker: PhantomData<C>,
}

#[derive(Debug, Clone)]
pub enum EngineType {
    Gcs,
    S3,
}

#[async_trait]
impl<C: OpenDalProperties> SplitEnumerator for OpendalEnumerator<C>
where
    C: Sized + Send + Clone + PartialEq + 'static + Sync,
{
    type Properties = C;
    type Split = OpendalFsSplit<C>;

    async fn new(
        properties: Self::Properties,
        _context: SourceEnumeratorContextRef,
    ) -> anyhow::Result<OpendalEnumerator<C>> {
        Self::Properties::new_enumerator(properties)
    }

    async fn list_splits(&mut self) -> anyhow::Result<Vec<OpendalFsSplit<C>>> {
        Ok(vec![])
    }
}

impl<C: OpenDalProperties> OpendalEnumerator<C>
where
    C: Send + Clone + Sized + PartialEq + 'static + Sync,
{
    pub async fn list(&self) -> anyhow::Result<ObjectMetadataIter> {
        let _prefix = match &self.prefix {
            Some(prefix) => prefix,
            None => "",
        };

        // Currently, we need to do full list and then filter the prefix and matcher,
        // After OpenDAL implementing the list prefix, we can use the user-specified prefix.
        // https://github.com/apache/incubator-opendal/issues/3247

        let object_lister = self
            .op
            .lister_with("/")
            .delimiter("")
            .metakey(Metakey::ContentLength | Metakey::LastModified)
            .await?;
        let stream = stream::unfold(object_lister, |mut object_lister| async move {
            match object_lister.next().await {
                Some(Ok(object)) => {
                    // todo: manual filtering prefix
                    let name = object.path().to_string();
                    let om = object.metadata();

                    let t = match om.last_modified() {
                        Some(t) => t.naive_utc(),
                        None => {
                            let timestamp = 0;
                            NaiveDateTime::from_timestamp_opt(timestamp, 0).unwrap()
                        }
                    };
                    let timestamp = Timestamp::new(t);
                    let size = om.content_length() as i64;
                    let metadata = FsPageItem {
                        name,
                        size,
                        timestamp,
                    };
                    Some((Ok(metadata), object_lister))
                }
                Some(Err(err)) => {
                    tracing::error!("list object fail, err {}", err);
                    Some((Err(err.into()), object_lister))
                }
                None => {
                    tracing::info!("list object completed.");
                    None
                }
            }
        });

        Ok(stream.boxed())
    }

    pub fn get_prefix(&self) -> &Option<String> {
        &self.prefix
    }

    pub fn get_matcher(&self) -> &Option<glob::Pattern> {
        &self.matcher
    }
}
pub type ObjectMetadataIter = BoxStream<'static, anyhow::Result<FsPageItem>>;

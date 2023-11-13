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

use async_trait::async_trait;
use chrono::NaiveDateTime;
use futures::stream::{self, BoxStream};
use futures::StreamExt;
use opendal::{Metakey, Operator};
use risingwave_common::types::Timestamp;

use super::GcsProperties;
use crate::source::filesystem::file_common::Gcs;
use crate::source::filesystem::{FsPageItem, OpendalFsSplit};
use crate::source::{SourceEnumeratorContextRef, SplitEnumerator};
pub struct OpendalConnector {
    pub(crate) op: Operator,
    pub(crate) engine_type: EngineType,
}

#[derive(Clone)]
pub enum EngineType {
    Gcs,
    S3,
}

#[async_trait]
impl SplitEnumerator for OpendalConnector {
    type Properties = GcsProperties;
    type Split = OpendalFsSplit<Gcs>;

    async fn new(
        properties: Self::Properties,
        _context: SourceEnumeratorContextRef,
    ) -> anyhow::Result<OpendalConnector> {
        // match properties {
        //     OpenDALProperties::GcsProperties(gcs_properties) => {
        //         OpendalConnector::new_gcs_source(gcs_properties)
        //     }
        //     OpenDALProperties::S3Properties(s3_properties) => {
        //         OpendalConnector::new_s3_source(s3_properties)
        //     }
        // }
        OpendalConnector::new_gcs_source(properties)
    }

    async fn list_splits(&mut self) -> anyhow::Result<Vec<OpendalFsSplit<Gcs>>> {
        todo!()
    }
}

impl OpendalConnector {
    pub async fn list(&self, prefix: &str) -> anyhow::Result<ObjectMetadataIter> {
        let object_lister = self
            .op
            .lister_with(prefix)
            .delimiter("")
            .metakey(Metakey::ContentLength | Metakey::ContentType)
            .await?;

        let stream = stream::unfold(object_lister, |mut object_lister| async move {
            match object_lister.next().await {
                Some(Ok(object)) => {
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
                Some(Err(err)) => Some((Err(err.into()), object_lister)),
                None => None,
            }
        });

        Ok(stream.boxed())
    }
}
pub type ObjectMetadataIter = BoxStream<'static, anyhow::Result<FsPageItem>>;

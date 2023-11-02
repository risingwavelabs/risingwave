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
use opendal::{Lister, Metakey, Operator};
use risingwave_common::types::Timestamp;

use super::{GCSProperties, OpenDALProperties};
use crate::source::filesystem::{FsPageItem, GcsSplit};
use crate::source::{FsListInner, SourceEnumeratorContextRef, SplitEnumerator};
pub struct OpenDALConnector {
    pub(crate) op: Operator,
    pub(crate) engine_type: EngineType,
}

#[derive(Clone)]
pub enum EngineType {
    Gcs,
    S3,
}

#[async_trait]
impl SplitEnumerator for OpenDALConnector {
    type Properties = GCSProperties;
    type Split = GcsSplit;

    async fn new(
        properties: Self::Properties,
        _context: SourceEnumeratorContextRef,
    ) -> anyhow::Result<OpenDALConnector> {
        // match properties {
        //     OpenDALProperties::GCSProperties(gcs_properties) => {
        //         OpenDALConnector::new_gcs_source(gcs_properties)
        //     }
        //     OpenDALProperties::S3Properties(s3_properties) => {
        //         OpenDALConnector::new_s3_source(s3_properties)
        //     }
        // }
        OpenDALConnector::new_gcs_source(properties)
    }

    async fn list_splits(&mut self) -> anyhow::Result<Vec<GcsSplit>> {
        todo!()
    }
}

impl OpenDALConnector {
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
                            NaiveDateTime::from_timestamp(timestamp, 0)
                        }
                    };
                    let timestamp = Timestamp::new(t);
                    let size = om.content_length() as i64;
                    let metadata = FsPageItem {
                        name,
                        timestamp,
                        size,
                    };
                    Some((Ok(metadata), object_lister))
                }
                Some(Err(err)) => Some((Err(err.into()), object_lister)),
                None => None,
            }
        });

        Ok(stream.boxed())
    }

    // #[try_stream(boxed, ok = StreamChunkWithState, error = RwError)]
    // async fn into_chunk_stream(self) {
    //     for split in self.splits {
    //         let actor_id = self.source_ctx.source_info.actor_id.to_string();
    //         let source_id = self.source_ctx.source_info.source_id.to_string();
    //         let source_ctx = self.source_ctx.clone();

    //         let split_id = split.id();

    //         let data_stream = Self::stream_read_object(
    //             self.s3_client.clone(),
    //             self.bucket_name.clone(),
    //             split,
    //             self.source_ctx.clone(),
    //         );

    //         let parser =
    //             ByteStreamSourceParserImpl::create(self.parser_config.clone(), source_ctx).await?;
    //         let msg_stream = if matches!(
    //             parser,
    //             ByteStreamSourceParserImpl::Json(_) | ByteStreamSourceParserImpl::Csv(_)
    //         ) {
    //             parser.into_stream(nd_streaming::split_stream(data_stream))
    //         } else {
    //             parser.into_stream(data_stream)
    //         };
    //         #[for_await]
    //         for msg in msg_stream {
    //             let msg = msg?;
    //             self.source_ctx
    //                 .metrics
    //                 .partition_input_count
    //                 .with_label_values(&[&actor_id, &source_id, &split_id])
    //                 .inc_by(msg.chunk.cardinality() as u64);
    //             yield msg;
    //         }
    //     }
    // }
}
pub type ObjectMetadataIter = BoxStream<'static, anyhow::Result<FsPageItem>>;

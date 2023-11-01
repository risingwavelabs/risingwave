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
use async_trait::async_trait;
use futures::stream::{self, BoxStream};
use futures::StreamExt;
use opendal::{Lister, Metakey, Operator};
use risingwave_common::types::Timestamp;

use crate::source::filesystem::FsPageItem;
use crate::source::{FsListInner, SourceEnumeratorContextRef, SplitEnumerator};
pub struct OpenDALSplitEnumerator {
    pub(crate) op: Operator,
    pub(crate) engine_type: EngineType,
}

impl OpenDALSplitEnumerator {
    pub async fn list(&self, prefix: &str) -> anyhow::Result<FsPageItem> {
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
                        Some(t) => t.timestamp() as f64,
                        None => 0_f64,
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
}

#[derive(Clone)]
pub enum EngineType {
    Gcs,
}

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
use opendal::{Operator, Lister, Metakey};
use futures::{stream::{self, BoxStream}, StreamExt};

use crate::source::{FsListInner, SplitEnumerator, SourceEnumeratorContextRef};
pub struct OpenDALSplitEnumerator {
    pub(crate) op: Operator,
    pub(crate) engine_type: EngineType,
}

#[async_trait]
impl SplitEnumerator for OpenDALSplitEnumerator{
    async fn new(
        properties: Self::Properties,
        _context: SourceEnumeratorContextRef,
    ) -> anyhow::Result<Self> {
        todo!()
    }

    async fn list_splits(&mut self) -> anyhow::Result<Vec<Self::Split>> {
        todo!()
    }
}
impl OpenDALSplitEnumerator{
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




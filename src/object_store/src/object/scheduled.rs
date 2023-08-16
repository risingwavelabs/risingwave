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

use bytes::Bytes;
use tokio::io::AsyncRead;

use super::{BoxedStreamingUploader, ObjectMetadata};
use crate::object::{BlockLocation, ObjectResult, ObjectStore};

pub struct ScheduledObjectStore<OS>
where
    OS: ObjectStore,
{
    store: OS,
}

#[async_trait::async_trait]
impl<OS> ObjectStore for ScheduledObjectStore<OS>
where
    OS: ObjectStore,
{
    fn get_object_prefix(&self, obj_id: u64) -> String {
        self.store.get_object_prefix(obj_id)
    }

    async fn upload(&self, path: &str, obj: Bytes) -> ObjectResult<()> {
        self.store.upload(path, obj).await
    }

    async fn streaming_upload(&self, path: &str) -> ObjectResult<BoxedStreamingUploader> {
        self.store.streaming_upload(path).await
    }

    async fn read(&self, path: &str, block_loc: Option<BlockLocation>) -> ObjectResult<Bytes> {
        self.store.read(path, block_loc).await
    }

    async fn readv(&self, path: &str, block_locs: &[BlockLocation]) -> ObjectResult<Vec<Bytes>> {
        self.store.readv(path, block_locs).await
    }

    async fn streaming_read(
        &self,
        path: &str,
        start_pos: Option<usize>,
    ) -> ObjectResult<Box<dyn AsyncRead + Unpin + Send + Sync>> {
        self.store.streaming_read(path, start_pos).await
    }

    async fn metadata(&self, path: &str) -> ObjectResult<ObjectMetadata> {
        self.store.metadata(path).await
    }

    async fn delete(&self, path: &str) -> ObjectResult<()> {
        self.store.delete(path).await
    }

    async fn delete_objects(&self, paths: &[String]) -> ObjectResult<()> {
        self.store.delete_objects(paths).await
    }

    async fn list(&self, prefix: &str) -> ObjectResult<Vec<ObjectMetadata>> {
        self.store.list(prefix).await
    }

    fn store_media_type(&self) -> &'static str {
        self.store.store_media_type()
    }
}

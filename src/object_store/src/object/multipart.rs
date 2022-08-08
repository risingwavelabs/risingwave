// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use bytes::Bytes;

use crate::object::object_metrics::ObjectStoreMetrics;
use crate::object::{
    object_store_impl_method_body, parse_object_store_path, InMemMultipartUploadHandle,
    MonitoredObjectStore, ObjectResult, ObjectStore, ObjectStoreImpl, ObjectStorePath,
    S3MultipartUploadHandle, S3PartIdGenerator,
};

/// Unique identifier of a part of a multipart upload task.
pub type PartId = u64;

/// Types that support uploading an object in multiple parts.
#[async_trait::async_trait]
pub trait MultipartUpload: Send + Sync {
    type Handle: MultipartUploadHandle;
    type IdGen: PartIdGenerator;

    /// Initiate a multipart upload task.
    /// The returned handle can be used to upload the parts out of order and finish the task.
    ///
    /// It will be the caller's responsibility to ensure the multipart upload is
    /// finished.
    async fn create_multipart_upload(
        &self,
        path: &str,
    ) -> ObjectResult<(Self::Handle, Self::IdGen)>;
}

/// Generator for part id in ascending order.
pub trait PartIdGenerator: Sync {
    fn gen(&self) -> ObjectResult<PartId>;
}

pub struct LocalPartIdGenerator {
    next_id: AtomicU64,
}

impl LocalPartIdGenerator {
    pub fn new() -> LocalPartIdGenerator {
        Self {
            next_id: AtomicU64::new(0),
        }
    }
}

impl Default for LocalPartIdGenerator {
    fn default() -> Self {
        Self::new()
    }
}

impl PartIdGenerator for LocalPartIdGenerator {
    fn gen(&self) -> ObjectResult<PartId> {
        Ok(self.next_id.fetch_add(1, Ordering::Relaxed))
    }
}

/// Each handle corresponds to one object.
///
/// The parts can be uploaded out of order. Upon finish, the parts will be logically
/// assembled in ascending order of `part_id`.
#[async_trait::async_trait]
pub trait MultipartUploadHandle: Send + Sync + Clone {
    /// Upload a part of the object.
    async fn upload_part(&self, part_id: PartId, part: Bytes) -> ObjectResult<()>;

    /// All the parts need to be uploaded before calling `finish`.
    async fn finish(self) -> ObjectResult<()>;
}

pub enum PartIdGeneratorImpl {
    S3(S3PartIdGenerator),
    Local(LocalPartIdGenerator),
}

impl PartIdGenerator for PartIdGeneratorImpl {
    fn gen(&self) -> ObjectResult<PartId> {
        match self {
            Self::S3(inner) => inner.gen(),
            Self::Local(inner) => inner.gen(),
        }
    }
}

#[derive(Clone)]
pub enum MultipartUploadHandleImpl {
    S3(S3MultipartUploadHandle),
    InMem(InMemMultipartUploadHandle),
}

#[async_trait::async_trait]
impl MultipartUploadHandle for MultipartUploadHandleImpl {
    async fn upload_part(&self, part_id: PartId, part: Bytes) -> ObjectResult<()> {
        match self {
            Self::S3(inner) => inner.upload_part(part_id, part).await,
            Self::InMem(inner) => inner.upload_part(part_id, part).await,
        }
    }

    async fn finish(self) -> ObjectResult<()> {
        match self {
            Self::S3(inner) => inner.finish().await,
            Self::InMem(inner) => inner.finish().await,
        }
    }
}

#[async_trait::async_trait]
impl MultipartUpload for ObjectStoreImpl {
    type Handle = MonitoredMultipartUploadHandle<MultipartUploadHandleImpl>;
    type IdGen = PartIdGeneratorImpl;

    async fn create_multipart_upload(
        &self,
        path: &str,
    ) -> ObjectResult<(Self::Handle, Self::IdGen)> {
        object_store_impl_method_body!(self, create_multipart_upload, path)
    }
}

#[derive(Clone)]
pub struct MonitoredMultipartUploadHandle<Handle: MultipartUploadHandle> {
    inner: Handle,
    object_store_metrics: Arc<ObjectStoreMetrics>,
}

impl<Handle: MultipartUploadHandle> MonitoredMultipartUploadHandle<Handle> {
    pub fn new(handle: Handle, object_store_metrics: Arc<ObjectStoreMetrics>) -> Self {
        Self {
            inner: handle,
            object_store_metrics,
        }
    }
}

#[async_trait::async_trait]
impl<Handle: MultipartUploadHandle> MultipartUploadHandle
    for MonitoredMultipartUploadHandle<Handle>
{
    async fn upload_part(&self, part_id: PartId, part: Bytes) -> ObjectResult<()> {
        self.object_store_metrics
            .write_bytes
            .inc_by(part.len() as u64);
        let _timer = self
            .object_store_metrics
            .operation_latency
            .with_label_values(&["upload_part"])
            .start_timer();
        self.object_store_metrics
            .operation_size
            .with_label_values(&["upload_part"])
            .observe(part.len() as f64);
        self.inner.upload_part(part_id, part).await
    }

    async fn finish(self) -> ObjectResult<()> {
        let _timer = self
            .object_store_metrics
            .operation_latency
            .with_label_values(&["finish_multipart_upload"])
            .start_timer();
        self.inner.finish().await
    }
}

#[async_trait::async_trait]
impl<OS: ObjectStore> MultipartUpload for MonitoredObjectStore<OS> {
    type Handle = MonitoredMultipartUploadHandle<OS::Handle>;
    type IdGen = OS::IdGen;

    async fn create_multipart_upload(
        &self,
        path: &str,
    ) -> ObjectResult<(Self::Handle, Self::IdGen)> {
        let _timer = self
            .object_store_metrics
            .operation_latency
            .with_label_values(&["create_multipart_upload"])
            .start_timer();
        let (handle, id_gen) = self.inner.create_multipart_upload(path).await?;
        Ok((
            MonitoredMultipartUploadHandle::new(handle, self.object_store_metrics.clone()),
            id_gen,
        ))
    }
}

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

use std::sync::Arc;

use bytes::Bytes;

pub mod mem;
pub use mem::*;

pub mod s3;
pub use s3::*;

mod disk;
pub mod error;
pub use error::*;

use crate::monitor::ObjectStoreMetrics;
use crate::object::disk::LocalDiskObjectStore;

#[derive(Debug, Copy, Clone)]
pub struct BlockLocation {
    pub offset: usize,
    pub size: usize,
}

pub struct ObjectMetadata {
    pub total_size: usize,
}

impl BlockLocation {
    /// Generates the http bytes range specifier.
    pub fn byte_range_specifier(&self) -> Option<String> {
        Some(format!(
            "bytes={}-{}",
            self.offset,
            self.offset + self.size - 1
        ))
    }
}

/// The implementation must be thread-safe.
#[async_trait::async_trait]
pub trait ObjectStore: Send + Sync {
    /// Uploads the object to `ObjectStore`.
    async fn upload(&self, path: &str, obj: Bytes) -> ObjectResult<()>;

    /// If the `block_loc` is None, the whole object will be return.
    /// If objects are PUT using a multipart upload, it’s a good practice to GET them in the same
    /// part sizes (or at least aligned to part boundaries) for best performance.
    /// <https://d1.awsstatic.com/whitepapers/AmazonS3BestPractices.pdf?stod_obj2>
    async fn read(&self, path: &str, block_loc: Option<BlockLocation>) -> ObjectResult<Bytes>;

    async fn readv(&self, path: &str, block_locs: &[BlockLocation]) -> ObjectResult<Vec<Bytes>>;

    /// Obtains the object metadata.
    async fn metadata(&self, path: &str) -> ObjectResult<ObjectMetadata>;

    /// Deletes blob permanently.
    async fn delete(&self, path: &str) -> ObjectResult<()>;
}

pub type ObjectStoreRef = Arc<ObjectStoreImpl>;

pub struct ObjectStoreImpl {
    inner: Box<dyn ObjectStore>,
    object_store_metrics: Arc<ObjectStoreMetrics>,
}

/// Manually dispatch trait methods.
impl ObjectStoreImpl {
    pub fn new(store: Box<dyn ObjectStore>, object_store_metrics: Arc<ObjectStoreMetrics>) -> Self {
        Self {
            inner: store,
            object_store_metrics,
        }
    }

    /// only for test
    pub fn new_mem() -> Self {
        Self {
            inner: Box::new(InMemObjectStore::new()),
            object_store_metrics: Arc::new(ObjectStoreMetrics::unused()),
        }
    }

    pub async fn upload(&self, path: &str, obj: Bytes) -> ObjectResult<()> {
        self.object_store_metrics
            .write_bytes
            .inc_by(obj.len() as u64);
        let _timer = self
            .object_store_metrics
            .operation_latency
            .with_label_values(&["upload"])
            .start_timer();
        self.inner.upload(path, obj).await?;
        Ok(())
    }

    pub async fn read(&self, path: &str, block_loc: Option<BlockLocation>) -> ObjectResult<Bytes> {
        let _timer = self
            .object_store_metrics
            .operation_latency
            .with_label_values(&["read"])
            .start_timer();
        let ret = self.inner.read(path, block_loc).await?;
        self.object_store_metrics
            .read_bytes
            .inc_by(ret.len() as u64);
        Ok(ret)
    }

    pub async fn readv(
        &self,
        path: &str,
        block_locs: &[BlockLocation],
    ) -> ObjectResult<Vec<Bytes>> {
        let _timer = self
            .object_store_metrics
            .operation_latency
            .with_label_values(&["readv"])
            .start_timer();
        let ret = self.inner.readv(path, block_locs).await?;
        self.object_store_metrics
            .read_bytes
            .inc_by(ret.iter().map(|block| block.len()).sum::<usize>() as u64);
        Ok(ret)
    }

    pub async fn metadata(&self, path: &str) -> ObjectResult<ObjectMetadata> {
        let _timer = self
            .object_store_metrics
            .operation_latency
            .with_label_values(&["metadata"])
            .start_timer();
        self.inner.metadata(path).await
    }

    pub async fn delete(&self, path: &str) -> ObjectResult<()> {
        let _timer = self
            .object_store_metrics
            .operation_latency
            .with_label_values(&["delete"])
            .start_timer();
        self.inner.delete(path).await
    }
}

pub async fn parse_object_store(
    url: &str,
    object_store_metrics: Arc<ObjectStoreMetrics>,
) -> ObjectStoreImpl {
    let store: Box<dyn ObjectStore> = match url {
        s3 if s3.starts_with("s3://") => {
            Box::new(S3ObjectStore::new(s3.strip_prefix("s3://").unwrap().to_string()).await)
        }
        minio if minio.starts_with("minio://") => Box::new(S3ObjectStore::with_minio(minio).await),
        disk if disk.starts_with("disk://") => Box::new(LocalDiskObjectStore::new(
            disk.strip_prefix("disk://").unwrap(),
        )),
        memory if memory.starts_with("memory") => {
            tracing::warn!("You're using Hummock in-memory object store. This should never be used in benchmarks and production environment.");
            Box::new(InMemObjectStore::new())
        }
        other => {
            unimplemented!(
                "{} Hummock only supports s3, minio and memory for now.",
                other
            )
        }
    };
    ObjectStoreImpl::new(store, object_store_metrics)
}

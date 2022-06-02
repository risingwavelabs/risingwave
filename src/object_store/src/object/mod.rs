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
pub mod object_metrics;

pub use error::*;
use object_metrics::ObjectStoreMetrics;

use crate::object::disk::LocalDiskObjectStore;

pub const LOCAL_OBJECT_STORE_PATH_PREFIX: &str = "@local:";

pub fn is_local_path(path: &str) -> bool {
    path.starts_with(LOCAL_OBJECT_STORE_PATH_PREFIX)
}

pub fn get_local_path(path: &str) -> String {
    LOCAL_OBJECT_STORE_PATH_PREFIX.to_string() + path
}

pub fn strip_path_local(path: &str, is_local: bool) -> &str {
    assert_eq!(is_local_path(path), is_local);
    if is_local {
        // Since it passes the `is_local_path` check, it's safe to unwrap
        path.strip_prefix(LOCAL_OBJECT_STORE_PATH_PREFIX).unwrap()
    } else {
        path
    }
}

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
/// For `path`, if the `path` starts with `LOCAL_OBJECT_STORE_PATH_PREFIX`, it indicates that the
/// operation should be performed on the local object store.
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

pub struct HybridObjectStore {
    local: Arc<dyn ObjectStore>,
    remote: Arc<dyn ObjectStore>,
}

impl HybridObjectStore {
    pub fn new(local: Arc<dyn ObjectStore>, remote: Arc<dyn ObjectStore>) -> Self {
        HybridObjectStore { local, remote }
    }

    pub fn local(&self) -> &Arc<dyn ObjectStore> {
        &self.local
    }

    pub fn remote(&self) -> &Arc<dyn ObjectStore> {
        &self.remote
    }
}

#[async_trait::async_trait]
impl ObjectStore for HybridObjectStore {
    async fn upload(&self, path: &str, obj: Bytes) -> ObjectResult<()> {
        if is_local_path(path) {
            self.local.upload(path, obj).await
        } else {
            self.remote.upload(path, obj).await
        }
    }

    async fn read(&self, path: &str, block_loc: Option<BlockLocation>) -> ObjectResult<Bytes> {
        if is_local_path(path) {
            self.local.read(path, block_loc).await
        } else {
            self.remote.read(path, block_loc).await
        }
    }

    async fn readv(&self, path: &str, block_locs: &[BlockLocation]) -> ObjectResult<Vec<Bytes>> {
        if is_local_path(path) {
            self.local.readv(path, block_locs).await
        } else {
            self.remote.readv(path, block_locs).await
        }
    }

    async fn metadata(&self, path: &str) -> ObjectResult<ObjectMetadata> {
        if is_local_path(path) {
            self.local.metadata(path).await
        } else {
            self.remote.metadata(path).await
        }
    }

    async fn delete(&self, path: &str) -> ObjectResult<()> {
        if is_local_path(path) {
            self.local.delete(path).await
        } else {
            self.remote.delete(path).await
        }
    }
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

    pub async fn upload(&self, path: &str, obj: Bytes) -> ObjectResult<()> {
        self.object_store_metrics
            .write_bytes
            .inc_by(obj.len() as u64);
        let _timer = self
            .object_store_metrics
            .operation_latency
            .with_label_values(&["upload"])
            .start_timer();
        self.object_store_metrics
            .operation_size
            .with_label_values(&["upload"])
            .observe(obj.len() as f64);
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
        self.object_store_metrics
            .operation_size
            .with_label_values(&["read"])
            .observe(ret.len() as f64);
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

pub async fn parse_object_store(url: &str, is_local: bool) -> Box<dyn ObjectStore> {
    match url {
        s3 if s3.starts_with("s3://") => {
            assert!(!is_local, "s3 cannot be used as local object store");
            Box::new(S3ObjectStore::new(s3.strip_prefix("s3://").unwrap().to_string()).await)
        }
        minio if minio.starts_with("minio://") => {
            assert!(!is_local, "minio cannot be used as local object store");
            Box::new(S3ObjectStore::with_minio(minio).await)
        }
        disk if disk.starts_with("disk://") => Box::new(LocalDiskObjectStore::new(
            disk.strip_prefix("disk://").unwrap(),
            is_local,
        )),
        temp_disk if temp_disk.starts_with("tempdisk") => {
            assert!(is_local, "tempdisk cannot be used as remote object store");
            let path = tempfile::TempDir::new()
                .expect("should be able to create temp dir")
                .into_path()
                .to_str()
                .expect("should be able to convert to str")
                .to_owned();
            Box::new(LocalDiskObjectStore::new(path.as_str(), true))
        }
        memory if memory.starts_with("memory") => {
            tracing::warn!("You're using Hummock in-memory object store. This should never be used in benchmarks and production environment.");
            Box::new(InMemObjectStore::new(is_local))
        }
        other => {
            unimplemented!(
                "{} Hummock only supports s3, minio, disk, and  memory for now.",
                other
            )
        }
    }
}

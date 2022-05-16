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

pub enum ObjectStoreImpl {
    Mem(InMemObjectStore),
    S3(S3ObjectStore),
    Disk(LocalDiskObjectStore),
}

/// Manually dispatch trait methods.
impl ObjectStoreImpl {
    pub async fn upload(&self, path: &str, obj: Bytes) -> ObjectResult<()> {
        match self {
            ObjectStoreImpl::Mem(mem) => mem.upload(path, obj).await,
            ObjectStoreImpl::S3(s3) => s3.upload(path, obj).await,
            ObjectStoreImpl::Disk(disk) => disk.upload(path, obj).await,
        }
    }

    pub async fn read(&self, path: &str, block_loc: Option<BlockLocation>) -> ObjectResult<Bytes> {
        match self {
            ObjectStoreImpl::Mem(mem) => mem.read(path, block_loc).await,
            ObjectStoreImpl::S3(s3) => s3.read(path, block_loc).await,
            ObjectStoreImpl::Disk(disk) => disk.read(path, block_loc).await,
        }
    }

    pub async fn readv(
        &self,
        path: &str,
        block_locs: &[BlockLocation],
    ) -> ObjectResult<Vec<Bytes>> {
        match self {
            ObjectStoreImpl::Mem(mem) => mem.readv(path, block_locs).await,
            ObjectStoreImpl::S3(s3) => s3.readv(path, block_locs).await,
            ObjectStoreImpl::Disk(disk) => disk.readv(path, block_locs).await,
        }
    }

    pub async fn metadata(&self, path: &str) -> ObjectResult<ObjectMetadata> {
        match self {
            ObjectStoreImpl::Mem(mem) => mem.metadata(path).await,
            ObjectStoreImpl::S3(s3) => s3.metadata(path).await,
            ObjectStoreImpl::Disk(disk) => disk.metadata(path).await,
        }
    }

    pub async fn delete(&self, path: &str) -> ObjectResult<()> {
        match self {
            ObjectStoreImpl::Mem(mem) => mem.delete(path).await,
            ObjectStoreImpl::S3(s3) => s3.delete(path).await,
            ObjectStoreImpl::Disk(disk) => disk.delete(path).await,
        }
    }
}

pub async fn parse_object_store(url: &str, is_remote: bool) -> ObjectStoreImpl {
    match url {
        s3 if s3.starts_with("s3://") => {
            assert!(
                is_remote,
                "S3 object store is only supported for remote stores"
            );
            ObjectStoreImpl::S3(
                S3ObjectStore::new(s3.strip_prefix("s3://").unwrap().to_string()).await,
            )
        }
        minio if minio.starts_with("minio://") => {
            assert!(
                is_remote,
                "S3 object store is only supported for remote stores"
            );
            ObjectStoreImpl::S3(S3ObjectStore::with_minio(minio).await)
        }
        disk if disk.starts_with("disk://") => ObjectStoreImpl::Disk(LocalDiskObjectStore::new(
            disk.strip_prefix("disk://").unwrap(),
        )),
        memory if memory.starts_with("memory") => {
            tracing::warn!("You're using Hummock in-memory object store. This should never be used in benchmarks and production environment.");
            ObjectStoreImpl::Mem(InMemObjectStore::new())
        }
        other => {
            unimplemented!(
                "{} Hummock only supports s3, minio and memory for now.",
                other
            )
        }
    }
}

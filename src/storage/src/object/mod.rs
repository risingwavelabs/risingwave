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
use risingwave_common::error::Result;

pub mod mem;
pub use mem::*;

pub mod s3;
pub use s3::*;

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
    async fn upload(&self, path: &str, obj: Bytes) -> Result<()>;

    /// If the `block_loc` is None, the whole object will be return.
    /// If objects are PUT using a multipart upload, it’s a good practice to GET them in the same
    /// part sizes (or at least aligned to part boundaries) for best performance.
    /// <https://d1.awsstatic.com/whitepapers/AmazonS3BestPractices.pdf?stod_obj2>
    async fn read(&self, path: &str, block_loc: Option<BlockLocation>) -> Result<Bytes>;

    async fn readv(&self, path: &str, block_locs: Vec<BlockLocation>) -> Result<Vec<Bytes>>;

    /// Obtains the object metadata.
    async fn metadata(&self, path: &str) -> Result<ObjectMetadata>;

    /// Deletes blob permanently.
    async fn delete(&self, path: &str) -> Result<()>;
}

pub type ObjectStoreRef = Arc<ObjectStoreImpl>;

pub enum ObjectStoreImpl {
    Mem(InMemObjectStore),
    S3(S3ObjectStore),
}

/// Manually dispatch trait methods.
impl ObjectStoreImpl {
    pub async fn upload(&self, path: &str, obj: Bytes) -> Result<()> {
        match self {
            ObjectStoreImpl::Mem(mem) => mem.upload(path, obj).await,
            ObjectStoreImpl::S3(s3) => s3.upload(path, obj).await,
        }
    }

    pub async fn read(&self, path: &str, block_loc: Option<BlockLocation>) -> Result<Bytes> {
        match self {
            ObjectStoreImpl::Mem(mem) => mem.read(path, block_loc).await,
            ObjectStoreImpl::S3(s3) => s3.read(path, block_loc).await,
        }
    }

    pub async fn readv(&self, path: &str, block_locs: Vec<BlockLocation>) -> Result<Vec<Bytes>> {
        match self {
            ObjectStoreImpl::Mem(mem) => mem.readv(path, block_locs).await,
            ObjectStoreImpl::S3(s3) => s3.readv(path, block_locs).await,
        }
    }

    pub async fn metadata(&self, path: &str) -> Result<ObjectMetadata> {
        match self {
            ObjectStoreImpl::Mem(mem) => mem.metadata(path).await,
            ObjectStoreImpl::S3(s3) => s3.metadata(path).await,
        }
    }

    pub async fn delete(&self, path: &str) -> Result<()> {
        match self {
            ObjectStoreImpl::Mem(mem) => mem.delete(path).await,
            ObjectStoreImpl::S3(s3) => s3.delete(path).await,
        }
    }
}

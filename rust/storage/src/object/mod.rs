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
    /// Generate the http bytes range specifer.
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
    /// Upload the object to `ObjectStore`.
    async fn upload(&self, path: &str, obj: Bytes) -> Result<()>;

    /// If the block_loc is None, the whole object will be return.
    /// If objects are PUT using a multipart upload, it’s a good practice to GET them in the same
    /// part sizes (or at least aligned to part boundaries) for best performance.
    /// https://d1.awsstatic.com/whitepapers/AmazonS3BestPractices.pdf?stod_obj2
    async fn read(&self, path: &str, block_loc: Option<BlockLocation>) -> Result<Bytes>;

    async fn readv(&self, path: &str, block_locs: Vec<BlockLocation>) -> Result<Vec<Bytes>>;

    /// Obtain the object metadata.
    async fn metadata(&self, path: &str) -> Result<ObjectMetadata>;

    /// Delete blob permanently.
    async fn delete(&self, path: &str) -> Result<()>;
}

pub type ObjectStoreRef = Arc<dyn ObjectStore>;

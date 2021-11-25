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

/// The implementation must be thread-safe.
#[async_trait::async_trait]
pub trait ObjectStore: Send + Sync {
    /// Upload the object to `ObjectStore`.
    async fn upload(&self, path: &str, obj: Bytes) -> Result<()>;

    /// If objects are PUT using a multipart upload, it’s a good practice to GET them in the same
    /// part sizes (or at least aligned to part boundaries) for best performance.
    /// https://d1.awsstatic.com/whitepapers/AmazonS3BestPractices.pdf?stod_obj2
    async fn read(&self, path: &str, block_loc: BlockLocation) -> Result<Vec<u8>>;

    /// Obtain the object metadata.
    async fn metadata(&self, path: &str) -> Result<ObjectMetadata>;

    /// Release the path on the blob, which hints store that reference of the object is decremented.
    /// When the reference count of an object drops to 0, it would be safe to perform compaction or
    /// conditional vacuuming.
    async fn close(&self, path: &str) -> Result<()>;

    /// Delete blob permanantly.
    async fn delete(&self, path: &str) -> Result<()>;
}

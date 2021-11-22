use bytes::Bytes;
use risingwave_common::error::Result;

pub mod mem;
pub use mem::*;

#[derive(Debug, Copy, Clone)]
pub struct BlockLocation {
    pub offset: usize,
    pub size: usize,
}

pub struct ObjectMetadata {
    pub total_size: usize,
}

/// For local filesystem, the handle is normally a file descriptor, i.e. `std::fs::File`.
/// For remote object store, it can be a URL.
pub trait ObjectHandle: Send + Sync + 'static {}

/// The implementation must be thread-safe.
#[async_trait::async_trait]
pub trait ObjectStore<H: ObjectHandle>: Send + Sync {
    async fn upload(&self, handle: &H, obj: Bytes) -> Result<()>;

    /// If objects are PUT using a multipart upload, it’s a good practice to GET them in the same
    /// part sizes (or at least aligned to part boundaries) for best performance.
    /// https://d1.awsstatic.com/whitepapers/AmazonS3BestPractices.pdf?stod_obj2
    async fn read(&self, handle: &H, block_loc: BlockLocation) -> Result<Vec<u8>>;

    /// Obtain the object metadata.
    async fn metadata(&self, handle: &H) -> Result<ObjectMetadata>;
}

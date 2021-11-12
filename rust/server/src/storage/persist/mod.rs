pub use fs::*;
mod fs;

#[async_trait::async_trait]
pub trait Blob {
  /// Put a blob specified by `k` and contents of `v` asynchronously.
  /// The operation should be atomic.
  async fn put(&self, k: &str, v: Vec<u8>, properties: u32) -> Result<(), Error>;

  /// Get a blob specified by `k` and returns its contents asynchronously.
  /// The operation should be atomic.
  /// The operation should implicitly mark a reference count increase on the blob.
  async fn get(&self, k: &str) -> Result<Option<Vec<u8>>, Error>;

  /// Set properties of object specified by `k` asynchronously.
  async fn set_properties(&self, k: &str) -> Result<Option<u32>, Error>;

  /// Get properties of object specified by `k` asynchronously.
  async fn get_properties(&self, k: &str) -> Result<Option<u32>, Error>;

  /// Release the handle on the blob.
  async fn close(&self, k: &str) -> Result<(), Error>;

  /// Delete blob.
  async fn delete(&self, k: &str) -> Result<(), Error>;

  /// TODO: Add range operations.
}

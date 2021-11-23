use super::super::{HummockIterator, HummockResult};
use async_trait::async_trait;

/// Iterates on a table
pub struct TableIterator {}

#[async_trait]
impl HummockIterator for TableIterator {
  async fn next(&mut self) -> HummockResult<Option<(&[u8], &[u8])>> {
    Ok(None)
  }

  async fn rewind(&mut self) {}

  async fn seek(&mut self, key: &[u8]) -> HummockResult<()> {
    Ok(())
  }
}

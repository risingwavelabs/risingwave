use bytes::Bytes;
use risingwave_common::error::Result;

use crate::hummock::iterator::UserIterator;

/// `HummockIndexIterator` iterates on the keys of the Hummock storage as indexes are encoded
/// to the keys only. The keys read and written in this iterator are all composite keys of
/// secondary key and primary key.
pub struct HummockIndexIterator<'a>(UserIterator<'a>);

impl<'a> HummockIndexIterator<'a> {
    async fn next(&mut self) -> Result<Option<Bytes>> {
        let iter = &mut self.0;

        if iter.is_valid() {
            let k = Bytes::copy_from_slice(iter.key());
            iter.next().await?;
            Ok(Some(k))
        } else {
            Ok(None)
        }
    }

    async fn rewind(&mut self) -> Result<()> {
        self.0.rewind().await?;
        Ok(())
    }

    async fn seek(&mut self, key: &[u8]) -> Result<()> {
        self.0.seek(key).await?;
        Ok(())
    }
}

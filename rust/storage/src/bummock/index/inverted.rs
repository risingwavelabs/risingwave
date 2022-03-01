use std::collections::BTreeSet;
use std::ptr;

use bytes::BufMut;
use parking_lot::Mutex;
use risingwave_common::error::Result;

use crate::bummock::PK_SIZE;
use crate::hummock::cf::HummockColumnFamily;
use crate::hummock::HummockStorage;

type IndexEntry = (Vec<u8>, Option<Vec<u8>>);

#[derive(Debug)]
pub struct HummockInvertedIndexer {
    /// The column family to store the inverted index.
    cf: HummockColumnFamily,

    /// Indexing is based on batches.
    wbuffer: Mutex<BTreeSet<IndexEntry>>,
}

/// Use Hummock storage as inverted indexing backend.
/// Only Hummock keys are used.
/// `| sk | pk | epoch |`
impl HummockInvertedIndexer {
    pub fn new(storage: HummockStorage, cf_name: Vec<u8>) -> Self {
        let cf = HummockColumnFamily::new(storage, cf_name);

        Self {
            cf,
            wbuffer: Mutex::new(BTreeSet::new()),
        }
    }

    /// Index an `sk` and `pk` as an inverted index entry.
    /// TODO(xiangyhu) multithreaded indexing should be supported.
    pub fn index_entry(&self, sk: &[u8], pk: u64) -> Result<()> {
        self.wbuffer
            .lock()
            .insert((self.compose_key(sk.to_vec(), pk), None));
        Ok(())
    }

    /// Get an inverted index entry.
    pub async fn get_entry(&self, _k: &[u8]) -> Result<(Vec<u8>, Option<Vec<u8>>)> {
        todo!()
    }

    /// Stamp an epoch and persist the inverted index.
    pub async fn commit(&self, epoch: u64) -> Result<()> {
        self.cf
            .put_batch(self.wbuffer.lock().to_owned(), epoch)
            .await?;

        Ok(())
    }

    /// Compose secondary key and primary key (document id).
    pub fn compose_key(&self, mut sk: Vec<u8>, pk: u64) -> Vec<u8> {
        sk.reserve(PK_SIZE);
        let buf = sk.chunk_mut();

        unsafe {
            ptr::copy_nonoverlapping(
                &pk as *const _ as *const u8,
                buf.as_mut_ptr() as *mut _,
                PK_SIZE,
            );
            sk.advance_mut(PK_SIZE);
        }

        sk
    }

    /// Decompose into a secondary key and a composite key.
    pub fn decompose_key(ck: &[u8]) -> (&[u8], &[u8]) {
        let pos = ck
            .len()
            .checked_sub(PK_SIZE)
            .expect("bad inverted index entry");
        ck.split_at(pos)
    }
}

use async_trait::async_trait;
use bytes::Bytes;
use risingwave_common::error::{Result, ToRwResult};

use super::iterator::UserKeyIterator;
use super::HummockStorage;
use crate::{StateStore, StateStoreIter};

/// A wrapper over [`HummockStorage`] as a state store.
///
/// TODO: this wrapper introduces extra overhead of async trait, may be turned into an enum if
/// possible.
#[derive(Clone)]
pub struct HummockStateStore {
    storage: HummockStorage,
}

impl HummockStateStore {
    pub fn new(storage: HummockStorage) -> Self {
        Self { storage }
    }
}

// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

/// Computes the next key of the given key.
///
/// If the key has no successor key (e.g. the input is "\xff\xff"), the result
/// would be an empty vector.
///
/// # Examples
///
/// ```
/// use risingwave_storage::hummock::next_key;
/// assert_eq!(next_key(b"123"), b"124");
/// assert_eq!(next_key(b"12\xff"), b"13");
/// assert_eq!(next_key(b"\xff\xff"), b"");
/// assert_eq!(next_key(b"\xff\xfe"), b"\xff\xff");
/// assert_eq!(next_key(b"T"), b"U");
/// assert_eq!(next_key(b""), b"");
/// ```
pub fn next_key(key: &[u8]) -> Vec<u8> {
    if let Some((s, e)) = next_key_no_alloc(key) {
        let mut res = Vec::with_capacity(s.len() + 1);
        res.extend_from_slice(s);
        res.push(e);
        res
    } else {
        Vec::new()
    }
}

pub(crate) fn next_key_no_alloc(key: &[u8]) -> Option<(&[u8], u8)> {
    let pos = key.iter().rposition(|b| *b != 0xff)?;
    Some((&key[..pos], key[pos] + 1))
}

// End Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

// Note(eric): How about removing HummockStateStore and just impl StateStore for HummockStorage?
#[async_trait]
impl StateStore for HummockStateStore {
    type Iter = HummockStateStoreIter;

    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.storage
            .get(key)
            .await
            .map(|x| x.map(Bytes::from))
            // TODO: make the HummockError into an I/O Error.
            .map_err(anyhow::Error::new)
            .to_rw_result()
    }

    async fn scan(&self, prefix: &[u8], limit: Option<usize>) -> Result<Vec<(Bytes, Bytes)>> {
        let mut iter = self
            .storage
            .range_scan(Some(prefix.to_vec()), Some(next_key(prefix)))
            .await
            .map_err(anyhow::Error::new)
            .to_rw_result()?;
        iter.rewind()
            .await
            .map_err(anyhow::Error::new)
            .to_rw_result()?;
        let mut result = vec![];
        while iter.is_valid() {
            // TODO: remove this condition when range_scan supports exclusion bound
            if iter.key().starts_with(prefix) {
                result.push((
                    Bytes::copy_from_slice(iter.key()),
                    Bytes::copy_from_slice(iter.value()),
                ));
            } else {
                break;
            }
            if let Some(limit) = limit {
                if result.len() >= limit {
                    break;
                }
            }
            iter.next()
                .await
                .map_err(anyhow::Error::new)
                .to_rw_result()?;
        }
        Ok(result)
    }

    async fn ingest_batch(&self, mut kv_pairs: Vec<(Bytes, Option<Bytes>)>) -> Result<()> {
        // TODO: reduce the redundant vec clone
        kv_pairs.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));
        self.storage
            .write_batch(
                kv_pairs
                    .into_iter()
                    .map(|(k, v)| (k.to_vec(), v.map(|x| x.to_vec()).into())),
            )
            .await
            .map_err(anyhow::Error::new)
            .to_rw_result()
    }

    fn iter(&self, prefix: &[u8]) -> Self::Iter {
        HummockStateStoreIter {
            iter: None,
            prefix: prefix.to_vec(),
            storage: self.storage.clone(),
        }
    }
}

/// TODO: if `.iter()` is async, we can directly forward `UserKeyIterator` to user.
pub struct HummockStateStoreIter {
    iter: Option<UserKeyIterator>,
    prefix: Vec<u8>,
    storage: HummockStorage,
}

#[async_trait]
impl StateStoreIter for HummockStateStoreIter {
    type Item = (Bytes, Bytes);

    async fn open(&mut self) -> Result<()> {
        assert!(self.iter.is_none());
        let mut iter = self
            .storage
            .range_scan(Some(self.prefix.clone()), Some(next_key(&self.prefix)))
            .await
            .map_err(anyhow::Error::new)
            .to_rw_result()?;
        iter.rewind()
            .await
            .map_err(anyhow::Error::new)
            .to_rw_result()?;
        self.iter = Some(iter);
        Ok(())
    }

    async fn next(&mut self) -> Result<Option<Self::Item>> {
        let iter = self.iter.as_mut().expect("Hummock iterator not opened!");

        // TODO: directly return `&[u8]` to user instead of `Bytes`.

        let result;

        if iter.is_valid() {
            // TODO: remove this condition when range_scan supports exclusion bound
            if iter.key().starts_with(&self.prefix) {
                result = (
                    Bytes::copy_from_slice(iter.key()),
                    Bytes::copy_from_slice(iter.value()),
                );
            } else {
                return Ok(None);
            }

            iter.next()
                .await
                .map_err(anyhow::Error::new)
                .to_rw_result()?;
        } else {
            return Ok(None);
        }

        Ok(Some(result))
    }
}

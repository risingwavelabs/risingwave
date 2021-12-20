use std::{ptr, u64};

use bytes::BufMut;

use super::version_cmp::VersionedComparator;

type Timestamp = u64;
const TS_LEN: usize = std::mem::size_of::<Timestamp>();

/// Convert user key to full key by appending `u64::MAX - timestamp` to the user key.
///
/// In this way, the keys can be comparable even with the timestamp, and a key with a larger
/// timestamp will be smaller and thus be sorted to a upper position.
pub fn key_with_ts(mut user_key: Vec<u8>, ts: Timestamp) -> Vec<u8> {
    let res = (Timestamp::MAX - ts).to_be();
    user_key.reserve(TS_LEN);
    let buf = user_key.chunk_mut();

    // todo: check whether this hack improves performance
    unsafe {
        ptr::copy_nonoverlapping(
            &res as *const _ as *const u8,
            buf.as_mut_ptr() as *mut _,
            TS_LEN,
        );
        user_key.advance_mut(TS_LEN);
    }

    user_key
}

/// Split a full key into its user key part and timestamp part.
#[inline]
pub fn split_key_timestamp(full_key: &[u8]) -> (&[u8], &[u8]) {
    let pos = full_key
        .len()
        .checked_sub(TS_LEN)
        .expect("bad full key format");
    full_key.split_at(pos)
}

/// Extract timestamp part from key
pub fn get_ts(full_key: &[u8]) -> Timestamp {
    let mut ts: Timestamp = 0;

    // todo: check whether this hack improves performance
    unsafe {
        let src = &full_key[full_key.len() - TS_LEN..];
        ptr::copy_nonoverlapping(src.as_ptr(), &mut ts as *mut _ as *mut u8, TS_LEN);
    }
    Timestamp::MAX - Timestamp::from_be(ts)
}

/// Extract user key without timestamp part
pub fn user_key(full_key: &[u8]) -> &[u8] {
    split_key_timestamp(full_key).0
}

/// [`FullKey`] can be created on either a `Vec<u8>` or a `&[u8]`.
///
/// Its format is (`user_key`, `u64::MAX - timestamp`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FullKey<T: AsRef<[u8]>>(T);

impl<'a> FullKey<&'a [u8]> {
    pub fn from_slice(full_key: &'a [u8]) -> Self {
        Self(full_key)
    }
}

impl FullKey<Vec<u8>> {
    fn from(full_key: Vec<u8>) -> Self {
        Self(full_key)
    }

    pub fn from_user_key(user_key: Vec<u8>, timestamp: u64) -> Self {
        Self(key_with_ts(user_key, timestamp))
    }

    pub fn from_user_key_slice(user_key: &[u8], timestamp: u64) -> Self {
        Self(key_with_ts(user_key.to_vec(), timestamp))
    }

    pub fn to_user_key(&self) -> &[u8] {
        user_key(self.0.as_slice())
    }
}

impl<T: Eq + AsRef<[u8]>> Ord for FullKey<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        VersionedComparator::compare_key(self.0.as_ref(), other.0.as_ref())
    }
}

impl<T: Eq + AsRef<[u8]>> PartialOrd for FullKey<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_key_ts() {
        let full_key = key_with_ts(b"aaa".to_vec(), 233);
        assert_eq!(get_ts(&full_key), 233);
        assert_eq!(user_key(&full_key), b"aaa");
    }
}

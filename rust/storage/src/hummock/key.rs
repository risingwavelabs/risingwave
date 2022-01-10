use std::{ptr, u64};

use bytes::BufMut;

use super::version_cmp::VersionedComparator;

pub type Timestamp = u64;
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

// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

/// Computes the next key of the given key.
///
/// If the key has no successor key (e.g. the input is "\xff\xff"), the result
/// would be an empty vector.
///
/// # Examples
///
/// ```rust
/// use risingwave_storage::hummock::key::next_key;
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

/// Computes the previous key of the given key.
///
/// If the key has no predecessor key (e.g. the input is "\x00\x00"), the result
/// would be a "\xff\xff" vector.
///
/// # Examples
///
/// ```rust
/// use risingwave_storage::hummock::key::prev_key;
/// assert_eq!(prev_key(b"123"), b"122");
/// assert_eq!(prev_key(b"12\x00"), b"11\xff");
/// assert_eq!(prev_key(b"\x00\x00"), b"\xff\xff");
/// assert_eq!(prev_key(b"\x00\x01"), b"\x00\x00");
/// assert_eq!(prev_key(b"T"), b"S");
/// assert_eq!(prev_key(b""), b"");
/// ```
pub fn prev_key(key: &[u8]) -> Vec<u8> {
    let pos = key.iter().rposition(|b| *b != 0x00);
    match pos {
        Some(pos) => {
            let mut res = Vec::with_capacity(key.len());
            res.extend_from_slice(&key[0..pos]);
            res.push(key[pos] - 1);
            if pos + 1 < key.len() {
                res.push(b"\xff".to_owned()[0]);
            }
            res
        }
        None => {
            vec![0xff; key.len()]
        }
    }
}

fn next_key_no_alloc(key: &[u8]) -> Option<(&[u8], u8)> {
    let pos = key.iter().rposition(|b| *b != 0xff)?;
    Some((&key[..pos], key[pos] + 1))
}

// End Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

/// [`FullKey`] can be created on either a `Vec<u8>` or a `&[u8]`.
///
/// Its format is (`user_key`, `u64::MAX - timestamp`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FullKey<T: AsRef<[u8]>>(T);

impl<T: AsRef<[u8]>> FullKey<T> {
    pub fn into_inner(self) -> T {
        self.0
    }
}

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

    pub fn as_slice(&self) -> FullKey<&[u8]> {
        FullKey(self.0.as_slice())
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

    #[test]
    fn test_prev_key() {
        assert_eq!(prev_key(b"123"), b"122");
        assert_eq!(prev_key(b"12\x00"), b"11\xff");
        assert_eq!(prev_key(b"\x00\x00"), b"\xff\xff");
        assert_eq!(prev_key(b"\x00\x01"), b"\x00\x00");
        assert_eq!(prev_key(b"T"), b"S");
        assert_eq!(prev_key(b""), b"");
    }
}

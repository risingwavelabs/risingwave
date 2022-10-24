// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::cmp::Ordering;
use std::ops::Bound::*;
use std::ops::{Bound, RangeBounds};
use std::{ptr, u64};

use bytes::{Buf, BufMut, BytesMut};

use super::version_cmp::VersionedComparator;
use crate::HummockEpoch;

pub const EPOCH_LEN: usize = std::mem::size_of::<HummockEpoch>();
pub const TABLE_PREFIX_LEN: usize = std::mem::size_of::<u32>();

/// Converts user key to full key by appending `epoch` to the user key.
pub fn key_with_epoch(mut user_key: Vec<u8>, epoch: HummockEpoch) -> Vec<u8> {
    let res = epoch.to_be();
    user_key.reserve(EPOCH_LEN);
    let buf = user_key.chunk_mut();

    // TODO: check whether this hack improves performance
    unsafe {
        ptr::copy_nonoverlapping(
            &res as *const _ as *const u8,
            buf.as_mut_ptr() as *mut _,
            EPOCH_LEN,
        );
        user_key.advance_mut(EPOCH_LEN);
    }

    user_key
}

/// Splits a full key into its user key part and epoch part.
#[inline]
pub fn split_key_epoch(full_key: &[u8]) -> (&[u8], &[u8]) {
    let pos = full_key
        .len()
        .checked_sub(EPOCH_LEN)
        .unwrap_or_else(|| panic!("bad full key format: {:?}", full_key));
    full_key.split_at(pos)
}

/// Extracts epoch part from key
#[inline(always)]
pub fn get_epoch(full_key: &[u8]) -> HummockEpoch {
    let mut epoch: HummockEpoch = 0;

    // TODO: check whether this hack improves performance
    unsafe {
        let src = &full_key[full_key.len() - EPOCH_LEN..];
        ptr::copy_nonoverlapping(src.as_ptr(), &mut epoch as *mut _ as *mut u8, EPOCH_LEN);
    }
    HummockEpoch::from_be(epoch)
}

/// Extract user key without epoch part
pub fn user_key(full_key: &[u8]) -> &[u8] {
    split_key_epoch(full_key).0
}

/// Extract table id in key prefix
#[inline(always)]
pub fn get_table_id(full_key: &[u8]) -> u32 {
    let mut buf = full_key;
    buf.get_u32()
}

pub fn extract_table_id_and_epoch(full_key: &[u8]) -> (u32, HummockEpoch) {
    (get_table_id(full_key), get_epoch(full_key))
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
/// use risingwave_hummock_sdk::key::next_key;
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
        // slice 是&[T]
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
/// use risingwave_hummock_sdk::key::prev_key;
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

/// compute the next full key of the given full key
///
/// if the `user_key` has no successor key, the result will be a empty vec

pub fn next_full_key(full_key: &[u8]) -> Vec<u8> {
    let (user_key, epoch) = split_key_epoch(full_key);
    let next_epoch = prev_key(epoch);
    let mut res = Vec::with_capacity(full_key.len());
    if next_epoch.cmp(&vec![0xff; next_epoch.len()]) == Ordering::Equal {
        let next_user_key = next_key(user_key);
        if next_user_key.is_empty() {
            return Vec::new();
        }
        res.extend_from_slice(next_user_key.as_slice());
        res.extend_from_slice(next_epoch.as_slice());
        res
    } else {
        res.extend_from_slice(user_key);
        res.extend_from_slice(next_epoch.as_slice());
        res
    }
}

/// compute the prev full key of the given full key
///
/// if the `user_key` has no predecessor key, the result will be a empty vec

pub fn prev_full_key(full_key: &[u8]) -> Vec<u8> {
    let (user_key, epoch) = split_key_epoch(full_key);
    let mut prev_epoch = next_key(epoch);
    let mut res = Vec::with_capacity(full_key.len());
    if prev_epoch.is_empty() {
        let prev_user_key = prev_key(user_key);
        if prev_user_key.cmp(&vec![0xff; prev_user_key.len()]) == Ordering::Equal {
            return Vec::new();
        }
        prev_epoch = vec![0; 8];
        res.extend_from_slice(prev_user_key.as_slice());
        res.extend_from_slice(prev_epoch.as_slice());
        res
    } else {
        res.extend_from_slice(user_key);
        res.extend_from_slice(prev_epoch.as_slice());
        res
    }
}

// End Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

/// Get the end bound of the given `prefix` when transforming it to a key range.
pub fn end_bound_of_prefix(prefix: &[u8]) -> Bound<Vec<u8>> {
    if let Some((s, e)) = next_key_no_alloc(prefix) {
        let mut res = Vec::with_capacity(s.len() + 1);
        res.extend_from_slice(s);
        res.push(e);
        Excluded(res)
    } else {
        Unbounded
    }
}

/// Get the start bound of the given `prefix` when it is excluded from the range.
pub fn start_bound_of_excluded_prefix(prefix: &[u8]) -> Bound<Vec<u8>> {
    if let Some((s, e)) = next_key_no_alloc(prefix) {
        let mut res = Vec::with_capacity(s.len() + 1);
        res.extend_from_slice(s);
        res.push(e);
        Included(res)
    } else {
        panic!("the prefix is the maximum value")
    }
}

/// Transform the given `prefix` to a key range.
pub fn range_of_prefix(prefix: &[u8]) -> (Bound<Vec<u8>>, Bound<Vec<u8>>) {
    if prefix.is_empty() {
        (Unbounded, Unbounded)
    } else {
        (Included(prefix.to_vec()), end_bound_of_prefix(prefix))
    }
}

/// Prepend the `prefix` to the given key `range`.
pub fn prefixed_range<B: AsRef<[u8]>>(
    range: impl RangeBounds<B>,
    prefix: &[u8],
) -> (Bound<Vec<u8>>, Bound<Vec<u8>>) {
    let start = match range.start_bound() {
        Included(b) => Included([prefix, b.as_ref()].concat()),
        Excluded(b) => {
            let b = b.as_ref();
            assert!(!b.is_empty());
            Excluded([prefix, b].concat())
        }
        Unbounded => Included(prefix.to_vec()),
    };

    let end = match range.end_bound() {
        Included(b) => Included([prefix, b.as_ref()].concat()),
        Excluded(b) => {
            let b = b.as_ref();
            assert!(!b.is_empty());
            Excluded([prefix, b].concat())
        }
        Unbounded => end_bound_of_prefix(prefix),
    };

    (start, end)
}

pub fn table_prefix(table_id: u32) -> Vec<u8> {
    let mut buf = BytesMut::with_capacity(TABLE_PREFIX_LEN);
    buf.put_u32(table_id);
    buf.to_vec()
}

/// [`FullKey`] can be created on either a `Vec<u8>` or a `&[u8]`.
///
/// Its format is (`user_key`, `epoch`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FullKey<T: AsRef<[u8]>>(T);

impl<T: AsRef<[u8]>> FullKey<T> {
    pub fn into_inner(self) -> T {
        self.0
    }

    pub fn inner(&self) -> &T {
        &self.0
    }
}

impl<'a> FullKey<&'a [u8]> {
    pub fn from_slice(full_key: &'a [u8]) -> Self {
        Self(full_key)
    }
}

impl FullKey<Vec<u8>> {
    pub fn from_user_key(user_key: Vec<u8>, epoch: u64) -> Self {
        Self(key_with_epoch(user_key, epoch))
    }

    pub fn from_user_key_slice(user_key: &[u8], epoch: u64) -> Self {
        Self(key_with_epoch(user_key.to_vec(), epoch))
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
    fn test_key_epoch() {
        let full_key = key_with_epoch(b"aaa".to_vec(), 233);
        assert_eq!(get_epoch(&full_key), 233);
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

    #[test]
    fn test_next_full_key() {
        let user_key = b"aaa".to_vec();
        let epoch: HummockEpoch = 3;
        let mut full_key = key_with_epoch(user_key, epoch);
        full_key = next_full_key(full_key.as_slice());
        assert_eq!(full_key, key_with_epoch(b"aaa".to_vec(), 2));
        full_key = next_full_key(full_key.as_slice());
        assert_eq!(full_key, key_with_epoch(b"aaa".to_vec(), 1));
        full_key = next_full_key(full_key.as_slice());
        assert_eq!(full_key, key_with_epoch(b"aaa".to_vec(), 0));
        full_key = next_full_key(full_key.as_slice());
        assert_eq!(
            full_key,
            key_with_epoch("aab".as_bytes().to_vec(), HummockEpoch::MAX)
        );
        assert_eq!(
            next_full_key(&key_with_epoch(b"\xff".to_vec(), 0)),
            Vec::<u8>::new()
        );
    }

    #[test]
    fn test_prev_full_key() {
        let user_key = b"aab";
        let epoch: HummockEpoch = HummockEpoch::MAX - 3;
        let mut full_key = key_with_epoch(user_key.to_vec(), epoch);
        full_key = prev_full_key(full_key.as_slice());
        assert_eq!(
            full_key,
            key_with_epoch(b"aab".to_vec(), HummockEpoch::MAX - 2)
        );
        full_key = prev_full_key(full_key.as_slice());
        assert_eq!(
            full_key,
            key_with_epoch(b"aab".to_vec(), HummockEpoch::MAX - 1)
        );
        full_key = prev_full_key(full_key.as_slice());
        assert_eq!(full_key, key_with_epoch(b"aab".to_vec(), HummockEpoch::MAX));
        full_key = prev_full_key(full_key.as_slice());
        assert_eq!(full_key, key_with_epoch(b"aaa".to_vec(), 0));

        assert_eq!(
            prev_full_key(&key_with_epoch(b"\x00".to_vec(), HummockEpoch::MAX)),
            Vec::<u8>::new()
        );
    }
}

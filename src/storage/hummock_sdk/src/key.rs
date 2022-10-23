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

use std::ops::Bound::*;
use std::ops::{Bound, RangeBounds};
use std::ptr;

use bytes::{Buf, BufMut, BytesMut};
use risingwave_common::catalog::TableId;

use crate::HummockEpoch;

const EPOCH_LEN: usize = std::mem::size_of::<HummockEpoch>();
pub const TABLE_PREFIX_LEN: usize = 4;

/// Converts user key to full key by appending `u64::MAX - epoch` to the user key.
///
/// In this way, the keys can be comparable even with the epoch, and a key with a larger
/// epoch will be smaller and thus be sorted to an upper position.
pub fn key_with_epoch(mut user_key: Vec<u8>, epoch: HummockEpoch) -> Vec<u8> {
    let res = (HummockEpoch::MAX - epoch).to_be();
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

// TODO: Move to a method of `FullKey`.
/// Extracts epoch part from key
#[inline(always)]
pub fn get_epoch(full_key: &[u8]) -> HummockEpoch {
    let mut epoch: HummockEpoch = 0;

    // TODO: check whether this hack improves performance
    unsafe {
        let src = &full_key[full_key.len() - EPOCH_LEN..];
        ptr::copy_nonoverlapping(src.as_ptr(), &mut epoch as *mut _ as *mut u8, EPOCH_LEN);
    }
    HummockEpoch::MAX - HummockEpoch::from_be(epoch)
}

// TODO: Remove
/// Extract user key without epoch part
pub fn user_key(full_key: &[u8]) -> &[u8] {
    split_key_epoch(full_key).0
}

// TODO: Remove
pub fn user_key_from_table_id_and_table_key(table_id: &TableId, table_key: &[u8]) -> Vec<u8> {
    let mut ret = Vec::with_capacity(TABLE_PREFIX_LEN + table_key.len());
    ret.put_u8(b't');
    ret.put_u32(table_id.table_id());
    ret.put_slice(table_key);
    ret
}

// TODO: Remove
/// Extract table id in key prefix
#[inline(always)]
pub fn get_table_id(full_key: &[u8]) -> Option<u32> {
    if full_key[0] == b't' {
        let mut buf = &full_key[1..];
        Some(buf.get_u32())
    } else {
        None
    }
}

pub fn extract_table_id_and_epoch(full_key: &[u8]) -> (Option<u32>, HummockEpoch) {
    match get_table_id(full_key) {
        Some(table_id) => {
            let epoch = get_epoch(full_key);
            (Some(table_id), epoch)
        }

        None => (None, 0),
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
    buf.put_u8(b't');
    buf.put_u32(table_id);
    buf.to_vec()
}

/// [`UserKey`] is the interface for the user to read or write KV pairs in the storage.
///
/// The encoded format is | `table_id` | `table_key` |.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct UserKey<T: AsRef<[u8]>> {
    pub table_id: TableId,
    pub table_key: T,
}

impl<T: AsRef<[u8]>> UserKey<T> {
    /// Encode in to a buffer.
    pub fn encode_into(&self, buf: &mut impl BufMut) {
        buf.put_u32(self.table_id.table_id());
        buf.put_slice(self.table_key.as_ref());
    }
}

/// [`FullKey`] is an internal concept in storage. It associates [`UserKey`] with an epoch.
/// It can be created on either a `Vec<u8>` or a `&[u8]`.
///
/// The encoded format is | `user_key` | `epoch` |.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FullKey<T: AsRef<[u8]>> {
    pub user_key: UserKey<T>,
    pub epoch: HummockEpoch,
}

impl<T: AsRef<[u8]>> FullKey<T> {
    pub fn new(table_id: TableId, table_key: T, epoch: HummockEpoch) -> Self {
        Self {
            user_key: UserKey {
                table_id,
                table_key,
            },
            epoch,
        }
    }

    /// Encode in to a buffer.
    pub fn encode_into(&self, buf: &mut impl BufMut) {
        self.user_key.encode_into(buf);
        buf.put_u64(self.epoch);
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut ret = Vec::with_capacity(
            TABLE_PREFIX_LEN + self.user_key.table_key.as_ref().len() + EPOCH_LEN,
        );
        self.encode_into(&mut ret);
        ret
    }
}

impl<'a> FullKey<&'a [u8]> {
    /// Construct a ['FullKey`] from a byte slice. Its `table_key` will be a part of the input
    /// `slice`.
    pub fn decode(slice: &'a [u8]) -> Self {
        let mut table_id: u32 = 0;
        // TODO: check whether this hack improves performance
        unsafe {
            ptr::copy_nonoverlapping(
                slice.as_ptr(),
                &mut table_id as *mut _ as *mut u8,
                TABLE_PREFIX_LEN,
            );
        }

        let epoch_pos = slice.len() - EPOCH_LEN;
        let mut epoch: HummockEpoch = 0;
        // TODO: check whether this hack improves performance
        unsafe {
            let src = &slice[epoch_pos..];
            ptr::copy_nonoverlapping(src.as_ptr(), &mut epoch as *mut _ as *mut u8, EPOCH_LEN);
        }

        Self {
            user_key: UserKey {
                table_id: TableId::new(table_id),
                table_key: &slice[TABLE_PREFIX_LEN..epoch_pos],
            },
            epoch,
        }
    }
}

impl FullKey<Vec<u8>> {
    pub fn as_slice(&self) -> FullKey<&[u8]> {
        FullKey {
            user_key: UserKey {
                table_id: self.user_key.table_id,
                table_key: self.user_key.table_key.as_slice(),
            },
            epoch: self.epoch,
        }
    }
}

impl<T: AsRef<[u8]> + Ord + Eq> Ord for FullKey<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // When `user_key` is the same, greater epoch comes first.
        self.user_key
            .cmp(&other.user_key)
            .then_with(|| other.epoch.cmp(&self.epoch))
    }
}

impl<T: AsRef<[u8]> + Ord + Eq> PartialOrd for FullKey<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;

    use super::*;

    #[test]
    fn test_encode_decode() {
        let table_key = b"aaa".to_vec();
        let key = FullKey::new(TableId::new(0), &table_key[..], 0);
        let buf = key.encode();
        assert_eq!(FullKey::decode(&buf), key);
    }

    #[test]
    fn test_cmp() {
        let key1 = FullKey::new(TableId::new(0), b"0".to_vec(), 0);
        let key2 = FullKey::new(TableId::new(1), b"0".to_vec(), 0);
        let key3 = FullKey::new(TableId::new(1), b"1".to_vec(), 0);
        let key4 = FullKey::new(TableId::new(1), b"1".to_vec(), 1);

        assert_eq!(key1.cmp(&key1), Ordering::Equal);
        assert_eq!(key1.cmp(&key2), Ordering::Less);
        assert_eq!(key1.cmp(&key3), Ordering::Less);
        assert_eq!(key1.cmp(&key4), Ordering::Less);
        assert_eq!(key2.cmp(&key3), Ordering::Less);
        assert_eq!(key2.cmp(&key4), Ordering::Less);
        assert_eq!(key3.cmp(&key4), Ordering::Greater);
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

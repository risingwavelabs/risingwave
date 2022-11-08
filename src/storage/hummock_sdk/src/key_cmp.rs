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

use std::cmp::{self, Ordering};

use bytes::Buf;

use super::key::split_key_epoch;
use crate::key::{FullKey, UserKey, TABLE_PREFIX_LEN};

/// A comparator for comparing [`FullKey`] and [`UserKey`] with possibly different table key types.
pub struct KeyComparator;

impl KeyComparator {
    /// Suppose parameter as `full_key` = (`user_key`, `u64::MAX - epoch`), this function compares
    /// `&[u8]` as if compare tuple mentioned before.
    #[inline]
    pub fn compare_encoded_full_key(lhs: &[u8], rhs: &[u8]) -> cmp::Ordering {
        let (l_p, l_s) = split_key_epoch(lhs);
        let (r_p, r_s) = split_key_epoch(rhs);
        l_p.cmp(r_p).then_with(|| r_s.cmp(l_s))
    }

    /// Used to compare [`FullKey`] with different inner `table_key` types.
    #[inline]
    pub fn compare_full_key(
        lhs: &FullKey<impl AsRef<[u8]>>,
        rhs: &FullKey<impl AsRef<[u8]>>,
    ) -> Ordering {
        Self::compare_user_key(&lhs.user_key, &rhs.user_key).then_with(|| rhs.epoch.cmp(&lhs.epoch))
    }

    /// Used to compare [`UserKey`] with different inner `table_key` types.
    #[inline]
    pub fn compare_user_key(
        lhs: &UserKey<impl AsRef<[u8]>>,
        rhs: &UserKey<impl AsRef<[u8]>>,
    ) -> Ordering {
        lhs.table_id
            .cmp(&rhs.table_id)
            .then_with(|| lhs.table_key.as_ref().cmp(rhs.table_key.as_ref()))
    }

    /// Used to compare [`UserKey`] and its encoded format.
    pub fn compare_user_key_cross_format(
        encoded: impl AsRef<[u8]>,
        unencoded: &UserKey<impl AsRef<[u8]>>,
    ) -> Ordering {
        let encoded = encoded.as_ref();
        (&encoded[..TABLE_PREFIX_LEN])
            .get_u32()
            .cmp(&unencoded.table_id.table_id())
            .then_with(|| encoded[TABLE_PREFIX_LEN..].cmp(unencoded.table_key.as_ref()))
    }
}

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;

    use risingwave_common::catalog::TableId;

    use crate::key::{FullKey, UserKey};
    use crate::KeyComparator;

    #[test]
    fn test_cmp_user_key() {
        let key1 = UserKey::new(TableId::new(0), b"0".to_vec());
        let key2 = UserKey::new(TableId::new(0), b"1".to_vec());
        let key2 = key2.table_key_as_slice();
        let key3 = UserKey::new(TableId::new(1), b"0".to_vec());

        assert_eq!(
            KeyComparator::compare_user_key(&key1, &key1),
            Ordering::Equal
        );
        assert_eq!(
            KeyComparator::compare_user_key(&key1, &key2),
            Ordering::Less
        );
        assert_eq!(
            KeyComparator::compare_user_key(&key2, &key3),
            Ordering::Less
        );
    }

    #[test]
    fn test_cmp_full_key() {
        // 1 compared with 256 under little-endian encoding would return wrong result.
        let key1 = FullKey::new(TableId::new(0), b"0".to_vec(), 1);
        let key2 = FullKey::new(TableId::new(1), b"0".to_vec(), 1);
        let key2 = key2.table_key_as_slice();
        let key3 = FullKey::new(TableId::new(1), b"1".to_vec(), 256);
        let key3 = key3.table_key_as_slice();
        let key4 = FullKey::new(TableId::new(1), b"1".to_vec(), 1);

        assert_eq!(
            KeyComparator::compare_full_key(&key1, &key1),
            Ordering::Equal
        );
        assert_eq!(
            KeyComparator::compare_full_key(&key1, &key2),
            Ordering::Less
        );
        assert_eq!(
            KeyComparator::compare_full_key(&key2, &key3),
            Ordering::Less
        );
        assert_eq!(
            KeyComparator::compare_full_key(&key3, &key4),
            Ordering::Less
        );
    }

    #[test]
    fn test_cmp_encoded_full_key() {
        // 1 compared with 256 under little-endian encoding would return wrong result.
        let key1 = FullKey::new(TableId::new(0), b"0".to_vec(), 1);
        let key2 = FullKey::new(TableId::new(1), b"0".to_vec(), 1);
        let key3 = FullKey::new(TableId::new(1), b"1".to_vec(), 256);
        let key4 = FullKey::new(TableId::new(1), b"1".to_vec(), 1);

        assert_eq!(
            KeyComparator::compare_encoded_full_key(&key1.encode(), &key1.encode()),
            Ordering::Equal
        );
        assert_eq!(
            KeyComparator::compare_encoded_full_key(&key1.encode(), &key2.encode()),
            Ordering::Less
        );
        assert_eq!(
            KeyComparator::compare_encoded_full_key(&key2.encode(), &key3.encode()),
            Ordering::Less
        );
        assert_eq!(
            KeyComparator::compare_encoded_full_key(&key3.encode(), &key4.encode()),
            Ordering::Less
        );
    }

    #[test]
    fn test_cmp_user_key_cross_format() {
        let key1 = UserKey::new(TableId::new(0), b"0".to_vec());
        let key2 = UserKey::new(TableId::new(0), b"1".to_vec());
        let key3 = UserKey::new(TableId::new(1), b"0".to_vec());

        assert_eq!(
            KeyComparator::compare_user_key_cross_format(&key1.encode(), &key1),
            Ordering::Equal
        );
        assert_eq!(
            KeyComparator::compare_user_key_cross_format(&key1.encode(), &key2),
            Ordering::Less
        );
        assert_eq!(
            KeyComparator::compare_user_key_cross_format(&key2.encode(), &key3),
            Ordering::Less
        );
    }
}

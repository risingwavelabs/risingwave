// Copyright 2025 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::cmp::{self, Ordering};

use super::key::split_key_epoch;
use crate::key::UserKey;

/// A comparator for comparing [`crate::key::FullKey`] and [`crate::key::UserKey`] with possibly
/// different table key types.
pub struct KeyComparator;

impl KeyComparator {
    /// Suppose parameter as `full_key` = (`user_key`, `epoch`), this function compares
    /// `&[u8]` as if comparing the above tuple.
    #[inline]
    pub fn compare_encoded_full_key(lhs: &[u8], rhs: &[u8]) -> cmp::Ordering {
        let (l_p, l_s) = split_key_epoch(lhs);
        let (r_p, r_s) = split_key_epoch(rhs);
        l_p.cmp(r_p).then_with(|| r_s.cmp(l_s))
    }

    #[inline]
    pub fn encoded_full_key_less_than(lhs: &[u8], rhs: &[u8]) -> bool {
        Self::compare_encoded_full_key(lhs, rhs) == cmp::Ordering::Less
    }

    /// Used to compare [`UserKey`] and its encoded format.
    pub fn compare_user_key_cross_format(
        encoded: impl AsRef<[u8]>,
        unencoded: &UserKey<impl AsRef<[u8]>>,
    ) -> Ordering {
        UserKey::decode(encoded.as_ref()).cmp(&unencoded.as_ref())
    }

    #[inline(always)]
    /// Used to compare [`UserKey`] and its encoded format.
    pub fn encoded_less_than_unencoded(
        encoded: impl AsRef<[u8]>,
        unencoded: &UserKey<impl AsRef<[u8]>>,
    ) -> bool {
        Self::compare_user_key_cross_format(encoded, unencoded) == Ordering::Less
    }

    #[inline(always)]
    /// Used to compare [`UserKey`] and its encoded format.
    pub fn encoded_less_equal_unencoded(
        encoded: impl AsRef<[u8]>,
        unencoded: &UserKey<impl AsRef<[u8]>>,
    ) -> bool {
        Self::compare_user_key_cross_format(encoded, unencoded) != Ordering::Greater
    }

    #[inline(always)]
    /// Used to compare [`UserKey`] and its encoded format.
    pub fn encoded_greater_than_unencoded(
        encoded: impl AsRef<[u8]>,
        unencoded: &UserKey<impl AsRef<[u8]>>,
    ) -> bool {
        Self::compare_user_key_cross_format(encoded, unencoded) == Ordering::Greater
    }
}

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;

    use risingwave_common::catalog::TableId;
    use risingwave_common::util::epoch::test_epoch;

    use crate::KeyComparator;
    use crate::key::{FullKey, UserKey};

    #[test]
    fn test_cmp_encoded_full_key() {
        // 1 compared with 256 under little-endian encoding would return wrong result.

        let epoch = test_epoch(1);
        let epoch2 = test_epoch(2);
        let key1 = FullKey::for_test(TableId::new(0), b"0".to_vec(), epoch);
        let key2 = FullKey::for_test(TableId::new(1), b"0".to_vec(), epoch);
        let key3 = FullKey::for_test(TableId::new(1), b"1".to_vec(), epoch2);
        let key4 = FullKey::for_test(TableId::new(1), b"1".to_vec(), epoch);

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
        let key1 = UserKey::for_test(TableId::new(0), b"0".to_vec());
        let key2 = UserKey::for_test(TableId::new(0), b"1".to_vec());
        let key3 = UserKey::for_test(TableId::new(1), b"0".to_vec());

        assert_eq!(
            KeyComparator::compare_user_key_cross_format(key1.encode(), &key1),
            Ordering::Equal
        );
        assert_eq!(
            KeyComparator::compare_user_key_cross_format(key1.encode(), &key2),
            Ordering::Less
        );
        assert_eq!(
            KeyComparator::compare_user_key_cross_format(key2.encode(), &key3),
            Ordering::Less
        );
    }
}

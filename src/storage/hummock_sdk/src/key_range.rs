// Copyright 2023 RisingWave Labs
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

use std::cmp;

use bytes::Bytes;

use super::key_cmp::KeyComparator;
use crate::key::{FullKey, UserKey};

#[derive(PartialEq, Eq, Clone, Debug)]
pub struct KeyRange {
    pub left: Bytes,
    pub right: Bytes,
    pub right_exclusive: bool,
}

impl KeyRange {
    pub fn new(left: Bytes, right: Bytes) -> Self {
        Self {
            left,
            right,
            right_exclusive: false,
        }
    }

    pub fn inf() -> Self {
        Self {
            left: Bytes::new(),
            right: Bytes::new(),
            right_exclusive: false,
        }
    }

    #[inline]
    fn start_bound_inf(&self) -> bool {
        self.left.is_empty()
    }

    #[inline]
    fn end_bound_inf(&self) -> bool {
        self.right.is_empty()
    }
}

pub trait KeyRangeCommon {
    fn full_key_overlap(&self, other: &Self) -> bool;
    fn full_key_extend(&mut self, other: &Self);
    fn sstable_overlap(&self, other: &Self) -> bool;
    fn sstable_include(&self, other: &Self) -> bool;
    fn compare_right_with(&self, full_key: &[u8]) -> std::cmp::Ordering {
        self.compare_right_with_user_key(FullKey::decode(full_key).user_key)
    }
    fn compare_right_with_user_key(&self, ukey: UserKey<&[u8]>) -> std::cmp::Ordering;
}

#[macro_export]
macro_rules! impl_key_range_common {
    ($T:ty) => {
        impl KeyRangeCommon for $T {
            fn full_key_overlap(&self, other: &Self) -> bool {
                (self.end_bound_inf()
                    || other.start_bound_inf()
                    || KeyComparator::compare_encoded_full_key(&self.right, &other.left)
                        != cmp::Ordering::Less)
                    && (other.end_bound_inf()
                        || self.start_bound_inf()
                        || KeyComparator::compare_encoded_full_key(&other.right, &self.left)
                            != cmp::Ordering::Less)
            }

            fn full_key_extend(&mut self, other: &Self) {
                if !self.start_bound_inf()
                    && (other.start_bound_inf()
                        || KeyComparator::compare_encoded_full_key(&other.left, &self.left)
                            == cmp::Ordering::Less)
                {
                    self.left = other.left.clone();
                }
                if !self.end_bound_inf()
                    && (other.end_bound_inf()
                        || KeyComparator::compare_encoded_full_key(&other.right, &self.right)
                            == cmp::Ordering::Greater)
                {
                    self.right = other.right.clone();
                    self.right_exclusive = other.right_exclusive;
                }
            }

            fn sstable_overlap(&self, other: &Self) -> bool {
                (self.end_bound_inf()
                    || other.start_bound_inf()
                    || self.compare_right_with(&other.left) != std::cmp::Ordering::Less)
                    && (other.end_bound_inf()
                        || self.start_bound_inf()
                        || other.compare_right_with(&self.left) != std::cmp::Ordering::Less)
            }

            fn sstable_include(&self, other: &Self) -> bool {
                use $crate::key::FullKey;
                (self.end_bound_inf()
                    || self.start_bound_inf()
                    || (self.compare_right_with(&other.right) != std::cmp::Ordering::Less
                        && FullKey::decode(&self.left)
                            .user_key
                            .cmp(&FullKey::decode(&other.left).user_key)
                            != std::cmp::Ordering::Greater))
            }

            fn compare_right_with_user_key(
                &self,
                ukey: $crate::key::UserKey<&[u8]>,
            ) -> std::cmp::Ordering {
                use $crate::key::FullKey;
                let ret = FullKey::decode(&self.right).user_key.cmp(&ukey);
                if ret == cmp::Ordering::Equal && self.right_exclusive {
                    cmp::Ordering::Less
                } else {
                    ret
                }
            }
        }
    };
}

#[macro_export]
macro_rules! key_range_cmp {
    ($left:expr, $right:expr) => {{
        let ret = if $left.start_bound_inf() && $right.start_bound_inf() {
            cmp::Ordering::Equal
        } else if !$left.start_bound_inf() && !$right.start_bound_inf() {
            KeyComparator::compare_encoded_full_key(&$left.left, &$right.left)
        } else if $left.left.is_empty() {
            cmp::Ordering::Less
        } else {
            cmp::Ordering::Greater
        };
        if ret != cmp::Ordering::Equal {
            return ret;
        }
        if $left.end_bound_inf() && $right.end_bound_inf() {
            cmp::Ordering::Equal
        } else if !$left.end_bound_inf() && !$right.end_bound_inf() {
            KeyComparator::compare_encoded_full_key(&$left.right, &$right.right)
        } else if $left.end_bound_inf() {
            cmp::Ordering::Greater
        } else {
            cmp::Ordering::Less
        }
    }};
}

impl_key_range_common!(KeyRange);

impl Ord for KeyRange {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        key_range_cmp!(self, other)
    }
}

impl PartialOrd for KeyRange {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl From<KeyRange> for risingwave_pb::hummock::KeyRange {
    fn from(kr: KeyRange) -> Self {
        risingwave_pb::hummock::KeyRange {
            left: kr.left.to_vec(),
            right: kr.right.to_vec(),
            right_exclusive: kr.right_exclusive,
        }
    }
}

impl From<&risingwave_pb::hummock::KeyRange> for KeyRange {
    fn from(kr: &risingwave_pb::hummock::KeyRange) -> Self {
        KeyRange {
            left: Bytes::copy_from_slice(&kr.left),
            right: Bytes::copy_from_slice(&kr.right),
            right_exclusive: kr.right_exclusive,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::key::key_with_epoch;

    #[test]
    fn test_key_range_compare() {
        let a1_slice = &key_with_epoch(Vec::from("a"), 1);
        let a2_slice = &key_with_epoch(Vec::from("a"), 2);
        let b1_slice = &key_with_epoch(Vec::from("b"), 1);
        let a1 = Bytes::copy_from_slice(a1_slice);
        let a2 = Bytes::copy_from_slice(a2_slice);
        let b1 = Bytes::copy_from_slice(b1_slice);
        assert_eq!(
            KeyRange::new(a1.clone(), a2.clone()).cmp(&KeyRange::new(a2.clone(), a2.clone())),
            cmp::Ordering::Greater
        );
        assert_eq!(
            KeyRange::new(a1.clone(), a2).partial_cmp(&KeyRange::new(a1, b1)),
            Some(cmp::Ordering::Less)
        );
    }
}

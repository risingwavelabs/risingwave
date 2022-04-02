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

use std::cmp;

use bytes::Bytes;

use super::version_cmp::VersionedComparator;

#[derive(PartialEq, Eq, Clone, Debug)]
pub struct KeyRange {
    pub left: Bytes,
    pub right: Bytes,
    pub inf: bool,
}

impl KeyRange {
    pub fn new(left: Bytes, right: Bytes) -> Self {
        Self {
            left,
            right,
            inf: false,
        }
    }

    pub fn inf() -> Self {
        Self {
            left: Bytes::new(),
            right: Bytes::new(),
            inf: true,
        }
    }

    pub fn full_key_overlap(&self, other: &Self) -> bool {
        self.inf
            || other.inf
            || (VersionedComparator::compare_key(&self.right, &other.left) != cmp::Ordering::Less
                && VersionedComparator::compare_key(&other.right, &self.left)
                    != cmp::Ordering::Less)
    }

    pub fn full_key_extend(&mut self, other: &Self) {
        if self.inf {
            return;
        }
        if other.inf {
            self.inf = true;
            self.left = Bytes::new();
            self.right = Bytes::new();
            return;
        }
        if VersionedComparator::compare_key(&other.left, &self.left) == cmp::Ordering::Less {
            self.left = other.left.clone();
        }
        if VersionedComparator::compare_key(&other.right, &self.right) == cmp::Ordering::Greater {
            self.right = other.right.clone();
        }
    }
}

impl Ord for KeyRange {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        match (self.inf, other.inf) {
            (false, false) => VersionedComparator::compare_key(&self.left, &other.left)
                .then_with(|| VersionedComparator::compare_key(&self.right, &other.right)),

            (false, true) => cmp::Ordering::Less,
            (true, false) => cmp::Ordering::Greater,
            (true, true) => cmp::Ordering::Equal,
        }
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
            inf: kr.inf,
        }
    }
}

impl From<&risingwave_pb::hummock::KeyRange> for KeyRange {
    fn from(kr: &risingwave_pb::hummock::KeyRange) -> Self {
        KeyRange::new(
            Bytes::copy_from_slice(&kr.left),
            Bytes::copy_from_slice(&kr.right),
        )
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::key::key_with_epoch;

    use super::*;

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
        assert!(VersionedComparator::same_user_key(a1_slice, a2_slice));
        assert!(!VersionedComparator::same_user_key(a1_slice, b1_slice));
    }
}

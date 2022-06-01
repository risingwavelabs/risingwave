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

use risingwave_hummock_sdk::VersionedComparator;
use risingwave_pb::hummock::KeyRange;

/// Implements a subset methods of `hummock_sdk::KeyRange` for prost `KeyRange`
pub trait KeyRangeExt {
    fn full_key_overlap(&self, other: &Self) -> bool;
    fn full_key_extend(&mut self, other: &Self);
    fn inf() -> Self;
    fn new(left: Vec<u8>, right: Vec<u8>) -> Self;
    fn cmp(&self, other: &Self) -> cmp::Ordering;
}

impl KeyRangeExt for KeyRange {
    fn full_key_overlap(&self, other: &Self) -> bool {
        self.inf
            || other.inf
            || (VersionedComparator::compare_key(&self.right, &other.left) != cmp::Ordering::Less
                && VersionedComparator::compare_key(&other.right, &self.left)
                    != cmp::Ordering::Less)
    }

    fn full_key_extend(&mut self, other: &Self) {
        if self.inf {
            return;
        }
        if other.inf {
            *self = Self::inf();
            return;
        }
        if VersionedComparator::compare_key(&other.left, &self.left) == cmp::Ordering::Less {
            self.left = other.left.clone();
        }
        if VersionedComparator::compare_key(&other.right, &self.right) == cmp::Ordering::Greater {
            self.right = other.right.clone();
        }
    }

    fn inf() -> Self {
        Self {
            left: vec![],
            right: vec![],
            inf: true,
        }
    }

    fn new(left: Vec<u8>, right: Vec<u8>) -> Self {
        Self {
            left,
            right,
            inf: false,
        }
    }

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

#[cfg(test)]
mod tests {
    use std::cmp;

    use risingwave_hummock_sdk::key::key_with_epoch;
    use risingwave_pb::hummock::KeyRange;

    use crate::hummock::model::key_range::KeyRangeExt;

    #[test]
    fn test_cmp() {
        let a1 = key_with_epoch(Vec::from("a"), 1);
        let a2 = key_with_epoch(Vec::from("a"), 2);
        let b1 = key_with_epoch(Vec::from("b"), 1);

        assert_eq!(a1.cmp(&a1), cmp::Ordering::Equal);
        assert_eq!(a2.cmp(&a2), cmp::Ordering::Equal);
        assert_eq!(b1.cmp(&b1), cmp::Ordering::Equal);
        assert_eq!(a1.cmp(&a2), cmp::Ordering::Greater);
        assert_eq!(a2.cmp(&a1), cmp::Ordering::Less);
        assert_eq!(a1.cmp(&b1), cmp::Ordering::Less);
        assert_eq!(b1.cmp(&a1), cmp::Ordering::Greater);
        assert_eq!(a2.cmp(&b1), cmp::Ordering::Less);
        assert_eq!(b1.cmp(&a2), cmp::Ordering::Greater);

        let kr1 = KeyRange::new(a2.clone(), a1.clone());
        let kr2 = KeyRange::new(a2.clone(), b1.clone());
        let kr3 = KeyRange::new(a1.clone(), b1.clone());
        assert_eq!(kr1.cmp(&kr1), cmp::Ordering::Equal);
        assert_eq!(kr2.cmp(&kr2), cmp::Ordering::Equal);
        assert_eq!(kr3.cmp(&kr3), cmp::Ordering::Equal);
        assert_eq!(kr1.cmp(&kr2), cmp::Ordering::Less);
        assert_eq!(kr1.cmp(&kr3), cmp::Ordering::Less);
        assert_eq!(kr2.cmp(&kr3), cmp::Ordering::Less);
        assert_eq!(kr2.cmp(&kr1), cmp::Ordering::Greater);
        assert_eq!(kr3.cmp(&kr1), cmp::Ordering::Greater);
        assert_eq!(kr3.cmp(&kr2), cmp::Ordering::Greater);

        let kr_inf = KeyRange::inf();
        assert_eq!(kr_inf.cmp(&kr_inf), cmp::Ordering::Equal);
        assert_eq!(kr1.cmp(&kr_inf), cmp::Ordering::Less);
    }

    #[test]
    fn test_full_key_overlap() {
        let a1 = key_with_epoch(Vec::from("a"), 1);
        let a2 = key_with_epoch(Vec::from("a"), 2);
        let b1 = key_with_epoch(Vec::from("b"), 1);
        let b2 = key_with_epoch(Vec::from("b"), 2);

        let kr1 = KeyRange::new(a2.clone(), a1.clone());
        let kr2 = KeyRange::new(a2.clone(), b1.clone());
        let kr3 = KeyRange::new(a1.clone(), b2.clone());
        let kr4 = KeyRange::new(a1.clone(), b1.clone());
        let kr5 = KeyRange::new(b2.clone(), b1.clone());

        assert!(kr1.full_key_overlap(&kr2));
        assert!(kr1.full_key_overlap(&kr3));
        assert!(kr1.full_key_overlap(&kr4));
        assert!(kr2.full_key_overlap(&kr3));
        assert!(kr2.full_key_overlap(&kr4));
        assert!(kr3.full_key_overlap(&kr4));
        assert!(!kr1.full_key_overlap(&kr5));
        assert!(kr2.full_key_overlap(&kr5));
        assert!(kr3.full_key_overlap(&kr5));
        assert!(kr4.full_key_overlap(&kr5));

        let kr_inf = KeyRange::inf();
        assert!(kr_inf.full_key_overlap(&kr_inf));
        assert!(kr1.full_key_overlap(&kr_inf));
    }

    #[test]
    fn full_key_extend() {
        let a1 = key_with_epoch(Vec::from("a"), 1);
        let a2 = key_with_epoch(Vec::from("a"), 2);
        let b1 = key_with_epoch(Vec::from("b"), 1);
        let b2 = key_with_epoch(Vec::from("b"), 2);

        let kr1 = KeyRange::new(a2.clone(), a1.clone());
        let kr2 = KeyRange::new(a2.clone(), b2.clone());
        let kr3 = KeyRange::new(a1.clone(), b1.clone());

        let mut kr = kr1.clone();
        kr.full_key_extend(&kr2);
        assert_eq!(kr.cmp(&kr2), cmp::Ordering::Equal);
        kr.full_key_extend(&kr3);
        assert_eq!(kr.cmp(&KeyRange::new(a2, b1)), cmp::Ordering::Equal);

        let kr_inf = KeyRange::inf();
        kr.full_key_extend(&kr_inf);
        assert_eq!(kr.cmp(&kr_inf), cmp::Ordering::Equal);

        kr.full_key_extend(&kr_inf);
        assert_eq!(kr.cmp(&kr_inf), cmp::Ordering::Equal);
    }
}

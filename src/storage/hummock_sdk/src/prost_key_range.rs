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

use risingwave_pb::hummock::KeyRange;

use crate::key_range::KeyRangeCommon;
use crate::{impl_key_range_common, key_range_cmp, VersionedComparator};

impl_key_range_common!(KeyRange);

pub trait KeyRangeExt {
    fn inf() -> Self;
    fn new(left: Vec<u8>, right: Vec<u8>) -> Self;
    fn compare(&self, other: &Self) -> cmp::Ordering;
}

impl KeyRangeExt for KeyRange {
    fn inf() -> Self {
        Self {
            left: vec![],
            right: vec![],
        }
    }

    fn new(left: Vec<u8>, right: Vec<u8>) -> Self {
        Self { left, right }
    }

    fn compare(&self, other: &Self) -> cmp::Ordering {
        key_range_cmp!(self, other)
    }
}

#[cfg(test)]
mod tests {
    use std::cmp;

    use risingwave_pb::hummock::KeyRange;

    use crate::key::key_with_epoch;
    use crate::key_range::KeyRangeCommon;
    use crate::prost_key_range::KeyRangeExt;

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
        let kr2 = KeyRange::new(a2, b1.clone());
        let kr3 = KeyRange::new(a1, b1);
        assert_eq!(kr1.compare(&kr1), cmp::Ordering::Equal);
        assert_eq!(kr2.compare(&kr2), cmp::Ordering::Equal);
        assert_eq!(kr3.compare(&kr3), cmp::Ordering::Equal);
        assert_eq!(kr1.compare(&kr2), cmp::Ordering::Less);
        assert_eq!(kr1.compare(&kr3), cmp::Ordering::Less);
        assert_eq!(kr2.compare(&kr3), cmp::Ordering::Less);
        assert_eq!(kr2.compare(&kr1), cmp::Ordering::Greater);
        assert_eq!(kr3.compare(&kr1), cmp::Ordering::Greater);
        assert_eq!(kr3.compare(&kr2), cmp::Ordering::Greater);

        let kr_inf = KeyRange::inf();
        assert_eq!(kr_inf.compare(&kr_inf), cmp::Ordering::Equal);
        assert_eq!(kr1.compare(&kr_inf), cmp::Ordering::Greater);
    }

    #[test]
    fn test_full_key_overlap() {
        let a1 = key_with_epoch(Vec::from("a"), 1);
        let a2 = key_with_epoch(Vec::from("a"), 2);
        let b1 = key_with_epoch(Vec::from("b"), 1);
        let b2 = key_with_epoch(Vec::from("b"), 2);

        let kr1 = KeyRange::new(a2.clone(), a1.clone());
        let kr2 = KeyRange::new(a2, b1.clone());
        let kr3 = KeyRange::new(a1.clone(), b2.clone());
        let kr4 = KeyRange::new(a1, b1.clone());
        let kr5 = KeyRange::new(b2, b1);

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
        let kr2 = KeyRange::new(a2.clone(), b2);
        let kr3 = KeyRange::new(a1, b1.clone());

        let mut kr = kr1;
        kr.full_key_extend(&kr2);
        assert_eq!(kr.compare(&kr2), cmp::Ordering::Equal);
        kr.full_key_extend(&kr3);
        assert_eq!(kr.compare(&KeyRange::new(a2, b1)), cmp::Ordering::Equal);

        let kr_inf = KeyRange::inf();
        kr.full_key_extend(&kr_inf);
        assert_eq!(kr.compare(&kr_inf), cmp::Ordering::Equal);

        kr.full_key_extend(&kr_inf);
        assert_eq!(kr.compare(&kr_inf), cmp::Ordering::Equal);
    }
}

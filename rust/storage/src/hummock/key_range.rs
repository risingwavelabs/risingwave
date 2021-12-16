use std::cmp;

use bytes::Bytes;

use super::version_cmp::VersionComparator;

/// TODO: Ord Trait with 'a'+ts>'aa'+ts issue
#[derive(PartialEq, Eq, Clone, Debug)]
pub struct KeyRange {
    pub left: Bytes,
    pub right: Bytes,
    inf: bool,
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
            || (VersionComparator::compare_key(&self.right, &other.left) != cmp::Ordering::Less
                && VersionComparator::compare_key(&other.right, &self.left) != cmp::Ordering::Less)
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
        if VersionComparator::compare_key(&other.left, &self.left) == cmp::Ordering::Less {
            self.left = other.left.clone();
        }
        if VersionComparator::compare_key(&other.right, &self.right) == cmp::Ordering::Greater {
            self.right = other.right.clone();
        }
    }
}

impl Ord for KeyRange {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        match (self.inf, other.inf) {
            (false, false) => VersionComparator::compare_key(&self.left, &other.left)
                .then_with(|| VersionComparator::compare_key(&self.right, &other.right)),

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hummock::key::key_with_ts;

    #[test]
    fn test_key_range_compare() {
        let a1_slice = &key_with_ts(Vec::from("a"), 1);
        let a2_slice = &key_with_ts(Vec::from("a"), 2);
        let b1_slice = &key_with_ts(Vec::from("b"), 1);
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
        assert!(VersionComparator::same_user_key(a1_slice, a2_slice));
        assert!(!VersionComparator::same_user_key(a1_slice, b1_slice));
    }
}

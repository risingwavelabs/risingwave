use std::cmp;

use bytes::Bytes;

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

const VERSION_KEY_SUFFIX_LEN: usize = 8;
pub struct VersionComparator();
impl VersionComparator {
    /// Suppose parameter as `full_key` = (`user_key`, `u64::MAX - timestamp`), this function
    /// compare `&[u8]` as if compare tuple mentioned before.
    #[inline]
    pub fn compare_key(lhs: &[u8], rhs: &[u8]) -> cmp::Ordering {
        let (l_p, l_s) = lhs.split_at(lhs.len() - VERSION_KEY_SUFFIX_LEN);
        let (r_p, r_s) = rhs.split_at(rhs.len() - VERSION_KEY_SUFFIX_LEN);
        let res = l_p.cmp(r_p);
        match res {
            cmp::Ordering::Greater | cmp::Ordering::Less => res,
            cmp::Ordering::Equal => l_s.cmp(r_s),
        }
    }

    #[inline]
    pub fn same_user_key(lhs: &[u8], rhs: &[u8]) -> bool {
        let (l_p, _) = lhs.split_at(lhs.len() - VERSION_KEY_SUFFIX_LEN);
        let (r_p, _) = rhs.split_at(rhs.len() - VERSION_KEY_SUFFIX_LEN);
        l_p == r_p
    }
}

impl Ord for KeyRange {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        match (self.inf, other.inf) {
            (false, false) => {
                let lft_res = VersionComparator::compare_key(&self.left, &other.left);
                match lft_res {
                    cmp::Ordering::Greater | cmp::Ordering::Less => lft_res,
                    cmp::Ordering::Equal => {
                        VersionComparator::compare_key(&self.right, &other.right)
                    }
                }
            }
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
    use super::super::table::format::key_with_ts;
    use super::*;

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

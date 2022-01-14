use std::cmp;

use super::key::{split_key_epoch, user_key};

/// Compare two full keys first by their user keys, then by their versions (epochs).
pub struct VersionedComparator;

impl VersionedComparator {
    /// Suppose parameter as `full_key` = (`user_key`, `u64::MAX - epoch`), this function compare
    /// `&[u8]` as if compare tuple mentioned before.
    #[inline]
    pub fn compare_key(lhs: &[u8], rhs: &[u8]) -> cmp::Ordering {
        let (l_p, l_s) = split_key_epoch(lhs);
        let (r_p, r_s) = split_key_epoch(rhs);

        l_p.cmp(r_p).then_with(|| l_s.cmp(r_s))
    }

    #[inline]
    pub fn same_user_key(lhs: &[u8], rhs: &[u8]) -> bool {
        user_key(lhs) == user_key(rhs)
    }
}

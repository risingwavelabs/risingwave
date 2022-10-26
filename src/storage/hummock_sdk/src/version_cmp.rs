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

use super::key::split_key_epoch;
use crate::key::{FullKey, UserKey};

/// Compares two full keys first by their user keys, then by their versions (epochs).
pub struct VersionedComparator;

// TODO: rename to probably `KeyComparator`?
impl VersionedComparator {
    /// Suppose parameter as `full_key` = (`user_key`, `u64::MAX - epoch`), this function compares
    /// `&[u8]` as if compare tuple mentioned before.
    #[inline]
    pub fn compare_encoded_full_key(lhs: &[u8], rhs: &[u8]) -> cmp::Ordering {
        let (l_p, l_s) = split_key_epoch(lhs);
        let (r_p, r_s) = split_key_epoch(rhs);
        l_p.cmp(r_p).then_with(|| l_s.cmp(r_s))
    }

    /// Used to compare [`FullKey`] with different inner `table_key` types.
    pub fn compare_full_key(
        lhs: &FullKey<impl AsRef<[u8]>>,
        rhs: &FullKey<impl AsRef<[u8]>>,
    ) -> Ordering {
        Self::compare_user_key(&lhs.user_key, &rhs.user_key).then_with(|| rhs.epoch.cmp(&lhs.epoch))
    }

    /// Used to compare [`UserKey`] with different inner `table_key` types.
    pub fn compare_user_key(
        lhs: &UserKey<impl AsRef<[u8]>>,
        rhs: &UserKey<impl AsRef<[u8]>>,
    ) -> Ordering {
        lhs.table_id
            .cmp(&rhs.table_id)
            .then_with(|| lhs.table_key.as_ref().cmp(&rhs.table_key.as_ref()))
    }
}

// TODO: ut

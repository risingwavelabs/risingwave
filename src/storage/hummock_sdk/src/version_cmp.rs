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

use super::key::{split_key_epoch, user_key};

/// Compares two full keys first by their user keys, then by their versions (epochs).
pub struct VersionedComparator;

impl VersionedComparator {
    /// Suppose parameter as `full_key` = (`user_key`, `epoch`), this function compares
    /// `&[u8]` as if compare tuple mentioned before.
    #[inline]
    pub fn compare_key(lhs: &[u8], rhs: &[u8]) -> cmp::Ordering {
        let (l_p, l_s) = split_key_epoch(lhs);
        let (r_p, r_s) = split_key_epoch(rhs);
        l_p.cmp(r_p).then_with(|| r_s.cmp(&l_s))
    }

    #[inline]
    pub fn same_user_key(lhs: &[u8], rhs: &[u8]) -> bool {
        user_key(lhs) == user_key(rhs)
    }
}

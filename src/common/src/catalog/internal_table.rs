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

use std::sync::LazyLock;

use itertools::Itertools;
use regex::Regex;

pub const RW_INTERNAL_TABLE_FUNCTION_NAME: &str = "rw_table";

pub fn generate_internal_table_name_with_type(
    mview_name: &str,
    fragment_id: u32,
    table_id: u32,
    table_type: &str,
) -> String {
    format!(
        "__internal_{}_{}_{}_{}",
        mview_name,
        fragment_id,
        table_type.to_lowercase(),
        table_id
    )
}

pub fn valid_table_name(table_name: &str) -> bool {
    static INTERNAL_TABLE_NAME: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r"__internal_.*_\d+").unwrap());
    !INTERNAL_TABLE_NAME.is_match(table_name)
}

pub fn get_dist_key_in_pk_indices(dist_key_indices: &[usize], pk_indices: &[usize]) -> Vec<usize> {
    let dist_key_in_pk_indices = dist_key_indices
        .iter()
        .map(|&di| {
            pk_indices
                .iter()
                .position(|&pi| di == pi)
                .unwrap_or_else(|| {
                    panic!(
                        "distribution key {:?} must be a subset of primary key {:?}",
                        dist_key_indices, pk_indices
                    )
                })
        })
        .collect_vec();
    dist_key_in_pk_indices
}

/// Get distribution key start index in pk, and return None if `dist_key_in_pk_indices` is not empty
/// or continuous.
/// Note that `dist_key_in_pk_indices` may be shuffled, the start index should be the
/// minimum value.
pub fn get_dist_key_start_index_in_pk(dist_key_in_pk_indices: &[usize]) -> Option<usize> {
    let mut sorted_dist_key = dist_key_in_pk_indices.iter().sorted();
    if let Some(min_idx) = sorted_dist_key.next() {
        let mut prev_idx = min_idx;
        for idx in sorted_dist_key {
            if *idx != prev_idx + 1 {
                return None;
            }
            prev_idx = idx;
        }
        Some(*min_idx)
    } else {
        None
    }
}

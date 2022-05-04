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

use std::cmp::Ordering;
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::ops::RangeBounds;

use risingwave_hummock_sdk::key::user_key;
use risingwave_pb::hummock::{Level, SstableInfo};

use super::{HummockError, HummockResult};

pub fn range_overlap<R, B>(
    search_key_range: &R,
    inclusive_start_key: &[u8],
    inclusive_end_key: &[u8],
    backward: bool,
) -> bool
where
    R: RangeBounds<B>,
    B: AsRef<[u8]>,
{
    let (start_bound, end_bound) = if backward {
        (search_key_range.end_bound(), search_key_range.start_bound())
    } else {
        (search_key_range.start_bound(), search_key_range.end_bound())
    };

    //        RANGE
    // TABLE
    let too_left = match start_bound {
        Included(range_start) => range_start.as_ref() > inclusive_end_key,
        Excluded(range_start) => range_start.as_ref() >= inclusive_end_key,
        Unbounded => false,
    };
    // RANGE
    //        TABLE
    let too_right = match end_bound {
        Included(range_end) => range_end.as_ref() < inclusive_start_key,
        Excluded(range_end) => range_end.as_ref() <= inclusive_start_key,
        Unbounded => false,
    };

    !too_left && !too_right
}

pub fn validate_epoch(safe_epoch: u64, epoch: u64) -> HummockResult<()> {
    if epoch < safe_epoch {
        return Err(HummockError::expired_epoch(safe_epoch, epoch));
    }

    Ok(())
}

pub fn validate_table_key_range(levels: &[Level]) -> HummockResult<()> {
    for l in levels {
        for t in &l.table_infos {
            if t.key_range.is_none() {
                return Err(HummockError::meta_error(format!(
                    "key_range in table [{}] is none",
                    t.id
                )));
            }
        }
    }
    Ok(())
}

/// Prune SSTs that does not overlap with a specific key range.
/// Returns the sst ids after pruning
pub fn prune_ssts<'a, R, B>(
    ssts: impl Iterator<Item = &'a SstableInfo>,
    key_range: &R,
    backward: bool,
) -> Vec<&'a SstableInfo>
where
    R: RangeBounds<B> + Send,
    B: AsRef<[u8]> + Send,
{
    let result_sst_ids: Vec<&'a SstableInfo> = ssts
        .filter(|info| {
            let table_range = info.key_range.as_ref().unwrap();
            let table_start = user_key(table_range.left.as_slice());
            let table_end = user_key(table_range.right.as_slice());
            range_overlap(key_range, table_start, table_end, backward)
        })
        .collect();
    result_sst_ids
}

/// Search the SST containing the specified key within a level, using binary search.
pub(crate) fn search_sst_idx<B>(level: &Level, key: &B) -> usize
where
    B: AsRef<[u8]> + Send + ?Sized,
{
    level
        .table_infos
        .partition_point(|table| {
            let ord = user_key(&table.key_range.as_ref().unwrap().left).cmp(key.as_ref());
            ord == Ordering::Less || ord == Ordering::Equal
        })
        .saturating_sub(1) // considering the boundary of 0
}

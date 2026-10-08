// Copyright 2022 RisingWave Labs
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

use std::cmp;
use std::fmt::Debug;
use std::ops::Range;

use itertools::Itertools;
use risingwave_hummock_sdk::KeyComparator;
use risingwave_hummock_sdk::key_range::{KeyRange, KeyRangeCommon};
use risingwave_hummock_sdk::sstable_info::SstableInfo;

pub trait OverlapInfo: Debug {
    fn check_overlap(&self, a: &SstableInfo) -> bool;
    fn check_multiple_overlap(&self, others: &[SstableInfo]) -> Range<usize>;
    fn check_multiple_include(&self, others: &[SstableInfo]) -> Range<usize>;
    fn update(&mut self, range: &KeyRange);
}

pub trait OverlapStrategy: Send + Sync {
    fn check_overlap(&self, a: &SstableInfo, b: &SstableInfo) -> bool;
    fn check_base_level_overlap(
        &self,
        tables: &[SstableInfo],
        others: &[SstableInfo],
    ) -> Vec<SstableInfo> {
        let mut info = self.create_overlap_info();
        for table in tables {
            info.update(&table.key_range);
        }
        let range = info.check_multiple_overlap(others);
        if range.is_empty() {
            vec![]
        } else {
            others[range].to_vec()
        }
    }
    fn check_overlap_with_range(
        &self,
        range: &KeyRange,
        others: &[SstableInfo],
    ) -> Vec<SstableInfo> {
        if others.is_empty() {
            return vec![];
        }
        let mut info = self.create_overlap_info();
        info.update(range);
        others
            .iter()
            .filter(|table| info.check_overlap(table))
            .cloned()
            .collect_vec()
    }

    /// Find overlaps in ordered, non-overlapping `others`.
    /// `previous` may contain the preceding query's result on the same `others`;
    /// both query boundaries must be nondecreasing. Strategies may ignore the hint.
    fn check_overlap_range_with_hint(
        &self,
        range: &KeyRange,
        others: &[SstableInfo],
        _previous: Option<Range<usize>>,
    ) -> Range<usize> {
        let mut info = self.create_overlap_info();
        info.update(range);
        info.check_multiple_overlap(others)
    }

    fn create_overlap_info(&self) -> Box<dyn OverlapInfo>;
}

#[derive(Default, Debug)]
pub struct RangeOverlapInfo {
    target_range: Option<KeyRange>,
}

impl OverlapInfo for RangeOverlapInfo {
    fn check_overlap(&self, a: &SstableInfo) -> bool {
        match self.target_range.as_ref() {
            Some(range) => check_table_overlap(range, a),
            None => false,
        }
    }

    fn check_multiple_overlap(&self, others: &[SstableInfo]) -> Range<usize> {
        match self.target_range.as_ref() {
            Some(key_range) => {
                let overlap_begin = others.partition_point(|table_status| {
                    table_status.key_range.compare_right_with(&key_range.left)
                        == cmp::Ordering::Less
                });
                if overlap_begin >= others.len() {
                    return overlap_begin..overlap_begin;
                }
                let overlap_end = others.partition_point(|table_status| {
                    key_range.compare_right_with(&table_status.key_range.left)
                        != cmp::Ordering::Less
                });
                overlap_begin..overlap_end
            }
            None => others.len()..others.len(),
        }
    }

    fn check_multiple_include(&self, others: &[SstableInfo]) -> Range<usize> {
        match self.target_range.as_ref() {
            Some(key_range) => {
                let overlap_begin = others.partition_point(|table_status| {
                    KeyComparator::compare_encoded_full_key(
                        &table_status.key_range.left,
                        &key_range.left,
                    ) == cmp::Ordering::Less
                });
                if overlap_begin >= others.len() {
                    return overlap_begin..overlap_begin;
                }
                let mut overlap_end = overlap_begin;
                for table in &others[overlap_begin..] {
                    if key_range.compare_right_with(&table.key_range.right) == cmp::Ordering::Less {
                        break;
                    }
                    overlap_end += 1;
                }
                overlap_begin..overlap_end
            }
            None => others.len()..others.len(),
        }
    }

    fn update(&mut self, range: &KeyRange) {
        let other = range;
        if let Some(range) = self.target_range.as_mut() {
            range.full_key_extend(other);
            return;
        }
        self.target_range = Some(other.clone());
    }
}

#[derive(Default)]
pub struct RangeOverlapStrategy {}

impl OverlapStrategy for RangeOverlapStrategy {
    fn check_overlap(&self, a: &SstableInfo, b: &SstableInfo) -> bool {
        check_table_overlap(&a.key_range, b)
    }

    fn check_overlap_range_with_hint(
        &self,
        range: &KeyRange,
        others: &[SstableInfo],
        previous: Option<Range<usize>>,
    ) -> Range<usize> {
        let before = |table: &SstableInfo| {
            table.key_range.compare_right_with(&range.left) == cmp::Ordering::Less
        };
        let overlaps = |table: &SstableInfo| {
            range.compare_right_with(&table.key_range.left) != cmp::Ordering::Less
        };
        if let Some(previous) = previous {
            let begin = previous.start + partition_point_forward(&others[previous.start..], before);
            let end = previous.end.max(begin);
            let end = end + partition_point_forward(&others[end..], overlaps);
            begin..end
        } else {
            // A first query near the end of a large level still costs O(log M).
            let begin = others.partition_point(before);
            let end = begin + others[begin..].partition_point(overlaps);
            begin..end
        }
    }

    fn create_overlap_info(&self) -> Box<dyn OverlapInfo> {
        Box::<RangeOverlapInfo>::default()
    }
}

fn check_table_overlap(key_range: &KeyRange, table: &SstableInfo) -> bool {
    key_range.sstable_overlap(&table.key_range)
}

// Bracket the boundary exponentially, then binary-search the bracket. Starting
// at the previous boundary costs O(1 + log(1 + advancement)), without linearly
// scanning irrelevant SSTs when source ranges are sparse.
fn partition_point_forward<T>(items: &[T], mut pred: impl FnMut(&T) -> bool) -> usize {
    if items.is_empty() || !pred(&items[0]) {
        return 0;
    }
    let mut lo = 1;
    let mut hi = 2.min(items.len());
    while hi < items.len() && pred(&items[hi - 1]) {
        lo = hi;
        hi = hi.saturating_mul(2).min(items.len());
    }
    lo + items[lo..hi].partition_point(pred)
}

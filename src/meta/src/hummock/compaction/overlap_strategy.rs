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

use itertools::Itertools;
use risingwave_hummock_sdk::key::user_key;
use risingwave_hummock_sdk::key_range::KeyRangeCommon;
use risingwave_pb::hummock::{KeyRange, SstableInfo};

pub trait OverlapInfo {
    fn check_overlap(&self, a: &SstableInfo) -> bool;
    fn check_multiple_overlap(&self, others: &[SstableInfo]) -> Vec<SstableInfo>;
    fn update(&mut self, table: &SstableInfo);
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
            info.update(table);
        }
        info.check_multiple_overlap(others)
    }
    fn check_overlap_with_tables(
        &self,
        tables: &[SstableInfo],
        others: &[SstableInfo],
    ) -> Vec<SstableInfo> {
        if tables.is_empty() || others.is_empty() {
            return vec![];
        }
        let mut info = self.create_overlap_info();
        for table in tables {
            info.update(table);
        }
        others
            .iter()
            .filter(|table| info.check_overlap(table))
            .cloned()
            .collect_vec()
    }

    fn create_overlap_info(&self) -> Box<dyn OverlapInfo>;
}

#[derive(Default)]
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

    fn check_multiple_overlap(&self, others: &[SstableInfo]) -> Vec<SstableInfo> {
        match self.target_range.as_ref() {
            Some(key_range) => {
                let mut tables = vec![];
                let overlap_begin = others.partition_point(|table_status| {
                    user_key(&table_status.key_range.as_ref().unwrap().right)
                        < user_key(&key_range.left)
                });
                if overlap_begin >= others.len() {
                    return vec![];
                }
                for table in &others[overlap_begin..] {
                    if user_key(&table.key_range.as_ref().unwrap().left)
                        > user_key(&key_range.right)
                    {
                        break;
                    }
                    tables.push(table.clone());
                }
                tables
            }
            None => vec![],
        }
    }

    fn update(&mut self, table: &SstableInfo) {
        let other = table.key_range.as_ref().unwrap();
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
        let key_range = a.key_range.as_ref().unwrap();
        check_table_overlap(key_range, b)
    }

    fn create_overlap_info(&self) -> Box<dyn OverlapInfo> {
        Box::new(RangeOverlapInfo::default())
    }
}

fn check_table_overlap(key_range: &KeyRange, table: &SstableInfo) -> bool {
    let other = table.key_range.as_ref().unwrap();
    key_range.full_key_overlap(other)
}

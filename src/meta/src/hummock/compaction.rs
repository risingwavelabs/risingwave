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
use std::collections::HashMap;
use std::io::Cursor;

use bytes::Bytes;
use itertools::Itertools;
use prost::Message;
use rand::seq::SliceRandom;
use rand::thread_rng;
use risingwave_common::error::Result;
use risingwave_hummock_sdk::key::{user_key, FullKey};
use risingwave_hummock_sdk::key_range::KeyRange;
use risingwave_hummock_sdk::HummockEpoch;
use risingwave_pb::hummock::{
    CompactMetrics, CompactTask, HummockVersion, Level, LevelEntry, LevelType, SstableInfo,
    TableSetStatistics,
};

use crate::hummock::level_handler::{LevelHandler, SSTableInfo};
use crate::hummock::model::HUMMOCK_DEFAULT_CF_NAME;
use crate::model::Transactional;
use crate::storage;
use crate::storage::{MetaStore, Transaction};

/// Hummock `compact_status` key
/// `cf(hummock_default)`: `hummock_compact_status_key` -> `CompactStatus`
pub(crate) const HUMMOCK_COMPACT_STATUS_KEY: &str = "compact_status";

#[derive(Clone, PartialEq, Debug)]
pub struct CompactStatus {
    pub(crate) level_handlers: Vec<LevelHandler>,
    pub(crate) next_compact_task_id: u64,
}

impl CompactStatus {
    pub fn new() -> CompactStatus {
        let vec_handler_having_l0 = vec![
            LevelHandler::Overlapping(HashMap::new(), vec![]),
            LevelHandler::Nonoverlapping(HashMap::new(), vec![]),
        ];
        CompactStatus {
            level_handlers: vec_handler_having_l0,
            next_compact_task_id: 1,
        }
    }

    fn cf_name() -> &'static str {
        HUMMOCK_DEFAULT_CF_NAME
    }

    fn key() -> &'static str {
        HUMMOCK_COMPACT_STATUS_KEY
    }

    pub async fn get<S: MetaStore>(meta_store: &S) -> Result<Option<CompactStatus>> {
        match meta_store
            .get_cf(CompactStatus::cf_name(), CompactStatus::key().as_bytes())
            .await
            .map(|v| risingwave_pb::hummock::CompactStatus::decode(&mut Cursor::new(v)).unwrap())
            .map(|s| (&s).into())
        {
            Ok(compact_status) => Ok(Some(compact_status)),
            Err(err) => {
                if !matches!(err, storage::Error::ItemNotFound(_)) {
                    return Err(err.into());
                }
                Ok(None)
            }
        }
    }

    pub fn get_compact_task(&mut self, levels: Vec<Level>) -> Option<CompactTask> {
        // When we compact the files, we must make the result of compaction meet the following
        // conditions, for any user key, the epoch of it in the file existing in the lower
        // layer must be larger.

        let get_level_ssts = |level_idx: usize| -> Vec<SSTableInfo> {
            // Sort is required.
            return levels[level_idx]
                .table_infos
                .iter()
                .map_into::<SSTableInfo>()
                .sorted_by(|l, r| l.key_range.cmp(&r.key_range))
                .collect();
        };

        let num_levels = self.level_handlers.len();
        let level_expect_size = (0..num_levels).map(|x| 16 << (2 * x)).collect_vec();
        let mut idle_levels = Vec::with_capacity(num_levels - 1);
        let mut last_level_idle = true;
        for (level_handler_idx, level_handler) in self.level_handlers[..num_levels - 1]
            .iter()
            .enumerate()
            .rev()
        {
            match level_handler {
                LevelHandler::Overlapping(_, compacting_key_ranges)
                | LevelHandler::Nonoverlapping(_, compacting_key_ranges) => {
                    last_level_idle = if compacting_key_ranges.is_empty() {
                        if last_level_idle {
                            idle_levels.push(level_handler_idx);
                        }
                        true
                    } else {
                        false
                    }
                }
            }
        }
        let (select_level, must_l0_to_l0) = if idle_levels.is_empty() {
            (0, true)
        } else {
            (*idle_levels.last().unwrap() as u32, false)
        };

        let l0_idle_sst_num = {
            let l0 = match self.level_handlers.first().unwrap() {
                LevelHandler::Overlapping(l_0, _) => l_0,
                LevelHandler::Nonoverlapping(l_0, _) => l_0,
            };
            levels.first().unwrap().table_infos.len() - l0.len()
        };
        if must_l0_to_l0 && l0_idle_sst_num <= *level_expect_size.first().unwrap() {
            return None;
        }

        enum SearchResult {
            Found(Vec<SstableInfo>, Vec<SstableInfo>, Vec<KeyRange>),
            NotFound,
        }

        let mut found = SearchResult::NotFound;
        let next_task_id = self.next_compact_task_id;
        let (prior, posterior) = self.level_handlers.split_at_mut(select_level as usize + 1);
        let target_level = select_level + 1;
        let (prior, posterior) = (prior.last_mut().unwrap(), posterior.first_mut().unwrap());
        let is_select_level_leveling = matches!(prior, LevelHandler::Nonoverlapping(_, _));
        let is_target_level_leveling = matches!(posterior, LevelHandler::Nonoverlapping(_, _));
        let ord_table_id = |x: u64, y: u64| {
            if x == y {
                Ordering::Equal
            } else if y.wrapping_sub(x) <= u64::MAX / 2 {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        };
        // Try to select and merge table(s) in `select_level` into `target_level`
        match prior {
            LevelHandler::Overlapping(compacting_ssts_l_n, compacting_key_ranges_l_n)
            | LevelHandler::Nonoverlapping(compacting_ssts_l_n, compacting_key_ranges_l_n) => {
                let l_n = get_level_ssts(select_level as usize);
                let l_n_len = l_n.len();
                let mut polysst_candidates = Vec::with_capacity(l_n_len);
                {
                    let mut sst_idx = 0;
                    while sst_idx < l_n_len {
                        let mut next_sst_idx = sst_idx + 1;
                        let sst_key_range = l_n[sst_idx].key_range.clone();
                        let mut select_level_inputs = vec![l_n[sst_idx].clone().into()];
                        let mut earliest_table_id = l_n[sst_idx].table_id;
                        // Must ensure that there exists no SSTs in `select_level` which have
                        // overlapping user key with `select_level_inputs`
                        let key_range = if !is_select_level_leveling {
                            let mut tier_key_range = sst_key_range.clone();

                            next_sst_idx = sst_idx;
                            for (delta_idx, other) in l_n[sst_idx + 1..].iter().enumerate() {
                                if user_key(&other.key_range.left)
                                    <= user_key(&tier_key_range.right)
                                {
                                    if ord_table_id(other.table_id, earliest_table_id)
                                        == Ordering::Less
                                    {
                                        earliest_table_id = other.table_id;
                                    }
                                    tier_key_range.full_key_extend(&other.key_range);
                                    select_level_inputs.push(other.into());
                                } else {
                                    next_sst_idx = sst_idx + 1 + delta_idx;
                                    break;
                                }
                            }
                            if next_sst_idx == sst_idx {
                                next_sst_idx = l_n_len;
                            }

                            tier_key_range
                        } else {
                            sst_key_range
                        };

                        polysst_candidates.push((
                            earliest_table_id,
                            (sst_idx, next_sst_idx),
                            select_level_inputs,
                            key_range.clone(),
                        ));

                        sst_idx = next_sst_idx;
                    }
                }

                if select_level == 0 {
                    polysst_candidates.sort_by(|(table_id_a, ..), (table_id_b, ..)| {
                        ord_table_id(*table_id_a, *table_id_b)
                    });
                } else {
                    let mut rng = thread_rng();
                    polysst_candidates.shuffle(&mut rng);
                }

                for (_, (sst_idx, next_sst_idx), select_level_inputs, key_range) in
                    polysst_candidates
                {
                    let mut is_select_idle = true;
                    for SSTableInfo { table_id, .. } in &l_n[sst_idx..next_sst_idx] {
                        if compacting_ssts_l_n.contains_key(table_id) {
                            is_select_idle = false;
                            break;
                        }
                    }

                    if is_select_idle {
                        let insert_point = compacting_key_ranges_l_n.partition_point(
                            |(ongoing_key_range, _, _)| {
                                user_key(&ongoing_key_range.right) < user_key(&key_range.left)
                            },
                        );
                        // if following condition is not satisfied, it may result in two overlapping
                        // SSTs in target level
                        if insert_point >= compacting_key_ranges_l_n.len()
                            || user_key(&compacting_key_ranges_l_n[insert_point].0.left)
                                > user_key(&key_range.right)
                        {
                            match posterior {
                                LevelHandler::Overlapping(_, _) => unimplemented!(),
                                LevelHandler::Nonoverlapping(compacting_ssts_l_n_suc, _) => {
                                    let l_n_suc = &get_level_ssts(target_level as usize);
                                    let mut overlap_all_idle = true;
                                    let overlap_begin = l_n_suc.partition_point(|table_status| {
                                        user_key(&table_status.key_range.right)
                                            < user_key(&key_range.left)
                                    });
                                    let mut overlap_end = overlap_begin;
                                    let l_n_suc_len = l_n_suc.len();
                                    while overlap_end < l_n_suc_len
                                        && user_key(&l_n_suc[overlap_end].key_range.left)
                                            <= user_key(&key_range.right)
                                    {
                                        if compacting_ssts_l_n_suc
                                            .contains_key(&l_n_suc[overlap_end].table_id)
                                        {
                                            overlap_all_idle = false;
                                            break;
                                        }
                                        overlap_end += 1;
                                    }
                                    if overlap_all_idle {
                                        // Here, we have known that `select_level_input` is valid
                                        compacting_key_ranges_l_n.insert(
                                            insert_point,
                                            (
                                                key_range,
                                                next_task_id,
                                                select_level_inputs.len() as u64,
                                            ),
                                        );

                                        let mut suc_table_ids =
                                            Vec::with_capacity(overlap_end - overlap_begin);

                                        let mut splits =
                                            Vec::with_capacity(overlap_end - overlap_begin);
                                        splits.push(KeyRange::new(Bytes::new(), Bytes::new()));
                                        let mut key_split_append = |key_before_last: &Bytes| {
                                            splits.last_mut().unwrap().right =
                                                key_before_last.clone();
                                            splits.push(KeyRange::new(
                                                key_before_last.clone(),
                                                Bytes::new(),
                                            ));
                                        };

                                        let mut overlap_idx = overlap_begin;
                                        while overlap_idx < overlap_end {
                                            compacting_ssts_l_n_suc.insert(
                                                l_n_suc[overlap_idx].table_id,
                                                next_task_id,
                                            );
                                            suc_table_ids.push(l_n_suc[overlap_idx].clone().into());
                                            if overlap_idx > overlap_begin {
                                                // TODO: We do not need to add splits every time. We
                                                // can add every K SSTs.
                                                key_split_append(
                                                    &FullKey::from_user_key_slice(
                                                        user_key(
                                                            &l_n_suc[overlap_idx].key_range.left,
                                                        ),
                                                        HummockEpoch::MAX,
                                                    )
                                                    .into_inner()
                                                    .into(),
                                                );
                                            }
                                            overlap_idx += 1;
                                        }

                                        found = SearchResult::Found(
                                            select_level_inputs,
                                            suc_table_ids,
                                            splits,
                                        );
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
                match &found {
                    SearchResult::Found(select_ln_ids, _, _) => {
                        // Set compact task id for selected SSTs
                        for table in select_ln_ids {
                            compacting_ssts_l_n.insert(table.id, next_task_id);
                        }
                    }
                    SearchResult::NotFound => {}
                }
            }
        }
        match found {
            SearchResult::Found(select_ln_ids, select_lnsuc_ids, splits) => {
                self.next_compact_task_id += 1;
                let select_level_type = if is_select_level_leveling {
                    LevelType::Nonoverlapping as i32
                } else {
                    LevelType::Overlapping as i32
                };
                let target_level_type = if is_target_level_leveling {
                    LevelType::Nonoverlapping as i32
                } else {
                    LevelType::Overlapping as i32
                };
                let compact_task = CompactTask {
                    input_ssts: vec![
                        LevelEntry {
                            level_idx: select_level,
                            level: Some(Level {
                                level_type: select_level_type,
                                table_infos: select_ln_ids,
                            }),
                        },
                        LevelEntry {
                            level_idx: target_level,
                            level: Some(Level {
                                level_type: target_level_type,
                                table_infos: select_lnsuc_ids,
                            }),
                        },
                    ],
                    splits: splits.iter().map(|v| v.clone().into()).collect_vec(),
                    watermark: HummockEpoch::MAX,
                    sorted_output_ssts: vec![],
                    task_id: next_task_id,
                    target_level,
                    is_target_ultimate_and_leveling: target_level as usize
                        == self.level_handlers.len() - 1
                        && is_target_level_leveling,
                    metrics: Some(CompactMetrics {
                        read_level_n: Some(TableSetStatistics {
                            level_idx: select_level,
                            size_gb: 0f64,
                            cnt: 0,
                        }),
                        read_level_nplus1: Some(TableSetStatistics {
                            level_idx: target_level,
                            size_gb: 0f64,
                            cnt: 0,
                        }),
                        write: Some(TableSetStatistics {
                            level_idx: target_level,
                            size_gb: 0f64,
                            cnt: 0,
                        }),
                    }),
                    task_status: false,
                };
                Some(compact_task)
            }
            SearchResult::NotFound => None,
        }
    }

    /// Declares a task is either finished or canceled.
    pub fn report_compact_task(&mut self, compact_task: &CompactTask) {
        for LevelEntry { level_idx, .. } in &compact_task.input_ssts {
            self.level_handlers[*level_idx as usize].remove_task(compact_task.task_id);
        }
    }

    /// Applies the compact task result and get a new hummock version.
    pub fn apply_compact_result(
        compact_task: &CompactTask,
        based_hummock_version: HummockVersion,
    ) -> HummockVersion {
        let mut new_version = based_hummock_version;
        new_version.safe_epoch = std::cmp::max(new_version.safe_epoch, compact_task.watermark);
        for (idx, input_level) in compact_task.input_ssts.iter().enumerate() {
            new_version.levels[idx].table_infos.retain(|sst| {
                input_level
                    .level
                    .as_ref()
                    .unwrap()
                    .table_infos
                    .iter()
                    .all(|stale| sst.id != stale.id)
            });
        }
        new_version.levels[compact_task.target_level as usize]
            .table_infos
            .extend(compact_task.sorted_output_ssts.clone());
        new_version
    }
}

impl Transactional for CompactStatus {
    fn upsert_in_transaction(&self, trx: &mut Transaction) -> Result<()> {
        trx.put(
            CompactStatus::cf_name().to_string(),
            CompactStatus::key().as_bytes().to_vec(),
            risingwave_pb::hummock::CompactStatus::from(self).encode_to_vec(),
        );
        Ok(())
    }

    fn delete_in_transaction(&self, trx: &mut Transaction) -> Result<()> {
        trx.delete(
            CompactStatus::cf_name().to_string(),
            CompactStatus::key().as_bytes().to_vec(),
        );
        Ok(())
    }
}

impl Default for CompactStatus {
    fn default() -> Self {
        Self::new()
    }
}

impl From<&CompactStatus> for risingwave_pb::hummock::CompactStatus {
    fn from(status: &CompactStatus) -> Self {
        risingwave_pb::hummock::CompactStatus {
            level_handlers: status.level_handlers.iter().map_into().collect(),
            next_compact_task_id: status.next_compact_task_id,
        }
    }
}

impl From<&risingwave_pb::hummock::CompactStatus> for CompactStatus {
    fn from(status: &risingwave_pb::hummock::CompactStatus) -> Self {
        CompactStatus {
            level_handlers: status.level_handlers.iter().map_into().collect(),
            next_compact_task_id: status.next_compact_task_id,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_serde() -> Result<()> {
        let origin = CompactStatus {
            level_handlers: vec![],
            next_compact_task_id: 3,
        };
        let ser = risingwave_pb::hummock::CompactStatus::from(&origin).encode_to_vec();
        let de = risingwave_pb::hummock::CompactStatus::decode(&mut Cursor::new(ser));
        let de = (&de.unwrap()).into();
        assert_eq!(origin, de);

        Ok(())
    }
}

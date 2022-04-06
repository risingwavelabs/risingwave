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

use std::io::Cursor;

use bytes::Bytes;
use itertools::{EitherOrBoth, Itertools};
use prost::Message;
use rand::seq::SliceRandom;
use rand::thread_rng;
use risingwave_common::error::Result;
use risingwave_common::storage::key::{user_key, FullKey};
use risingwave_common::storage::key_range::KeyRange;
use risingwave_common::storage::{HummockEpoch, HummockSSTableId};
use risingwave_pb::hummock::{
    CompactMetrics, CompactTask, Level, LevelEntry, LevelType, TableSetStatistics,
};

use crate::hummock::level_handler::{LevelHandler, SSTableStat};
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
            LevelHandler::Overlapping(vec![], vec![]),
            LevelHandler::Nonoverlapping(vec![], vec![]),
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

    pub fn get_compact_task(&mut self) -> Option<CompactTask> {
        // When we compact the files, we must make the result of compaction meet the following
        // conditions, for any user key, the epoch of it in the file existing in the lower
        // layer must be larger.
        let num_levels = self.level_handlers.len();
        let mut idle_levels = Vec::with_capacity(num_levels - 1);
        for (level_handler_idx, level_handler) in
            self.level_handlers[..num_levels - 1].iter().enumerate()
        {
            match level_handler {
                LevelHandler::Overlapping(_, compacting_key_ranges)
                | LevelHandler::Nonoverlapping(_, compacting_key_ranges) => {
                    if compacting_key_ranges.is_empty() {
                        idle_levels.push(level_handler_idx);
                    }
                }
            }
        }
        let select_level = if idle_levels.is_empty() {
            0
        } else {
            *idle_levels.first().unwrap() as u32
        };

        enum SearchResult {
            Found(Vec<u64>, Vec<u64>, Vec<KeyRange>),
            NotFound,
        }

        let mut found = SearchResult::NotFound;
        let next_task_id = self.next_compact_task_id;
        let (prior, posterior) = self.level_handlers.split_at_mut(select_level as usize + 1);
        let target_level = select_level + 1;
        let (prior, posterior) = (prior.last_mut().unwrap(), posterior.first_mut().unwrap());
        let is_select_level_leveling = matches!(prior, LevelHandler::Nonoverlapping(_, _));
        let is_target_level_leveling = matches!(posterior, LevelHandler::Nonoverlapping(_, _));
        // Try to select and merge table(s) in `select_level` into `target_level`
        match prior {
            LevelHandler::Overlapping(l_n, compacting_key_ranges)
            | LevelHandler::Nonoverlapping(l_n, compacting_key_ranges) => {
                let l_n_len = l_n.len();
                let mut polysst_candidates = Vec::with_capacity(l_n_len);
                {
                    let mut sst_idx = 0;
                    while sst_idx < l_n_len {
                        let mut next_sst_idx = sst_idx + 1;
                        let SSTableStat {
                            key_range: sst_key_range,
                            table_id,
                            ..
                        } = &l_n[sst_idx];
                        let mut select_level_inputs = vec![*table_id];
                        let key_range;
                        let mut tier_key_range;
                        // Must ensure that there exists no SSTs in `select_level` which have
                        // overlapping user key with `select_level_inputs`
                        if !is_select_level_leveling {
                            tier_key_range = sst_key_range.clone();

                            next_sst_idx = sst_idx;
                            for (
                                delta_idx,
                                SSTableStat {
                                    key_range: other_key_range,
                                    table_id: other_table_id,
                                    ..
                                },
                            ) in l_n[sst_idx + 1..].iter().enumerate()
                            {
                                if user_key(&other_key_range.left)
                                    <= user_key(&tier_key_range.right)
                                {
                                    select_level_inputs.push(*other_table_id);
                                    tier_key_range.full_key_extend(other_key_range);
                                } else {
                                    next_sst_idx = sst_idx + 1 + delta_idx;
                                    break;
                                }
                            }
                            if next_sst_idx == sst_idx {
                                next_sst_idx = l_n_len;
                            }

                            key_range = &tier_key_range;
                        } else {
                            key_range = sst_key_range;
                        }

                        polysst_candidates.push((
                            (sst_idx, next_sst_idx),
                            select_level_inputs,
                            key_range.clone(),
                        ));

                        sst_idx = next_sst_idx;
                    }
                }

                let mut rng = thread_rng();
                polysst_candidates.shuffle(&mut rng);

                for ((sst_idx, next_sst_idx), select_level_inputs, key_range) in polysst_candidates
                {
                    let mut is_select_idle = true;
                    for SSTableStat { compact_task, .. } in &l_n[sst_idx..next_sst_idx] {
                        if compact_task.is_some() {
                            is_select_idle = false;
                            break;
                        }
                    }

                    if is_select_idle {
                        let insert_point =
                            compacting_key_ranges.partition_point(|(ongoing_key_range, _, _)| {
                                user_key(&ongoing_key_range.right) < user_key(&key_range.left)
                            });
                        // if following condition is not satisfied, it may result in two overlapping
                        // SSTs in target level
                        if insert_point >= compacting_key_ranges.len()
                            || user_key(&compacting_key_ranges[insert_point].0.left)
                                > user_key(&key_range.right)
                        {
                            match posterior {
                                LevelHandler::Overlapping(_, _) => unimplemented!(),
                                LevelHandler::Nonoverlapping(l_n_suc, _) => {
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
                                        if l_n_suc[overlap_end].compact_task.is_some() {
                                            overlap_all_idle = false;
                                            break;
                                        }
                                        overlap_end += 1;
                                    }
                                    if overlap_all_idle {
                                        // Here, we have known that `select_level_input` is valid
                                        compacting_key_ranges.insert(
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
                                            l_n_suc[overlap_idx].compact_task = Some(next_task_id);
                                            suc_table_ids.push(l_n_suc[overlap_idx].table_id);
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
                        let mut select_ln_iter = select_ln_ids.iter();
                        if let Some(first_id) = select_ln_iter.next() {
                            let mut current_id = first_id;
                            for SSTableStat {
                                table_id,
                                compact_task,
                                ..
                            } in l_n
                            {
                                if table_id == current_id {
                                    *compact_task = Some(next_task_id);
                                    match select_ln_iter.next() {
                                        Some(next_id) => {
                                            current_id = next_id;
                                        }
                                        None => break,
                                    }
                                }
                            }
                        }
                    }
                    SearchResult::NotFound => {}
                }
            }
        }
        match found {
            SearchResult::Found(select_ln_ids, select_lnsuc_ids, splits) => {
                self.next_compact_task_id += 1;
                let compact_task = CompactTask {
                    input_ssts: vec![
                        LevelEntry {
                            level_idx: select_level,
                            level: if is_select_level_leveling {
                                Some(Level {
                                    level_type: LevelType::Nonoverlapping as i32,
                                    table_ids: select_ln_ids,
                                })
                            } else {
                                Some(Level {
                                    level_type: LevelType::Overlapping as i32,
                                    table_ids: select_ln_ids,
                                })
                            },
                        },
                        LevelEntry {
                            level_idx: target_level,
                            level: if is_target_level_leveling {
                                Some(Level {
                                    level_type: LevelType::Nonoverlapping as i32,
                                    table_ids: select_lnsuc_ids,
                                })
                            } else {
                                Some(Level {
                                    level_type: LevelType::Overlapping as i32,
                                    table_ids: select_lnsuc_ids,
                                })
                            },
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

    /// Return Some(Vec<table id to delete>) if succeeds.
    /// Return None if the task has been processed previously.
    #[allow(clippy::needless_collect)]
    pub fn report_compact_task(
        &mut self,
        output_table_compact_entries: Vec<SSTableStat>,
        compact_task: CompactTask,
    ) -> Option<Vec<HummockSSTableId>> {
        let mut delete_table_ids = vec![];
        let task_result = compact_task.task_status;
        match task_result {
            true => {
                for LevelEntry { level_idx, .. } in compact_task.input_ssts {
                    delete_table_ids.extend(
                        self.level_handlers[level_idx as usize]
                            .pop_task_input(compact_task.task_id)
                            .into_iter(),
                    );
                }
                if delete_table_ids.is_empty() {
                    // The task has been processed previously.
                    return None;
                }
                match &mut self.level_handlers[compact_task.target_level as usize] {
                    LevelHandler::Overlapping(l_n, _) | LevelHandler::Nonoverlapping(l_n, _) => {
                        let old_ln = std::mem::take(l_n);
                        *l_n = itertools::merge_join_by(
                            old_ln,
                            output_table_compact_entries,
                            |l, r| l.key_range.cmp(&r.key_range),
                        )
                        .flat_map(|either_or_both| match either_or_both {
                            EitherOrBoth::Both(a, b) => vec![a, b].into_iter(),
                            EitherOrBoth::Left(a) => vec![a].into_iter(),
                            EitherOrBoth::Right(b) => vec![b].into_iter(),
                        })
                        .collect();
                    }
                }
                // The task is finished successfully.
                Some(delete_table_ids)
            }
            false => {
                if !self.cancel_compact_task(&compact_task) {
                    // The task has been processed previously.
                    return None;
                }
                // The task is cancelled successfully.
                Some(vec![])
            }
        }
    }

    pub fn cancel_compact_task(&mut self, compact_task: &CompactTask) -> bool {
        let mut changed = false;
        for LevelEntry { level_idx, .. } in &compact_task.input_ssts {
            changed = changed
                || self.level_handlers[*level_idx as usize].unassign_task(compact_task.task_id);
        }
        changed
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

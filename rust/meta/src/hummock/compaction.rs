use bytes::Bytes;
use itertools::{EitherOrBoth, Itertools};
use risingwave_common::error::Result;
use risingwave_pb::hummock::{CompactTask, Level, LevelEntry, LevelType, SstableInfo};
use risingwave_storage::hummock::key::{user_key, FullKey};
use risingwave_storage::hummock::key_range::KeyRange;
use risingwave_storage::hummock::{HummockError, HummockSSTableId, HummockSnapshotId};
use serde::{Deserialize, Serialize};

use crate::hummock::level_handler::{LevelHandler, SSTableStat};
use crate::manager::{MetaSrvEnv, SINGLE_VERSION_EPOCH};
use crate::storage::{ColumnFamilyUtils, Operation, Transaction};

// TODO define CompactStatus in prost instead
#[derive(Clone, PartialEq, Debug, Serialize, Deserialize)]
pub struct CompactStatus {
    pub(crate) level_handlers: Vec<LevelHandler>,
    pub(crate) next_compact_task_id: u64,
}

pub(crate) struct CompactionInner {
    env: MetaSrvEnv,
}

impl CompactionInner {
    pub fn new(env: MetaSrvEnv) -> CompactionInner {
        CompactionInner { env }
    }

    pub async fn load_compact_status(&self) -> Result<CompactStatus> {
        self.env
            .meta_store_ref()
            .get_cf(
                self.env.config().get_hummock_default_cf(),
                self.env
                    .config()
                    .get_hummock_compact_status_key()
                    .as_bytes(),
                SINGLE_VERSION_EPOCH,
            )
            .await
            .map(|v| bincode::deserialize(&v).unwrap())
    }

    pub fn save_compact_status_in_transaction(
        &self,
        trx: &mut Box<dyn Transaction>,
        compact_status: &CompactStatus,
    ) {
        trx.add_operations(vec![Operation::Put(
            ColumnFamilyUtils::prefix_key_with_cf(
                self.env
                    .config()
                    .get_hummock_compact_status_key()
                    .as_bytes(),
                self.env.config().get_hummock_default_cf().as_bytes(),
            ),
            bincode::serialize(&compact_status).unwrap(),
            vec![],
        )]);
    }

    /// We assume that SSTs will only be deleted in compaction, otherwise `get_compact_task` need to
    /// `pin`
    pub async fn get_compact_task(
        &self,
        mut compact_status: CompactStatus,
    ) -> Result<(CompactStatus, CompactTask)> {
        let select_level = 0u32;

        enum SearchResult {
            Found(Vec<u64>, Vec<u64>, Vec<KeyRange>),
            NotFound,
        }

        let mut found = SearchResult::NotFound;
        let next_task_id = compact_status.next_compact_task_id;
        let (prior, posterior) = compact_status
            .level_handlers
            .split_at_mut(select_level as usize + 1);
        let (prior, posterior) = (prior.last_mut().unwrap(), posterior.first_mut().unwrap());
        let is_select_level_leveling = matches!(prior, LevelHandler::Nonoverlapping(_, _));
        let target_level = select_level + 1;
        let is_target_level_leveling = matches!(posterior, LevelHandler::Nonoverlapping(_, _));
        match prior {
            LevelHandler::Overlapping(l_n, compacting_key_ranges)
            | LevelHandler::Nonoverlapping(l_n, compacting_key_ranges) => {
                let mut sst_idx = 0;
                let l_n_len = l_n.len();
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
                            if user_key(&other_key_range.left) <= user_key(&tier_key_range.right) {
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

                    let mut is_select_idle = true;
                    for SSTableStat { compact_task, .. } in &l_n[sst_idx..next_sst_idx] {
                        if compact_task.is_some() {
                            is_select_idle = false;
                            break;
                        }
                    }

                    if is_select_idle {
                        let insert_point =
                            compacting_key_ranges.partition_point(|(ongoing_key_range, _)| {
                                user_key(&ongoing_key_range.right) < user_key(&key_range.left)
                            });
                        if insert_point >= compacting_key_ranges.len()
                            || user_key(&compacting_key_ranges[insert_point].0.left)
                                > user_key(&key_range.right)
                        {
                            match posterior {
                                LevelHandler::Overlapping(_, _) => unimplemented!(),
                                LevelHandler::Nonoverlapping(l_n_suc, _) => {
                                    let mut overlap_all_idle = true;
                                    // TODO: use pointer last time to avoid binary search
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
                                        compacting_key_ranges.insert(
                                            insert_point,
                                            (key_range.clone(), next_task_id),
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
                                                        HummockSnapshotId::MAX,
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
                    sst_idx = next_sst_idx;
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
                compact_status.next_compact_task_id += 1;
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
                    watermark: HummockSnapshotId::MAX,
                    sorted_output_ssts: vec![],
                    task_id: next_task_id,
                    target_level,
                    is_target_ultimate_and_leveling: target_level as usize
                        == compact_status.level_handlers.len() - 1
                        && is_target_level_leveling,
                };
                Ok((compact_status, compact_task))
            }
            SearchResult::NotFound => Err(HummockError::no_compact_task_found().into()),
        }
    }

    #[allow(clippy::needless_collect)]
    pub fn report_compact_task(
        &self,
        mut compact_status: CompactStatus,
        output_table_compact_entries: Vec<SSTableStat>,
        compact_task: CompactTask,
        task_result: bool,
    ) -> (CompactStatus, Vec<SstableInfo>, Vec<HummockSSTableId>) {
        let mut delete_table_ids = vec![];
        match task_result {
            true => {
                for LevelEntry { level_idx, .. } in compact_task.input_ssts {
                    delete_table_ids.extend(
                        compact_status.level_handlers[level_idx as usize]
                            .pop_task_input(compact_task.task_id)
                            .into_iter(),
                    );
                }
                match &mut compact_status.level_handlers[compact_task.target_level as usize] {
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
                (
                    compact_status,
                    compact_task.sorted_output_ssts,
                    delete_table_ids,
                )
            }
            false => {
                // TODO: loop only in input levels
                for level_handler in &mut compact_status.level_handlers {
                    level_handler.unassign_task(compact_task.task_id);
                }
                (compact_status, vec![], vec![])
            }
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
        let ser = bincode::serialize(&origin).unwrap();
        let de = bincode::deserialize(&ser).unwrap();
        assert_eq!(origin, de);

        Ok(())
    }
}

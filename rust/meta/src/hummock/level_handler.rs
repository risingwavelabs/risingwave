use bytes::Bytes;
use risingwave_pb::hummock::SstableInfo;
use risingwave_storage::hummock::key_range::KeyRange;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SSTableStat {
    pub key_range: KeyRange,
    pub table_id: u64,
    pub compact_task: Option<u64>,
}

impl From<&SstableInfo> for SSTableStat {
    fn from(info: &SstableInfo) -> Self {
        SSTableStat {
            key_range: KeyRange::new(
                Bytes::copy_from_slice(&info.key_range.as_ref().unwrap().left),
                Bytes::copy_from_slice(&info.key_range.as_ref().unwrap().right),
            ),
            table_id: info.id,
            compact_task: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum LevelHandler {
    /// * `Vec<SSTableStat>` - existing SSTs in this level, arranged in order no matter Tiering or
    ///   Leveling
    /// * `Vec<(KeyRange, u64, u64)>` - key ranges (and corresponding compaction task id, #SSTs) to
    ///   be merged to bottom level in order
    Nonoverlapping(Vec<SSTableStat>, Vec<(KeyRange, u64, u64)>),
    Overlapping(Vec<SSTableStat>, Vec<(KeyRange, u64, u64)>),
}

impl LevelHandler {
    fn clear_compacting_range(&mut self, clear_task_id: u64) {
        match self {
            LevelHandler::Overlapping(_, compacting_key_ranges)
            | LevelHandler::Nonoverlapping(_, compacting_key_ranges) => {
                compacting_key_ranges.retain(|(_, task_id, _)| *task_id != clear_task_id);
            }
        }
    }

    pub fn unassign_task(&mut self, unassign_task_id: u64) -> bool {
        self.clear_compacting_range(unassign_task_id);
        let mut changed = false;
        match self {
            LevelHandler::Overlapping(l_n, _) | LevelHandler::Nonoverlapping(l_n, _) => {
                for SSTableStat { compact_task, .. } in l_n {
                    if *compact_task == Some(unassign_task_id) {
                        *compact_task = None;
                        changed = true;
                    }
                }
            }
        }
        changed
    }

    pub fn pop_task_input(&mut self, finished_task_id: u64) -> Vec<u64> {
        self.clear_compacting_range(finished_task_id);

        let mut deleted_table_ids = vec![];
        let deleted_table_ids_ref = &mut deleted_table_ids;
        match self {
            LevelHandler::Overlapping(l_n, _) | LevelHandler::Nonoverlapping(l_n, _) => {
                l_n.retain(
                    |SSTableStat {
                         table_id,
                         compact_task,
                         ..
                     }| {
                        if *compact_task != Some(finished_task_id) {
                            true
                        } else {
                            deleted_table_ids_ref.push(*table_id);
                            false
                        }
                    },
                );
            }
        }
        deleted_table_ids
    }
}

use super::key_range::KeyRange;

// TODO: should store Arc<SSTable> instead of table_id in SSTableStat
#[derive(Clone)]
pub struct SSTableStat {
    pub key_range: KeyRange,
    pub sstable_id: u64,
    pub compact_task: Option<u64>,
}

#[derive(Clone)]
pub enum LevelHandler {
    /// * `Vec<SSTableStat>` - existing SSTs in this level, arranged in order no matter Tiering or
    ///   Leveling
    /// * `Vec<(KeyRange, u64)>` - key ranges (and corresponding compaction task id) to be merged
    ///   to bottom level in order
    Leveling(Vec<SSTableStat>, Vec<(KeyRange, u64)>),
    Tiering(Vec<SSTableStat>, Vec<(KeyRange, u64)>),
}

impl LevelHandler {
    fn clear_compacting_range(&mut self, clear_task_id: u64) {
        match self {
            LevelHandler::Tiering(_, compacting_key_ranges)
            | LevelHandler::Leveling(_, compacting_key_ranges) => {
                compacting_key_ranges.retain(|(_, task_id)| *task_id != clear_task_id);
            }
        }
    }

    pub fn unassign_task(&mut self, unassign_task_id: u64) {
        self.clear_compacting_range(unassign_task_id);

        match self {
            LevelHandler::Tiering(l_n, _) | LevelHandler::Leveling(l_n, _) => {
                for SSTableStat { compact_task, .. } in l_n {
                    if *compact_task == Some(unassign_task_id) {
                        *compact_task = None;
                    }
                }
            }
        }
    }

    pub fn pop_task_input(&mut self, finished_task_id: u64) -> Vec<u64> {
        self.clear_compacting_range(finished_task_id);

        let mut deleted_table_ids = vec![];
        let deleted_table_ids_ref = &mut deleted_table_ids;
        match self {
            LevelHandler::Tiering(l_n, _) | LevelHandler::Leveling(l_n, _) => {
                l_n.retain(
                    |SSTableStat {
                         sstable_id: table_id,
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

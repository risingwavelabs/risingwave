use super::key_range::KeyRange;

// TODO: should store Arc<Table> instead of table_id in TableStat
#[derive(Clone)]
pub struct TableStat {
    pub key_range: KeyRange,
    pub table_id: u64,
    pub compact_task: Option<u64>,
}

#[derive(Clone)]
pub enum LevelHandler {
    /// 
    /// * `Vec<TableStat>` - existing SSTs in this level, arranged in order if Leveling
    /// * `Vec<(KeyRange, u64)>` - upper key ranges (and corresponding compaction task id) to be
    ///   merged to this level in order
    Leveling(Vec<TableStat>, Vec<(KeyRange, u64)>),
    Tiering(Vec<TableStat>),
}

impl LevelHandler {
    fn clear_inserting_range(&mut self, clear_task_id: u64) {
        if let LevelHandler::Leveling(_, inserting_key_ranges) = self {
            inserting_key_ranges.retain(|(_, task_id)| *task_id != clear_task_id);
        }
    }

    pub fn unassign_task(&mut self, unassign_task_id: u64) {
        self.clear_inserting_range(unassign_task_id);

        match self {
            LevelHandler::Tiering(l_n) | LevelHandler::Leveling(l_n, _) => {
                for TableStat { compact_task, .. } in l_n {
                    if *compact_task == Some(unassign_task_id) {
                        *compact_task = None;
                    }
                }
            }
        }
    }

    pub fn pop_task_input(&mut self, finished_task_id: u64) -> Vec<u64> {
        self.clear_inserting_range(finished_task_id);

        let mut deleted_table_ids = vec![];
        let deleted_table_ids_ref = &mut deleted_table_ids;
        match self {
            LevelHandler::Tiering(l_n) | LevelHandler::Leveling(l_n, _) => {
                l_n.retain(
                    |TableStat {
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

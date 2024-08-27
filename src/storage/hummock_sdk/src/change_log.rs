// Copyright 2024 RisingWave Labs
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

use std::collections::HashMap;

use risingwave_common::catalog::TableId;
use risingwave_pb::hummock::hummock_version_delta::ChangeLogDelta;
use risingwave_pb::hummock::{EpochNewChangeLog, SstableInfo, TableChangeLog as PbTableChangeLog};
use tracing::warn;

#[derive(Debug, Clone, PartialEq)]
pub struct TableChangeLog(pub Vec<EpochNewChangeLog>);

impl TableChangeLog {
    pub fn filter_epoch(&self, (min_epoch, max_epoch): (u64, u64)) -> &[EpochNewChangeLog] {
        let start = self.0.partition_point(|epoch_change_log| {
            epoch_change_log.epochs.last().expect("non-empty") < &min_epoch
        });
        let end = self.0.partition_point(|epoch_change_log| {
            epoch_change_log.epochs.first().expect("non-empty") <= &max_epoch
        });
        &self.0[start..end]
    }

    /// Returns epochs where value is non-null and >= `min_epoch`.
    pub fn get_non_empty_epochs(&self, min_epoch: u64, max_count: usize) -> Vec<u64> {
        self.filter_epoch((min_epoch, u64::MAX))
            .iter()
            .filter(|epoch_change_log| {
                // Filter out empty change logs
                let new_value_empty = epoch_change_log.new_value.is_empty();
                let old_value_empty = epoch_change_log.old_value.is_empty();
                !new_value_empty || !old_value_empty
            })
            .flat_map(|epoch_change_log| epoch_change_log.epochs.iter().cloned())
            .filter(|a| a >= &min_epoch)
            .clone()
            .take(max_count)
            .collect()
    }

    pub fn truncate(&mut self, truncate_epoch: u64) {
        // TODO: may optimize by using VecDeque to maintain the log
        self.0
            .retain(|change_log| *change_log.epochs.last().expect("non-empty") > truncate_epoch);
        if let Some(first_log) = self.0.first_mut() {
            first_log.epochs.retain(|epoch| *epoch > truncate_epoch);
        }
    }
}

impl TableChangeLog {
    pub fn to_protobuf(&self) -> PbTableChangeLog {
        PbTableChangeLog {
            change_logs: self.0.clone(),
        }
    }

    pub fn from_protobuf(val: &PbTableChangeLog) -> Self {
        Self(val.change_logs.clone())
    }
}

pub fn build_table_change_log_delta<'a>(
    old_value_ssts: impl Iterator<Item = SstableInfo>,
    new_value_ssts: impl Iterator<Item = &'a SstableInfo>,
    epochs: &Vec<u64>,
    log_store_table_ids: impl Iterator<Item = (u32, u64)>,
) -> HashMap<TableId, ChangeLogDelta> {
    let mut table_change_log: HashMap<_, _> = log_store_table_ids
        .map(|(table_id, truncate_epoch)| {
            (
                TableId::new(table_id),
                ChangeLogDelta {
                    truncate_epoch,
                    new_log: Some(EpochNewChangeLog {
                        new_value: vec![],
                        old_value: vec![],
                        epochs: epochs.clone(),
                    }),
                },
            )
        })
        .collect();
    for sst in old_value_ssts {
        for table_id in &sst.table_ids {
            match table_change_log.get_mut(&TableId::new(*table_id)) {
                Some(log) => {
                    log.new_log.as_mut().unwrap().old_value.push(sst.clone());
                }
                None => {
                    warn!(table_id, ?sst, "old value sst contains non-log-store table");
                }
            }
        }
    }
    for sst in new_value_ssts {
        for table_id in &sst.table_ids {
            if let Some(log) = table_change_log.get_mut(&TableId::new(*table_id)) {
                log.new_log.as_mut().unwrap().new_value.push(sst.clone());
            }
        }
    }
    table_change_log
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;

    use crate::change_log::{EpochNewChangeLog, TableChangeLog};

    #[test]
    fn test_filter_epoch() {
        let table_change_log = TableChangeLog(vec![
            EpochNewChangeLog {
                new_value: vec![],
                old_value: vec![],
                epochs: vec![2],
            },
            EpochNewChangeLog {
                new_value: vec![],
                old_value: vec![],
                epochs: vec![3, 4],
            },
            EpochNewChangeLog {
                new_value: vec![],
                old_value: vec![],
                epochs: vec![5],
            },
        ]);

        let epochs = [1, 2, 3, 4, 5, 6];
        for i in 0..epochs.len() {
            for j in i..epochs.len() {
                let min_epoch = epochs[i];
                let max_epoch = epochs[j];
                let expected = table_change_log
                    .0
                    .iter()
                    .filter(|log| {
                        &min_epoch <= log.epochs.last().unwrap()
                            && log.epochs.first().unwrap() <= &max_epoch
                    })
                    .cloned()
                    .collect_vec();
                let actual = table_change_log.filter_epoch((min_epoch, max_epoch));
                assert_eq!(&expected, actual, "{:?}", (min_epoch, max_epoch));
            }
        }
    }

    #[test]
    fn test_truncate() {
        let mut table_change_log = TableChangeLog(vec![
            EpochNewChangeLog {
                new_value: vec![],
                old_value: vec![],
                epochs: vec![1],
            },
            EpochNewChangeLog {
                new_value: vec![],
                old_value: vec![],
                epochs: vec![2],
            },
            EpochNewChangeLog {
                new_value: vec![],
                old_value: vec![],
                epochs: vec![3, 4],
            },
            EpochNewChangeLog {
                new_value: vec![],
                old_value: vec![],
                epochs: vec![5],
            },
        ]);

        table_change_log.truncate(1);
        assert_eq!(
            table_change_log,
            TableChangeLog(vec![
                EpochNewChangeLog {
                    new_value: vec![],
                    old_value: vec![],
                    epochs: vec![2],
                },
                EpochNewChangeLog {
                    new_value: vec![],
                    old_value: vec![],
                    epochs: vec![3, 4],
                },
                EpochNewChangeLog {
                    new_value: vec![],
                    old_value: vec![],
                    epochs: vec![5],
                },
            ])
        );

        table_change_log.truncate(3);
        assert_eq!(
            table_change_log,
            TableChangeLog(vec![
                EpochNewChangeLog {
                    new_value: vec![],
                    old_value: vec![],
                    epochs: vec![4],
                },
                EpochNewChangeLog {
                    new_value: vec![],
                    old_value: vec![],
                    epochs: vec![5],
                },
            ])
        )
    }
}

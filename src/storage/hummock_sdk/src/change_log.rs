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

use std::collections::{HashMap, HashSet};

use risingwave_common::catalog::TableId;
use risingwave_pb::hummock::{
    EpochNewChangeLog as PbEpochNewChangeLog, PbTableChangeLog, SstableInfo,
};

#[derive(Debug, Clone, PartialEq)]
pub struct EpochNewChangeLog {
    pub new_value: Vec<SstableInfo>,
    pub old_value: Vec<SstableInfo>,
    pub epochs: Vec<u64>,
}

impl EpochNewChangeLog {
    pub fn to_protobuf(&self) -> PbEpochNewChangeLog {
        PbEpochNewChangeLog {
            epochs: self.epochs.clone(),
            new_value: self.new_value.clone(),
            old_value: self.old_value.clone(),
        }
    }

    pub fn from_protobuf(epoch_new_log: &PbEpochNewChangeLog) -> Self {
        Self {
            epochs: epoch_new_log.epochs.clone(),
            new_value: epoch_new_log.new_value.clone(),
            old_value: epoch_new_log.old_value.clone(),
        }
    }
}

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
}

impl TableChangeLog {
    pub fn to_protobuf(&self) -> PbTableChangeLog {
        PbTableChangeLog {
            change_logs: self
                .0
                .iter()
                .map(|epoch_new_log| epoch_new_log.to_protobuf())
                .collect(),
        }
    }

    pub fn from_protobuf(val: &PbTableChangeLog) -> Self {
        Self(
            val.change_logs
                .iter()
                .map(EpochNewChangeLog::from_protobuf)
                .collect(),
        )
    }
}

pub fn build_table_new_change_log<'a>(
    old_value_ssts: impl Iterator<Item = SstableInfo>,
    new_value_ssts: impl Iterator<Item = &'a SstableInfo>,
    epochs: &Vec<u64>,
    log_store_table_ids: impl Iterator<Item = u32>,
) -> HashMap<TableId, EpochNewChangeLog> {
    let log_store_table_ids: HashSet<u32> = log_store_table_ids.collect();
    let mut table_change_log = HashMap::new();
    for sst in old_value_ssts {
        for table_id in &sst.table_ids {
            assert!(log_store_table_ids.contains(table_id), "old value sst has table_id not enabled log store: log store tables: {:?} sst: {:?}", log_store_table_ids, sst);
            table_change_log
                .entry(TableId::from(*table_id))
                .or_insert_with(|| EpochNewChangeLog {
                    new_value: vec![],
                    old_value: vec![],
                    epochs: epochs.clone(),
                })
                .old_value
                .push(sst.clone());
        }
    }
    for sst in new_value_ssts {
        for table_id in &sst.table_ids {
            if log_store_table_ids.contains(table_id) {
                table_change_log
                    .entry(TableId::from(*table_id))
                    .or_insert_with(|| EpochNewChangeLog {
                        new_value: vec![],
                        old_value: vec![],
                        epochs: epochs.clone(),
                    })
                    .new_value
                    .push(sst.clone());
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
}

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

use risingwave_pb::hummock::{PbEpochNewChangeLog, PbTableChangeLog, SstableInfo};

#[derive(Debug, Clone, PartialEq)]
pub struct EpochNewChangeLog {
    pub new_value: Vec<SstableInfo>,
    pub old_value: Vec<SstableInfo>,
    pub epochs: Vec<u64>,
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
                .map(|epoch_new_log| PbEpochNewChangeLog {
                    epochs: epoch_new_log.epochs.clone(),
                    new_value: epoch_new_log.new_value.clone(),
                    old_value: epoch_new_log.old_value.clone(),
                })
                .collect(),
        }
    }

    pub fn from_protobuf(val: &PbTableChangeLog) -> Self {
        Self(
            val.change_logs
                .iter()
                .map(|epoch_new_log| EpochNewChangeLog {
                    epochs: epoch_new_log.epochs.clone(),
                    new_value: epoch_new_log.new_value.clone(),
                    old_value: epoch_new_log.old_value.clone(),
                })
                .collect(),
        )
    }
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

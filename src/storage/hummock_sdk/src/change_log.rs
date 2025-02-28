// Copyright 2025 RisingWave Labs
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

use std::collections::{HashMap, VecDeque};

use risingwave_common::catalog::TableId;
use risingwave_pb::hummock::hummock_version_delta::PbChangeLogDelta;
use risingwave_pb::hummock::{PbEpochNewChangeLog, PbSstableInfo, PbTableChangeLog};
use tracing::warn;

use crate::sstable_info::SstableInfo;

#[derive(Debug, Clone, PartialEq)]
pub struct TableChangeLogCommon<T>(
    // older log at the front
    VecDeque<EpochNewChangeLogCommon<T>>,
);

impl<T> TableChangeLogCommon<T> {
    pub fn new(logs: impl IntoIterator<Item = EpochNewChangeLogCommon<T>>) -> Self {
        let logs = logs.into_iter().collect::<VecDeque<_>>();
        debug_assert!(logs.iter().flat_map(|log| log.epochs.iter()).is_sorted());
        Self(logs)
    }

    pub fn iter(&self) -> impl Iterator<Item = &EpochNewChangeLogCommon<T>> {
        self.0.iter()
    }

    pub fn iter_mut(&mut self) -> impl Iterator<Item = &mut EpochNewChangeLogCommon<T>> {
        self.0.iter_mut()
    }

    pub fn add_change_log(&mut self, new_change_log: EpochNewChangeLogCommon<T>) {
        if let Some(prev_log) = self.0.back() {
            assert!(
                prev_log.epochs.last().expect("non-empty")
                    < new_change_log.epochs.first().expect("non-empty")
            );
        }
        self.0.push_back(new_change_log);
    }

    pub fn epochs(&self) -> impl Iterator<Item = u64> + '_ {
        self.0
            .iter()
            .flat_map(|epoch_change_log| epoch_change_log.epochs.iter())
            .cloned()
    }

    pub(crate) fn change_log_into_iter(self) -> impl Iterator<Item = EpochNewChangeLogCommon<T>> {
        self.0.into_iter()
    }

    pub(crate) fn change_log_iter_mut(
        &mut self,
    ) -> impl Iterator<Item = &mut EpochNewChangeLogCommon<T>> {
        self.0.iter_mut()
    }
}

pub type TableChangeLog = TableChangeLogCommon<SstableInfo>;

#[derive(Debug, Clone, PartialEq)]
pub struct EpochNewChangeLogCommon<T> {
    pub new_value: Vec<T>,
    pub old_value: Vec<T>,
    // epochs are sorted in ascending order
    pub epochs: Vec<u64>,
}

pub type EpochNewChangeLog = EpochNewChangeLogCommon<SstableInfo>;

impl<T> From<&EpochNewChangeLogCommon<T>> for PbEpochNewChangeLog
where
    PbSstableInfo: for<'a> From<&'a T>,
{
    fn from(val: &EpochNewChangeLogCommon<T>) -> Self {
        Self {
            new_value: val.new_value.iter().map(|a| a.into()).collect(),
            old_value: val.old_value.iter().map(|a| a.into()).collect(),
            epochs: val.epochs.clone(),
        }
    }
}

impl<T> From<&PbEpochNewChangeLog> for EpochNewChangeLogCommon<T>
where
    T: for<'a> From<&'a PbSstableInfo>,
{
    fn from(value: &PbEpochNewChangeLog) -> Self {
        Self {
            new_value: value.new_value.iter().map(|a| a.into()).collect(),
            old_value: value.old_value.iter().map(|a| a.into()).collect(),
            epochs: value.epochs.clone(),
        }
    }
}

impl<T> From<EpochNewChangeLogCommon<T>> for PbEpochNewChangeLog
where
    PbSstableInfo: From<T>,
{
    fn from(val: EpochNewChangeLogCommon<T>) -> Self {
        Self {
            new_value: val.new_value.into_iter().map(|a| a.into()).collect(),
            old_value: val.old_value.into_iter().map(|a| a.into()).collect(),
            epochs: val.epochs,
        }
    }
}

impl<T> From<PbEpochNewChangeLog> for EpochNewChangeLogCommon<T>
where
    T: From<PbSstableInfo>,
{
    fn from(value: PbEpochNewChangeLog) -> Self {
        Self {
            new_value: value.new_value.into_iter().map(|a| a.into()).collect(),
            old_value: value.old_value.into_iter().map(|a| a.into()).collect(),
            epochs: value.epochs,
        }
    }
}

impl<T> TableChangeLogCommon<T> {
    pub fn filter_epoch(
        &self,
        (min_epoch, max_epoch): (u64, u64),
    ) -> impl Iterator<Item = &EpochNewChangeLogCommon<T>> + '_ {
        let start = self.0.partition_point(|epoch_change_log| {
            epoch_change_log.epochs.last().expect("non-empty") < &min_epoch
        });
        let end = self.0.partition_point(|epoch_change_log| {
            epoch_change_log.epochs.first().expect("non-empty") <= &max_epoch
        });
        self.0.range(start..end)
    }

    /// Get the `next_epoch` of the given `epoch`
    /// Return:
    ///     - Ok(Some(`next_epoch`)): the `next_epoch` of `epoch`
    ///     - Ok(None): `next_epoch` of `epoch` is not added to change log yet
    ///     - Err(()): `epoch` is not an existing or to exist one
    #[expect(clippy::result_unit_err)]
    pub fn next_epoch(&self, epoch: u64) -> Result<Option<u64>, ()> {
        let start = self.0.partition_point(|epoch_change_log| {
            epoch_change_log.epochs.last().expect("non-empty") < &epoch
        });
        debug_assert!(
            self.0
                .range(start..)
                .flat_map(|epoch_change_log| epoch_change_log.epochs.iter())
                .is_sorted()
        );
        let mut later_epochs = self
            .0
            .range(start..)
            .flat_map(|epoch_change_log| epoch_change_log.epochs.iter())
            .skip_while(|log_epoch| **log_epoch < epoch);
        if let Some(first_epoch) = later_epochs.next() {
            assert!(
                *first_epoch >= epoch,
                "first_epoch {} < epoch {}",
                first_epoch,
                epoch
            );
            if *first_epoch != epoch {
                return Err(());
            }
            if let Some(next_epoch) = later_epochs.next() {
                assert!(
                    *next_epoch > epoch,
                    "next_epoch {} not exceed epoch {}",
                    next_epoch,
                    epoch
                );
                Ok(Some(*next_epoch))
            } else {
                // `epoch` is latest
                Ok(None)
            }
        } else {
            // all epochs are less than `epoch`
            Ok(None)
        }
    }

    /// Returns epochs where value is non-null and >= `min_epoch`.
    pub fn get_non_empty_epochs(&self, min_epoch: u64, max_count: usize) -> Vec<u64> {
        self.filter_epoch((min_epoch, u64::MAX))
            .filter(|epoch_change_log| {
                // Filter out empty change logs
                let new_value_empty = epoch_change_log.new_value.is_empty();
                let old_value_empty = epoch_change_log.old_value.is_empty();
                !new_value_empty || !old_value_empty
            })
            .flat_map(|epoch_change_log| epoch_change_log.epochs.iter().cloned())
            .filter(|a| a >= &min_epoch)
            .take(max_count)
            .collect()
    }

    pub fn truncate(&mut self, truncate_epoch: u64) {
        while let Some(change_log) = self.0.front()
            && *change_log.epochs.last().expect("non-empty") < truncate_epoch
        {
            let _change_log = self.0.pop_front().expect("non-empty");
        }
        if let Some(first_log) = self.0.front_mut() {
            first_log.epochs.retain(|epoch| *epoch >= truncate_epoch);
        }
    }
}

impl<T> TableChangeLogCommon<T>
where
    PbSstableInfo: for<'a> From<&'a T>,
{
    pub fn to_protobuf(&self) -> PbTableChangeLog {
        PbTableChangeLog {
            change_logs: self.0.iter().map(|a| a.into()).collect(),
        }
    }
}

impl<T> TableChangeLogCommon<T>
where
    T: for<'a> From<&'a PbSstableInfo>,
{
    pub fn from_protobuf(val: &PbTableChangeLog) -> Self {
        Self(val.change_logs.iter().map(|a| a.into()).collect())
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
                    new_log: EpochNewChangeLog {
                        new_value: vec![],
                        old_value: vec![],
                        epochs: epochs.clone(),
                    },
                },
            )
        })
        .collect();
    for sst in old_value_ssts {
        for table_id in &sst.table_ids {
            match table_change_log.get_mut(&TableId::new(*table_id)) {
                Some(log) => {
                    log.new_log.old_value.push(sst.clone());
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
                log.new_log.new_value.push(sst.clone());
            }
        }
    }
    table_change_log
}

#[derive(Debug, PartialEq, Clone)]
pub struct ChangeLogDeltaCommon<T> {
    pub truncate_epoch: u64,
    pub new_log: EpochNewChangeLogCommon<T>,
}

pub type ChangeLogDelta = ChangeLogDeltaCommon<SstableInfo>;

impl<T> From<&ChangeLogDeltaCommon<T>> for PbChangeLogDelta
where
    PbSstableInfo: for<'a> From<&'a T>,
{
    fn from(val: &ChangeLogDeltaCommon<T>) -> Self {
        Self {
            truncate_epoch: val.truncate_epoch,
            new_log: Some((&val.new_log).into()),
        }
    }
}

impl<T> From<&PbChangeLogDelta> for ChangeLogDeltaCommon<T>
where
    T: for<'a> From<&'a PbSstableInfo>,
{
    fn from(val: &PbChangeLogDelta) -> Self {
        Self {
            truncate_epoch: val.truncate_epoch,
            new_log: val.new_log.as_ref().unwrap().into(),
        }
    }
}

impl<T> From<ChangeLogDeltaCommon<T>> for PbChangeLogDelta
where
    PbSstableInfo: From<T>,
{
    fn from(val: ChangeLogDeltaCommon<T>) -> Self {
        Self {
            truncate_epoch: val.truncate_epoch,
            new_log: Some(val.new_log.into()),
        }
    }
}

impl<T> From<PbChangeLogDelta> for ChangeLogDeltaCommon<T>
where
    T: From<PbSstableInfo>,
{
    fn from(val: PbChangeLogDelta) -> Self {
        Self {
            truncate_epoch: val.truncate_epoch,
            new_log: val.new_log.unwrap().into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;

    use crate::change_log::{EpochNewChangeLog, TableChangeLogCommon};
    use crate::sstable_info::SstableInfo;

    #[test]
    fn test_filter_epoch() {
        let table_change_log = TableChangeLogCommon::<SstableInfo>::new([
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
                epochs: vec![6],
            },
            EpochNewChangeLog {
                new_value: vec![],
                old_value: vec![],
                epochs: vec![8, 10],
            },
        ]);

        let epochs = (1..=11).collect_vec();
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
                let actual = table_change_log
                    .filter_epoch((min_epoch, max_epoch))
                    .cloned()
                    .collect_vec();
                assert_eq!(expected, actual, "{:?}", (min_epoch, max_epoch));
            }
        }

        let existing_epochs = table_change_log.epochs().collect_vec();
        assert!(existing_epochs.is_sorted());
        for &epoch in &epochs {
            let expected = match existing_epochs
                .iter()
                .position(|existing_epoch| *existing_epoch >= epoch)
            {
                None => {
                    // all existing epochs are less than epoch
                    Ok(None)
                }
                Some(i) => {
                    let this_epoch = existing_epochs[i];
                    assert!(this_epoch >= epoch);
                    if this_epoch == epoch {
                        if i + 1 == existing_epochs.len() {
                            // epoch is the latest epoch
                            Ok(None)
                        } else {
                            Ok(Some(existing_epochs[i + 1]))
                        }
                    } else {
                        // epoch not a existing epoch
                        Err(())
                    }
                }
            };
            assert_eq!(expected, table_change_log.next_epoch(epoch));
        }
    }

    #[test]
    fn test_truncate() {
        let mut table_change_log = TableChangeLogCommon::<SstableInfo>::new([
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
        let origin_table_change_log = table_change_log.clone();
        for truncate_epoch in 0..6 {
            table_change_log.truncate(truncate_epoch);
            let expected_table_change_log = TableChangeLogCommon(
                origin_table_change_log
                    .0
                    .iter()
                    .filter_map(|epoch_change_log| {
                        let mut epoch_change_log = epoch_change_log.clone();
                        epoch_change_log
                            .epochs
                            .retain(|epoch| *epoch >= truncate_epoch);
                        if epoch_change_log.epochs.is_empty() {
                            None
                        } else {
                            Some(epoch_change_log)
                        }
                    })
                    .collect(),
            );
            assert_eq!(expected_table_change_log, table_change_log);
        }
    }
}

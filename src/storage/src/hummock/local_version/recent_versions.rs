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

use std::cmp::Ordering;
use std::sync::Arc;

use risingwave_common::catalog::TableId;
use risingwave_hummock_sdk::HummockEpoch;

use crate::hummock::local_version::pinned_version::PinnedVersion;
use crate::monitor::HummockStateStoreMetrics;

pub struct RecentVersions {
    latest_version: PinnedVersion,
    is_latest_committed: bool,
    recent_versions: Vec<PinnedVersion>, // earlier version at the front
    max_version_num: usize,
    metric: Arc<HummockStateStoreMetrics>,
}

impl RecentVersions {
    pub fn new(
        version: PinnedVersion,
        max_version_num: usize,
        metric: Arc<HummockStateStoreMetrics>,
    ) -> Self {
        assert!(max_version_num > 0);
        Self {
            latest_version: version,
            is_latest_committed: true, // The first version is always treated as committed epochs
            recent_versions: Vec::new(),
            max_version_num,
            metric,
        }
    }

    fn has_table_committed(&self, new_version: &PinnedVersion) -> bool {
        let mut has_table_committed = false;
        for (table_id, info) in new_version.state_table_info.info() {
            if let Some(prev_info) = self.latest_version.state_table_info.info().get(table_id) {
                match info.committed_epoch.cmp(&prev_info.committed_epoch) {
                    Ordering::Less => {
                        unreachable!(
                            "table {} has regress committed epoch {}, prev committed epoch {}",
                            table_id, info.committed_epoch, prev_info.committed_epoch
                        );
                    }
                    Ordering::Equal => {}
                    Ordering::Greater => {
                        has_table_committed = true;
                    }
                }
            } else {
                has_table_committed = true;
            }
        }
        has_table_committed
    }

    #[must_use]
    pub fn with_new_version(&self, version: PinnedVersion) -> Self {
        assert!(version.id > self.latest_version.id);
        let is_committed = self.has_table_committed(&version);
        let recent_versions = if self.is_latest_committed {
            let prev_recent_versions = if self.recent_versions.len() >= self.max_version_num {
                assert_eq!(self.recent_versions.len(), self.max_version_num);
                &self.recent_versions[1..]
            } else {
                &self.recent_versions[..]
            };
            let mut recent_versions = Vec::with_capacity(prev_recent_versions.len() + 1);
            recent_versions.extend(prev_recent_versions.iter().cloned());
            recent_versions.push(self.latest_version.clone());
            recent_versions
        } else {
            self.recent_versions.clone()
        };
        Self {
            latest_version: version,
            is_latest_committed: is_committed,
            recent_versions,
            max_version_num: self.max_version_num,
            metric: self.metric.clone(),
        }
    }

    pub fn latest_version(&self) -> &PinnedVersion {
        &self.latest_version
    }

    /// Return the latest version that is safe to read `epoch` on `table_id`.
    ///
    /// `safe to read` means that the `committed_epoch` of the `table_id` in the version won't be greater than the given `epoch`
    pub fn get_safe_version(
        &self,
        table_id: TableId,
        epoch: HummockEpoch,
    ) -> Option<PinnedVersion> {
        let result = if let Some(info) = self.latest_version.state_table_info.info().get(&table_id)
        {
            if info.committed_epoch <= epoch {
                Some(self.latest_version.clone())
            } else {
                self.get_safe_version_from_recent(table_id, epoch)
            }
        } else {
            None
        };
        if result.is_some() {
            self.metric.safe_version_hit.inc();
        } else {
            self.metric.safe_version_miss.inc();
        }
        result
    }

    fn get_safe_version_from_recent(
        &self,
        table_id: TableId,
        epoch: HummockEpoch,
    ) -> Option<PinnedVersion> {
        if cfg!(debug_assertions) {
            assert!(
                epoch
                    < self
                        .latest_version
                        .state_table_info
                        .info()
                        .get(&table_id)
                        .expect("should exist")
                        .committed_epoch
            );
        }
        let result = self.recent_versions.binary_search_by(|version| {
            let committed_epoch = version.table_committed_epoch(table_id);
            if let Some(committed_epoch) = committed_epoch {
                committed_epoch.cmp(&epoch)
            } else {
                // We have ensured that the table_id exists in the latest version, so if the table_id does not exist in a
                // previous version, the table must have not created yet, and therefore has  less ordering.
                Ordering::Less
            }
        });
        match result {
            Ok(index) => Some(self.recent_versions[index].clone()),
            Err(index) => {
                // `index` is index of the first version that has `committed_epoch` greater than `epoch`
                // or `index` equals `recent_version.len()` when `epoch` is greater than all `committed_epoch`
                let version = if index >= self.recent_versions.len() {
                    assert_eq!(index, self.recent_versions.len());
                    self.recent_versions.last().cloned()
                } else if index == 0 {
                    // The earliest version has a higher committed epoch
                    None
                } else {
                    self.recent_versions.get(index - 1).cloned()
                };
                version.and_then(|version| {
                    if version.state_table_info.info().contains_key(&table_id) {
                        Some(version)
                    } else {
                        // if the table does not exist in the version, return `None` to try get a time travel version
                        None
                    }
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use risingwave_common::catalog::TableId;
    use risingwave_hummock_sdk::version::HummockVersion;
    use risingwave_pb::hummock::{PbHummockVersion, StateTableInfo};
    use tokio::sync::mpsc::unbounded_channel;

    use crate::hummock::local_version::pinned_version::PinnedVersion;
    use crate::hummock::local_version::recent_versions::RecentVersions;
    use crate::monitor::HummockStateStoreMetrics;

    const TEST_TABLE_ID1: TableId = TableId::new(233);
    const TEST_TABLE_ID2: TableId = TableId::new(234);

    fn gen_pin_version(
        version_id: u64,
        table_committed_epoch: impl IntoIterator<Item = (TableId, u64)>,
    ) -> PinnedVersion {
        PinnedVersion::new(
            HummockVersion::from_rpc_protobuf(&PbHummockVersion {
                id: version_id,
                state_table_info: HashMap::from_iter(table_committed_epoch.into_iter().map(
                    |(table_id, committed_epoch)| {
                        (
                            table_id.as_raw_id(),
                            StateTableInfo {
                                committed_epoch,
                                compaction_group_id: 0,
                            },
                        )
                    },
                )),
                ..Default::default()
            }),
            unbounded_channel().0,
        )
    }

    fn assert_query_equal(
        recent_version: &RecentVersions,
        expected: &[(TableId, u64, Option<&PinnedVersion>)],
    ) {
        for (table_id, epoch, expected_version) in expected
            .iter()
            .cloned()
            .chain([(TEST_TABLE_ID1, 0, None), (TEST_TABLE_ID2, 0, None)])
        {
            let version = recent_version.get_safe_version(table_id, epoch);
            assert_eq!(
                version.as_ref().map(|version| version.id()),
                expected_version.map(|version| version.id())
            );
        }
    }

    #[test]
    fn test_basic() {
        let epoch1 = 233;
        let epoch0 = epoch1 - 1;
        let epoch2 = epoch1 + 1;
        let epoch3 = epoch2 + 1;
        let epoch4 = epoch3 + 1;
        let version1 = gen_pin_version(1, [(TEST_TABLE_ID1, epoch1)]);
        // with at most 2 historical versions
        let recent_versions = RecentVersions::new(
            version1.clone(),
            2,
            HummockStateStoreMetrics::unused().into(),
        );
        assert!(recent_versions.recent_versions.is_empty());
        assert!(recent_versions.is_latest_committed);
        assert_query_equal(
            &recent_versions,
            &[
                (TEST_TABLE_ID1, epoch0, None),
                (TEST_TABLE_ID1, epoch1, Some(&version1)),
                (TEST_TABLE_ID1, epoch2, Some(&version1)),
            ],
        );

        let recent_versions =
            recent_versions.with_new_version(gen_pin_version(2, [(TEST_TABLE_ID1, epoch1)]));
        assert_eq!(recent_versions.recent_versions.len(), 1);
        assert!(!recent_versions.is_latest_committed);

        let version3 = gen_pin_version(3, [(TEST_TABLE_ID1, epoch2)]);
        let recent_versions = recent_versions.with_new_version(version3.clone());
        assert_eq!(recent_versions.recent_versions.len(), 1);
        assert!(recent_versions.is_latest_committed);
        assert_query_equal(
            &recent_versions,
            &[
                (TEST_TABLE_ID1, epoch0, None),
                (TEST_TABLE_ID1, epoch1, Some(&version1)),
                (TEST_TABLE_ID1, epoch2, Some(&version3)),
                (TEST_TABLE_ID1, epoch3, Some(&version3)),
            ],
        );

        let version4 = gen_pin_version(4, [(TEST_TABLE_ID2, epoch1), (TEST_TABLE_ID1, epoch2)]);
        let recent_versions = recent_versions.with_new_version(version4.clone());
        assert_eq!(recent_versions.recent_versions.len(), 2);
        assert!(recent_versions.is_latest_committed);
        assert_query_equal(
            &recent_versions,
            &[
                (TEST_TABLE_ID1, epoch0, None),
                (TEST_TABLE_ID1, epoch1, Some(&version1)),
                (TEST_TABLE_ID1, epoch2, Some(&version4)),
                (TEST_TABLE_ID1, epoch3, Some(&version4)),
                (TEST_TABLE_ID2, epoch0, None),
                (TEST_TABLE_ID2, epoch1, Some(&version4)),
                (TEST_TABLE_ID2, epoch2, Some(&version4)),
            ],
        );

        let version5 = gen_pin_version(5, [(TEST_TABLE_ID2, epoch1), (TEST_TABLE_ID1, epoch3)]);
        let recent_versions = recent_versions.with_new_version(version5.clone());
        assert_eq!(recent_versions.recent_versions.len(), 2);
        assert!(recent_versions.is_latest_committed);
        assert_query_equal(
            &recent_versions,
            &[
                (TEST_TABLE_ID1, epoch0, None),
                (TEST_TABLE_ID1, epoch1, None),
                (TEST_TABLE_ID1, epoch2, Some(&version4)),
                (TEST_TABLE_ID1, epoch3, Some(&version5)),
                (TEST_TABLE_ID1, epoch4, Some(&version5)),
                (TEST_TABLE_ID2, epoch0, None),
                (TEST_TABLE_ID2, epoch1, Some(&version5)),
                (TEST_TABLE_ID2, epoch2, Some(&version5)),
            ],
        );
    }
}

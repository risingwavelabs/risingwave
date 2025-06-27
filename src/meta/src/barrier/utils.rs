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

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};

use itertools::Itertools;
use risingwave_common::catalog::TableId;
use risingwave_hummock_sdk::sstable_info::SstableInfo;
use risingwave_hummock_sdk::table_stats::from_prost_table_stats_map;
use risingwave_hummock_sdk::table_watermark::{
    TableWatermarks, merge_multiple_new_table_watermarks,
};
use risingwave_hummock_sdk::{HummockSstableObjectId, LocalSstableInfo};
use risingwave_meta_model::WorkerId;
use risingwave_pb::stream_service::BarrierCompleteResponse;

use crate::hummock::{CommitEpochInfo, NewTableFragmentInfo};

#[expect(clippy::type_complexity)]
pub(super) fn collect_resp_info(
    resps: Vec<BarrierCompleteResponse>,
) -> (
    HashMap<HummockSstableObjectId, u32>,
    Vec<LocalSstableInfo>,
    HashMap<TableId, TableWatermarks>,
    Vec<SstableInfo>,
) {
    let mut sst_to_worker: HashMap<HummockSstableObjectId, u32> = HashMap::new();
    let mut synced_ssts: Vec<LocalSstableInfo> = vec![];
    let mut table_watermarks = Vec::with_capacity(resps.len());
    let mut old_value_ssts = Vec::with_capacity(resps.len());

    for resp in resps {
        let ssts_iter = resp.synced_sstables.into_iter().map(|local_sst| {
            let sst_info = local_sst.sst.expect("field not None");
            sst_to_worker.insert(sst_info.object_id, resp.worker_id);
            LocalSstableInfo::new(
                sst_info.into(),
                from_prost_table_stats_map(local_sst.table_stats_map),
                local_sst.created_at,
            )
        });
        synced_ssts.extend(ssts_iter);
        table_watermarks.push(resp.table_watermarks);
        old_value_ssts.extend(resp.old_value_sstables.into_iter().map(|s| s.into()));
    }

    (
        sst_to_worker,
        synced_ssts,
        merge_multiple_new_table_watermarks(
            table_watermarks
                .into_iter()
                .map(|watermarks| {
                    watermarks
                        .into_iter()
                        .map(|(table_id, watermarks)| {
                            (TableId::new(table_id), TableWatermarks::from(&watermarks))
                        })
                        .collect()
                })
                .collect_vec(),
        ),
        old_value_ssts,
    )
}

pub(super) fn collect_creating_job_commit_epoch_info(
    commit_info: &mut CommitEpochInfo,
    epoch: u64,
    resps: Vec<BarrierCompleteResponse>,
    tables_to_commit: impl Iterator<Item = TableId>,
    is_first_time: bool,
) {
    let (sst_to_context, sstables, new_table_watermarks, old_value_sst) = collect_resp_info(resps);
    assert!(old_value_sst.is_empty());
    commit_info.sst_to_context.extend(sst_to_context);
    commit_info.sstables.extend(sstables);
    commit_info
        .new_table_watermarks
        .extend(new_table_watermarks);
    let tables_to_commit: HashSet<_> = tables_to_commit.collect();
    tables_to_commit.iter().for_each(|table_id| {
        commit_info
            .tables_to_commit
            .try_insert(*table_id, epoch)
            .expect("non duplicate");
    });
    if is_first_time {
        commit_info
            .new_table_fragment_infos
            .push(NewTableFragmentInfo {
                table_ids: tables_to_commit,
            });
    };
}

pub(super) type NodeToCollect = HashMap<WorkerId, bool>;
pub(super) fn is_valid_after_worker_err(
    node_to_collect: &mut NodeToCollect,
    worker_id: WorkerId,
) -> bool {
    match node_to_collect.entry(worker_id) {
        Entry::Occupied(entry) => {
            if *entry.get() {
                entry.remove();
                true
            } else {
                false
            }
        }
        Entry::Vacant(_) => true,
    }
}

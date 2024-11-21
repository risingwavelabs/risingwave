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

use itertools::Itertools;
use risingwave_common::catalog::TableId;
use risingwave_hummock_sdk::sstable_info::SstableInfo;
use risingwave_hummock_sdk::table_stats::from_prost_table_stats_map;
use risingwave_hummock_sdk::table_watermark::{
    merge_multiple_new_table_watermarks, TableWatermarks,
};
use risingwave_hummock_sdk::{HummockSstableObjectId, LocalSstableInfo};
use risingwave_meta_model::WorkerId;
use risingwave_pb::stream_service::BarrierCompleteResponse;

use crate::hummock::CommitEpochInfo;

pub(super) fn collect_resp_info(
    resps: HashMap<WorkerId, BarrierCompleteResponse>,
) -> (CommitEpochInfo, Vec<SstableInfo>) {
    let mut sst_to_worker: HashMap<HummockSstableObjectId, u32> = HashMap::new();
    let mut synced_ssts: Vec<LocalSstableInfo> = vec![];
    let mut table_watermarks = Vec::with_capacity(resps.len());
    let mut old_value_ssts = Vec::with_capacity(resps.len());

    for resp in resps.into_values() {
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
        CommitEpochInfo {
            sstables: synced_ssts,
            new_table_watermarks: merge_multiple_new_table_watermarks(
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
            sst_to_context: sst_to_worker,
            new_table_fragment_infos: vec![],
            change_log_delta: Default::default(),
            tables_to_commit: Default::default(),
        },
        old_value_ssts,
    )
}

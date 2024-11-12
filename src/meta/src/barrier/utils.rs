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

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};

use itertools::Itertools;
use risingwave_common::catalog::TableId;
use risingwave_common::must_match;
use risingwave_hummock_sdk::change_log::build_table_change_log_delta;
use risingwave_hummock_sdk::sstable_info::SstableInfo;
use risingwave_hummock_sdk::table_stats::from_prost_table_stats_map;
use risingwave_hummock_sdk::table_watermark::{
    merge_multiple_new_table_watermarks, TableWatermarks,
};
use risingwave_hummock_sdk::{HummockSstableObjectId, LocalSstableInfo};
use risingwave_pb::stream_service::BarrierCompleteResponse;

use crate::barrier::command::CommandContext;
use crate::barrier::{BarrierKind, Command, CreateStreamingJobType};
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

pub(super) fn collect_commit_epoch_info(
    info: &mut CommitEpochInfo,
    resps: Vec<BarrierCompleteResponse>,
    command_ctx: &CommandContext,
    backfill_pinned_log_epoch: HashMap<TableId, (u64, HashSet<TableId>)>,
) {
    let (sst_to_context, synced_ssts, new_table_watermarks, old_value_ssts) =
        collect_resp_info(resps);

    let new_table_fragment_infos = if let Some(Command::CreateStreamingJob { info, job_type }) =
        &command_ctx.command
        && !matches!(job_type, CreateStreamingJobType::SnapshotBackfill(_))
    {
        let table_fragments = &info.table_fragments;
        let mut table_ids: HashSet<_> = table_fragments
            .internal_table_ids()
            .into_iter()
            .map(TableId::new)
            .collect();
        if let Some(mv_table_id) = table_fragments.mv_table_id() {
            table_ids.insert(TableId::new(mv_table_id));
        }

        vec![NewTableFragmentInfo { table_ids }]
    } else {
        vec![]
    };

    let mut mv_log_store_truncate_epoch = HashMap::new();
    let mut update_truncate_epoch =
        |table_id: TableId, truncate_epoch| match mv_log_store_truncate_epoch
            .entry(table_id.table_id)
        {
            Entry::Occupied(mut entry) => {
                let prev_truncate_epoch = entry.get_mut();
                if truncate_epoch < *prev_truncate_epoch {
                    *prev_truncate_epoch = truncate_epoch;
                }
            }
            Entry::Vacant(entry) => {
                entry.insert(truncate_epoch);
            }
        };
    for (mv_table_id, subscriptions) in &command_ctx.subscription_info.mv_depended_subscriptions {
        if let Some(truncate_epoch) = subscriptions
            .values()
            .max()
            .map(|max_retention| command_ctx.get_truncate_epoch(*max_retention).0)
        {
            update_truncate_epoch(*mv_table_id, truncate_epoch);
        }
    }
    for (_, (backfill_epoch, upstream_mv_table_ids)) in backfill_pinned_log_epoch {
        for mv_table_id in upstream_mv_table_ids {
            update_truncate_epoch(mv_table_id, backfill_epoch);
        }
    }

    let table_new_change_log = build_table_change_log_delta(
        old_value_ssts.into_iter(),
        synced_ssts.iter().map(|sst| &sst.sst_info),
        must_match!(&command_ctx.barrier_info.kind, BarrierKind::Checkpoint(epochs) => epochs),
        mv_log_store_truncate_epoch.into_iter(),
    );

    let epoch = command_ctx.barrier_info.prev_epoch();
    for table_id in &command_ctx.table_ids_to_commit {
        info.tables_to_commit
            .try_insert(*table_id, epoch)
            .expect("non duplicate");
    }

    info.sstables.extend(synced_ssts);
    info.new_table_watermarks.extend(new_table_watermarks);
    info.sst_to_context.extend(sst_to_context);
    info.new_table_fragment_infos
        .extend(new_table_fragment_infos);
    info.change_log_delta.extend(table_new_change_log);
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

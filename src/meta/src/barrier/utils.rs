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

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};

use itertools::Itertools;
use risingwave_common::catalog::TableId;
use risingwave_common::util::epoch::EpochPair;
use risingwave_common::util::stream_graph_visitor::visit_stream_node_cont;
use risingwave_hummock_sdk::sstable_info::SstableInfo;
use risingwave_hummock_sdk::table_stats::from_prost_table_stats_map;
use risingwave_hummock_sdk::table_watermark::{
    TableWatermarks, merge_multiple_new_table_watermarks,
};
use risingwave_hummock_sdk::vector_index::{VectorIndexAdd, VectorIndexDelta};
use risingwave_hummock_sdk::{HummockSstableObjectId, LocalSstableInfo};
use risingwave_meta_model::WorkerId;
use risingwave_pb::catalog::PbTable;
use risingwave_pb::catalog::table::PbTableType;
use risingwave_pb::hummock::vector_index_delta::PbVectorIndexInit;
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_service::BarrierCompleteResponse;

use crate::barrier::CreateStreamingJobCommandInfo;
use crate::barrier::command::PostCollectCommand;
use crate::barrier::partial_graph::PartialGraphBarrierInfo;
use crate::hummock::{CommitEpochInfo, NewTableFragmentInfo};

#[expect(clippy::type_complexity)]
pub(super) fn collect_resp_info(
    resps: Vec<BarrierCompleteResponse>,
) -> (
    HashMap<HummockSstableObjectId, WorkerId>,
    Vec<LocalSstableInfo>,
    HashMap<TableId, TableWatermarks>,
    Vec<SstableInfo>,
    HashMap<TableId, Vec<VectorIndexAdd>>,
    HashSet<TableId>,
) {
    let mut sst_to_worker: HashMap<HummockSstableObjectId, _> = HashMap::new();
    let mut synced_ssts: Vec<LocalSstableInfo> = vec![];
    let mut table_watermarks = Vec::with_capacity(resps.len());
    let mut old_value_ssts = Vec::with_capacity(resps.len());
    let mut vector_index_adds = HashMap::new();
    let mut truncate_tables: HashSet<TableId> = HashSet::new();

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
        for (table_id, vector_index_add) in resp.vector_index_adds {
            vector_index_adds
                .try_insert(
                    table_id,
                    vector_index_add
                        .adds
                        .into_iter()
                        .map(VectorIndexAdd::from)
                        .collect(),
                )
                .expect("non-duplicate");
        }
        truncate_tables.extend(resp.truncate_tables);
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
                            (table_id, TableWatermarks::from(&watermarks))
                        })
                        .collect()
                })
                .collect_vec(),
        ),
        old_value_ssts,
        vector_index_adds,
        truncate_tables,
    )
}

pub(super) fn collect_new_vector_index_info(
    info: &CreateStreamingJobCommandInfo,
) -> Option<&PbTable> {
    let mut vector_index_table = None;
    {
        for fragment in info.stream_job_fragments.fragments.values() {
            visit_stream_node_cont(&fragment.nodes, |node| {
                match node.node_body.as_ref().unwrap() {
                    NodeBody::VectorIndexWrite(vector_index_write) => {
                        let index_table = vector_index_write.table.as_ref().unwrap();
                        assert_eq!(index_table.table_type, PbTableType::VectorIndex as i32);
                        vector_index_table = Some(index_table);
                        false
                    }
                    _ => true,
                }
            })
        }
        vector_index_table
    }
}

pub(super) fn collect_independent_job_commit_epoch_info(
    commit_info: &mut CommitEpochInfo,
    epoch: u64,
    resps: Vec<BarrierCompleteResponse>,
    barrier_info: &PartialGraphBarrierInfo,
) {
    let (
        sst_to_context,
        sstables,
        new_table_watermarks,
        old_value_sst,
        vector_index_adds,
        truncate_tables,
    ) = collect_resp_info(resps);
    assert!(old_value_sst.is_empty());
    commit_info.sst_to_context.extend(sst_to_context);
    commit_info.sstables.extend(sstables);
    commit_info
        .new_table_watermarks
        .extend(new_table_watermarks);
    for (table_id, vector_index_adds) in vector_index_adds {
        commit_info
            .vector_index_delta
            .try_insert(table_id, VectorIndexDelta::Adds(vector_index_adds))
            .expect("non-duplicate");
    }
    commit_info.truncate_tables.extend(truncate_tables);
    barrier_info
        .table_ids_to_commit
        .iter()
        .for_each(|table_id| {
            commit_info
                .tables_to_commit
                .try_insert(*table_id, epoch)
                .expect("non duplicate");
        });
    if let PostCollectCommand::CreateStreamingJob { info, .. } = &barrier_info.post_collect_command
    {
        commit_info
            .new_table_fragment_infos
            .push(NewTableFragmentInfo {
                table_ids: barrier_info.table_ids_to_commit.clone(),
            });
        if let Some(index_table) = collect_new_vector_index_info(info) {
            commit_info
                .vector_index_delta
                .try_insert(
                    index_table.id,
                    VectorIndexDelta::Init(PbVectorIndexInit {
                        info: Some(index_table.vector_index_info.unwrap()),
                    }),
                )
                .expect("non-duplicate");
        }
    };
}

pub(super) type NodeToCollect = HashSet<WorkerId>;
pub(super) fn is_valid_after_worker_err(
    node_to_collect: &NodeToCollect,
    worker_id: WorkerId,
) -> bool {
    !node_to_collect.contains(&worker_id)
}

#[derive(Debug)]
struct InflightBarrierNode<K, Item, Info> {
    epoch: EpochPair,
    to_collect: HashSet<K>,
    collected: HashMap<K, Item>,
    info: Info,
}

#[derive(Debug)]
struct CollectedBarrierNode<K, Item, Info> {
    epoch: EpochPair,
    collected: HashMap<K, Item>,
    info: Info,
}

#[derive(Debug)]
pub(super) struct BarrierItemCollector<K, Item, Info> {
    /// `prev_epoch` -> barrier
    inflight_barriers: BTreeMap<u64, InflightBarrierNode<K, Item, Info>>,
    /// newer epoch at the back. `push_back` and `pop_front`
    collected_barriers: VecDeque<CollectedBarrierNode<K, Item, Info>>,
}

impl<K: std::fmt::Debug + Eq + std::hash::Hash, Item: std::fmt::Debug, Info: std::fmt::Debug>
    BarrierItemCollector<K, Item, Info>
{
    pub(super) fn new() -> Self {
        Self {
            inflight_barriers: Default::default(),
            collected_barriers: Default::default(),
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        self.inflight_barriers.is_empty() && self.collected_barriers.is_empty()
    }

    pub(super) fn inflight_barrier_num(&self) -> usize {
        self.inflight_barriers.len()
    }

    pub(super) fn collected_barrier_num(&self) -> usize {
        self.collected_barriers.len()
    }

    pub(super) fn enqueue(&mut self, epoch: EpochPair, to_collect: HashSet<K>, info: Info) {
        assert!(!to_collect.is_empty());
        if let Some((last_prev_epoch, last_barrier)) = self.inflight_barriers.last_key_value() {
            assert_eq!(last_barrier.epoch.curr, epoch.prev);
            assert!(*last_prev_epoch < epoch.prev);
        }
        self.inflight_barriers
            .try_insert(
                epoch.prev,
                InflightBarrierNode {
                    epoch,
                    to_collect,
                    collected: Default::default(),
                    info,
                },
            )
            .expect("non-duplicated");
    }

    pub(super) fn collect(&mut self, prev_epoch: u64, key: K, item: Item) {
        let inflight_barrier = self
            .inflight_barriers
            .get_mut(&prev_epoch)
            .expect("should exist");
        assert!(inflight_barrier.to_collect.remove(&key));
        inflight_barrier
            .collected
            .try_insert(key, item)
            .expect("non-duplicate");
    }

    pub(super) fn barrier_collected(&mut self) -> Option<(EpochPair, &Info)> {
        if let Some(entry) = self.inflight_barriers.first_entry()
            && entry.get().to_collect.is_empty()
        {
            let InflightBarrierNode {
                epoch,
                collected,
                info,
                ..
            } = entry.remove();
            if let Some(prev_barrier) = self.collected_barriers.back() {
                assert_eq!(prev_barrier.epoch.curr, epoch.prev);
            }
            self.collected_barriers.push_back(CollectedBarrierNode {
                epoch,
                collected,
                info,
            });
            Some((
                epoch,
                &self.collected_barriers.back().expect("non-empty").info,
            ))
        } else {
            None
        }
    }

    pub(super) fn advance_collected(&mut self) {
        while self.barrier_collected().is_some() {}
    }

    pub(super) fn first_inflight_epoch(&self) -> Option<EpochPair> {
        self.inflight_barriers
            .first_key_value()
            .map(|(_, barrier)| barrier.epoch)
    }

    pub(super) fn last_collected(&self) -> Option<(EpochPair, &HashMap<K, Item>, &Info)> {
        self.collected_barriers
            .back()
            .map(|barrier| (barrier.epoch, &barrier.collected, &barrier.info))
    }

    pub(super) fn iter_infos(&self) -> impl Iterator<Item = &Info> + '_ {
        self.inflight_barriers
            .values()
            .map(|barrier| &barrier.info)
            .chain(self.collected_barriers.iter().map(|barrier| &barrier.info))
    }

    pub(super) fn into_infos(self) -> impl Iterator<Item = Info> {
        self.inflight_barriers
            .into_values()
            .map(|barrier| barrier.info)
            .chain(
                self.collected_barriers
                    .into_iter()
                    .map(|barrier| barrier.info),
            )
    }

    pub(super) fn iter_to_collect(&self) -> impl Iterator<Item = &HashSet<K>> + '_ {
        self.inflight_barriers
            .values()
            .map(|barrier| &barrier.to_collect)
    }

    pub(super) fn take_collected_if(
        &mut self,
        cond: impl FnOnce(EpochPair) -> bool,
    ) -> Option<(EpochPair, HashMap<K, Item>, Info)> {
        self.collected_barriers
            .pop_front_if(move |barrier| cond(barrier.epoch))
            .map(|barrier| (barrier.epoch, barrier.collected, barrier.info))
    }
}

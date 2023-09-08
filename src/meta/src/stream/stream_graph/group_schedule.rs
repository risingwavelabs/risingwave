// Copyright 2023 RisingWave Labs
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

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use itertools::Itertools;
use risingwave_common::bail;
use risingwave_common::buffer::Bitmap;
use risingwave_common::hash::{ActorGroupId, ActorGroupMapping, ActorId};
use risingwave_pb::common::{ActorInfo, WorkerNode};
use risingwave_pb::meta::table_fragments::fragment::FragmentDistributionType;
use risingwave_pb::stream_plan::DispatcherType;

use super::id::GlobalFragmentId;
use super::CompleteStreamFragmentGraph;
use crate::manager::{IdCategory, IdGeneratorManagerRef, WorkerId, WorkerKey, WorkerLocations};
use crate::MetaResult;

#[derive(Clone, Debug)]
pub(super) struct Scheduling {
    pub fragment_group_id: GlobalFragmentId,
    pub actor_group_mapping: Arc<ActorGroupMapping>,
    pub distribution_type: FragmentDistributionType,
}

impl Scheduling {
    pub fn parallelism(&self) -> usize {
        self.actor_groups().count()
    }

    pub fn actor_groups(&self) -> impl Iterator<Item = ActorGroupId> + '_ {
        self.actor_group_mapping.iter_unique()
    }

    pub fn to_bitmaps(&self) -> Option<HashMap<ActorGroupId, Bitmap>> {
        match self.distribution_type {
            FragmentDistributionType::Unspecified => unreachable!(),
            FragmentDistributionType::Single => None,
            FragmentDistributionType::Hash => Some(self.actor_group_mapping.to_bitmaps()),
        }
    }
}

#[derive(Clone, Debug)]
pub(super) struct FragmentSchedulings {
    pub building: HashMap<GlobalFragmentId, Scheduling>,
    pub existing: HashMap<GlobalFragmentId, Scheduling>,

    pub new_assignments: HashMap<ActorGroupId, WorkerId>,
}

pub(super) struct Scheduler<'a> {
    graph: &'a CompleteStreamFragmentGraph,

    id_gen_manager: IdGeneratorManagerRef,

    default_parallelism: usize,

    worker_nodes: HashMap<WorkerId, WorkerNode>,
}

impl<'a> Scheduler<'a> {
    pub(super) fn new(
        graph: &'a CompleteStreamFragmentGraph,
        id_gen_manager: IdGeneratorManagerRef,
        default_parallelism: usize,
        worker_nodes: HashMap<WorkerId, WorkerNode>,
    ) -> Self {
        Self {
            graph,
            id_gen_manager,
            default_parallelism,
            worker_nodes,
        }
    }

    fn split_into_groups(&self) -> Vec<BTreeSet<GlobalFragmentId>> {
        let mut visited = HashSet::new();
        let mut groups = Vec::new(); // fragment group id -> fragment group

        for id in self.graph.all_fragment_ids() {
            if visited.contains(&id) {
                continue;
            }

            let mut group = vec![id];

            for i in 0.. {
                if i >= group.len() {
                    break;
                }

                let id = group[i];
                visited.insert(id);

                for next_id in self
                    .graph
                    .get_downstreams(id)
                    .chain(self.graph.get_upstreams(id))
                    .filter(|(_, e)| e.dispatch_strategy.r#type() == DispatcherType::NoShuffle)
                    .map(|(id, _)| id)
                {
                    if visited.contains(&next_id) {
                        continue;
                    }
                    group.push(next_id);
                }
            }

            let group: BTreeSet<_> = group.into_iter().collect();
            groups.push(group);
        }

        groups
    }

    pub async fn schedule(&self) -> MetaResult<FragmentSchedulings> {
        let mut round_robin_assignment =
            RoundRobinAssignment::new(self.worker_nodes.values().map(|w| w.id).collect_vec());

        let mut building = HashMap::new();
        let mut existing = HashMap::new();
        let mut new_actor_group_ids = Vec::new();

        for group in self.split_into_groups() {
            let scheduling = match *group
                .iter()
                .filter_map(|id| self.graph.existing_fragments().get(id))
                .unique_by(|f| f.fragment_group_id)
                .collect_vec()
                .as_slice()
            {
                [] => {
                    let new_group_id = *group.first().unwrap();

                    let requires_singleton = group
                        .iter()
                        .map(|id| self.graph.building_fragments().get(id).unwrap())
                        .any(|f| f.requires_singleton);

                    let (distribution_type, parallelism) = if requires_singleton {
                        (FragmentDistributionType::Single, 1)
                    } else {
                        (FragmentDistributionType::Hash, self.default_parallelism)
                    };

                    let start_actor_group_id = self
                        .id_gen_manager
                        .generate_interval::<{ IdCategory::ActorGroup }>(parallelism as _)
                        .await? as ActorGroupId;
                    let actor_group_ids =
                        start_actor_group_id..(start_actor_group_id + parallelism as ActorGroupId);

                    new_actor_group_ids.extend(actor_group_ids.clone());

                    let actor_group_mapping =
                        ActorGroupMapping::new_uniform(actor_group_ids).into();

                    Scheduling {
                        fragment_group_id: new_group_id,
                        actor_group_mapping,
                        distribution_type,
                    }
                }

                [ref_fragment] => Scheduling {
                    fragment_group_id: ref_fragment.fragment_group_id.into(),
                    actor_group_mapping: ActorGroupMapping::from_protobuf(
                        ref_fragment.actor_group_mapping.as_ref().unwrap(),
                    )
                    .into(),
                    distribution_type: ref_fragment.distribution_type(),
                },

                [ref_fragment_1, ref_fragment_2, ..] => {
                    bail!(
                        "failed to schedule: fragment group {} is not compatible with {}",
                        ref_fragment_1.fragment_group_id,
                        ref_fragment_2.fragment_group_id
                    );
                }
            };

            for fragment_id in group {
                if self.graph.existing_fragments().contains_key(&fragment_id) {
                    existing
                        .try_insert(fragment_id, scheduling.clone())
                        .unwrap();
                } else {
                    building
                        .try_insert(fragment_id, scheduling.clone())
                        .unwrap();
                }
            }
        }

        let new_assignments: HashMap<_, _> = new_actor_group_ids
            .into_iter()
            .zip(round_robin_assignment)
            .collect();

        if new_assignments.is_empty() {
            bail!("empty worker nodes");
        }

        Ok(FragmentSchedulings {
            building,
            existing,
            new_assignments,
        })
    }
}

// TODO: weighted round robin based on parallelism
pub struct RoundRobinAssignment {
    worker_nodes: Vec<WorkerId>,
    i: usize,
}

impl RoundRobinAssignment {
    pub fn new(worker_nodes: impl IntoIterator<Item = WorkerId>) -> Self {
        Self {
            worker_nodes: worker_nodes.into_iter().collect(),
            i: 0,
        }
    }
}

impl Iterator for RoundRobinAssignment {
    type Item = WorkerId;

    fn next(&mut self) -> Option<Self::Item> {
        let worker_node = self.worker_nodes.get(self.i)?;
        self.i = (self.i + 1) % self.worker_nodes.len();
        Some(*worker_node)
    }
}

// pub struct LocationsV2 {}

/// [`Locations`] represents the parallel unit and worker locations of the actors.
#[cfg_attr(test, derive(Default))]
pub struct LocationsV2 {
    /// actor location map.
    pub actor_groups: BTreeMap<ActorId, ActorGroupId>,

    pub assignments: HashMap<ActorGroupId, WorkerId>,

    /// worker location map.
    pub worker_locations: WorkerLocations,
}

impl LocationsV2 {
    /// Returns all actors for every worker node.
    pub fn worker_actors(&self) -> HashMap<WorkerId, Vec<ActorId>> {
        self.actor_groups
            .iter()
            .map(|(&actor_id, actor_group_id)| (self.assignments[actor_group_id], actor_id))
            .into_group_map()
    }

    /// Returns an iterator of `ActorInfo`.
    pub fn actor_infos(&self) -> impl Iterator<Item = ActorInfo> + '_ {
        self.actor_groups
            .iter()
            .map(|(&actor_id, actor_group_id)| ActorInfo {
                actor_id,
                host: self.worker_locations[&self.assignments[actor_group_id]]
                    .host
                    .clone(),
            })
    }
}

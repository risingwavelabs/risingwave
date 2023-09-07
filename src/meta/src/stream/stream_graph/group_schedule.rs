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

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use itertools::Itertools;
use risingwave_common::bail;
use risingwave_common::hash::{ActorGroupId, ActorGroupMapping};
use risingwave_pb::meta::table_fragments::fragment::FragmentDistributionType;
use risingwave_pb::stream_plan::DispatcherType;

use super::id::GlobalFragmentId;
use super::CompleteStreamFragmentGraph;
use crate::manager::{IdCategory, IdGeneratorManagerRef};
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
}

#[derive(Clone, Debug)]
pub(super) struct FragmentSchedulings {
    pub building: HashMap<GlobalFragmentId, Scheduling>,
    pub existing: HashMap<GlobalFragmentId, Scheduling>,
}

pub(super) struct Scheduler<'a> {
    graph: &'a CompleteStreamFragmentGraph,

    id_gen_manager: IdGeneratorManagerRef,

    default_parallelism: usize,
}

impl<'a> Scheduler<'a> {
    pub(super) fn new(
        graph: &'a CompleteStreamFragmentGraph,
        id_gen_manager: IdGeneratorManagerRef,
        default_parallelism: usize,
    ) -> Self {
        Self {
            graph,
            id_gen_manager,
            default_parallelism,
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
        let mut building = HashMap::new();
        let mut existing = HashMap::new();

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

                    let actor_group_mapping = ActorGroupMapping::new_uniform(
                        start_actor_group_id..(start_actor_group_id + parallelism as u32),
                    )
                    .into();

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

        Ok(FragmentSchedulings { building, existing })
    }
}

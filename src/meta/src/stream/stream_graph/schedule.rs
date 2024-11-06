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

#![allow(
    clippy::collapsible_if,
    clippy::explicit_iter_loop,
    reason = "generated by crepe"
)]

use std::collections::{BTreeMap, HashMap};
use std::num::NonZeroUsize;

use either::Either;
use enum_as_inner::EnumAsInner;
use itertools::Itertools;
use risingwave_common::bitmap::Bitmap;
use risingwave_common::hash::{ActorMapping, VnodeCountCompat, WorkerSlotId, WorkerSlotMapping};
use risingwave_common::util::stream_graph_visitor::visit_fragment;
use risingwave_common::{bail, hash};
use risingwave_meta_model::WorkerId;
use risingwave_pb::common::{ActorInfo, WorkerNode};
use risingwave_pb::meta::table_fragments::fragment::{
    FragmentDistributionType, PbFragmentDistributionType,
};
use risingwave_pb::stream_plan::DispatcherType::{self, *};

use crate::model::ActorId;
use crate::stream::schedule_units_for_slots;
use crate::stream::stream_graph::fragment::CompleteStreamFragmentGraph;
use crate::stream::stream_graph::id::GlobalFragmentId as Id;
use crate::MetaResult;

type HashMappingId = usize;

/// The internal distribution structure for processing in the scheduler.
///
/// See [`Distribution`] for the public interface.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Req {
    Singleton(WorkerSlotId),
    Hash(HashMappingId),
    AnySingleton,
    AnyVnodeCount(usize),
}

/// Facts as the input of the scheduler.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Fact {
    /// An edge in the fragment graph.
    Edge {
        from: Id,
        to: Id,
        dt: DispatcherType,
    },
    /// A distribution requirement for an external(existing) fragment.
    Req { id: Id, req: Req },
}

crepe::crepe! {
    @input
    struct Input(Fact);

    struct Edge(Id, Id, DispatcherType);
    struct ExternalReq(Id, Req);

    @output
    struct Requirement(Id, Req);

    // Extract facts.
    Edge(from, to, dt) <- Input(f), let Fact::Edge { from, to, dt } = f;
    Requirement(id, req) <- Input(f), let Fact::Req { id, req } = f;

    // The downstream fragment of a `Simple` edge must be singleton.
    Requirement(y, Req::AnySingleton) <- Edge(_, y, Simple);
    // Requirements propagate through `NoShuffle` edges.
    Requirement(x, d) <- Edge(x, y, NoShuffle), Requirement(y, d);
    Requirement(y, d) <- Edge(x, y, NoShuffle), Requirement(x, d);
}

/// The distribution of a fragment.
#[derive(Debug, Clone, EnumAsInner)]
pub(super) enum Distribution {
    /// The fragment is singleton and is scheduled to the given worker slot.
    Singleton(WorkerSlotId),

    /// The fragment is hash-distributed and is scheduled by the given hash mapping.
    Hash(WorkerSlotMapping),
}

impl Distribution {
    /// The parallelism required by the distribution.
    pub fn parallelism(&self) -> usize {
        self.worker_slots().count()
    }

    /// All worker slots required by the distribution.
    pub fn worker_slots(&self) -> impl Iterator<Item = WorkerSlotId> + '_ {
        match self {
            Distribution::Singleton(p) => Either::Left(std::iter::once(*p)),
            Distribution::Hash(mapping) => Either::Right(mapping.iter_unique()),
        }
    }

    /// Get the vnode count of the distribution.
    pub fn vnode_count(&self) -> usize {
        match self {
            Distribution::Singleton(_) => 1, // only `SINGLETON_VNODE`
            Distribution::Hash(mapping) => mapping.len(),
        }
    }

    /// Create a distribution from a persisted protobuf `Fragment`.
    pub fn from_fragment(
        fragment: &risingwave_pb::meta::table_fragments::Fragment,
        actor_location: &HashMap<ActorId, WorkerId>,
    ) -> Self {
        match fragment.get_distribution_type().unwrap() {
            FragmentDistributionType::Unspecified => unreachable!(),
            FragmentDistributionType::Single => {
                let actor_id = fragment.actors.iter().exactly_one().unwrap().actor_id;
                let location = actor_location.get(&actor_id).unwrap();
                let worker_slot_id = WorkerSlotId::new(*location as _, 0);
                Distribution::Singleton(worker_slot_id)
            }
            FragmentDistributionType::Hash => {
                let actor_bitmaps: HashMap<_, _> = fragment
                    .actors
                    .iter()
                    .map(|actor| {
                        (
                            actor.actor_id as hash::ActorId,
                            Bitmap::from(actor.vnode_bitmap.as_ref().unwrap()),
                        )
                    })
                    .collect();

                let actor_mapping = ActorMapping::from_bitmaps(&actor_bitmaps);
                let actor_location = actor_location
                    .iter()
                    .map(|(&k, &v)| (k, v as u32))
                    .collect();
                let mapping = actor_mapping.to_worker_slot(&actor_location);

                Distribution::Hash(mapping)
            }
        }
    }

    /// Convert the distribution to [`PbFragmentDistributionType`].
    pub fn to_distribution_type(&self) -> PbFragmentDistributionType {
        match self {
            Distribution::Singleton(_) => PbFragmentDistributionType::Single,
            Distribution::Hash(_) => PbFragmentDistributionType::Hash,
        }
    }
}

/// [`Scheduler`] schedules the distribution of fragments in a stream graph.
pub(super) struct Scheduler {
    scheduled_worker_slots: Vec<WorkerSlotId>,

    /// The default hash mapping for hash-distributed fragments, if there's no requirement derived.
    default_hash_mapping: WorkerSlotMapping,

    /// The default worker slot for singleton fragments, if there's no requirement derived.
    default_singleton_worker_slot: WorkerSlotId,
}

impl Scheduler {
    /// Create a new [`Scheduler`] with the given worker slots and the default parallelism.
    ///
    /// Each hash-distributed fragment will be scheduled to at most `default_parallelism` parallel
    /// units, in a round-robin fashion on all compute nodes. If the `default_parallelism` is
    /// `None`, all worker slots will be used.
    ///
    /// For different streaming jobs, we even out possible scheduling skew by using the streaming job id as the salt for the scheduling algorithm.
    pub fn new(
        streaming_job_id: u32,
        workers: &HashMap<u32, WorkerNode>,
        default_parallelism: NonZeroUsize,
        expected_vnode_count: usize,
    ) -> MetaResult<Self> {
        // Group worker slots with worker node.

        let slots = workers
            .iter()
            .map(|(worker_id, worker)| (*worker_id as WorkerId, worker.parallelism as usize))
            .collect();

        let parallelism = default_parallelism.get();
        assert!(
            parallelism <= expected_vnode_count,
            "parallelism should be limited by vnode count in previous steps"
        );

        let scheduled = schedule_units_for_slots(&slots, parallelism, streaming_job_id)?;

        let scheduled_worker_slots = scheduled
            .into_iter()
            .flat_map(|(worker_id, size)| {
                (0..size).map(move |slot| WorkerSlotId::new(worker_id as _, slot))
            })
            .collect_vec();

        assert_eq!(scheduled_worker_slots.len(), parallelism);

        // Build the default hash mapping uniformly.
        let default_hash_mapping =
            WorkerSlotMapping::build_from_ids(&scheduled_worker_slots, expected_vnode_count);

        let single_scheduled = schedule_units_for_slots(&slots, 1, streaming_job_id)?;
        let default_single_worker_id = single_scheduled.keys().exactly_one().cloned().unwrap();
        let default_singleton_worker_slot = WorkerSlotId::new(default_single_worker_id as _, 0);

        Ok(Self {
            scheduled_worker_slots,
            default_hash_mapping,
            default_singleton_worker_slot,
        })
    }

    /// Schedule the given complete graph and returns the distribution of each **building
    /// fragment**.
    pub fn schedule(
        &self,
        graph: &CompleteStreamFragmentGraph,
    ) -> MetaResult<HashMap<Id, Distribution>> {
        let existing_distribution = graph.existing_distribution();

        // Build an index map for all hash mappings.
        let all_hash_mappings = existing_distribution
            .values()
            .flat_map(|dist| dist.as_hash())
            .cloned()
            .unique()
            .collect_vec();
        let hash_mapping_id: HashMap<_, _> = all_hash_mappings
            .iter()
            .enumerate()
            .map(|(i, m)| (m.clone(), i))
            .collect();

        let mut facts = Vec::new();

        // Singletons.
        for (&id, fragment) in graph.building_fragments() {
            if fragment.requires_singleton {
                facts.push(Fact::Req {
                    id,
                    req: Req::AnySingleton,
                });
            }
        }
        // Vnode count requirements if looking up existing tables.
        for (&id, fragment) in graph.building_fragments() {
            visit_fragment(&mut (*fragment).clone(), |node| {
                use risingwave_pb::stream_plan::stream_node::NodeBody;
                let vnode_count = match node {
                    NodeBody::StreamScan(node) => {
                        if let Some(table) = &node.arrangement_table {
                            table.vnode_count()
                        } else if let Some(table) = &node.table_desc {
                            table.vnode_count()
                        } else {
                            return;
                        }
                    }
                    NodeBody::TemporalJoin(node) => node.get_table_desc().unwrap().vnode_count(),
                    NodeBody::BatchPlan(node) => node.get_table_desc().unwrap().vnode_count(),
                    NodeBody::Lookup(node) => node
                        .get_arrangement_table_info()
                        .unwrap()
                        .get_table_desc()
                        .unwrap()
                        .vnode_count(),
                    _ => return,
                };
                facts.push(Fact::Req {
                    id,
                    req: Req::AnyVnodeCount(vnode_count),
                });
            });
        }
        // Distributions of existing fragments.
        for (id, dist) in existing_distribution {
            let req = match dist {
                Distribution::Singleton(worker_slot_id) => Req::Singleton(worker_slot_id),
                Distribution::Hash(mapping) => Req::Hash(hash_mapping_id[&mapping]),
            };
            facts.push(Fact::Req { id, req });
        }
        // Edges.
        for (from, to, edge) in graph.all_edges() {
            facts.push(Fact::Edge {
                from,
                to,
                dt: edge.dispatch_strategy.r#type(),
            });
        }

        // Run the algorithm.
        let mut crepe = Crepe::new();
        crepe.extend(facts.into_iter().map(Input));
        let (reqs,) = crepe.run();
        let reqs = reqs
            .into_iter()
            .map(|Requirement(id, req)| (id, req))
            .into_group_map();

        let mut distributions = HashMap::new();
        for &id in graph.building_fragments().keys() {
            let dist = match reqs.get(&id) {
                Some(reqs) => {
                    let req = (reqs.iter().copied())
                        .try_reduce(|a, b| {
                            let merge = |a, b| match (a, b) {
                                (Req::AnySingleton, Req::Singleton(id)) => Some(Req::Singleton(id)),
                                (Req::AnyVnodeCount(count), Req::Hash(mapping))
                                    if all_hash_mappings[mapping].len() == count =>
                                {
                                    Some(Req::Hash(mapping))
                                }
                                _ => None,
                            };

                            match merge(a, b).or_else(|| merge(b, a)) {
                                Some(req) => MetaResult::Ok(req),
                                None => bail!("incompatible requirements `{a:?}` and `{b:?}` for fragment {id:?}"),
                            }
                        })?
                        .unwrap();

                    match req {
                        Req::Singleton(worker_slot) => Distribution::Singleton(worker_slot),
                        Req::Hash(mapping) => {
                            Distribution::Hash(all_hash_mappings[mapping].clone())
                        }
                        Req::AnySingleton => {
                            Distribution::Singleton(self.default_singleton_worker_slot)
                        }
                        Req::AnyVnodeCount(vnode_count) => {
                            let len = self.scheduled_worker_slots.len().min(vnode_count);
                            let mapping = WorkerSlotMapping::build_from_ids(
                                &self.scheduled_worker_slots[..len],
                                vnode_count,
                            );
                            Distribution::Hash(mapping)
                        }
                    }
                }
                None => Distribution::Hash(self.default_hash_mapping.clone()),
            };

            distributions.insert(id, dist);
        }

        tracing::debug!(?distributions, "schedule fragments");

        Ok(distributions)
    }
}

/// [`Locations`] represents the worker slot and worker locations of the actors.
#[cfg_attr(test, derive(Default))]
pub struct Locations {
    /// actor location map.
    pub actor_locations: BTreeMap<ActorId, WorkerSlotId>,
    /// worker location map.
    pub worker_locations: HashMap<WorkerId, WorkerNode>,
}

impl Locations {
    /// Returns all actors for every worker node.
    pub fn worker_actors(&self) -> HashMap<WorkerId, Vec<ActorId>> {
        self.actor_locations
            .iter()
            .map(|(actor_id, worker_slot_id)| (worker_slot_id.worker_id() as WorkerId, *actor_id))
            .into_group_map()
    }

    /// Returns an iterator of `ActorInfo`.
    pub fn actor_infos(&self) -> impl Iterator<Item = ActorInfo> + '_ {
        self.actor_locations
            .iter()
            .map(|(actor_id, worker_slot_id)| ActorInfo {
                actor_id: *actor_id,
                host: self.worker_locations[&(worker_slot_id.worker_id() as WorkerId)]
                    .host
                    .clone(),
            })
    }
}

#[cfg(test)]
#[cfg(any())]
mod tests {
    use super::*;

    fn test_success(facts: impl IntoIterator<Item = Fact>, expected: HashMap<Id, Result>) {
        let mut crepe = Crepe::new();
        crepe.extend(facts.into_iter().map(Input));
        let (success, failed) = crepe.run();

        assert!(failed.is_empty());

        let success: HashMap<_, _> = success
            .into_iter()
            .map(|Output(id, result)| (id, result))
            .collect();

        assert_eq!(success, expected);
    }

    fn test_failed(facts: impl IntoIterator<Item = Fact>) {
        let mut crepe = Crepe::new();
        crepe.extend(facts.into_iter().map(Input));
        let (_success, failed) = crepe.run();

        assert!(!failed.is_empty());
    }

    // 101
    #[test]
    fn test_single_fragment_hash() {
        #[rustfmt::skip]
        let facts = [
            Fact::Fragment(101.into()),
        ];

        let expected = maplit::hashmap! {
            101.into() => Result::DefaultHash,
        };

        test_success(facts, expected);
    }

    // 101
    #[test]
    fn test_single_fragment_singleton() {
        #[rustfmt::skip]
        let facts = [
            Fact::Fragment(101.into()),
            Fact::SingletonReq(101.into()),
        ];

        let expected = maplit::hashmap! {
            101.into() => Result::DefaultSingleton,
        };

        test_success(facts, expected);
    }

    // 1 -|-> 101 -->
    //                103 --> 104
    // 2 -|-> 102 -->
    #[test]
    fn test_scheduling_mv_on_mv() {
        #[rustfmt::skip]
        let facts = [
            Fact::Fragment(101.into()),
            Fact::Fragment(102.into()),
            Fact::Fragment(103.into()),
            Fact::Fragment(104.into()),
            Fact::Req { id: 1.into(), req: Req::Hash(1) },
            Fact::Req { id: 2.into(), req: Req::Singleton(WorkerSlotId::new(0, 2)) },
            Fact::Edge { from: 1.into(), to: 101.into(), dt: NoShuffle },
            Fact::Edge { from: 2.into(), to: 102.into(), dt: NoShuffle },
            Fact::Edge { from: 101.into(), to: 103.into(), dt: Hash },
            Fact::Edge { from: 102.into(), to: 103.into(), dt: Hash },
            Fact::Edge { from: 103.into(), to: 104.into(), dt: Simple },
        ];

        let expected = maplit::hashmap! {
            101.into() => Result::Required(Req::Hash(1)),
            102.into() => Result::Required(Req::Singleton(WorkerSlotId::new(0, 2))),
            103.into() => Result::DefaultHash,
            104.into() => Result::DefaultSingleton,
        };

        test_success(facts, expected);
    }

    // 1 -|-> 101 --> 103 -->
    //             X          105
    // 2 -|-> 102 --> 104 -->
    #[test]
    fn test_delta_join() {
        #[rustfmt::skip]
        let facts = [
            Fact::Fragment(101.into()),
            Fact::Fragment(102.into()),
            Fact::Fragment(103.into()),
            Fact::Fragment(104.into()),
            Fact::Fragment(105.into()),
            Fact::Req { id: 1.into(), req: Req::Hash(1) },
            Fact::Req { id: 2.into(), req: Req::Hash(2) },
            Fact::Edge { from: 1.into(), to: 101.into(), dt: NoShuffle },
            Fact::Edge { from: 2.into(), to: 102.into(), dt: NoShuffle },
            Fact::Edge { from: 101.into(), to: 103.into(), dt: NoShuffle },
            Fact::Edge { from: 102.into(), to: 104.into(), dt: NoShuffle },
            Fact::Edge { from: 101.into(), to: 104.into(), dt: Hash },
            Fact::Edge { from: 102.into(), to: 103.into(), dt: Hash },
            Fact::Edge { from: 103.into(), to: 105.into(), dt: Hash },
            Fact::Edge { from: 104.into(), to: 105.into(), dt: Hash },
        ];

        let expected = maplit::hashmap! {
            101.into() => Result::Required(Req::Hash(1)),
            102.into() => Result::Required(Req::Hash(2)),
            103.into() => Result::Required(Req::Hash(1)),
            104.into() => Result::Required(Req::Hash(2)),
            105.into() => Result::DefaultHash,
        };

        test_success(facts, expected);
    }

    // 1 -|-> 101 -->
    //                103
    //        102 -->
    #[test]
    fn test_singleton_leaf() {
        #[rustfmt::skip]
        let facts = [
            Fact::Fragment(101.into()),
            Fact::Fragment(102.into()),
            Fact::Fragment(103.into()),
            Fact::Req { id: 1.into(), req: Req::Hash(1) },
            Fact::Edge { from: 1.into(), to: 101.into(), dt: NoShuffle },
            Fact::SingletonReq(102.into()), // like `Now`
            Fact::Edge { from: 101.into(), to: 103.into(), dt: Hash },
            Fact::Edge { from: 102.into(), to: 103.into(), dt: Broadcast },
        ];

        let expected = maplit::hashmap! {
            101.into() => Result::Required(Req::Hash(1)),
            102.into() => Result::DefaultSingleton,
            103.into() => Result::DefaultHash,
        };

        test_success(facts, expected);
    }

    // 1 -|->
    //        101
    // 2 -|->
    #[test]
    fn test_upstream_hash_shard_failed() {
        #[rustfmt::skip]
        let facts = [
            Fact::Fragment(101.into()),
            Fact::Req { id: 1.into(), req: Req::Hash(1) },
            Fact::Req { id: 2.into(), req: Req::Hash(2) },
            Fact::Edge { from: 1.into(), to: 101.into(), dt: NoShuffle },
            Fact::Edge { from: 2.into(), to: 101.into(), dt: NoShuffle },
        ];

        test_failed(facts);
    }
}

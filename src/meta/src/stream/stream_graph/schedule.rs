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

#![allow(
    clippy::collapsible_if,
    clippy::explicit_iter_loop,
    reason = "generated by crepe"
)]

use std::collections::{BTreeMap, HashMap, LinkedList};
use std::num::NonZeroUsize;

use either::Either;
use enum_as_inner::EnumAsInner;
use itertools::Itertools;
use rand::seq::SliceRandom;
use rand::thread_rng;
use risingwave_common::bail;
use risingwave_common::hash::{ParallelUnitId, ParallelUnitMapping};
use risingwave_pb::common::ParallelUnit;
use risingwave_pb::meta::table_fragments::fragment::FragmentDistributionType;
use risingwave_pb::stream_plan::DispatcherType::{self, *};

use super::{CompleteStreamFragmentGraph, GlobalFragmentId as Id};
use crate::MetaResult;

type HashMappingId = usize;

/// The internal distribution structure for processing in the scheduler.
///
/// See [`Distribution`] for the public interface.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum DistId {
    Singleton(ParallelUnitId),
    Hash(HashMappingId),
}

/// Facts as the input of the scheduler.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Fact {
    /// An edge in the stream graph.
    Edge {
        from: Id,
        to: Id,
        dt: DispatcherType,
    },
    /// A distribution requirement for an external(existing) fragment.
    ExternalReq { id: Id, dist: DistId },
    /// A singleton requirement for a building fragment.
    /// Note that the physical parallel unit is not determined yet.
    SingletonReq(Id),
}

/// Results of all building fragments, as the output of the scheduler.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Result {
    /// This fragment is required to be distributed by the given [`DistId`].
    Required(DistId),
    /// This fragment is singleton, and should be scheduled to the default parallel unit.
    DefaultSingleton,
    /// This fragment is hash-distributed, and should be scheduled by the default hash mapping.
    DefaultHash,
}

crepe::crepe! {
    @input
    struct Input(Fact);

    struct Edge(Id, Id, DispatcherType);
    struct ExternalReq(Id, DistId);
    struct SingletonReq(Id);
    struct Fragment(Id);
    struct Requirement(Id, DistId);

    @output
    struct Success(Id, Result);
    @output
    #[derive(Debug)]
    struct Failed(Id);

    // Extract facts.
    Edge(from, to, dt) <- Input(f), let Fact::Edge { from, to, dt } = f;
    ExternalReq(id, dist) <- Input(f), let Fact::ExternalReq { id, dist } = f;
    SingletonReq(id) <- Input(f), let Fact::SingletonReq(id) = f;

    // Internal fragments.
    Fragment(x) <- Edge(x, _, _), !ExternalReq(x, _);
    Fragment(y) <- Edge(_, y, _), !ExternalReq(y, _);

    // Requirements from the facts.
    Requirement(x, d) <- ExternalReq(x, d);
    // Requirements of `NoShuffle` edges.
    Requirement(x, d) <- Edge(x, y, NoShuffle), Requirement(y, d);
    Requirement(y, d) <- Edge(x, y, NoShuffle), Requirement(x, d);

    // The downstream fragment of a `Simple` edge must be singleton.
    SingletonReq(y) <- Edge(_, y, Simple);

    // Multiple requirements conflict.
    Failed(x) <- Requirement(x, d1), Requirement(x, d2), (d1 != d2);
    // Singleton requirement conflicts with hash requirement.
    Failed(x) <- SingletonReq(x), Requirement(x, d), let DistId::Hash(_) = d;

    // Take the required distribution as the result.
    Success(x, Result::Required(d)) <- Fragment(x), Requirement(x, d), !Failed(x);
    // Take the default singleton distribution as the result, if no other requirement.
    Success(x, Result::DefaultSingleton) <- Fragment(x), SingletonReq(x), !Requirement(x, _);
    // Take the default hash distribution as the result, if no other requirement.
    Success(x, Result::DefaultHash) <- Fragment(x), !SingletonReq(x), !Requirement(x, _);
}

/// The distribution of a fragment.
#[derive(Debug, Clone, EnumAsInner)]
pub(super) enum Distribution {
    /// The fragment is singleton and is scheduled to the given parallel unit.
    Singleton(ParallelUnitId),

    /// The fragment is hash-distributed and is scheduled by the given hash mapping.
    Hash(ParallelUnitMapping),
}

impl Distribution {
    /// The parallelism required by the distribution.
    pub fn parallelism(&self) -> usize {
        self.parallel_units().count()
    }

    /// All parallel units required by the distribution.
    pub fn parallel_units(&self) -> impl Iterator<Item = ParallelUnitId> + '_ {
        match self {
            Distribution::Singleton(p) => Either::Left(std::iter::once(*p)),
            Distribution::Hash(mapping) => Either::Right(mapping.iter_unique()),
        }
    }

    /// Convert the distribution to a [`ParallelUnitMapping`].
    ///
    /// - For singleton distribution, all of the virtual nodes are mapped to the same parallel unit.
    /// - For hash distribution, the mapping is returned as is.
    pub fn into_mapping(self) -> ParallelUnitMapping {
        match self {
            Distribution::Singleton(p) => ParallelUnitMapping::new_single(p),
            Distribution::Hash(mapping) => mapping,
        }
    }

    /// Create a distribution from a persisted protobuf `Fragment`.
    pub fn from_fragment(fragment: &risingwave_pb::meta::table_fragments::Fragment) -> Self {
        let mapping = ParallelUnitMapping::from_protobuf(fragment.get_vnode_mapping().unwrap());

        match fragment.get_distribution_type().unwrap() {
            FragmentDistributionType::Unspecified => unreachable!(),
            FragmentDistributionType::Single => {
                let parallel_unit = mapping.to_single().unwrap();
                Distribution::Singleton(parallel_unit)
            }
            FragmentDistributionType::Hash => Distribution::Hash(mapping),
        }
    }
}

/// [`Scheduler`] schedules the distribution of fragments in a stream graph.
pub(super) struct Scheduler {
    /// The default hash mapping for hash-distributed fragments, if there's no requirement derived.
    default_hash_mapping: ParallelUnitMapping,

    /// The default parallel unit for singleton fragments, if there's no requirement derived.
    default_singleton_parallel_unit: ParallelUnitId,
}

impl Scheduler {
    /// Create a new [`Scheduler`] with the given parallel units and the default parallelism.
    ///
    /// Each hash-distributed fragment will be scheduled to at most `default_parallelism` parallel
    /// units, in a round-robin fashion on all compute nodes. If the `default_parallelism` is
    /// `None`, all parallel units will be used.
    pub fn new(
        parallel_units: impl IntoIterator<Item = ParallelUnit>,
        default_parallelism: Option<NonZeroUsize>,
    ) -> MetaResult<Self> {
        // Group parallel units with worker node.
        let mut parallel_units_map = BTreeMap::new();
        for p in parallel_units {
            parallel_units_map
                .entry(p.worker_node_id)
                .or_insert_with(Vec::new)
                .push(p);
        }

        // Use all parallel units if no default parallelism is specified.
        let default_parallelism = default_parallelism.map_or_else(
            || parallel_units_map.values().map(|p| p.len()).sum::<usize>(),
            NonZeroUsize::get,
        );

        let mut parallel_units: LinkedList<_> = parallel_units_map
            .into_values()
            .map(|v| v.into_iter().sorted_by_key(|p| p.id))
            .collect();

        // Visit the parallel units in a round-robin manner on each worker.
        let mut round_robin = Vec::new();
        while !parallel_units.is_empty() {
            parallel_units.drain_filter(|ps| {
                if let Some(p) = ps.next() {
                    round_robin.push(p);
                    false
                } else {
                    true
                }
            });
        }
        round_robin.truncate(default_parallelism);

        if round_robin.len() < default_parallelism {
            bail!(
                "Not enough parallel units to schedule {} parallelism",
                default_parallelism
            );
        }

        // Build the default hash mapping uniformly.
        let default_hash_mapping = ParallelUnitMapping::build(&round_robin);
        // Randomly choose a parallel unit as the default singleton parallel unit.
        let default_singleton_parallel_unit = round_robin.choose(&mut thread_rng()).unwrap().id;

        Ok(Self {
            default_hash_mapping,
            default_singleton_parallel_unit,
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

        // Singletons
        for (&id, fragment) in graph.building_fragments() {
            if fragment.is_singleton {
                facts.push(Fact::SingletonReq(id));
            }
        }
        // External
        for (id, req) in existing_distribution {
            let dist = match req {
                Distribution::Singleton(parallel_unit) => DistId::Singleton(parallel_unit),
                Distribution::Hash(mapping) => DistId::Hash(hash_mapping_id[&mapping]),
            };
            facts.push(Fact::ExternalReq { id, dist });
        }
        // Edges
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
        let (success, failed) = crepe.run();
        if !failed.is_empty() {
            bail!("Failed to schedule: {:?}", failed);
        }
        // Should not contain any existing fragments.
        assert_eq!(success.len(), graph.building_fragments().len());

        // Extract the results.
        let distributions = success
            .into_iter()
            .map(|Success(id, result)| {
                let distribution = match result {
                    // Required
                    Result::Required(DistId::Singleton(parallel_unit)) => {
                        Distribution::Singleton(parallel_unit)
                    }
                    Result::Required(DistId::Hash(mapping)) => {
                        Distribution::Hash(all_hash_mappings[mapping].clone())
                    }

                    // Default
                    Result::DefaultSingleton => {
                        Distribution::Singleton(self.default_singleton_parallel_unit)
                    }
                    Result::DefaultHash => Distribution::Hash(self.default_hash_mapping.clone()),
                };
                (id, distribution)
            })
            .collect();

        Ok(distributions)
    }
}

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

#![allow(clippy::explicit_iter_loop, reason = "crepe")]

use std::collections::{BTreeMap, HashMap, LinkedList};

use enum_as_inner::EnumAsInner;
use itertools::Itertools;
use risingwave_common::bail;
use risingwave_common::hash::ParallelUnitMapping;
use risingwave_pb::common::ParallelUnit;
use risingwave_pb::stream_plan::DispatcherType::{self, *};

use super::{CompleteStreamFragmentGraph, GlobalFragmentId as Id};
use crate::MetaResult;

type HashMappingId = usize;

/// The internal distribution structure for processing in the scheduler.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum DistId {
    Hash(HashMappingId),
    Singleton,
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
    /// A distribution requirement for a building fragment.
    InternalReq { id: Id, dist: DistId },
    /// A distribution requirement for an existing fragment.
    ExternalReq { id: Id, dist: DistId },
    /// The default distribution for a building fragment.
    Default(DistId),
}

crepe::crepe! {
    @input
    struct Input(Fact);

    struct Edge(Id, Id, DispatcherType);
    struct Internal(Id, DistId);
    struct External(Id, DistId);
    struct Default(DistId);
    struct Fragment(Id);

    struct Requirement(Id, DistId);

    @output
    struct Success(Id, DistId);
    @output
    #[derive(Debug)]
    struct Failed(Id);

    // Extract facts.
    Edge(from, to, dt) <- Input(f), let Fact::Edge { from, to, dt } = f;
    Internal(id, dist) <- Input(f), let Fact::InternalReq { id, dist } = f;
    External(id, dist) <- Input(f), let Fact::ExternalReq { id, dist } = f;
    Default(dist) <- Input(f), let Fact::Default(dist) = f;

    // Internal fragments.
    Fragment(x) <- Edge(x, _, _), !External(x, _);
    Fragment(y) <- Edge(_, y, _), !External(y, _);

    // Requirements in the facts.
    Requirement(x, d) <- Internal(x, d);
    Requirement(x, d) <- External(x, d);
    // Requirements of `NoShuffle` edges.
    Requirement(x, d) <- Edge(x, y, NoShuffle), Requirement(y, d);
    Requirement(y, d) <- Edge(x, y, NoShuffle), Requirement(x, d);
    // Requirements of `Simple` edges.
    Requirement(y, DistId::Singleton) <- Edge(_, y, Simple);

    // Multiple requirements lead to failure.
    Failed(x) <- Requirement(x, d1), Requirement(x, d2), (d1 != d2);

    // Take the single requirement as the result.
    Success(x, d) <- Fragment(x), Requirement(x, d), !Failed(x);
    // Take the default distribution as the result, if no requirement.
    Success(x, d) <- Fragment(x), Default(d), !Requirement(x, _);
}

/// The distribution of a fragment.
#[derive(EnumAsInner)]
pub(super) enum Distribution {
    Singleton,
    Hash(ParallelUnitMapping),
}

impl Distribution {
    /// The parallelism required by the distribution.
    pub fn parallelism(&self) -> usize {
        match self {
            Distribution::Singleton => 1,
            Distribution::Hash(mapping) => mapping.iter_unique().count(),
        }
    }
}

/// [`Scheduler`] schedules the distribution of fragments in a stream graph.
pub(super) struct Scheduler {
    /// The default distribution for fragments, if there's no requirement derived.
    default_hash_mapping: ParallelUnitMapping,
}

impl Scheduler {
    /// Create a new [`Scheduler`].
    pub fn new(
        parallel_units: impl IntoIterator<Item = ParallelUnit>,
        default_parallelism: usize,
    ) -> MetaResult<Self> {
        // Group parallel units with worker node.
        let mut parallel_units_map = BTreeMap::new();
        for p in parallel_units {
            parallel_units_map
                .entry(p.worker_node_id)
                .or_insert_with(Vec::new)
                .push(p);
        }
        let mut parallel_units: LinkedList<_> = parallel_units_map
            .into_values()
            .map(|v| v.into_iter())
            .collect();

        // Visit the parallel units in a round-robin manner on each worker.
        let mut round_robin = Vec::new();
        while !parallel_units.is_empty() && round_robin.len() < default_parallelism {
            parallel_units.drain_filter(|ps| {
                if let Some(p) = ps.next() {
                    round_robin.push(p);
                    false
                } else {
                    true
                }
            });
        }

        if round_robin.len() < default_parallelism {
            bail!(
                "Not enough parallel units to schedule {} parallelism",
                default_parallelism
            );
        }

        // Build the default hash mapping uniformly.
        let default_hash_mapping = ParallelUnitMapping::build(&round_robin);

        Ok(Self {
            default_hash_mapping,
        })
    }

    /// Schedule the given complete graph and returns the distribution of each fragment.
    pub fn schedule(
        &self,
        graph: &CompleteStreamFragmentGraph,
    ) -> MetaResult<HashMap<Id, Distribution>> {
        let existing_distribution = graph.existing_distribution();

        // Build an index map for all hash mappings.
        let all_hash_mappings = existing_distribution
            .values()
            .flat_map(|dist| dist.as_hash())
            .chain(std::iter::once(&self.default_hash_mapping))
            .cloned()
            .unique()
            .collect_vec();
        let hash_mapping_id: HashMap<_, _> = all_hash_mappings
            .iter()
            .enumerate()
            .map(|(i, m)| (m.clone(), i))
            .collect();

        let mut facts = Vec::new();

        // Default
        facts.push(Fact::Default(DistId::Hash(
            hash_mapping_id[&self.default_hash_mapping],
        )));
        // Internal
        for (&id, fragment) in &graph.graph.fragments {
            if fragment.is_singleton {
                facts.push(Fact::InternalReq {
                    id,
                    dist: DistId::Singleton,
                });
            }
        }
        // External
        for (id, req) in existing_distribution {
            let dist = match req {
                Distribution::Singleton => DistId::Singleton,
                Distribution::Hash(mapping) => DistId::Hash(hash_mapping_id[&mapping]),
            };
            facts.push(Fact::ExternalReq { id, dist });
        }
        // Edges
        for (from, to, dt) in graph.dispatch_edges() {
            facts.push(Fact::Edge { from, to, dt });
        }

        // Run the algorithm.
        let mut crepe = Crepe::new();
        crepe.extend(facts.into_iter().map(Input));
        let (success, failed) = crepe.run();
        if !failed.is_empty() {
            bail!("Failed to schedule: {:?}", failed);
        }
        assert_eq!(success.len(), graph.graph.fragments.len());

        // Extract the results.
        let distributions = success
            .into_iter()
            .map(|Success(id, distribution)| {
                let distribution = match distribution {
                    DistId::Hash(mapping) => Distribution::Hash(all_hash_mappings[mapping].clone()),
                    DistId::Singleton => Distribution::Singleton,
                };
                (id, distribution)
            })
            .collect();

        Ok(distributions)
    }
}

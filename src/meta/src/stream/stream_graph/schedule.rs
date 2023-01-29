// Copyright 2023 Singularity Data
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

use std::collections::{BTreeMap, HashMap, LinkedList};
use std::rc::Rc;

use enum_as_inner::EnumAsInner;
use itertools::Itertools;
use risingwave_common::bail;
use risingwave_common::catalog::TableId;
use risingwave_common::hash::{ParallelUnitId, ParallelUnitMapping};
use risingwave_pb::common::ParallelUnit;
use risingwave_pb::stream_plan::DispatcherType::{self, *};

use super::{GlobalFragmentId as Id, StreamFragmentGraph};
use crate::manager::FragmentManager;
use crate::storage::MetaStore;
use crate::stream::CreateStreamingJobContext;
use crate::MetaResult;

type HashMappingId = usize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Distribution {
    Hash(HashMappingId),
    Singleton,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Fact {
    Edge {
        from: Id,
        to: Id,
        dt: DispatcherType,
    },
    Distribution {
        id: Id,
        dist: Distribution,
    },
    Default(Distribution),
}

crepe::crepe! {
    @input
    struct Input(Fact);

    struct Edge(Id, Id, DispatcherType);
    struct Fragment(Id);
    struct Default(Distribution);

    struct Requirement(Id, Distribution);

    @output
    struct Success(Id, Distribution);
    @output
    struct Failed(Id);

    Edge(from, to, dt) <- Input(f), let Fact::Edge { from, to, dt } = f;
    Requirement(id, dist) <- Input(f), let Fact::Distribution { id, dist } = f;
    Default(dist) <- Input(f), let Fact::Default(dist) = f;
    Fragment(x) <- Edge(x, _, _);
    Fragment(y) <- Edge(_, y, _);

    Requirement(x, d) <- Edge(x, y, NoShuffle), Requirement(y, d);
    Requirement(y, d) <- Edge(x, y, NoShuffle), Requirement(x, d);

    Requirement(y, Distribution::Singleton) <- Edge(_, y, Simple);

    Failed(x) <- Requirement(x, d1), Requirement(x, d2), (d1 != d2);
    Success(x, d) <- Requirement(x, d), !Failed(x);
    Success(x, d) <- Fragment(x), Default(d), !Requirement(x, _);
}

#[derive(EnumAsInner)]
pub(super) enum ExternalRequirement {
    Hash(ParallelUnitMapping),
    Singleton,
}

pub(super) struct ExternalRequirements(HashMap<Id, ExternalRequirement>);

impl ExternalRequirements {
    pub async fn for_create_streaming_job<S: MetaStore>(
        ctx: &CreateStreamingJobContext,
    ) -> MetaResult<Self> {
        todo!()
    }
}

/// [`Scheduler`] defines schedule logic for mv actors.
pub(super) struct Scheduler {
    /// The parallel units of the cluster in a round-robin manner on each worker.
    all_parallel_units: Vec<ParallelUnit>,

    default_parallelism: usize,

    default_hash_mapping: ParallelUnitMapping,
}

impl Scheduler {
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

        let default_hash_mapping = ParallelUnitMapping::build(&round_robin);

        Ok(Self {
            all_parallel_units: round_robin,
            default_parallelism,
            default_hash_mapping,
        })
    }

    pub fn schedule(
        &self,
        graph: &StreamFragmentGraph,
        external_requirements: &[(Id, ExternalRequirement)],
    ) -> HashMap<Id, usize> {
        let all_hash_mappings = external_requirements
            .iter()
            .flat_map(|(_, req)| req.as_hash())
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

        facts.push(Fact::Default(Distribution::Hash(
            hash_mapping_id[&self.default_hash_mapping],
        )));

        for (from, to, edge) in graph.edges() {
            let dt = edge.get_dispatch_strategy().unwrap().get_type().unwrap();
            facts.push(Fact::Edge { from, to, dt });
        }

        for (id, req) in external_requirements {
            let dist = match req {
                ExternalRequirement::Hash(mapping) => Distribution::Hash(hash_mapping_id[mapping]),
                ExternalRequirement::Singleton => Distribution::Singleton,
            };
            facts.push(Fact::Distribution { id: *id, dist });
        }

        let mut crepe = Crepe::new();
        crepe.extend(facts.into_iter().map(Input));

        let (success, failed) = crepe.run();
        assert!(failed.is_empty());

        // TODO
        success
            .into_iter()
            .map(|Success(id, distribution)| {
                let parallelism = match distribution {
                    Distribution::Hash(mapping) => {
                        all_hash_mappings[mapping].iter().unique().count()
                    }
                    Distribution::Singleton => 1,
                };
                (id, parallelism)
            })
            .collect()
    }
}

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

//!   "A -> B" represent A satisfies B
//!                                 x
//!  only as a required property    x  can used as both required
//!                                 x  and provided property
//!                                 x
//!            ┌───┐                x┌──────┐
//!            │Any◄─────────────────┤single│
//!            └─▲─┘                x└──────┘
//!              │                  x
//!              │                  x
//!              │                  x
//!          ┌───┴────┐             x┌──────────┐
//!          │AnyShard◄──────────────┤SomeShard │
//!          └───▲────┘             x└──────────┘
//!              │                  x
//!          ┌───┴───────────┐      x┌──────────────┐ ┌──────────────┐
//!          │ShardByKey(a,b)◄───┬───┤HashShard(a,b)│ │HashShard(b,a)│
//!          └───▲──▲────────┘   │  x└──────────────┘ └┬─────────────┘
//!              │  │            │  x                  │
//!              │  │            └─────────────────────┘
//!              │  │               x
//!              │ ┌┴────────────┐  x┌────────────┐
//!              │ │ShardByKey(a)◄───┤HashShard(a)│
//!              │ └─────────────┘  x└────────────┘
//!              │                  x
//!             ┌┴────────────┐     x┌────────────┐
//!             │ShardByKey(b)◄──────┤HashShard(b)│
//!             └─────────────┘     x└────────────┘
//!                                 x
//!                                 x
use std::collections::HashMap;
use std::fmt;
use std::fmt::Debug;

use fixedbitset::FixedBitSet;
use itertools::Itertools;
use risingwave_common::catalog::{FieldDisplay, Schema, TableId};
use risingwave_common::error::Result;
use risingwave_common::hash::{ParallelUnitId, ParallelUnitMapping};
use risingwave_pb::batch_plan::exchange_info::{
    ConsistentHashInfo, Distribution as DistributionPb, DistributionMode, HashInfo,
};
use risingwave_pb::batch_plan::ExchangeInfo;

use super::super::plan_node::*;
use crate::catalog::catalog_service::CatalogReader;
use crate::optimizer::plan_node::stream::StreamPlanRef;
use crate::optimizer::property::Order;
use crate::optimizer::PlanRef;
use crate::scheduler::worker_node_manager::WorkerNodeManagerRef;

/// the distribution property provided by a operator.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Distribution {
    /// There is only one partition. All records are placed on it.
    Single,
    /// Records are sharded into partitions, and satisfy the `AnyShard` but without any guarantee
    /// about their placement rules.
    SomeShard,
    /// Records are sharded into partitions based on the hash value of some columns, which means
    /// the records with the same hash values must be on the same partition.
    /// `usize` is the index of column used as the distribution key.
    HashShard(Vec<usize>),
    /// A special kind of provided distribution which is almost the same as
    /// [`Distribution::HashShard`], but may have different vnode mapping.
    ///
    /// It exists because the upstream MV can be scaled independently. So we use
    /// `UpstreamHashShard` to force an exchange is inserted.
    ///
    /// Alternatively, [`Distribution::SomeShard`] can also be used to insert an exchange, but
    /// `UpstreamHashShard` contains distribution keys, which might be useful in some cases, e.g.,
    /// two-phase Agg. It also satisfies [`RequiredDist::ShardByKey`].
    ///
    /// TableId is used to represent the data distribution(`vnode_mapping`) of this
    /// UpstreamHashShard. The scheduler can fetch TableId's corresponding `vnode_mapping` to do
    /// shuffle.
    UpstreamHashShard(Vec<usize>, TableId),
    /// Records are available on all downstream shards.
    Broadcast,
}

/// the distribution property requirement.
#[derive(Debug, Clone, PartialEq)]
pub enum RequiredDist {
    /// with any distribution
    Any,
    /// records are shard on partitions, which means every record should belong to a partition
    AnyShard,
    /// records are shard on partitions based on some keys(order-irrelevance, ShardByKey({a,b}) is
    /// equivalent with ShardByKey({b,a})), which means the records with same keys must be on
    /// the same partition, as required property only.
    ShardByKey(FixedBitSet),
    /// must be the same with the physical distribution
    PhysicalDist(Distribution),
}

impl Distribution {
    pub fn to_prost(
        &self,
        output_count: u32,
        catalog_reader: &CatalogReader,
        worker_node_manager: &WorkerNodeManagerRef,
    ) -> ExchangeInfo {
        ExchangeInfo {
            mode: match self {
                Distribution::Single => DistributionMode::Single,
                Distribution::HashShard(_) => DistributionMode::Hash,
                // TODO: add round robin DistributionMode
                Distribution::SomeShard => DistributionMode::Single,
                Distribution::Broadcast => DistributionMode::Broadcast,
                Distribution::UpstreamHashShard(_, _) => DistributionMode::ConsistentHash,
            } as i32,
            distribution: match self {
                Distribution::Single => None,
                Distribution::HashShard(key) => {
                    assert!(
                        !key.is_empty(),
                        "hash key should not be empty, use `Single` instead"
                    );
                    Some(DistributionPb::HashInfo(HashInfo {
                        output_count,
                        key: key.iter().map(|num| *num as u32).collect(),
                    }))
                }
                // TODO: add round robin distribution
                Distribution::SomeShard => None,
                Distribution::Broadcast => None,
                Distribution::UpstreamHashShard(key, table_id) => {
                    assert!(
                        !key.is_empty(),
                        "hash key should not be empty, use `Single` instead"
                    );

                    let vnode_mapping =
                        Self::get_vnode_mapping(catalog_reader, worker_node_manager, table_id)
                            .expect("vnode_mapping of UpstreamHashShard should not be none");

                    let pu2id_map: HashMap<ParallelUnitId, u32> = vnode_mapping
                        .iter_unique()
                        .enumerate()
                        .map(|(i, pu)| (pu, i as u32))
                        .collect();

                    Some(DistributionPb::ConsistentHashInfo(ConsistentHashInfo {
                        vmap: vnode_mapping.iter().map(|x| pu2id_map[&x]).collect_vec(),
                        key: key.iter().map(|num| *num as u32).collect(),
                    }))
                }
            },
        }
    }

    /// check if the distribution satisfies other required distribution
    pub fn satisfies(&self, required: &RequiredDist) -> bool {
        match required {
            RequiredDist::Any => true,
            RequiredDist::AnyShard => {
                matches!(
                    self,
                    Distribution::SomeShard
                        | Distribution::HashShard(_)
                        | Distribution::UpstreamHashShard(_, _)
                        | Distribution::Broadcast
                )
            }
            RequiredDist::ShardByKey(required_key) => match self {
                Distribution::HashShard(hash_key)
                | Distribution::UpstreamHashShard(hash_key, _) => {
                    hash_key.iter().all(|idx| required_key.contains(*idx))
                }
                _ => false,
            },
            RequiredDist::PhysicalDist(other) => self == other,
        }
    }

    /// Get distribution column indices. After optimization, only `HashShard` and `Single` are
    /// valid.
    pub fn dist_column_indices(&self) -> &[usize] {
        match self {
            Distribution::Single | Distribution::SomeShard | Distribution::Broadcast => {
                Default::default()
            }
            Distribution::HashShard(dists) | Distribution::UpstreamHashShard(dists, _) => dists,
        }
    }

    #[inline(always)]
    fn get_vnode_mapping(
        catalog_reader: &CatalogReader,
        worker_node_manager: &WorkerNodeManagerRef,
        table_id: &TableId,
    ) -> Option<ParallelUnitMapping> {
        catalog_reader
            .read_guard()
            .get_table_by_id(table_id)
            .map(|table| worker_node_manager.get_fragment_mapping(&table.fragment_id))
            .ok()
            .flatten()
    }
}

impl fmt::Display for Distribution {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("[")?;
        match self {
            Self::Single => f.write_str("Single")?,
            Self::SomeShard => f.write_str("SomeShard")?,
            Self::Broadcast => f.write_str("Broadcast")?,
            Self::HashShard(vec) | Self::UpstreamHashShard(vec, _) => {
                for key in vec {
                    std::fmt::Debug::fmt(&key, f)?;
                }
            }
        }
        f.write_str("]")
    }
}

pub struct DistributionDisplay<'a> {
    pub distribution: &'a Distribution,
    pub input_schema: &'a Schema,
}

impl DistributionDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let that = self.distribution;
        match that {
            Distribution::Single => f.write_str("Single"),
            Distribution::SomeShard => f.write_str("SomeShard"),
            Distribution::Broadcast => f.write_str("Broadcast"),
            Distribution::HashShard(vec) | Distribution::UpstreamHashShard(vec, _) => {
                if let Distribution::HashShard(_) = that {
                    f.write_str("HashShard(")?;
                } else {
                    f.write_str("UpstreamHashShard(")?;
                }
                for key in vec.iter().copied().with_position() {
                    std::fmt::Debug::fmt(
                        &FieldDisplay(self.input_schema.fields.get(key.into_inner()).unwrap()),
                        f,
                    )?;
                    match key {
                        itertools::Position::First(_) | itertools::Position::Middle(_) => {
                            f.write_str(", ")?;
                        }
                        _ => {}
                    }
                }
                f.write_str(")")
            }
        }
    }
}

impl fmt::Debug for DistributionDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.fmt(f)
    }
}

impl fmt::Display for DistributionDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.fmt(f)
    }
}

impl RequiredDist {
    pub fn single() -> Self {
        Self::PhysicalDist(Distribution::Single)
    }

    pub fn shard_by_key(tot_col_num: usize, key: &[usize]) -> Self {
        let mut cols = FixedBitSet::with_capacity(tot_col_num);
        for i in key {
            cols.insert(*i);
        }
        if cols.count_ones(..) == 0 {
            Self::Any
        } else {
            Self::ShardByKey(cols)
        }
    }

    pub fn hash_shard(key: &[usize]) -> Self {
        assert!(!key.is_empty());
        Self::PhysicalDist(Distribution::HashShard(key.to_vec()))
    }

    pub fn enforce_if_not_satisfies(
        &self,
        plan: PlanRef,
        required_order: &Order,
    ) -> Result<PlanRef> {
        if !plan.distribution().satisfies(self) {
            Ok(self.enforce(plan, required_order))
        } else {
            Ok(plan)
        }
    }

    pub fn no_shuffle(plan: PlanRef) -> PlanRef {
        match plan.convention() {
            Convention::Stream => StreamExchange::new_no_shuffle(plan).into(),
            Convention::Logical | Convention::Batch => unreachable!(),
        }
    }

    #[allow(dead_code)]
    pub fn enforce_stream_if_not_satisfies(
        &self,
        plan: stream::PlanRef,
    ) -> Result<stream::PlanRef> {
        if !plan.distribution().satisfies(self) {
            // FIXME(st1page);
            Ok(stream::Exchange {
                dist: self.to_dist(),
                input: plan,
            }
            .into())
        } else {
            Ok(plan)
        }
    }

    /// check if the distribution satisfies other required distribution
    pub fn satisfies(&self, required: &RequiredDist) -> bool {
        match self {
            RequiredDist::Any => matches!(required, RequiredDist::Any),
            RequiredDist::AnyShard => {
                matches!(required, RequiredDist::Any | RequiredDist::AnyShard)
            }
            RequiredDist::ShardByKey(key) => match required {
                RequiredDist::Any | RequiredDist::AnyShard => true,
                RequiredDist::ShardByKey(required_key) => key.is_subset(required_key),
                _ => false,
            },
            RequiredDist::PhysicalDist(dist) => dist.satisfies(required),
        }
    }

    fn enforce(&self, plan: PlanRef, required_order: &Order) -> PlanRef {
        let dist = self.to_dist();
        match plan.convention() {
            Convention::Batch => BatchExchange::new(plan, required_order.clone(), dist).into(),
            Convention::Stream => StreamExchange::new(plan, dist).into(),
            _ => unreachable!(),
        }
    }

    fn to_dist(&self) -> Distribution {
        match self {
            // all the distribution satisfy the Any, and the function can be only called by
            // `enforce_if_not_satisfies`
            RequiredDist::Any => unreachable!(),
            // TODO: add round robin distributed type
            RequiredDist::AnyShard => todo!(),
            RequiredDist::ShardByKey(required_keys) => {
                Distribution::HashShard(required_keys.ones().collect())
            }
            RequiredDist::PhysicalDist(dist) => dist.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Distribution, RequiredDist};

    #[test]
    fn hash_shard_satisfy() {
        let d1 = Distribution::HashShard(vec![0, 1]);
        let d2 = Distribution::HashShard(vec![1, 0]);
        let d3 = Distribution::HashShard(vec![0]);
        let d4 = Distribution::HashShard(vec![1]);

        let r1 = RequiredDist::shard_by_key(2, &[0, 1]);
        let r3 = RequiredDist::shard_by_key(2, &[0]);
        let r4 = RequiredDist::shard_by_key(2, &[1]);
        assert!(d1.satisfies(&RequiredDist::PhysicalDist(d1.clone())));
        assert!(d2.satisfies(&RequiredDist::PhysicalDist(d2.clone())));
        assert!(d3.satisfies(&RequiredDist::PhysicalDist(d3.clone())));
        assert!(d4.satisfies(&RequiredDist::PhysicalDist(d4.clone())));

        assert!(!d2.satisfies(&RequiredDist::PhysicalDist(d1.clone())));
        assert!(!d3.satisfies(&RequiredDist::PhysicalDist(d1.clone())));
        assert!(!d4.satisfies(&RequiredDist::PhysicalDist(d1.clone())));

        assert!(!d1.satisfies(&RequiredDist::PhysicalDist(d3.clone())));
        assert!(!d2.satisfies(&RequiredDist::PhysicalDist(d3.clone())));
        assert!(!d1.satisfies(&RequiredDist::PhysicalDist(d4.clone())));
        assert!(!d2.satisfies(&RequiredDist::PhysicalDist(d4.clone())));

        assert!(d1.satisfies(&r1));
        assert!(d2.satisfies(&r1));
        assert!(d3.satisfies(&r1));
        assert!(d4.satisfies(&r1));

        assert!(!d1.satisfies(&r3));
        assert!(!d2.satisfies(&r3));
        assert!(d3.satisfies(&r3));
        assert!(!d4.satisfies(&r3));

        assert!(!d1.satisfies(&r4));
        assert!(!d2.satisfies(&r4));
        assert!(!d3.satisfies(&r4));
        assert!(d4.satisfies(&r4));

        assert!(r3.satisfies(&r1));
        assert!(r4.satisfies(&r1));
        assert!(!r1.satisfies(&r3));
        assert!(!r1.satisfies(&r4));
        assert!(!r3.satisfies(&r4));
        assert!(!r4.satisfies(&r3));
    }
}

// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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
use fixedbitset::FixedBitSet;
use risingwave_common::error::Result;
use risingwave_pb::batch_plan::exchange_info::{
    Distribution as DistributionProst, DistributionMode, HashInfo,
};
use risingwave_pb::batch_plan::ExchangeInfo;

use super::super::plan_node::*;
use crate::optimizer::property::Order;
use crate::optimizer::PlanRef;

/// the distribution property provided by a operator.
#[derive(Debug, Clone, PartialEq)]
pub enum Distribution {
    /// there only one partition, and all records on it.
    Single,
    /// records are shard on partitions, and satisfy the `AnyShard` but without any guarantee about
    /// their placed rules.
    SomeShard,
    /// records are shard on partitions based on hash value of some keys, which means the records
    /// with same hash values must be on the same partition.
    HashShard(Vec<usize>),
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
    pub fn to_prost(&self, output_count: u32) -> ExchangeInfo {
        ExchangeInfo {
            mode: match self {
                Distribution::Single => DistributionMode::Single,
                Distribution::HashShard(_) => DistributionMode::Hash,
                // TODO: add round robin DistributionMode
                Distribution::SomeShard => DistributionMode::Single,
            } as i32,
            distribution: match self {
                Distribution::Single => None,
                Distribution::HashShard(keys) => Some(DistributionProst::HashInfo(HashInfo {
                    output_count,
                    keys: keys.iter().map(|num| *num as u32).collect(),
                })),
                // TODO: add round robin distribution
                Distribution::SomeShard => None,
            },
        }
    }

    /// check if the distribution satisfies other required distribution
    pub fn satisfies(&self, required: &RequiredDist) -> bool {
        match required {
            RequiredDist::Any => true,
            RequiredDist::AnyShard => {
                matches!(self, Distribution::SomeShard | Distribution::HashShard(_))
            }
            RequiredDist::ShardByKey(required_keys) => match self {
                Distribution::HashShard(hash_keys) => hash_keys
                    .iter()
                    .all(|hash_key| required_keys.contains(*hash_key)),
                _ => false,
            },
            RequiredDist::PhysicalDist(other) => self == other,
        }
    }

    /// Get distribution column indices. After optimization, only `HashShard` and `Single` are
    /// valid.
    pub fn dist_column_indices(&self) -> &[usize] {
        match self {
            Distribution::Single => Default::default(),
            Distribution::HashShard(dists) => dists,
            _ => unreachable!(),
        }
    }
}

impl RequiredDist {
    pub fn single() -> Self {
        Self::PhysicalDist(Distribution::Single)
    }

    pub fn shard_by_key(tot_col_num: usize, keys: &[usize]) -> Self {
        let mut cols = FixedBitSet::with_capacity(tot_col_num);
        for i in keys {
            cols.insert(*i);
        }
        if cols.count_ones(..) == 0 {
            Self::Any
        } else {
            Self::ShardByKey(cols)
        }
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

    /// check if the distribution satisfies other required distribution
    pub fn satisfies(&self, required: &RequiredDist) -> bool {
        match self {
            RequiredDist::Any => matches!(required, RequiredDist::Any),
            RequiredDist::AnyShard => {
                matches!(required, RequiredDist::Any | RequiredDist::AnyShard)
            }
            RequiredDist::ShardByKey(keys) => match required {
                RequiredDist::Any | RequiredDist::AnyShard => true,
                RequiredDist::ShardByKey(required_keys) => keys.is_subset(required_keys),
                _ => false,
            },
            RequiredDist::PhysicalDist(dist) => dist.satisfies(required),
        }
    }

    fn enforce(&self, plan: PlanRef, required_order: &Order) -> PlanRef {
        let dist = match self {
            RequiredDist::Any => unreachable!(),
            // TODO: add round robin distributed type
            RequiredDist::AnyShard => todo!(),
            RequiredDist::ShardByKey(required_keys) => {
                Distribution::HashShard(required_keys.ones().collect())
            }
            RequiredDist::PhysicalDist(dist) => dist.clone(),
        };
        match plan.convention() {
            Convention::Batch => BatchExchange::new(plan, required_order.clone(), dist).into(),
            Convention::Stream => StreamExchange::new(plan, dist).into(),
            _ => unreachable!(),
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

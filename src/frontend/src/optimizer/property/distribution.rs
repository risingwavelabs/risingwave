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

use risingwave_pb::plan::exchange_info::{
    BroadcastInfo, Distribution as DistributionProst, DistributionMode, HashInfo,
};
use risingwave_pb::plan::ExchangeInfo;

use super::super::plan_node::*;
use crate::optimizer::property::Order;
use crate::optimizer::PlanRef;

#[derive(Debug, Clone, PartialEq)]
pub enum Distribution {
    Any,
    Single,
    Broadcast,
    AnyShard,
    HashShard(Vec<usize>),
}

static ANY_DISTRIBUTION: Distribution = Distribution::Any;

impl Distribution {
    pub fn to_prost(&self, output_count: u32) -> ExchangeInfo {
        ExchangeInfo {
            mode: match self {
                Distribution::Single => DistributionMode::Single,
                Distribution::Broadcast => DistributionMode::Broadcast,
                Distribution::HashShard(_) => DistributionMode::Hash,
                // TODO: Should panic if AnyShard or Any
                _ => DistributionMode::Single,
            } as i32,
            distribution: match self {
                Distribution::Single => None,
                Distribution::Broadcast => Some(DistributionProst::BroadcastInfo(BroadcastInfo {
                    count: output_count,
                })),
                Distribution::HashShard(keys) => Some(DistributionProst::HashInfo(HashInfo {
                    output_count,
                    keys: keys.iter().map(|num| *num as u32).collect(),
                })),
                // TODO: Should panic if AnyShard or Any
                _ => None,
            },
        }
    }

    pub fn enforce_if_not_satisfies(&self, plan: PlanRef, required_order: &Order) -> PlanRef {
        if !plan.distribution().satisfies(self) {
            self.enforce(plan, required_order)
        } else {
            plan
        }
    }

    fn enforce(&self, plan: PlanRef, required_order: &Order) -> PlanRef {
        match plan.convention() {
            Convention::Batch => {
                BatchExchange::new(plan, required_order.clone(), self.clone()).into()
            }
            Convention::Stream => StreamExchange::new(plan, self.clone()).into(),
            _ => unreachable!(),
        }
    }
    // "A -> B" represent A satisfies B
    //                   +---+
    //                   |Any|
    //                   +---+
    //                     ^
    //         +-----------------------+
    //         |           |           |
    //     +---+----+   +--+---+  +----+----+
    //     |Anyshard|   |single|  |broadcast|
    //     +---+----+   +------+  +---------+
    //         ^
    //  +------+------+
    //  |hash_shard(a)|
    //  +------+------+
    //         ^
    // +-------+-------+
    // |hash_shard(a,b)|
    // +---------------+
    pub fn satisfies(&self, other: &Distribution) -> bool {
        match self {
            Distribution::Any => matches!(other, Distribution::Any),
            Distribution::Single => matches!(other, Distribution::Any | Distribution::Single),
            Distribution::Broadcast => matches!(other, Distribution::Any | Distribution::Broadcast),
            Distribution::AnyShard => matches!(other, Distribution::Any | Distribution::AnyShard),
            Distribution::HashShard(keys) => match other {
                Distribution::Any => true,
                Distribution::AnyShard => true,
                Distribution::HashShard(other_keys) => other_keys
                    .iter()
                    .all(|other_key| keys.iter().any(|key| key == other_key)),
                _ => false,
            },
        }
    }
    pub fn any() -> &'static Self {
        &ANY_DISTRIBUTION
    }
    pub fn is_any(&self) -> bool {
        matches!(self, Distribution::Any)
    }

    /// Get distribution column indices.
    pub fn dist_column_indices(&self) -> &[usize] {
        match self {
            Distribution::HashShard(dists) => dists,
            _ => &[],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Distribution;

    fn test_hash_shard_subset(uni: &Distribution, sub: &Distribution) {
        assert!(uni.satisfies(sub));
        assert!(!sub.satisfies(uni));
    }

    fn test_hash_shard_false(d1: &Distribution, d2: &Distribution) {
        assert!(!d1.satisfies(d2));
        assert!(!d2.satisfies(d1));
    }

    #[test]
    fn hash_shard_satisfy() {
        let d1 = Distribution::HashShard(vec![0, 2, 4, 6, 8]);
        let d2 = Distribution::HashShard(vec![2, 4]);
        let d3 = Distribution::HashShard(vec![4, 6]);
        let d4 = Distribution::HashShard(vec![6, 8]);
        test_hash_shard_subset(&d1, &d2);
        test_hash_shard_subset(&d1, &d3);
        test_hash_shard_subset(&d1, &d4);
        test_hash_shard_subset(&d1, &d2);
        test_hash_shard_false(&d2, &d3);
        test_hash_shard_false(&d2, &d4);
        test_hash_shard_false(&d3, &d4);
    }
}

use super::super::plan_node::*;
use crate::optimizer::property::{Convention, Order};
use crate::optimizer::PlanRef;
#[derive(Debug, Clone)]
pub enum Distribution {
    Any,
    Single,
    Broadcast,
    AnyShard,
    HashShard(Vec<usize>),
}
#[allow(dead_code)]
impl Distribution {
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
                BatchExchange::new(plan, required_order.clone(), self.clone()).into_plan_ref()
            }
            Convention::Stream => StreamExchange::new(plan, self.clone()).into_plan_ref(),
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
    fn satisfies(&self, other: &Distribution) -> bool {
        match self {
            Distribution::Any => true,
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
    pub fn any() -> Self {
        Distribution::Any
    }
    pub fn is_any(&self) -> bool {
        matches!(self, Distribution::Any)
    }
}
pub trait WithDistribution {
    // use the default impl will not affect correctness, but insert unnecessary Exchange in plan
    fn distribution(&self) -> Distribution {
        Distribution::any()
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

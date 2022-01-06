use std::rc::Rc;

use super::super::plan_node::*;
use crate::optimizer::property::{Convention, Order};
use crate::optimizer::PlanRef;
#[derive(Debug, Clone)]
pub enum Distribution {
    Any,
    Single,
    Broadcast,
    Shard,
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
            Convention::Batch => Rc::new(BatchExchange::new(
                plan,
                required_order.clone(),
                self.clone(),
            )),
            Convention::Stream => Rc::new(StreamExchange::new(plan, self.clone())),
            _ => unreachable!(),
        }
    }
    // "A -> B" represent A satisfies B
    //                 +---+
    //                 |Any|
    //                 +---+
    //                   ^
    //         +---------------------+
    //         |         |           |
    //      +--+--+   +--+---+  +----+----+
    //      |shard|   |single|  |broadcast|
    //      +--+--+   +------+  +---------+
    //         ^
    //  +------+------+
    //  |hash_shard(a)|
    //  +------+------+
    //         ^
    // +-------+-------+
    // |hash_shard(a,b)|
    // +---------------+
    fn satisfies(&self, _other: &Distribution) -> bool {
        todo!()
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

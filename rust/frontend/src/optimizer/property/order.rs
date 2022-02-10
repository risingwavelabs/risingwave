use super::super::plan_node::*;
use super::Convention;
use crate::optimizer::PlanRef;

#[allow(dead_code)]
#[derive(Debug, Clone, Default)]
pub struct Order {
    pub field_order: Vec<FieldOrder>,
}
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct FieldOrder {
    pub index: usize,
    pub direct: Direction,
}
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub enum Direction {
    Asc,
    Desc,
    Any, // only used in order requirement
}
#[allow(dead_code)]

impl Order {
    pub fn enforce_if_not_satisfies(&self, plan: PlanRef) -> PlanRef {
        if !plan.order().satisfies(self) {
            self.enforce(plan)
        } else {
            plan
        }
    }
    pub fn enforce(&self, plan: PlanRef) -> PlanRef {
        assert_eq!(plan.convention(), Convention::Batch);
        BatchSort::new(plan, self.clone()).into_plan_ref()
    }
    pub fn satisfies(&self, _other: &Order) -> bool {
        todo!()
    }
    pub fn any() -> Self {
        Order {
            field_order: vec![],
        }
    }
    pub fn is_any(&self) -> bool {
        self.field_order.is_empty()
    }
}
pub trait WithOrder {
    // use the default impl will not affect correctness, but insert unnecessary Sort in plan
    fn order(&self) -> Order {
        Order::any()
    }
}

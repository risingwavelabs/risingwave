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
#[derive(Debug, Clone, PartialEq)]
pub enum Direction {
    Asc,
    Desc,
    Any, // only used in order requirement
}

#[allow(dead_code)]
impl Direction {
    pub fn satisfies(&self, other: &Direction) -> bool {
        match other {
            Direction::Any => true,
            _ => self == other,
        }
    }
}
lazy_static::lazy_static! {
    static ref ANY_ORDER: Order = Order {
        field_order: vec![],
    };
}

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
    pub fn satisfies(&self, other: &Order) -> bool {
        if self.field_order.len() < other.field_order.len() {
            return false;
        }
        #[allow(clippy::disallowed_methods)]
        for (order, other_order) in self.field_order.iter().zip(other.field_order.iter()) {
            if order.index != other_order.index || !order.direct.satisfies(&other_order.direct) {
                return false;
            }
        }
        true
    }
    pub fn any() -> &'static Self {
        &ANY_ORDER
    }
    pub fn is_any(&self) -> bool {
        self.field_order.is_empty()
    }
}

pub trait WithOrder {
    // use the default impl will not affect correctness, but insert unnecessary Sort in plan
    fn order(&self) -> &Order {
        Order::any()
    }
}

#[cfg(test)]
mod tests {
    use super::{Direction, FieldOrder, Order};

    #[test]
    fn test_order_satisfy() {
        let o1 = Order {
            field_order: vec![
                FieldOrder {
                    index: 0,
                    direct: Direction::Asc,
                },
                FieldOrder {
                    index: 1,
                    direct: Direction::Desc,
                },
                FieldOrder {
                    index: 2,
                    direct: Direction::Asc,
                },
            ],
        };
        let o2 = Order {
            field_order: vec![
                FieldOrder {
                    index: 0,
                    direct: Direction::Asc,
                },
                FieldOrder {
                    index: 1,
                    direct: Direction::Desc,
                },
            ],
        };
        let o3 = Order {
            field_order: vec![
                FieldOrder {
                    index: 0,
                    direct: Direction::Asc,
                },
                FieldOrder {
                    index: 1,
                    direct: Direction::Asc,
                },
            ],
        };
        let o4 = Order {
            field_order: vec![
                FieldOrder {
                    index: 0,
                    direct: Direction::Asc,
                },
                FieldOrder {
                    index: 1,
                    direct: Direction::Any,
                },
            ],
        };

        assert!(o1.satisfies(&o2));
        assert!(!o2.satisfies(&o1));
        assert!(o1.satisfies(&o1));

        assert!(!o2.satisfies(&o3));
        assert!(!o3.satisfies(&o2));

        assert!(o3.satisfies(&o4));
        assert!(o3.satisfies(&o4));
        assert!(!o4.satisfies(&o2));
        assert!(!o4.satisfies(&o3));
    }
}

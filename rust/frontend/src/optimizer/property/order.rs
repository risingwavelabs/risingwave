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
//
use std::fmt;

use itertools::Itertools;
use paste::paste;
use risingwave_pb::expr::InputRefExpr;
use risingwave_pb::plan::OrderType;

use super::super::plan_node::*;
use super::Convention;
use crate::optimizer::PlanRef;
use crate::{for_batch_plan_nodes, for_logical_plan_nodes, for_stream_plan_nodes};

#[derive(Debug, Clone, Default)]
pub struct Order {
    pub field_order: Vec<FieldOrder>,
}

impl Order {
    pub fn to_protobuf(&self) -> Vec<(InputRefExpr, OrderType)> {
        self.field_order
            .iter()
            .map(FieldOrder::to_protobuf)
            .collect_vec()
    }
}

impl fmt::Display for Order {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("[")?;
        for (i, field_order) in self.field_order.iter().enumerate() {
            if i > 0 {
                f.write_str(", ")?;
            }
            field_order.fmt(f)?;
        }
        f.write_str("]")
    }
}

#[derive(Clone)]
pub struct FieldOrder {
    pub index: usize,
    pub direct: Direction,
}

impl std::fmt::Debug for FieldOrder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "${} {}", self.index, self.direct)
    }
}

impl FieldOrder {
    pub fn ascending(index: usize) -> Self {
        Self {
            index,
            direct: Direction::Asc,
        }
    }

    pub fn descending(index: usize) -> Self {
        Self {
            index,
            direct: Direction::Desc,
        }
    }

    pub fn to_protobuf(&self) -> (InputRefExpr, OrderType) {
        let input_ref_expr = InputRefExpr {
            column_idx: self.index as i32,
        };
        let order_type = self.direct.to_protobuf();
        (input_ref_expr, order_type)
    }
}

impl fmt::Display for FieldOrder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "${} {}", self.index, self.direct)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Direction {
    Asc,
    Desc,
    Any, // only used in order requirement
}

impl fmt::Display for Direction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Direction::Asc => "ASC",
            Direction::Desc => "DESC",
            Direction::Any => "ANY",
        };
        f.write_str(s)
    }
}

impl Direction {
    pub fn to_protobuf(&self) -> OrderType {
        match self {
            Self::Asc => OrderType::Ascending,
            Self::Desc => OrderType::Descending,
            _ => unimplemented!(),
        }
    }
}

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
        BatchSort::new(plan, self.clone()).into()
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
    /// the order property of the PlanNode's output
    fn order(&self) -> &Order;
}

macro_rules! impl_with_order_base {
    ([], $( { $convention:ident, $name:ident }),*) => {
        $(paste! {
            impl WithOrder for [<$convention $name>] {
                fn order(&self) -> &Order {
                    &self.base.order
                }
            }
        })*
    }
}
for_batch_plan_nodes! { impl_with_order_base }

macro_rules! impl_with_order_any {
    ([], $( { $convention:ident, $name:ident }),*) => {
        $(paste! {
            impl WithOrder for [<$convention $name>] {
                fn order(&self) -> &Order {
                    Order::any()
                }
            }
        })*
    }
}
for_logical_plan_nodes! { impl_with_order_any }
for_stream_plan_nodes! { impl_with_order_any }

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

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

use std::fmt;

use itertools::Itertools;
use risingwave_common::catalog::{FieldDisplay, Schema};
use risingwave_common::error::Result;
use risingwave_common::util::sort_util::{ColumnOrder, Direction, OrderType};
use risingwave_pb::common::{PbColumnOrder, PbOrderType};

use super::super::plan_node::*;
use crate::optimizer::PlanRef;

#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct Order {
    pub field_order: Vec<FieldOrder>,
}

impl Order {
    pub const fn new(field_order: Vec<FieldOrder>) -> Self {
        Self { field_order }
    }

    pub fn to_protobuf(&self) -> Vec<PbColumnOrder> {
        self.field_order
            .iter()
            .map(FieldOrder::to_protobuf)
            .collect_vec()
    }

    pub fn len(&self) -> usize {
        self.field_order.len()
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

pub struct OrderDisplay<'a> {
    pub order: &'a Order,
    pub input_schema: &'a Schema,
}

impl OrderDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let that = self.order;
        f.write_str("[")?;
        for (i, field_order) in that.field_order.iter().enumerate() {
            if i > 0 {
                f.write_str(", ")?;
            }
            FieldOrderDisplay {
                field_order,
                input_schema: self.input_schema,
            }
            .fmt(f)?;
        }
        f.write_str("]")
    }
}

impl fmt::Display for OrderDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.fmt(f)
    }
}

impl fmt::Debug for OrderDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.fmt(f)
    }
}

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct FieldOrder {
    pub index: usize,
    pub direct: Direction,
}

impl std::fmt::Debug for FieldOrder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "${} {}", self.index, self.direct)
    }
}

pub struct FieldOrderDisplay<'a> {
    pub field_order: &'a FieldOrder,
    pub input_schema: &'a Schema,
}

impl FieldOrderDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let that = self.field_order;
        write!(
            f,
            "{} {}",
            FieldDisplay(self.input_schema.fields.get(that.index).unwrap()),
            that.direct
        )
    }
}

impl fmt::Debug for FieldOrderDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.fmt(f)
    }
}

impl FieldOrder {
    pub fn new(index: usize, order_type: OrderType) -> Self {
        Self {
            index,
            direct: order_type.direction(),
        }
    }

    pub fn ascending(index: usize) -> Self {
        Self {
            index,
            direct: Direction::Ascending,
        }
    }

    pub fn descending(index: usize) -> Self {
        Self {
            index,
            direct: Direction::Descending,
        }
    }

    pub fn to_protobuf(&self) -> PbColumnOrder {
        PbColumnOrder {
            column_index: self.index as _,
            order_type: Some(PbOrderType {
                direction: self.direct.to_protobuf() as _,
            }),
        }
    }

    pub fn from_protobuf(column_order: &PbColumnOrder) -> Self {
        Self {
            index: column_order.column_index as _,
            direct: Direction::from_protobuf(&column_order.get_order_type().unwrap().direction()),
        }
    }

    // TODO(rc): unify them
    pub fn to_column_order(&self) -> ColumnOrder {
        ColumnOrder {
            column_idx: self.index,
            order_type: OrderType::new(self.direct),
        }
    }
}

impl fmt::Display for FieldOrder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "${} {}", self.index, self.direct)
    }
}

const ANY_ORDER: Order = Order {
    field_order: vec![],
};

impl Order {
    pub fn enforce_if_not_satisfies(&self, plan: PlanRef) -> Result<PlanRef> {
        if !plan.order().satisfies(self) {
            Ok(self.enforce(plan))
        } else {
            Ok(plan)
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
        #[expect(clippy::disallowed_methods)]
        for (order, other_order) in self.field_order.iter().zip(other.field_order.iter()) {
            if order != other_order {
                return false;
            }
        }
        true
    }

    #[inline(always)]
    pub const fn any() -> Self {
        ANY_ORDER
    }

    #[inline(always)]
    pub fn is_any(&self) -> bool {
        self.field_order.is_empty()
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
                    direct: Direction::Ascending,
                },
                FieldOrder {
                    index: 1,
                    direct: Direction::Descending,
                },
                FieldOrder {
                    index: 2,
                    direct: Direction::Ascending,
                },
            ],
        };
        let o2 = Order {
            field_order: vec![
                FieldOrder {
                    index: 0,
                    direct: Direction::Ascending,
                },
                FieldOrder {
                    index: 1,
                    direct: Direction::Descending,
                },
            ],
        };
        let o3 = Order {
            field_order: vec![
                FieldOrder {
                    index: 0,
                    direct: Direction::Ascending,
                },
                FieldOrder {
                    index: 1,
                    direct: Direction::Ascending,
                },
            ],
        };

        assert!(o1.satisfies(&o2));
        assert!(!o2.satisfies(&o1));
        assert!(o1.satisfies(&o1));

        assert!(!o2.satisfies(&o3));
        assert!(!o3.satisfies(&o2));
    }
}

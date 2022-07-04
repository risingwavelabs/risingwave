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

use std::fmt;

use itertools::Itertools;
use risingwave_common::catalog::Schema;
use risingwave_common::error::Result;
use risingwave_common::util::sort_util::OrderType;
use risingwave_pb::plan_common::{ColumnOrder, OrderType as ProstOrderType};

use super::super::plan_node::*;
use crate::optimizer::PlanRef;

#[derive(Debug, Clone, Default)]
pub struct Order {
    pub field_order: Vec<FieldOrder>,
}

impl Order {
    pub const fn new(field_order: Vec<FieldOrder>) -> Self {
        Self { field_order }
    }

    /// Convert into protobuf.
    pub fn to_protobuf(&self, _schema: &Schema) -> Vec<ColumnOrder> {
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

    pub fn to_protobuf(&self) -> ColumnOrder {
        ColumnOrder {
            order_type: self.direct.to_protobuf() as i32,
            index: self.index as u32,
        }
    }
}

impl fmt::Display for FieldOrder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "${} {}", self.index, self.direct)
    }
}

#[derive(Debug, Clone, PartialEq, Copy)]
pub enum Direction {
    Asc,
    Desc,
    Any, // only used in order requirement
}

impl From<Direction> for OrderType {
    fn from(dir: Direction) -> Self {
        match dir {
            Direction::Asc => OrderType::Ascending,
            Direction::Desc => OrderType::Descending,
            Direction::Any => OrderType::Ascending,
        }
    }
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
    pub fn to_protobuf(&self) -> ProstOrderType {
        match self {
            Self::Asc => ProstOrderType::Ascending,
            Self::Desc => ProstOrderType::Descending,
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
            if order.index != other_order.index || !order.direct.satisfies(&other_order.direct) {
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

// Copyright 2025 RisingWave Labs
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
use pretty_xmlish::Pretty;
use risingwave_common::catalog::Schema;
use risingwave_common::util::sort_util::{ColumnOrder, ColumnOrderDisplay};
use risingwave_pb::common::PbColumnOrder;

use super::super::plan_node::*;
use crate::error::Result;

// TODO(rc): use this type to replace all `Vec<ColumnOrder>`
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct Order {
    pub column_orders: Vec<ColumnOrder>,
}

impl Order {
    pub const fn new(column_orders: Vec<ColumnOrder>) -> Self {
        Self { column_orders }
    }

    pub fn to_protobuf(&self) -> Vec<PbColumnOrder> {
        self.column_orders
            .iter()
            .map(ColumnOrder::to_protobuf)
            .collect_vec()
    }

    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        self.column_orders.len()
    }

    pub fn concat(self, other: Self) -> Self {
        Self {
            column_orders: self
                .column_orders
                .into_iter()
                .chain(other.column_orders)
                .collect(),
        }
    }
}

impl fmt::Display for Order {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[")?;
        for (i, column_order) in self.column_orders.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{}", column_order)?;
        }
        write!(f, "]")
    }
}

pub struct OrderDisplay<'a> {
    pub order: &'a Order,
    pub input_schema: &'a Schema,
}

impl OrderDisplay<'_> {
    pub fn distill<'a>(self) -> Pretty<'a> {
        let iter = self.order.column_orders.iter();
        let vec = iter.map(|column_order| {
            Pretty::display(&ColumnOrderDisplay {
                column_order,
                input_schema: self.input_schema,
            })
        });
        Pretty::Array(vec.collect())
    }
}

const ANY_ORDER: Order = Order {
    column_orders: vec![],
};

impl Order {
    pub fn enforce_if_not_satisfies(&self, plan: PlanRef) -> Result<PlanRef> {
        use crate::optimizer::plan_node::batch::prelude::*;

        if !plan.order().satisfies(self) {
            Ok(self.enforce(plan))
        } else {
            Ok(plan)
        }
    }

    fn enforce(&self, plan: PlanRef) -> PlanRef {
        assert_eq!(plan.convention(), Convention::Batch);
        BatchSort::new(plan, self.clone()).into()
    }

    pub fn satisfies(&self, other: &Order) -> bool {
        if self.column_orders.len() < other.column_orders.len() {
            return false;
        }
        #[expect(clippy::disallowed_methods)]
        for (order, other_order) in self.column_orders.iter().zip(other.column_orders.iter()) {
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
        self.column_orders.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::util::sort_util::{ColumnOrder, OrderType};

    use super::Order;

    #[test]
    fn test_order_satisfy() {
        let o1 = Order {
            column_orders: vec![
                ColumnOrder {
                    column_index: 0,
                    order_type: OrderType::ascending(),
                },
                ColumnOrder {
                    column_index: 1,
                    order_type: OrderType::descending(),
                },
                ColumnOrder {
                    column_index: 2,
                    order_type: OrderType::ascending(),
                },
            ],
        };
        let o2 = Order {
            column_orders: vec![
                ColumnOrder {
                    column_index: 0,
                    order_type: OrderType::ascending(),
                },
                ColumnOrder {
                    column_index: 1,
                    order_type: OrderType::descending(),
                },
            ],
        };
        let o3 = Order {
            column_orders: vec![
                ColumnOrder {
                    column_index: 0,
                    order_type: OrderType::ascending(),
                },
                ColumnOrder {
                    column_index: 1,
                    order_type: OrderType::ascending(),
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

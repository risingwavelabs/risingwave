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

use risingwave_common::util::sort_util::{Direction, NullsAre, OrderType};
use risingwave_sqlparser::ast::OrderByExpr;

pub(crate) fn derive_order_type_from_order_by_expr(order_by_expr: &OrderByExpr) -> OrderType {
    let direction = match order_by_expr.asc {
        None => Direction::default(),
        Some(true) => Direction::Ascending,
        Some(false) => Direction::Descending,
    };
    match order_by_expr.nulls_first {
        None => OrderType::new(direction, NullsAre::default()),
        Some(true) => OrderType::nulls_first(direction),
        Some(false) => OrderType::nulls_last(direction),
    }
}

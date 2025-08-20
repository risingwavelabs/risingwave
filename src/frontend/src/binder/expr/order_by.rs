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

use risingwave_common::util::sort_util::OrderType;
use risingwave_sqlparser::ast::OrderByExpr;

use crate::Binder;
use crate::error::Result;
use crate::expr::OrderByExpr as BoundOrderByExpr;

impl Binder {
    /// Bind an `ORDER BY` expression in other places than
    /// [`Query`](risingwave_sqlparser::ast::Query),
    /// including in `OVER` and in aggregate functions.
    ///
    /// Different from a query-level `ORDER BY` ([`Self::bind_order_by_expr_in_query`]),
    /// output-column names or numbers are not allowed here.
    pub(super) fn bind_order_by_expr(
        &mut self,
        OrderByExpr {
            expr,
            asc,
            nulls_first,
        }: &OrderByExpr,
    ) -> Result<BoundOrderByExpr> {
        let order_type = OrderType::from_bools(*asc, *nulls_first);
        let expr = self.bind_expr_inner(expr)?;
        Ok(BoundOrderByExpr { expr, order_type })
    }
}

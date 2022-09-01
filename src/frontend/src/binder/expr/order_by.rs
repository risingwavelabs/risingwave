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

use risingwave_common::error::Result;
use risingwave_sqlparser::ast::OrderByExpr;

use crate::expr::OrderByExpr as BoundOrderByExpr;
use crate::optimizer::property::Direction;
use crate::Binder;

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
        }: OrderByExpr,
    ) -> Result<BoundOrderByExpr> {
        let direction = match asc {
            None | Some(true) => Direction::Asc,
            Some(false) => Direction::Desc,
        };

        let nulls_first = nulls_first.unwrap_or_else(|| match direction {
            Direction::Asc => false,
            Direction::Desc => true,
            Direction::Any => unreachable!(),
        });

        let expr = self.bind_expr(expr)?;

        Ok(BoundOrderByExpr {
            expr,
            direction,
            nulls_first,
        })
    }
}

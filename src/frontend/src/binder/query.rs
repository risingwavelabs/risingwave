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

use std::collections::HashMap;

use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::types::DataType;
use risingwave_sqlparser::ast::{Expr, OrderByExpr, Query};

use crate::binder::{Binder, BoundSetExpr};
use crate::optimizer::property::{Direction, FieldOrder};

/// A validated sql query, including order and union.
/// An example of its relationship with `BoundSetExpr` and `BoundSelect` can be found here: <https://bit.ly/3GQwgPz>
#[derive(Debug)]
pub struct BoundQuery {
    pub body: BoundSetExpr,
    pub order: Vec<FieldOrder>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

impl BoundQuery {
    /// The names returned by this [`BoundQuery`].
    pub fn names(&self) -> Vec<String> {
        self.body.names()
    }

    /// The types returned by this [`BoundQuery`].
    pub fn data_types(&self) -> Vec<DataType> {
        self.body.data_types()
    }

    pub fn is_correlated(&self) -> bool {
        self.body.is_correlated()
    }
}

impl Binder {
    /// Bind a [`Query`].
    ///
    /// Before binding the [`Query`], we push the current [`BindContext`](super::BindContext) to the
    /// stack and create a new context, because it may be a subquery.
    ///
    /// After finishing binding, we pop the previous context from the stack.
    pub fn bind_query(&mut self, query: Query) -> Result<BoundQuery> {
        self.push_context();
        let result = self.bind_query_inner(query);
        self.pop_context();
        result
    }

    /// Bind a [`Query`] using the current [`BindContext`](super::BindContext).
    pub(super) fn bind_query_inner(&mut self, query: Query) -> Result<BoundQuery> {
        let limit = query.get_limit_value();
        let offset = query.get_offset_value();
        let body = self.bind_set_expr(query.body)?;
        let mut name_to_index = HashMap::new();
        match &body {
            BoundSetExpr::Select(s) => s.aliases.iter().enumerate().for_each(|(index, alias)| {
                if let Some(name) = alias {
                    name_to_index.insert(name.clone(), index);
                }
            }),
            BoundSetExpr::Values(_) => {}
        };
        let order = query
            .order_by
            .into_iter()
            .map(|order_by_expr| self.bind_order_by_expr(order_by_expr, &name_to_index))
            .collect::<Result<_>>()?;
        Ok(BoundQuery {
            body,
            order,
            limit,
            offset,
        })
    }

    fn bind_order_by_expr(
        &mut self,
        order_by_expr: OrderByExpr,
        name_to_index: &HashMap<String, usize>,
    ) -> Result<FieldOrder> {
        let direct = match order_by_expr.asc {
            None | Some(true) => Direction::Asc,
            Some(false) => Direction::Desc,
        };
        let name = match order_by_expr.expr {
            Expr::Identifier(name) => name.value,
            expr => {
                return Err(
                    ErrorCode::NotImplemented(format!("ORDER BY {:?}", expr), 1635.into()).into(),
                )
            }
        };
        let index = *name_to_index
            .get(&name)
            .ok_or_else(|| ErrorCode::ItemNotFound(format!("output column \"{}\"", name)))?;
        Ok(FieldOrder { index, direct })
    }
}

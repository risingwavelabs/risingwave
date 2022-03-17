use std::collections::HashMap;

use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::types::DataType;
use risingwave_sqlparser::ast::{Expr, OrderByExpr, Query};

use crate::binder::{Binder, BoundSetExpr};
use crate::optimizer::property::{Direction, FieldOrder};

/// A validated sql query, including order and union.
/// An example of its relationship with BoundSetExpr and BoundSelect can be found here: https://bit.ly/3GQwgPz
#[derive(Debug, Clone)]
pub struct BoundQuery {
    pub body: BoundSetExpr,
    pub order: Vec<FieldOrder>,
}

impl BoundQuery {
    pub fn names(&self) -> Vec<String> {
        self.body.names()
    }

    pub fn data_types(&self) -> Vec<DataType> {
        self.body.data_types()
    }
}

impl Binder {
    /// Bind a [`Query`].
    pub fn bind_query(&mut self, query: Query) -> Result<BoundQuery> {
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
        Ok(BoundQuery { body, order })
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
                return Err(ErrorCode::NotImplementedError(format!("ORDER BY {:?}", expr)).into())
            }
        };
        let index = *name_to_index
            .get(&name)
            .ok_or_else(|| ErrorCode::ItemNotFound(format!("output column \"{}\"", name)))?;
        Ok(FieldOrder { index, direct })
    }
}

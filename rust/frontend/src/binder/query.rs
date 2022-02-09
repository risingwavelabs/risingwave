use risingwave_common::error::Result;
use risingwave_sqlparser::ast::Query;

use crate::binder::{Binder, BoundSetExpr};

/// A validated sql query, including order and union.
/// An example of its relationship with BoundSetExpr and BoundSelect can be found here: https://bit.ly/3GQwgPz
#[derive(Debug)]
pub struct BoundQuery {
    pub body: BoundSetExpr,
}

impl Binder<'_> {
    pub(super) async fn bind_query(&mut self, query: Query) -> Result<BoundQuery> {
        Ok(BoundQuery {
            body: self.bind_set_expr(query.body).await?,
        })
    }
}

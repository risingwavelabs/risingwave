use risingwave_common::error::{ErrorCode, Result};
use risingwave_sqlparser::ast::Query;

use crate::binder::Binder;
use crate::expr::{ExprImpl, Subquery, SubqueryKind};

impl Binder {
    pub(super) fn bind_subquery_expr(
        &mut self,
        query: Query,
        kind: SubqueryKind,
    ) -> Result<ExprImpl> {
        let r = self.bind_query(query);
        if let Ok(query) = r {
            // uncorrelated subquery
            if kind == SubqueryKind::Scalar && query.data_types().len() != 1 {
                return Err(ErrorCode::BindError(
                    "subquery must return only one column".to_string(),
                )
                .into());
            }
            return Ok(Subquery::new(query, kind).into());
        }

        Err(ErrorCode::NotImplemented("correlated subquery".to_string(), 1343.into()).into())
    }
}

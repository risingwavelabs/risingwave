use itertools::Itertools as _;
use risingwave_common::error::Result;
use risingwave_sqlparser::ast::{Query, TableAlias};

use crate::binder::{Binder, BoundQuery, UNNAMED_SUBQUERY};

#[derive(Debug)]
pub struct BoundSubquery {
    pub query: BoundQuery,
}

impl Binder {
    /// Binds a subquery using [`bind_query`](Self::bind_query), which will use a new empty
    /// [`BindContext`](super::BindContext) for it.
    ///
    /// After finishing binding, we update the current context with the output of the subquery.
    pub(super) fn bind_subquery_relation(
        &mut self,
        query: Query,
        alias: Option<TableAlias>,
    ) -> Result<BoundSubquery> {
        let query = self.bind_query(query)?;
        let sub_query_id = self.next_subquery_id();
        self.bind_context(
            query
                .names()
                .into_iter()
                .zip_eq(query.data_types().into_iter())
                .map(|(x, y)| (x, y, false)),
            format!("{}_{}", UNNAMED_SUBQUERY, sub_query_id),
            alias,
        )?;
        Ok(BoundSubquery { query })
    }
}

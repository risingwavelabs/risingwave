use risingwave_common::error::Result;
use risingwave_sqlparser::ast::{Ident, ObjectName, Query};

use crate::binder::{BaseTableRef, Binder, BoundQuery};

#[derive(Debug, Clone)]
pub struct BoundInsert {
    pub table: BaseTableRef,
    pub source: BoundQuery,
}

impl Binder {
    pub(super) fn bind_insert(
        &mut self,
        table_name: ObjectName,
        _columns: Vec<Ident>,
        source: Query,
    ) -> Result<BoundInsert> {
        Ok(BoundInsert {
            table: self.bind_table(table_name)?,
            source: self.bind_query(source)?,
        })
    }
}

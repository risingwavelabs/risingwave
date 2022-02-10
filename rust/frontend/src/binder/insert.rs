use risingwave_common::error::Result;
use risingwave_sqlparser::ast::{Ident, ObjectName, Query};

use crate::binder::{BaseTableRef, Binder, BoundQuery};

#[derive(Debug)]
pub struct BoundInsert {
    pub table: Box<BaseTableRef>,
    pub source: Box<BoundQuery>,
}

impl Binder {
    pub(super) fn bind_insert(
        &mut self,
        table_name: ObjectName,
        _columns: Vec<Ident>,
        source: Query,
    ) -> Result<BoundInsert> {
        Ok(BoundInsert {
            table: Box::new(self.bind_table(table_name)?),
            source: Box::new(self.bind_query(source)?),
        })
    }
}

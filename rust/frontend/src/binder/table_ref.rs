use risingwave_common::error::{ErrorCode, Result};
use risingwave_sqlparser::ast::{ObjectName, TableFactor, TableWithJoins};

use crate::binder::Binder;
use crate::catalog::catalog_service::DEFAULT_SCHEMA_NAME;
use crate::catalog::column_catalog::ColumnCatalog;
use crate::catalog::TableId;

#[derive(Debug)]
pub enum TableRef {
    BaseTable(Box<BaseTableRef>),
}
#[derive(Debug)]
pub struct BaseTableRef {
    pub table_id: TableId,
    pub columns: Vec<ColumnCatalog>,
}

impl Binder {
    pub(super) fn bind_vec_table_with_joins(
        &mut self,
        from: Vec<TableWithJoins>,
    ) -> Result<Option<TableRef>> {
        // Joins are not supported yet.
        let first = match from.into_iter().next() {
            Some(t) => t,
            None => return Ok(None),
        };
        self.bind_table_factor(first.relation).map(Some)
    }
    pub(super) fn bind_table_factor(&mut self, table_factor: TableFactor) -> Result<TableRef> {
        match table_factor {
            TableFactor::Table { name, .. } => {
                Ok(TableRef::BaseTable(Box::new(self.bind_table(name)?)))
            }
            _ => Err(ErrorCode::NotImplementedError(format!("{:?}", table_factor)).into()),
        }
    }
    pub(super) fn bind_table(&mut self, name: ObjectName) -> Result<BaseTableRef> {
        let mut identifiers = name.0;
        let table_name = identifiers
            .pop()
            .ok_or_else(|| ErrorCode::InternalError("empty table name".into()))?
            .value;
        let schema_name = identifiers
            .pop()
            .map(|ident| ident.value)
            .unwrap_or_else(|| DEFAULT_SCHEMA_NAME.into());

        let table_catalog = self
            .catalog
            .get_schema(&schema_name)
            .and_then(|c| c.get_table(&table_name))
            .ok_or_else(|| ErrorCode::ItemNotFound(format!("relation \"{}\"", table_name)))?;
        Ok(BaseTableRef {
            table_id: table_catalog.id(),
            columns: table_catalog.columns().into(),
        })
    }
}

use risingwave_common::catalog::ColumnDesc;
use risingwave_common::error::{ErrorCode, Result, RwError};
use risingwave_sqlparser::ast::{ObjectName, TableAlias};

use crate::binder::{Binder, Relation};
use crate::catalog::source_catalog::SourceCatalog;
use crate::catalog::table_catalog::TableCatalog;
use crate::catalog::{CatalogError, TableId};

#[derive(Debug)]
pub struct BoundBaseTable {
    pub name: String, // explain-only
    pub table_id: TableId,
    pub table_catalog: TableCatalog,
}

impl From<&TableCatalog> for BoundBaseTable {
    fn from(t: &TableCatalog) -> Self {
        Self {
            name: t.name.clone(),
            table_id: t.id,
            table_catalog: t.clone(),
        }
    }
}

/// `BoundTableSource` is used by DML statement on table source like insert, updata
#[derive(Debug)]
pub struct BoundTableSource {
    pub name: String,       // explain-only
    pub source_id: TableId, // TODO: refactor to source id
    pub columns: Vec<ColumnDesc>,
}

#[derive(Debug)]
pub struct BoundSource {
    pub catalog: SourceCatalog,
}

impl From<&SourceCatalog> for BoundSource {
    fn from(s: &SourceCatalog) -> Self {
        Self { catalog: s.clone() }
    }
}

impl Binder {
    pub(super) fn bind_table_or_source(
        &mut self,
        schema_name: &str,
        table_name: &str,
        alias: Option<TableAlias>,
    ) -> Result<Relation> {
        if schema_name == "pg_catalog" {
            // TODO: support pg_catalog.
            return Err(ErrorCode::NotImplemented(
                // TODO: We can ref the document of `SHOW` commands here if ready.
                r###"pg_catalog is not supported, please use `SHOW` commands for now.
`SHOW TABLES`,
`SHOW MATERIALIZED VIEWS`,
`DESCRIBE <table>`,
`SHOW COLUMNS FROM [table]`
"###
                .into(),
                1695.into(),
            )
            .into());
        }

        let (ret, columns) = {
            let catalog = &self.catalog;

            catalog
                .get_table_by_name(&self.db_name, schema_name, table_name)
                .map(|t| (Relation::BaseTable(Box::new(t.into())), t.columns.clone()))
                .or_else(|_| {
                    catalog
                        .get_source_by_name(&self.db_name, schema_name, table_name)
                        .map(|s| {
                            let source = s.clone().flatten();
                            (Relation::Source(Box::new((&source).into())), source.columns)
                        })
                })
                .map_err(|_| {
                    RwError::from(CatalogError::NotFound(
                        "table or source",
                        table_name.to_string(),
                    ))
                })?
        };

        self.bind_context(
            columns
                .iter()
                .cloned()
                .map(|c| (c.name().to_string(), c.data_type().clone(), c.is_hidden)),
            table_name.to_string(),
            alias,
        )?;
        Ok(ret)
    }

    pub(crate) fn bind_table(
        &mut self,
        schema_name: &str,
        table_name: &str,
        alias: Option<TableAlias>,
    ) -> Result<BoundBaseTable> {
        let table_catalog = self
            .catalog
            .get_table_by_name(&self.db_name, schema_name, table_name)?
            .clone();
        let columns = table_catalog.columns.clone();

        self.bind_context(
            columns
                .iter()
                .cloned()
                .map(|c| (c.name().to_string(), c.data_type().clone(), c.is_hidden)),
            table_name.to_string(),
            alias,
        )?;

        let table_id = table_catalog.id();
        Ok(BoundBaseTable {
            name: table_name.to_string(),
            table_id,
            table_catalog,
        })
    }

    pub(crate) fn bind_table_source(&mut self, name: ObjectName) -> Result<BoundTableSource> {
        let (schema_name, source_name) = Self::resolve_table_name(name)?;
        let source = self
            .catalog
            .get_source_by_name(&self.db_name, &schema_name, &source_name)?;

        let source_id = TableId::new(source.id);

        let columns = source
            .columns
            .iter()
            .filter(|c| !c.is_hidden)
            .map(|c| c.column_desc.clone())
            .collect();

        // Note(bugen): do not bind context here.

        Ok(BoundTableSource {
            name: source_name,
            source_id,
            columns,
        })
    }
}

use risingwave_common::catalog::ColumnDesc;

use crate::catalog::source_catalog::SourceCatalog;
use crate::catalog::table_catalog::TableCatalog;
use crate::catalog::TableId;

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

use risingwave_common::catalog::ColumnCatalog;

#[derive(Debug)]
pub struct SchemaChangeEnvelope {
    pub table_changes: Vec<TableSchemaChange>,
}

#[derive(Debug)]
pub struct TableSchemaChange {
    pub(crate) up_table_full_name: String,
    pub(crate) columns: Vec<ColumnCatalog>,
}

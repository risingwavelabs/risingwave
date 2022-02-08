use risingwave_common::array::RwError;
use risingwave_common::catalog::Schema;
use risingwave_common::error::Result;
use risingwave_pb::meta::Table;
use risingwave_pb::plan::ColumnDesc;

use crate::catalog::TableId;

pub struct TableCatalog {
    table_id: TableId,
    columns_schema: Schema,
}

impl TableCatalog {
    pub fn new(table_id: TableId, columns_schema: &[ColumnDesc]) -> Result<Self> {
        let columns_schema = Schema::try_from(columns_schema)?;
        Ok(Self {
            table_id,
            columns_schema,
        })
    }

    pub fn columns_schema(&self) -> &Schema {
        &self.columns_schema
    }

    pub fn id(&self) -> TableId {
        self.table_id
    }
}

impl TryFrom<&Table> for TableCatalog {
    type Error = RwError;

    fn try_from(tb: &Table) -> Result<Self> {
        let table_catalog = Self::new(tb.get_table_ref_id()?.table_id as u32, &tb.column_descs)?;
        Ok(table_catalog)
    }
}

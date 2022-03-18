use risingwave_common::catalog::{ColumnDesc, ColumnId};
use risingwave_common::types::DataType;
use risingwave_pb::plan::ColumnCatalog as ProstColumnCatalog;

#[derive(Debug, Clone)]
pub struct ColumnCatalog {
    pub column_desc: ColumnDesc,
    pub is_hidden: bool,
}

impl ColumnCatalog {
    /// Get the column catalog's is hidden.
    pub fn is_hidden(&self) -> bool {
        self.is_hidden
    }

    /// Get a reference to the column desc's data type.
    pub fn data_type(&self) -> &DataType {
        &self.column_desc.data_type
    }

    /// Get the column desc's column id.
    pub fn column_id(&self) -> ColumnId {
        self.column_desc.column_id
    }

    /// Get a reference to the column desc's name.
    pub fn name(&self) -> &str {
        self.column_desc.name.as_ref()
    }
}

impl From<ProstColumnCatalog> for ColumnCatalog {
    fn from(prost: ProstColumnCatalog) -> Self {
        Self {
            column_desc: prost.column_desc.unwrap().into(),
            is_hidden: prost.is_hidden,
        }
    }
}

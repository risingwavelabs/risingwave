use risingwave_common::types::DataType;
use risingwave_pb::plan::ColumnDesc as ProstColumnDesc;

use crate::catalog::ColumnId;

/// A descriptor of a column.
#[derive(Debug, Clone)]
pub struct ColumnDesc {
    data_type: DataType,
}

impl ColumnDesc {
    pub fn new(data_type: DataType) -> Self {
        ColumnDesc { data_type }
    }

    pub fn data_type(&self) -> DataType {
        self.data_type.clone()
    }
}

impl From<ProstColumnDesc> for ColumnDesc {
    fn from(col: ProstColumnDesc) -> Self {
        Self {
            data_type: col.get_column_type().expect("column type not found").into(),
        }
    }
}

/// The catalog of a column.
#[derive(Debug, Clone)]
pub struct ColumnCatalog {
    id: ColumnId,
    name: String,
    desc: ColumnDesc,
}

impl ColumnCatalog {
    pub fn new(id: ColumnId, name: String, desc: ColumnDesc) -> ColumnCatalog {
        ColumnCatalog { id, name, desc }
    }

    pub fn id(&self) -> ColumnId {
        self.id
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn data_type(&self) -> DataType {
        self.desc.data_type.clone()
    }

    pub fn col_desc_ref(&self) -> &ColumnDesc {
        &self.desc
    }
}

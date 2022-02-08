use risingwave_common::types::DataType;
use risingwave_pb::plan::ColumnDesc as ProstColumnDesc;

use crate::catalog::ColumnId;

/// A descriptor of a column.
#[derive(Debug, Clone)]
pub struct ColumnDesc {
    data_type: DataType,
    is_primary: bool,
}

impl ColumnDesc {
    pub fn new(data_type: DataType, is_primary: bool) -> Self {
        ColumnDesc {
            data_type,
            is_primary,
        }
    }

    pub fn is_primary(&self) -> bool {
        self.is_primary
    }

    pub fn data_type(&self) -> DataType {
        self.data_type
    }
}

impl From<ProstColumnDesc> for ColumnDesc {
    fn from(col: ProstColumnDesc) -> Self {
        Self {
            data_type: col.get_column_type().expect("column type not found").into(),
            is_primary: col.is_primary,
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
        self.desc.data_type
    }

    pub fn is_primary(&self) -> bool {
        self.desc.is_primary()
    }

    pub fn col_desc_ref(&self) -> &ColumnDesc {
        &self.desc
    }
}

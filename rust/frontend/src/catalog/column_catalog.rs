use risingwave_common::types::DataType;
use risingwave_pb::plan::ColumnDesc as ProstColumnDesc;

use crate::catalog::ColumnId;

/// A descriptor of a column.
#[derive(Debug, Clone)]
pub struct ColumnDesc {
    pub data_type: DataType,
    // The user-defined type's name. Empty if the column type is a builtin type.
    pub type_name: String,
}

impl ColumnDesc {
    pub fn new(data_type: DataType) -> Self {
        ColumnDesc {
            data_type,
            type_name: "".to_string(),
        }
    }

    pub fn data_type(&self) -> DataType {
        self.data_type.clone()
    }
}

impl From<ProstColumnDesc> for ColumnDesc {
    fn from(col: ProstColumnDesc) -> Self {
        ColumnDesc {
            data_type: col.get_column_type().expect("column type not found").into(),
            type_name: col.get_type_name().to_string(),
        }
    }
}

/// The catalog of a column.
#[derive(Debug, Clone)]
pub struct ColumnCatalog {
    id: ColumnId,
    name: String,
    desc: ColumnDesc,
    // For STRUCT type.
    pub fields: Vec<ColumnCatalog>,
}

impl ColumnCatalog {
    pub fn new(
        id: ColumnId,
        name: String,
        desc: ColumnDesc,
        fields: Vec<ColumnCatalog>,
    ) -> ColumnCatalog {
        ColumnCatalog {
            id,
            name,
            desc,
            fields,
        }
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

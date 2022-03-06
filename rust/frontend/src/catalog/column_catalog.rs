use itertools::Itertools;
use risingwave_common::types::DataType;
use risingwave_pb::data::data_type::TypeName;
use risingwave_pb::plan::ColumnDesc as ProstColumnDesc;

use crate::catalog::ColumnId;

/// A descriptor of a column.
#[derive(Debug, Clone)]
pub struct ColumnDesc {
    pub data_type: DataType,
    pub type_name: Option<String>,
    pub sub_name: String,
    pub sub_type_name: Vec<ColumnDesc>,
}

impl ColumnDesc {
    pub fn new(data_type: DataType) -> Self {
        ColumnDesc {
            data_type,
            type_name: Some("".to_string()),
            sub_name: "".to_string(),
            sub_type_name: vec![],
        }
    }

    pub fn data_type(&self) -> DataType {
        self.data_type.clone()
    }

    pub fn type_name(&self) -> String {
        self.type_name.as_ref().unwrap().to_string()
    }

    pub fn sub_type_name(&self) -> Vec<ColumnDesc> {
        self.sub_type_name.clone()
    }
}

pub fn to_column_desc(col: &ProstColumnDesc) -> ColumnDesc {
    if col.column_type.as_ref().expect("wrong type").type_name == TypeName::Struct as i32 {
        let v = col.column_descs.iter().map(to_column_desc).collect_vec();
        ColumnDesc {
            data_type: col.get_column_type().expect("column type not found").into(),
            type_name: Some(col.get_struct_name().to_string()),
            sub_name: col.name.clone(),
            sub_type_name: v,
        }
    } else {
        ColumnDesc {
            data_type: col.get_column_type().expect("column type not found").into(),
            type_name: Some(col.get_struct_name().to_string()),
            sub_name: col.name.clone(),
            sub_type_name: vec![],
        }
    }
}

impl From<ProstColumnDesc> for ColumnDesc {
    fn from(col: ProstColumnDesc) -> Self {
        to_column_desc(&col)
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

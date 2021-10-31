use risingwave_pb::plan::ColumnDesc;

use crate::error::{Result, RwError};
use crate::types::{build_from_prost, DataTypeRef};

/// the field in the schema of the executor's return data
#[derive(Clone)]
pub struct Field {
    // TODO: field_name
    pub data_type: DataTypeRef,
}

/// the schema of the executor's return data
#[derive(Clone)]
pub struct Schema {
    pub fields: Vec<Field>,
}

impl TryFrom<Vec<ColumnDesc>> for Schema {
    type Error = RwError;

    fn try_from(cols: Vec<ColumnDesc>) -> Result<Self> {
        Ok(Self {
            fields: cols
                .iter()
                .map(|col| {
                    Ok(Field {
                        data_type: build_from_prost(col.get_column_type())?,
                    })
                })
                .collect::<Result<Vec<_>>>()?,
        })
    }
}

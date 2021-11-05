use risingwave_pb::plan::ColumnDesc;

use crate::error::Result;
use crate::types::{build_from_prost, build_from_proto, DataTypeRef};
use risingwave_proto::data::DataType as DataTypeProto;

/// The field in the schema of the executor's return data
#[derive(Clone, Debug)]
pub struct Field {
    // TODO: field_name
    pub data_type: DataTypeRef,
}

/// the schema of the executor's return data
#[derive(Clone, Debug, Default)]
pub struct Schema {
    pub fields: Vec<Field>,
}

impl Schema {
    pub fn new(fields: Vec<Field>) -> Self {
        Self { fields }
    }

    pub fn data_types_clone(&self) -> Vec<DataTypeRef> {
        self.fields
            .iter()
            .map(|field| field.data_type.clone())
            .collect()
    }

    pub fn try_from<'a, I: IntoIterator<Item = &'a ColumnDesc>>(cols: I) -> Result<Self> {
        Ok(Self {
            fields: cols
                .into_iter()
                .map(|col| {
                    Ok(Field {
                        data_type: build_from_prost(col.get_column_type())?,
                    })
                })
                .collect::<Result<Vec<_>>>()?,
        })
    }
}

impl Field {
    pub fn try_from(data_type: &DataTypeProto) -> Result<Self> {
        Ok(Field {
            data_type: build_from_proto(data_type)?,
        })
    }
}

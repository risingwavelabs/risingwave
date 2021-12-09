use risingwave_pb::plan::ColumnDesc;
use std::ops::Index;

use crate::array::ArrayBuilderImpl;
use crate::error::Result;
use crate::types::{build_from_prost, DataType, DataTypeRef};
use risingwave_pb::data::DataType as ProstDataType;

/// The field in the schema of the executor's return data
#[derive(Clone, Debug)]
pub struct Field {
    pub data_type: DataTypeRef,
}

/// the schema of the executor's return data
#[derive(Clone, Debug, Default)]
pub struct Schema {
    pub fields: Vec<Field>,
}

impl Schema {
    pub fn len(&self) -> usize {
        self.fields.len()
    }

    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    pub fn new(fields: Vec<Field>) -> Self {
        Self { fields }
    }

    pub fn data_types_clone(&self) -> Vec<DataTypeRef> {
        self.fields
            .iter()
            .map(|field| field.data_type.clone())
            .collect()
    }

    pub fn fields(&self) -> &[Field] {
        &self.fields
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

    /// Create array builders for all fields in this schema.
    pub fn create_array_builders(&self, capacity: usize) -> Result<Vec<ArrayBuilderImpl>> {
        self.fields
            .iter()
            .map(|field| field.data_type.create_array_builder(capacity))
            .collect()
    }
}

impl Field {
    pub fn new(data_type: DataTypeRef) -> Self {
        Self { data_type }
    }

    pub fn try_from(data_type: &ProstDataType) -> Result<Self> {
        Ok(Field {
            data_type: build_from_prost(data_type)?,
        })
    }

    pub fn data_type(&self) -> DataTypeRef {
        self.data_type.clone()
    }

    pub fn data_type_ref(&self) -> &dyn DataType {
        self.data_type.as_ref()
    }
}

impl Index<usize> for Schema {
    type Output = Field;

    fn index(&self, index: usize) -> &Self::Output {
        &self.fields[index]
    }
}

use std::ops::Index;

use risingwave_pb::plan::Field as ProstField;

use crate::array::ArrayBuilderImpl;
use crate::error::Result;
use crate::types::DataType;

/// The field in the schema of the executor's return data
#[derive(Clone, PartialEq)]
pub struct Field {
    pub data_type: DataType,
    pub name: String,
}

impl std::fmt::Debug for Field {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{:?}", self.name, self.data_type)
    }
}

/// the schema of the executor's return data
#[derive(Clone, Debug, Default, PartialEq)]
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

    pub fn data_types(&self) -> Vec<DataType> {
        self.fields
            .iter()
            .map(|field| field.data_type.clone())
            .collect()
    }

    pub fn fields(&self) -> &[Field] {
        &self.fields
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
    pub fn with_name<S>(data_type: DataType, name: S) -> Self
    where
        S: Into<String>,
    {
        Self {
            data_type,
            name: name.into(),
        }
    }

    pub fn unnamed(data_type: DataType) -> Self {
        Self {
            data_type,
            name: String::new(),
        }
    }

    pub fn from(prost_field: &ProstField) -> Self {
        Self {
            data_type: DataType::from(prost_field.get_data_type().expect("data type not found")),
            name: prost_field.get_name().clone(),
        }
    }

    pub fn data_type(&self) -> DataType {
        self.data_type.clone()
    }
}

impl Index<usize> for Schema {
    type Output = Field;

    fn index(&self, index: usize) -> &Self::Output {
        &self.fields[index]
    }
}

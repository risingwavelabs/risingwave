use std::ops::Index;

use risingwave_pb::plan::{ColumnDesc, Field as ProstField};

use crate::array::ArrayBuilderImpl;
use crate::error::Result;
use crate::types::DataTypeKind;

/// The field in the schema of the executor's return data
#[derive(Clone)]
pub struct Field {
    pub data_type: DataTypeKind,
    pub name: String,
}

impl std::fmt::Debug for Field {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // force it to display in single-line style
        write!(
            f,
            "Field {{ name = {}, data_type = {:?} }}",
            self.name, self.data_type
        )
    }
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

    pub fn data_types(&self) -> Vec<DataTypeKind> {
        self.fields.iter().map(|field| field.data_type).collect()
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
                        data_type: DataTypeKind::from(col.get_column_type()?),
                        name: col.get_name().to_string(),
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
    pub fn with_name(data_type: DataTypeKind, name: String) -> Self {
        Self { data_type, name }
    }

    pub fn unnamed(data_type: DataTypeKind) -> Self {
        Self {
            data_type,
            name: String::new(),
        }
    }

    pub fn from(prost_field: &ProstField) -> Self {
        Self {
            data_type: DataTypeKind::from(
                prost_field.get_data_type().expect("data type not found"),
            ),
            name: prost_field.get_name().clone(),
        }
    }

    pub fn data_type(&self) -> DataTypeKind {
        self.data_type
    }
}

impl Index<usize> for Schema {
    type Output = Field;

    fn index(&self, index: usize) -> &Self::Output {
        &self.fields[index]
    }
}

// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::ops::Index;

use itertools::Itertools;
use risingwave_pb::plan_common::Field as ProstField;

use super::ColumnDesc;
use crate::array::ArrayBuilderImpl;
use crate::types::DataType;

/// The field in the schema of the executor's return data
#[derive(Clone, PartialEq)]
pub struct Field {
    pub data_type: DataType,
    pub name: String,
    /// For STRUCT type.
    pub sub_fields: Vec<Field>,
    /// The user-defined type's name, when the type is created from a protobuf schema file,
    /// this field will store the message name.
    pub type_name: String,
}

impl std::fmt::Debug for Field {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{:?}", self.name, self.data_type)
    }
}

impl Field {
    pub fn to_prost(&self) -> ProstField {
        ProstField {
            data_type: Some(self.data_type.to_protobuf()),
            name: self.name.to_string(),
        }
    }
}

pub struct FieldDisplay<'a>(pub &'a Field);

impl std::fmt::Debug for FieldDisplay<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.name)
    }
}

impl std::fmt::Display for FieldDisplay<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.name)
    }
}

/// `schema_unnamed` builds a `Schema` with the given types, but without names.
#[macro_export]
macro_rules! schema_unnamed {
    ($($t:expr),*) => {{
        $crate::catalog::Schema {
            fields: vec![
                $( $crate::catalog::Field::unnamed($t) ),*
            ],
        }
    }};
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

    pub fn names(&self) -> Vec<String> {
        self.fields().iter().map(|f| f.name.clone()).collect()
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

    pub fn into_fields(self) -> Vec<Field> {
        self.fields
    }

    /// Create array builders for all fields in this schema.
    pub fn create_array_builders(&self, capacity: usize) -> Vec<ArrayBuilderImpl> {
        self.fields
            .iter()
            .map(|field| field.data_type.create_array_builder(capacity))
            .collect()
    }

    pub fn to_prost(&self) -> Vec<ProstField> {
        self.fields
            .clone()
            .into_iter()
            .map(|field| field.to_prost())
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
            sub_fields: vec![],
            type_name: String::new(),
        }
    }

    pub fn with_struct<S>(
        data_type: DataType,
        name: S,
        sub_fields: Vec<Field>,
        type_name: S,
    ) -> Self
    where
        S: Into<String>,
    {
        Self {
            data_type,
            name: name.into(),
            sub_fields,
            type_name: type_name.into(),
        }
    }

    pub fn unnamed(data_type: DataType) -> Self {
        Self {
            data_type,
            name: String::new(),
            sub_fields: vec![],
            type_name: String::new(),
        }
    }

    pub fn data_type(&self) -> DataType {
        self.data_type.clone()
    }

    pub fn from_with_table_name_prefix(desc: &ColumnDesc, table_name: &str) -> Self {
        Self {
            data_type: desc.data_type.clone(),
            name: format!("{}.{}", table_name, desc.name),
            sub_fields: desc.field_descs.iter().map(|d| d.into()).collect_vec(),
            type_name: desc.type_name.clone(),
        }
    }
}

impl From<&ProstField> for Field {
    fn from(prost_field: &ProstField) -> Self {
        Self {
            data_type: DataType::from(prost_field.get_data_type().expect("data type not found")),
            name: prost_field.get_name().clone(),
            sub_fields: vec![],
            type_name: String::new(),
        }
    }
}

impl From<&ColumnDesc> for Field {
    fn from(desc: &ColumnDesc) -> Self {
        Self {
            data_type: desc.data_type.clone(),
            name: desc.name.clone(),
            sub_fields: desc.field_descs.iter().map(|d| d.into()).collect_vec(),
            type_name: desc.type_name.clone(),
        }
    }
}

impl Index<usize> for Schema {
    type Output = Field;

    fn index(&self, index: usize) -> &Self::Output {
        &self.fields[index]
    }
}

impl FromIterator<Field> for Schema {
    fn from_iter<I: IntoIterator<Item = Field>>(iter: I) -> Self {
        Schema {
            fields: iter.into_iter().collect::<Vec<_>>(),
        }
    }
}

pub mod test_utils {
    use super::*;

    pub fn field_n<const N: usize>(data_type: DataType) -> Schema {
        Schema::new(vec![Field::unnamed(data_type); N])
    }

    fn int32_n<const N: usize>() -> Schema {
        field_n::<N>(DataType::Int32)
    }

    /// Create a util schema **for test only** with two int32 fields.
    pub fn ii() -> Schema {
        int32_n::<2>()
    }

    /// Create a util schema **for test only** with three int32 fields.
    pub fn iii() -> Schema {
        int32_n::<3>()
    }

    fn varchar_n<const N: usize>() -> Schema {
        field_n::<N>(DataType::Varchar)
    }

    /// Create a util schema **for test only** with three varchar fields.
    pub fn sss() -> Schema {
        varchar_n::<3>()
    }

    fn decimal_n<const N: usize>() -> Schema {
        field_n::<N>(DataType::Decimal)
    }

    /// Create a util schema **for test only** with three decimal fields.
    pub fn ddd() -> Schema {
        decimal_n::<3>()
    }
}

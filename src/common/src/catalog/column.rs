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

use std::slice::SliceIndex;

use itertools::Itertools;
use risingwave_pb::plan_common::ColumnDesc as ProstColumnDesc;

use crate::catalog::Field;
use crate::error::ErrorCode;
use crate::types::DataType;

// Checkout ColumnID
/// Column ID is the unique identifier of a column in a table. Different from table ID, column ID is
/// not globally unique.
#[derive(Clone, Copy, Eq, PartialEq, Hash, PartialOrd)]
pub struct ColumnId(i32);

impl std::fmt::Debug for ColumnId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "#{}", self.0)
    }
}

impl ColumnId {
    pub const fn new(column_id: i32) -> Self {
        Self(column_id)
    }
}

impl ColumnId {
    pub fn get_id(&self) -> i32 {
        self.0
    }
}

impl From<i32> for ColumnId {
    fn from(column_id: i32) -> Self {
        Self::new(column_id)
    }
}

impl From<ColumnId> for i32 {
    fn from(id: ColumnId) -> i32 {
        id.0
    }
}

impl std::fmt::Display for ColumnId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ColumnDesc {
    pub data_type: DataType,
    pub column_id: ColumnId,
    pub name: String, // for debugging
    pub field_descs: Vec<ColumnDesc>,
    pub type_name: String,
}

impl ColumnDesc {
    pub fn unnamed(column_id: ColumnId, data_type: DataType) -> ColumnDesc {
        ColumnDesc {
            data_type,
            column_id,
            name: String::new(),
            field_descs: vec![],
            type_name: String::new(),
        }
    }

    /// Convert to proto
    pub fn to_protobuf(&self) -> ProstColumnDesc {
        ProstColumnDesc {
            column_type: Some(self.data_type.to_protobuf()),
            column_id: self.column_id.get_id(),
            name: self.name.clone(),
            field_descs: self
                .field_descs
                .clone()
                .into_iter()
                .map(|f| f.to_protobuf())
                .collect_vec(),
            type_name: self.type_name.clone(),
        }
    }

    /// Flatten a nested column to a list of columns (including itself).
    /// If the type is atomic, it returns simply itself.
    /// If the type has multiple nesting levels, it traverses for the tree-like schema,
    /// and returns every tree node.
    pub fn flatten(&self) -> Vec<ColumnDesc> {
        let mut descs = vec![self.clone()];
        descs.extend(self.field_descs.iter().flat_map(|d| {
            let mut desc = d.clone();
            desc.name = self.name.clone() + "." + &desc.name;
            desc.flatten()
        }));
        descs
    }

    /// Find `column_desc` in `field_descs` by name.
    pub fn field(&self, name: &String) -> crate::error::Result<(ColumnDesc, i32)> {
        if let DataType::Struct { .. } = self.data_type {
            for (index, col) in self.field_descs.iter().enumerate() {
                if col.name == *name {
                    return Ok((col.clone(), index as i32));
                }
            }
            Err(ErrorCode::ItemNotFound(format!("Invalid field name: {}", name)).into())
        } else {
            Err(ErrorCode::ItemNotFound(format!(
                "Cannot get field from non nested column: {}",
                self.name
            ))
            .into())
        }
    }

    pub fn new_atomic(data_type: DataType, name: &str, column_id: i32) -> Self {
        Self {
            data_type,
            column_id: ColumnId::new(column_id),
            name: name.to_string(),
            field_descs: vec![],
            type_name: "".to_string(),
        }
    }

    pub fn new_struct(
        name: &str,
        column_id: i32,
        type_name: &str,
        fields: Vec<ColumnDesc>,
    ) -> Self {
        let data_type = DataType::new_struct(
            fields.iter().map(|f| f.data_type.clone()).collect_vec(),
            fields.iter().map(|f| f.name.clone()).collect_vec(),
        );
        Self {
            data_type,
            column_id: ColumnId::new(column_id),
            name: name.to_string(),
            field_descs: fields,
            type_name: type_name.to_string(),
        }
    }

    pub fn from_field_with_column_id(field: &Field, id: i32) -> Self {
        Self {
            data_type: field.data_type.clone(),
            column_id: ColumnId::new(id),
            name: field.name.clone(),
            field_descs: field
                .sub_fields
                .iter()
                .map(Self::from_field_without_column_id)
                .collect_vec(),
            type_name: field.type_name.clone(),
        }
    }

    pub fn from_field_without_column_id(field: &Field) -> Self {
        Self::from_field_with_column_id(field, 0)
    }
}

impl From<ProstColumnDesc> for ColumnDesc {
    fn from(prost: ProstColumnDesc) -> Self {
        let field_descs: Vec<ColumnDesc> = prost
            .field_descs
            .into_iter()
            .map(ColumnDesc::from)
            .collect();
        Self {
            data_type: DataType::from(prost.column_type.as_ref().unwrap()),
            column_id: ColumnId::new(prost.column_id),
            name: prost.name,
            type_name: prost.type_name,
            field_descs,
        }
    }
}

impl From<&ProstColumnDesc> for ColumnDesc {
    fn from(prost: &ProstColumnDesc) -> Self {
        prost.clone().into()
    }
}

impl From<&ColumnDesc> for ProstColumnDesc {
    fn from(c: &ColumnDesc) -> Self {
        Self {
            column_type: c.data_type.to_protobuf().into(),
            column_id: c.column_id.into(),
            name: c.name.clone(),
            field_descs: c.field_descs.iter().map(ColumnDesc::to_protobuf).collect(),
            type_name: c.type_name.clone(),
        }
    }
}

#[cfg(test)]
pub mod tests {
    use risingwave_pb::plan_common::ColumnDesc as ProstColumnDesc;

    use crate::catalog::ColumnDesc;
    use crate::test_prelude::*;
    use crate::types::DataType;

    pub fn build_prost_desc() -> ProstColumnDesc {
        let city = vec![
            ProstColumnDesc::new_atomic(DataType::Varchar.to_protobuf(), "country.city.address", 2),
            ProstColumnDesc::new_atomic(DataType::Varchar.to_protobuf(), "country.city.zipcode", 3),
        ];
        let country = vec![
            ProstColumnDesc::new_atomic(DataType::Varchar.to_protobuf(), "country.address", 1),
            ProstColumnDesc::new_struct("country.city", 4, ".test.City", city),
        ];
        ProstColumnDesc::new_struct("country", 5, ".test.Country", country)
    }

    pub fn build_desc() -> ColumnDesc {
        let city = vec![
            ColumnDesc::new_atomic(DataType::Varchar, "country.city.address", 2),
            ColumnDesc::new_atomic(DataType::Varchar, "country.city.zipcode", 3),
        ];
        let country = vec![
            ColumnDesc::new_atomic(DataType::Varchar, "country.address", 1),
            ColumnDesc::new_struct("country.city", 4, ".test.City", city),
        ];
        ColumnDesc::new_struct("country", 5, ".test.Country", country)
    }

    #[test]
    fn test_into_column_catalog() {
        let desc: ColumnDesc = build_prost_desc().into();
        assert_eq!(desc, build_desc());
    }
}

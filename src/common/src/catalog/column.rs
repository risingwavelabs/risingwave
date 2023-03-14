// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::borrow::Cow;

use itertools::Itertools;
use risingwave_pb::plan_common::{PbColumnCatalog, PbColumnDesc};

use super::row_id_column_desc;
use crate::catalog::{Field, ROW_ID_COLUMN_ID};
use crate::error::ErrorCode;
use crate::types::DataType;

/// Column ID is the unique identifier of a column in a table. Different from table ID, column ID is
/// not globally unique.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
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
    pub const fn get_id(&self) -> i32 {
        self.0
    }

    /// Returns the subsequent column id.
    #[must_use]
    pub const fn next(self) -> Self {
        Self(self.0 + 1)
    }

    pub fn apply_delta_if_not_row_id(&mut self, delta: i32) {
        if self.0 != ROW_ID_COLUMN_ID.get_id() {
            self.0 += delta;
        }
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

impl From<&ColumnId> for i32 {
    fn from(id: &ColumnId) -> i32 {
        id.0
    }
}

impl std::fmt::Display for ColumnId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ColumnDesc {
    pub data_type: DataType,
    pub column_id: ColumnId,
    pub name: String,
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
    pub fn to_protobuf(&self) -> PbColumnDesc {
        PbColumnDesc {
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

impl From<PbColumnDesc> for ColumnDesc {
    fn from(prost: PbColumnDesc) -> Self {
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

impl From<&PbColumnDesc> for ColumnDesc {
    fn from(prost: &PbColumnDesc) -> Self {
        prost.clone().into()
    }
}

impl From<&ColumnDesc> for PbColumnDesc {
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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ColumnCatalog {
    pub column_desc: ColumnDesc,
    pub is_hidden: bool,
}

impl ColumnCatalog {
    /// Get the column catalog's is hidden.
    pub fn is_hidden(&self) -> bool {
        self.is_hidden
    }

    /// Get a reference to the column desc's data type.
    pub fn data_type(&self) -> &DataType {
        &self.column_desc.data_type
    }

    /// Get the column desc's column id.
    pub fn column_id(&self) -> ColumnId {
        self.column_desc.column_id
    }

    /// Get a reference to the column desc's name.
    pub fn name(&self) -> &str {
        self.column_desc.name.as_ref()
    }

    /// Convert column catalog to proto
    pub fn to_protobuf(&self) -> PbColumnCatalog {
        PbColumnCatalog {
            column_desc: Some(self.column_desc.to_protobuf()),
            is_hidden: self.is_hidden,
        }
    }

    /// Creates a row ID column (for implicit primary key).
    pub fn row_id_column() -> Self {
        Self {
            column_desc: row_id_column_desc(),
            is_hidden: true,
        }
    }
}

impl From<PbColumnCatalog> for ColumnCatalog {
    fn from(prost: PbColumnCatalog) -> Self {
        Self {
            column_desc: prost.column_desc.unwrap().into(),
            is_hidden: prost.is_hidden,
        }
    }
}

impl ColumnCatalog {
    pub fn name_with_hidden(&self) -> Cow<'_, str> {
        if self.is_hidden {
            Cow::Owned(format!("{}(hidden)", self.column_desc.name))
        } else {
            Cow::Borrowed(&self.column_desc.name)
        }
    }
}

pub fn columns_extend(preserved_columns: &mut Vec<ColumnCatalog>, columns: Vec<ColumnCatalog>) {
    debug_assert_eq!(ROW_ID_COLUMN_ID.get_id(), 0);
    let mut max_incoming_column_id = ROW_ID_COLUMN_ID.get_id();
    columns.iter().for_each(|column| {
        let column_id = column.column_id().get_id();
        if column_id > max_incoming_column_id {
            max_incoming_column_id = column_id;
        }
    });
    preserved_columns.iter_mut().for_each(|column| {
        column
            .column_desc
            .column_id
            .apply_delta_if_not_row_id(max_incoming_column_id)
    });

    preserved_columns.extend(columns);
}

pub fn is_column_ids_dedup(columns: &[ColumnCatalog]) -> bool {
    let mut column_ids = columns
        .iter()
        .map(|column| column.column_id().get_id())
        .collect_vec();
    column_ids.sort();
    let original_len = column_ids.len();
    column_ids.dedup();
    column_ids.len() == original_len
}

#[cfg(test)]
pub mod tests {
    use risingwave_pb::plan_common::PbColumnDesc;

    use crate::catalog::ColumnDesc;
    use crate::test_prelude::*;
    use crate::types::DataType;

    pub fn build_prost_desc() -> PbColumnDesc {
        let city = vec![
            PbColumnDesc::new_atomic(DataType::Varchar.to_protobuf(), "country.city.address", 2),
            PbColumnDesc::new_atomic(DataType::Varchar.to_protobuf(), "country.city.zipcode", 3),
        ];
        let country = vec![
            PbColumnDesc::new_atomic(DataType::Varchar.to_protobuf(), "country.address", 1),
            PbColumnDesc::new_struct("country.city", 4, ".test.City", city),
        ];
        PbColumnDesc::new_struct("country", 5, ".test.Country", country)
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

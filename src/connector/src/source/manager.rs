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

use std::fmt::Debug;

use risingwave_common::catalog::{
    ColumnDesc, ColumnId, KAFKA_TIMESTAMP_COLUMN_NAME, OFFSET_COLUMN_NAME, ROWID_PREFIX,
    TABLE_NAME_COLUMN_NAME,
};
use risingwave_common::types::DataType;

/// `SourceColumnDesc` is used to describe a column in the Source and is used as the column
/// counterpart in `StreamScan`
#[derive(Clone, Debug)]
pub struct SourceColumnDesc {
    pub name: String,
    pub data_type: DataType,
    pub column_id: ColumnId,
    pub fields: Vec<ColumnDesc>,
    pub column_type: SourceColumnType,

    // `is_pk` is used to indicate whether the column is part of the primary key columns.
    pub is_pk: bool,
}

/// `SourceColumnType` is used to indicate the type of a column emitted by the Source.
/// There are 4 types of columns:
/// - `Normal`: a visible column
/// - `RowId`: internal column to uniquely identify a row
/// - `Meta`: internal column to store source related metadata
/// - `Offset`: internal column to store upstream offset for a row, used in CDC source
#[derive(Clone, Debug, PartialEq)]
pub enum SourceColumnType {
    Normal,

    // internal columns
    RowId,
    Meta,
    Offset,
}

impl SourceColumnType {
    pub fn from_name(name: &str) -> Self {
        if name.starts_with(KAFKA_TIMESTAMP_COLUMN_NAME) || name.starts_with(TABLE_NAME_COLUMN_NAME)
        {
            Self::Meta
        } else if name == (ROWID_PREFIX) {
            Self::RowId
        } else if name == OFFSET_COLUMN_NAME {
            Self::Offset
        } else {
            Self::Normal
        }
    }
}

impl SourceColumnDesc {
    /// Create a [`SourceColumnDesc`] without composite types.
    #[track_caller]
    pub fn simple(name: impl Into<String>, data_type: DataType, column_id: ColumnId) -> Self {
        assert!(
            !matches!(data_type, DataType::List { .. } | DataType::Struct(..)),
            "called `SourceColumnDesc::simple` with a composite type."
        );
        let name = name.into();
        Self {
            name,
            data_type,
            column_id,
            fields: vec![],
            column_type: SourceColumnType::Normal,
            is_pk: false,
        }
    }

    pub fn is_row_id(&self) -> bool {
        self.column_type == SourceColumnType::RowId
    }

    pub fn is_meta(&self) -> bool {
        self.column_type == SourceColumnType::Meta
    }

    pub fn is_offset(&self) -> bool {
        self.column_type == SourceColumnType::Offset
    }

    #[inline]
    pub fn is_visible(&self) -> bool {
        self.column_type == SourceColumnType::Normal
    }
}

impl From<&ColumnDesc> for SourceColumnDesc {
    fn from(c: &ColumnDesc) -> Self {
        let column_type = SourceColumnType::from_name(c.name.as_str());
        Self {
            name: c.name.clone(),
            data_type: c.data_type.clone(),
            column_id: c.column_id,
            fields: c.field_descs.clone(),
            column_type,
            is_pk: false,
        }
    }
}

impl From<&SourceColumnDesc> for ColumnDesc {
    fn from(s: &SourceColumnDesc) -> Self {
        ColumnDesc {
            data_type: s.data_type.clone(),
            column_id: s.column_id,
            name: s.name.clone(),
            field_descs: s.fields.clone(),
            type_name: "".to_string(),
            generated_or_default_column: None,
            description: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_visible() {
        let mut c = SourceColumnDesc::simple("a", DataType::Int32, ColumnId::new(0));
        assert!(c.is_visible());
        c.column_type = SourceColumnType::RowId;
        assert!(!c.is_visible());
        c.column_type = SourceColumnType::Meta;
        assert!(!c.is_visible());
    }
}

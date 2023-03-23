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

use risingwave_common::catalog::{ColumnDesc, ColumnId};
use risingwave_common::types::DataType;

/// `SourceColumnDesc` is used to describe a column in the Source and is used as the column
/// counterpart in `StreamScan`
#[derive(Clone, Debug)]
pub struct SourceColumnDesc {
    pub name: String,
    pub name_in_lower_case: String,
    pub data_type: DataType,
    pub column_id: ColumnId,
    pub fields: Vec<ColumnDesc>,
    /// Now `skip_parse` is used to indicate whether the column is a row id column.
    pub is_row_id: bool,

    pub is_meta: bool,
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
        let name_in_lower_case = name.to_ascii_lowercase();
        Self {
            name,
            name_in_lower_case,
            data_type,
            column_id,
            fields: vec![],
            is_row_id: false,
            is_meta: false,
        }
    }

    #[inline]
    pub fn is_visible(&self) -> bool {
        !self.is_row_id && !self.is_meta
    }
}

impl From<&ColumnDesc> for SourceColumnDesc {
    fn from(c: &ColumnDesc) -> Self {
        let is_meta = c.name.starts_with("_rw_kafka_timestamp");
        Self {
            name: c.name.clone(),
            name_in_lower_case: c.name.to_ascii_lowercase(),
            data_type: c.data_type.clone(),
            column_id: c.column_id,
            fields: c.field_descs.clone(),
            is_row_id: false,
            is_meta,
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
        c.is_row_id = true;
        assert!(!c.is_visible());
        c.is_row_id = false;
        c.is_meta = true;
        assert!(!c.is_visible());
    }
}

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

use std::borrow::Cow;

use risingwave_common::catalog::{ColumnDesc, ColumnId};
use risingwave_common::types::DataType;
use risingwave_pb::plan_common::ColumnCatalog as ProstColumnCatalog;

use super::row_id_column_desc;

#[derive(Debug, Clone, PartialEq)]
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
    pub fn to_protobuf(&self) -> ProstColumnCatalog {
        ProstColumnCatalog {
            column_desc: Some(self.column_desc.to_protobuf()),
            is_hidden: self.is_hidden,
        }
    }

    /// Creates a row ID column (for implicit primary key).
    pub fn row_id_column(column_id: ColumnId) -> Self {
        Self {
            column_desc: row_id_column_desc(column_id),
            is_hidden: true,
        }
    }
}

impl From<ProstColumnCatalog> for ColumnCatalog {
    fn from(prost: ProstColumnCatalog) -> Self {
        Self {
            column_desc: prost.column_desc.unwrap().into(),
            is_hidden: prost.is_hidden,
        }
    }
}

impl ColumnCatalog {
    pub fn name_with_hidden(&self) -> Cow<str> {
        if self.is_hidden {
            Cow::Owned(format!("{}(hidden)", self.column_desc.name))
        } else {
            Cow::Borrowed(&self.column_desc.name)
        }
    }
}

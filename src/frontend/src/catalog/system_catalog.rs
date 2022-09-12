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

use risingwave_common::catalog::TableDesc;

use crate::catalog::column_catalog::ColumnCatalog;
use crate::catalog::TableId;

#[derive(Clone, Debug, PartialEq)]
pub struct SystemCatalog {
    pub id: TableId,

    pub name: String,

    // All columns in this table.
    pub columns: Vec<ColumnCatalog>,

    /// Primary key columns indices.
    pub pk: Vec<usize>,

    // owner of table, should always be default super user, keep it for compatibility.
    pub owner: u32,
}

impl SystemCatalog {
    /// Get a reference to the system catalog's table id.
    pub fn id(&self) -> TableId {
        self.id
    }

    /// Get a reference to the system catalog's columns.
    pub fn columns(&self) -> &[ColumnCatalog] {
        &self.columns
    }

    /// Get a [`TableDesc`] of the system table.
    pub fn table_desc(&self) -> TableDesc {
        TableDesc {
            table_id: self.id,
            columns: self.columns.iter().map(|c| c.column_desc.clone()).collect(),
            stream_key: self.pk.clone(),
            ..Default::default()
        }
    }

    /// Get a reference to the system catalog's name.
    pub fn name(&self) -> &str {
        self.name.as_ref()
    }
}

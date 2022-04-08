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

use risingwave_common::error::{ErrorCode, RwError};
use thiserror::Error;
pub(crate) mod catalog_service;

pub(crate) mod column_catalog;
pub(crate) mod database_catalog;
pub(crate) mod root_catalog;
pub(crate) mod schema_catalog;
pub(crate) mod source_catalog;
pub(crate) mod table_catalog;

#[allow(dead_code)]
pub(crate) type SourceId = u32;

pub(crate) type DatabaseId = u32;
pub(crate) type SchemaId = u32;
pub(crate) type TableId = risingwave_common::catalog::TableId;
pub(crate) type ColumnId = risingwave_common::catalog::ColumnId;

pub const ROWID_PREFIX: &str = "_row_id";

pub fn gen_row_id_column_name(idx: usize) -> String {
    ROWID_PREFIX.to_string() + "#" + &idx.to_string()
}

pub fn is_row_id_column_name(name: &str) -> bool {
    name.starts_with(ROWID_PREFIX)
}

#[derive(Error, Debug)]
pub enum CatalogError {
    #[error("{0} not found: {1}")]
    NotFound(&'static str, String),
    #[error("{0} with name {1} exists")]
    Duplicated(&'static str, String),
}

impl From<CatalogError> for RwError {
    fn from(e: CatalogError) -> Self {
        ErrorCode::CatalogError(Box::new(e)).into()
    }
}

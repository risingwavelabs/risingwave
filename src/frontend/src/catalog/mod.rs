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

//! Definitions of catalog structs.
//!
//! The main struct is [`root_catalog::Catalog`], which is the root containing other catalog
//! structs. It is accessed via [`catalog_service::CatalogReader`] and
//! [`catalog_service::CatalogWriter`], which is held by [`crate::session::FrontendEnv`].

use risingwave_common::catalog::{ColumnDesc, PG_CATALOG_SCHEMA_NAME};
use risingwave_common::error::{ErrorCode, Result, RwError};
use risingwave_common::types::DataType;
use thiserror::Error;
pub(crate) mod catalog_service;

pub(crate) mod column_catalog;
pub(crate) mod database_catalog;
pub(crate) mod index_catalog;
pub(crate) mod pg_catalog;
pub(crate) mod root_catalog;
pub(crate) mod schema_catalog;
pub(crate) mod sink_catalog;
pub(crate) mod source_catalog;
pub(crate) mod system_catalog;
pub(crate) mod table_catalog;
pub(crate) mod view_catalog;

pub use index_catalog::IndexCatalog;
pub use table_catalog::TableCatalog;

pub(crate) type SourceId = u32;
pub(crate) type SinkId = u32;
pub(crate) type ViewId = u32;
pub(crate) type DatabaseId = u32;
pub(crate) type SchemaId = u32;
pub(crate) type TableId = risingwave_common::catalog::TableId;
pub(crate) type ColumnId = risingwave_common::catalog::ColumnId;
pub(crate) type FragmentId = u32;

/// Check if the column name does not conflict with the internally reserved column name.
pub fn check_valid_column_name(column_name: &str) -> Result<()> {
    if is_row_id_column_name(column_name) {
        Err(ErrorCode::InternalError(format!(
            "column name prefixed with {:?} are reserved word.",
            ROWID_PREFIX
        ))
        .into())
    } else {
        Ok(())
    }
}

/// Check if modifications happen to system catalog.
pub fn check_schema_writable(schema: &str) -> Result<()> {
    if schema == PG_CATALOG_SCHEMA_NAME {
        Err(ErrorCode::ProtocolError(format!(
            "permission denied to write on \"{}\", System catalog modifications are currently disallowed.",
            schema
        )).into())
    } else {
        Ok(())
    }
}

const ROWID_PREFIX: &str = "_row_id";

pub fn row_id_column_name() -> String {
    ROWID_PREFIX.to_string()
}

pub fn is_row_id_column_name(name: &str) -> bool {
    name.starts_with(ROWID_PREFIX)
}

/// Creates a row ID column (for implicit primary key).
pub fn row_id_column_desc(column_id: ColumnId) -> ColumnDesc {
    ColumnDesc {
        data_type: DataType::Int64,
        // We should not assume the first column (i.e., column_id == 0) is `_row_id`.
        column_id,
        name: row_id_column_name(),
        field_descs: vec![],
        type_name: "".to_string(),
    }
}

#[derive(Error, Debug)]
pub enum CatalogError {
    #[error("{0} not found: {1}")]
    NotFound(&'static str, String),
    #[error("{0} with name {1} exists")]
    Duplicated(&'static str, String),
    #[error("cannot drop {0} {1} because {2} {3} depend on it")]
    NotEmpty(&'static str, String, &'static str, String),
}

impl From<CatalogError> for RwError {
    fn from(e: CatalogError) -> Self {
        ErrorCode::CatalogError(Box::new(e)).into()
    }
}

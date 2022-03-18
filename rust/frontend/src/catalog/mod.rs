#![allow(dead_code)]
use risingwave_common::error::{ErrorCode, RwError};
use thiserror::Error;
pub(crate) mod catalog_service;

pub(crate) mod column_catalog;
pub(crate) mod database_catalog;
pub(crate) mod root_catalog;
pub(crate) mod schema_catalog;
pub(crate) mod table_catalog;

pub(crate) type SourceId = u32;
pub(crate) type DatabaseId = u32;
pub(crate) type SchemaId = u32;
pub(crate) type TableId = risingwave_common::catalog::TableId;
pub(crate) type ColumnId = risingwave_common::catalog::ColumnId;

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

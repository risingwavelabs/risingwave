#![allow(dead_code)]
use risingwave_common::error::{ErrorCode, RwError};
use thiserror::Error;

pub(crate) mod catalog_connector;
pub(crate) mod catalog_service;
pub(crate) mod database_catalog;
mod schema_catalog;
pub(crate) mod table_catalog;

pub(crate) type DatabaseId = u32;
pub(crate) type SchemaId = u32;
pub(crate) type TableId = risingwave_common::catalog::TableId;
pub(crate) type ColumnId = risingwave_common::catalog::ColumnId;

#[derive(Error, Debug)]
pub enum CatalogError {
    #[error("{0} not found: {1}")]
    NotFound(&'static str, String),
    #[error("duplicated {0}: {1}")]
    Duplicated(&'static str, String),
}

impl From<CatalogError> for RwError {
    fn from(e: CatalogError) -> Self {
        ErrorCode::CatalogError(Box::new(e)).into()
    }
}

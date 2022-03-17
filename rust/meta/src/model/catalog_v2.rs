use risingwave_common::error::Result;
use risingwave_pb::catalog::{Database, Schema, Source, Table};

use crate::model::MetadataModel;

/// Column family name for source catalog.
const CATALOG_SOURCE_CF_NAME: &str = "cf/catalog_source";
/// Column family name for table catalog.
const CATALOG_TABLE_CF_NAME: &str = "cf/catalog_table";
/// Column family name for schema catalog.
const CATALOG_SCHEMA_CF_NAME: &str = "cf/catalog_schema";
/// Column family name for database catalog.
const CATALOG_DATABASE_CF_NAME: &str = "cf/catalog_database";

macro_rules! impl_model_for_catalog {
    ($name:ident, $cf:ident, $key_ty:ty, $key_fn:ident) => {
        impl MetadataModel for $name {
            type ProstType = Self;
            type KeyType = $key_ty;

            fn cf_name() -> String {
                $cf.to_string()
            }

            fn to_protobuf(&self) -> Self::ProstType {
                self.clone()
            }

            fn from_protobuf(prost: Self::ProstType) -> Self {
                prost
            }

            fn key(&self) -> Result<Self::KeyType> {
                Ok(self.$key_fn())
            }
        }
    };
}

impl_model_for_catalog!(Source, CATALOG_SOURCE_CF_NAME, u32, get_id);
impl_model_for_catalog!(Table, CATALOG_TABLE_CF_NAME, u32, get_id);
impl_model_for_catalog!(Schema, CATALOG_SCHEMA_CF_NAME, u32, get_id);
impl_model_for_catalog!(Database, CATALOG_DATABASE_CF_NAME, u32, get_id);

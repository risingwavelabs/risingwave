use std::sync::Arc;

use async_trait::async_trait;
use prost::Message;
use risingwave_common::error::Result;
use risingwave_pb::meta::{Catalog, Database, Schema, Table};

use crate::catalog::{DatabaseMetaManager, SchemaMetaManager, TableMetaManager};
use crate::manager::{Config, EpochGeneratorRef, MetaSrvEnv};
use crate::storage::MetaStoreRef;

#[async_trait]
pub trait CatalogManager:
    DatabaseMetaManager + SchemaMetaManager + TableMetaManager + Sync + Send + 'static
{
    async fn get_catalog(&self) -> Result<Catalog>;
}

pub type CatalogManagerRef = Arc<dyn CatalogManager>;

/// [`StoredCatalogManager`] implements catalog meta data management in [`MetaStore`].
pub struct StoredCatalogManager {
    pub config: Arc<Config>,
    pub meta_store_ref: MetaStoreRef,
    pub epoch_generator: EpochGeneratorRef,
}

impl StoredCatalogManager {
    pub fn new(env: MetaSrvEnv) -> Self {
        Self {
            config: env.config(),
            meta_store_ref: env.meta_store_ref(),
            epoch_generator: env.epoch_generator_ref(),
        }
    }
}

#[async_trait]
impl CatalogManager for StoredCatalogManager {
    async fn get_catalog(&self) -> Result<Catalog> {
        let catalog_pb = self
            .meta_store_ref
            .list_batch_cf(vec![
                self.config.get_database_cf(),
                self.config.get_schema_cf(),
                self.config.get_table_cf(),
            ])
            .await?;
        assert_eq!(catalog_pb.len(), 3);

        Ok(Catalog {
            databases: catalog_pb
                .get(0)
                .unwrap()
                .iter()
                .map(|d| Database::decode(d.as_slice()).unwrap())
                .collect::<Vec<_>>(),
            schemas: catalog_pb
                .get(1)
                .unwrap()
                .iter()
                .map(|d| Schema::decode(d.as_slice()).unwrap())
                .collect::<Vec<_>>(),
            tables: catalog_pb
                .get(2)
                .unwrap()
                .iter()
                .map(|d| Table::decode(d.as_slice()).unwrap())
                .collect::<Vec<_>>(),
        })
    }
}

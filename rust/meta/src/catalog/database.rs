use async_trait::async_trait;
use prost::Message;
use risingwave_common::error::Result;
use risingwave_pb::meta::Database;
use risingwave_pb::plan::DatabaseRefId;

use crate::catalog::StoredCatalogManager;
use crate::manager::Epoch;

#[async_trait]
pub trait DatabaseMetaManager {
    async fn list_databases(&self) -> Result<Vec<Database>>;
    async fn create_database(&self, mut database: Database) -> Result<Epoch>;
    async fn get_database(&self, database_id: &DatabaseRefId, version: Epoch) -> Result<Database>;
    async fn drop_database(&self, database_id: &DatabaseRefId) -> Result<Epoch>;
}

#[async_trait]
impl DatabaseMetaManager for StoredCatalogManager {
    async fn list_databases(&self) -> Result<Vec<Database>> {
        let databases_pb = self
            .meta_store_ref
            .list_cf(self.config.get_database_cf())
            .await?;

        Ok(databases_pb
            .iter()
            .map(|d| Database::decode(d.as_slice()).unwrap())
            .collect::<Vec<_>>())
    }

    async fn create_database(&self, mut database: Database) -> Result<Epoch> {
        let version = self.epoch_generator.generate()?;
        database.version = version.into_inner();
        let database_ref_id = database.get_database_ref_id()?;

        self.meta_store_ref
            .put_cf(
                self.config.get_database_cf(),
                &database_ref_id.encode_to_vec(),
                &database.encode_to_vec(),
                version,
            )
            .await?;

        Ok(version)
    }

    async fn get_database(&self, database_id: &DatabaseRefId, version: Epoch) -> Result<Database> {
        let database_proto = self
            .meta_store_ref
            .get_cf(
                self.config.get_database_cf(),
                &database_id.encode_to_vec(),
                version,
            )
            .await?;

        Ok(Database::decode(database_proto.as_slice())?)
    }

    async fn drop_database(&self, database_id: &DatabaseRefId) -> Result<Epoch> {
        let version = self.epoch_generator.generate()?;

        self.meta_store_ref
            .delete_all_cf(self.config.get_database_cf(), &database_id.encode_to_vec())
            .await?;

        Ok(version)
    }
}

#[cfg(test)]
mod tests {

    use futures::future;
    use risingwave_pb::meta::Database;

    use super::*;
    use crate::manager::MetaSrvEnv;

    #[tokio::test]
    async fn test_database_manager() -> Result<()> {
        let catalog_manager = StoredCatalogManager::new(MetaSrvEnv::for_test().await);

        assert!(catalog_manager.list_databases().await.is_ok());
        assert!(catalog_manager
            .get_database(&DatabaseRefId { database_id: 0 }, Epoch::from(0))
            .await
            .is_err());

        let versions = future::join_all((0..100).map(|i| {
            let catalog_manager = &catalog_manager;
            async move {
                let database = Database {
                    database_ref_id: Some(DatabaseRefId { database_id: i }),
                    database_name: format!("database_{}", i),
                    version: 0,
                };
                catalog_manager.create_database(database).await
            }
        }))
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()?;
        for (i, &version) in versions.iter().enumerate() {
            let database = catalog_manager
                .get_database(
                    &DatabaseRefId {
                        database_id: i as i32,
                    },
                    version,
                )
                .await?;
            assert_eq!(database.database_ref_id.unwrap().database_id, i as i32);
            assert_eq!(database.database_name, format!("database_{}", i));
            assert_eq!(database.version, version.into_inner());
        }

        let databases = catalog_manager.list_databases().await?;
        assert_eq!(databases.len(), 100);

        let version = catalog_manager
            .create_database(Database {
                database_ref_id: Some(DatabaseRefId { database_id: 0 }),
                database_name: "database_0".to_string(),
                version: 0,
            })
            .await?;
        assert_ne!(version, versions[0]);

        for i in 0..100 {
            assert!(catalog_manager
                .drop_database(&DatabaseRefId { database_id: i })
                .await
                .is_ok());
        }
        let databases = catalog_manager.list_databases().await?;
        assert_eq!(databases.len(), 0);

        Ok(())
    }
}

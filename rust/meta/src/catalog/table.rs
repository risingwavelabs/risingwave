use async_trait::async_trait;
use prost::Message;
use risingwave_common::error::Result;
use risingwave_pb::meta::Table;
use risingwave_pb::plan::TableRefId;

use crate::catalog::StoredCatalogManager;
use crate::manager::Epoch;

#[async_trait]
pub trait TableMetaManager {
    async fn list_tables(&self) -> Result<Vec<Table>>;
    async fn create_table(&self, mut table: Table) -> Result<Epoch>;
    async fn get_table(&self, table_id: &TableRefId, version: Epoch) -> Result<Table>;
    async fn drop_table(&self, table_id: &TableRefId) -> Result<Epoch>;
}

#[async_trait]
impl TableMetaManager for StoredCatalogManager {
    async fn list_tables(&self) -> Result<Vec<Table>> {
        let tables_pb = self
            .meta_store_ref
            .list_cf(self.config.get_table_cf())
            .await?;

        Ok(tables_pb
            .iter()
            .map(|t| Table::decode(t.as_slice()).unwrap())
            .collect::<Vec<_>>())
    }

    async fn create_table(&self, mut table: Table) -> Result<Epoch> {
        let version = self.epoch_generator.generate()?;
        table.version = version.into_inner();
        let table_ref_id = table.get_table_ref_id();

        self.meta_store_ref
            .put_cf(
                self.config.get_table_cf(),
                &table_ref_id.encode_to_vec(),
                &table.encode_to_vec(),
                version,
            )
            .await?;

        Ok(version)
    }

    async fn get_table(&self, table_id: &TableRefId, version: Epoch) -> Result<Table> {
        let table_pb = self
            .meta_store_ref
            .get_cf(
                self.config.get_table_cf(),
                &table_id.encode_to_vec(),
                version,
            )
            .await?;

        Ok(Table::decode(table_pb.as_slice())?)
    }

    async fn drop_table(&self, table_id: &TableRefId) -> Result<Epoch> {
        let version = self.epoch_generator.generate()?;

        self.meta_store_ref
            .delete_all_cf(self.config.get_table_cf(), &table_id.encode_to_vec())
            .await?;

        Ok(version)
    }
}

#[cfg(test)]
mod tests {

    use futures::future;

    use super::*;
    use crate::manager::MetaSrvEnv;

    #[tokio::test]
    async fn test_table_manager() -> Result<()> {
        let catalog_manager = StoredCatalogManager::new(MetaSrvEnv::for_test().await);

        assert!(catalog_manager.list_tables().await.is_ok());
        assert!(catalog_manager
            .get_table(
                &TableRefId {
                    schema_ref_id: None,
                    table_id: 0
                },
                Epoch::from(0)
            )
            .await
            .is_err());

        let versions = future::join_all((0..100).map(|i| {
            let catalog_manager = &catalog_manager;
            async move {
                let table = Table {
                    table_ref_id: Some(TableRefId {
                        schema_ref_id: None,
                        table_id: i as i32,
                    }),
                    column_count: (i + 1) as u32,
                    table_name: format!("table_{}", i),
                    ..Default::default()
                };
                catalog_manager.create_table(table).await
            }
        }))
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()?;
        for (i, &version) in versions.iter().enumerate() {
            let table = catalog_manager
                .get_table(
                    &TableRefId {
                        schema_ref_id: None,
                        table_id: i as i32,
                    },
                    version,
                )
                .await?;
            assert_eq!(table.table_ref_id.unwrap().table_id, i as i32);
            assert_eq!(table.table_name, format!("table_{}", i));
            assert_eq!(table.column_count, (i + 1) as u32);
            assert_eq!(table.version, version.into_inner());
        }

        let tables = catalog_manager.list_tables().await?;
        assert_eq!(tables.len(), 100);

        let table = Table {
            table_ref_id: Some(TableRefId {
                schema_ref_id: None,
                table_id: 0,
            }),
            column_count: 10,
            table_name: "table_0".to_string(),
            ..Default::default()
        };

        let version = catalog_manager.create_table(table).await?;
        assert_ne!(version, versions[0]);

        for i in 0..100 {
            assert!(catalog_manager
                .drop_table(&TableRefId {
                    schema_ref_id: None,
                    table_id: i as i32
                })
                .await
                .is_ok());
        }
        let tables = catalog_manager.list_tables().await?;
        assert_eq!(tables.len(), 0);

        Ok(())
    }
}

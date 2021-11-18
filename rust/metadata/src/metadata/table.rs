use crate::metadata::{Epoch, MetaManager};
use async_trait::async_trait;
use prost::Message;
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::Result;
use risingwave_common::error::RwError;
use risingwave_pb::metadata::{ColumnTable, RowTable};
use risingwave_pb::plan::TableRefId;

#[async_trait]
pub trait TableMetaManager {
    async fn list_tables(&self) -> Result<Vec<ColumnTable>>;
    async fn create_table(&self, mut table: ColumnTable) -> Result<Epoch>;
    async fn get_table(&self, table_id: &TableRefId, version: Epoch) -> Result<ColumnTable>;
    async fn drop_table(&self, table_id: &TableRefId) -> Result<Epoch>;

    async fn list_materialized_views(&self) -> Result<Vec<RowTable>>;
    async fn create_materialized_view(&self, mut table: RowTable) -> Result<Epoch>;
    async fn get_materialized_view(
        &self,
        table_id: &TableRefId,
        version: Epoch,
    ) -> Result<RowTable>;
    async fn drop_materialized_view(&self, table_id: &TableRefId) -> Result<Epoch>;
}

#[async_trait]
impl TableMetaManager for MetaManager {
    async fn list_tables(&self) -> Result<Vec<ColumnTable>> {
        let tables_pb = self
            .meta_store_ref
            .list_cf(self.config.get_column_table_cf())
            .await?;

        Ok(tables_pb
            .iter()
            .map(|t| ColumnTable::decode(t.as_slice()).unwrap())
            .collect::<Vec<_>>())
    }

    async fn create_table(&self, mut table: ColumnTable) -> Result<Epoch> {
        // TODO: add lock here, ensure sequentially creation of same table with incremental epoch.
        let version = self.epoch_generator.generate()?;
        table.version = version.into_inner();
        let table_ref_id = table.get_table_ref_id();
        self.meta_store_ref
            .put_cf(
                self.config.get_column_table_cf(),
                &table_ref_id.encode_to_vec(),
                &table.encode_to_vec(),
                version,
            )
            .await?;

        Ok(version)
    }

    async fn get_table(&self, table_id: &TableRefId, version: Epoch) -> Result<ColumnTable> {
        let table_pb = self
            .meta_store_ref
            .get_cf(
                self.config.get_column_table_cf(),
                &table_id.encode_to_vec(),
                version,
            )
            .await?;

        ColumnTable::decode(table_pb.as_slice())
            .map_err(|e| RwError::from(InternalError(e.to_string())))
    }

    async fn drop_table(&self, table_id: &TableRefId) -> Result<Epoch> {
        // TODO: add lock here.
        let version = self.epoch_generator.generate()?;

        self.meta_store_ref
            .delete_all_cf(self.config.get_column_table_cf(), &table_id.encode_to_vec())
            .await?;

        Ok(version)
    }

    async fn list_materialized_views(&self) -> Result<Vec<RowTable>> {
        let tables_pb = self
            .meta_store_ref
            .list_cf(self.config.get_row_table_cf())
            .await?;

        Ok(tables_pb
            .iter()
            .map(|t| RowTable::decode(t.as_slice()).unwrap())
            .collect::<Vec<_>>())
    }

    async fn create_materialized_view(&self, mut table: RowTable) -> Result<Epoch> {
        let version = self.epoch_generator.generate()?;
        table.version = version.into_inner();
        let table_ref_id = table.get_table_ref_id();
        self.meta_store_ref
            .put_cf(
                self.config.get_row_table_cf(),
                &table_ref_id.encode_to_vec(),
                &table.encode_to_vec(),
                version,
            )
            .await?;

        Ok(version)
    }

    async fn get_materialized_view(
        &self,
        table_id: &TableRefId,
        version: Epoch,
    ) -> Result<RowTable> {
        let table_pb = self
            .meta_store_ref
            .get_cf(
                self.config.get_row_table_cf(),
                &table_id.encode_to_vec(),
                version,
            )
            .await?;

        RowTable::decode(table_pb.as_slice())
            .map_err(|e| RwError::from(InternalError(e.to_string())))
    }

    async fn drop_materialized_view(&self, table_id: &TableRefId) -> Result<Epoch> {
        // TODO: add lock here, ensure sequentially creation of same schema with incremental epoch.
        let version = self.epoch_generator.generate()?;

        self.meta_store_ref
            .delete_all_cf(self.config.get_row_table_cf(), &table_id.encode_to_vec())
            .await?;

        Ok(version)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::metadata::{Config, MemEpochGenerator, MemStore};
    use futures::future;

    #[tokio::test]
    async fn test_table_manager() -> Result<()> {
        let meta_manager = MetaManager::new(
            Box::new(MemStore::new()),
            Box::new(MemEpochGenerator::new()),
            Config::default(),
        )
        .await;

        assert!(meta_manager.list_tables().await.is_ok());
        assert!(meta_manager
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
            let meta_manager = &meta_manager;
            async move {
                let table = ColumnTable {
                    table_ref_id: Some(TableRefId {
                        schema_ref_id: None,
                        table_id: i as u64,
                    }),
                    column_count: (i + 1) as u32,
                    table_name: format!("table_{}", i),
                    version: 0,
                };
                meta_manager.create_table(table).await
            }
        }))
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()?;
        for (i, &version) in versions.iter().enumerate() {
            let table = meta_manager
                .get_table(
                    &TableRefId {
                        schema_ref_id: None,
                        table_id: i as u64,
                    },
                    version,
                )
                .await?;
            assert_eq!(table.table_ref_id.unwrap().table_id, i as u64);
            assert_eq!(table.table_name, format!("table_{}", i));
            assert_eq!(table.column_count, (i + 1) as u32);
            assert_eq!(table.version, version.into_inner());
        }

        let tables = meta_manager.list_tables().await?;
        assert_eq!(tables.len(), 100);

        let version = meta_manager
            .create_table(ColumnTable {
                table_ref_id: Some(TableRefId {
                    schema_ref_id: None,
                    table_id: 0,
                }),
                column_count: 10,
                table_name: "table_0".to_string(),
                version: 0,
            })
            .await?;
        assert_ne!(version, versions[0]);

        for i in 0..100 {
            assert!(meta_manager
                .drop_table(&TableRefId {
                    schema_ref_id: None,
                    table_id: i as u64
                })
                .await
                .is_ok());
        }
        let tables = meta_manager.list_tables().await?;
        assert_eq!(tables.len(), 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_mv_manager() -> Result<()> {
        let meta_manager = MetaManager::new(
            Box::new(MemStore::new()),
            Box::new(MemEpochGenerator::new()),
            Config::default(),
        )
        .await;

        assert!(meta_manager.list_materialized_views().await.is_ok());
        assert!(meta_manager
            .get_materialized_view(
                &TableRefId {
                    schema_ref_id: None,
                    table_id: 0
                },
                Epoch::from(0)
            )
            .await
            .is_err());

        let versions = future::join_all((0..100).map(|i| {
            let meta_manager = &meta_manager;
            async move {
                let table = RowTable {
                    table_ref_id: Some(TableRefId {
                        schema_ref_id: None,
                        table_id: i as u64,
                    }),
                    columns: vec![],
                    pk_columns: vec![],
                    version: 0,
                };
                meta_manager.create_materialized_view(table).await
            }
        }))
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()?;
        for (i, &version) in versions.iter().enumerate() {
            let table = meta_manager
                .get_materialized_view(
                    &TableRefId {
                        schema_ref_id: None,
                        table_id: i as u64,
                    },
                    version,
                )
                .await?;
            assert_eq!(table.table_ref_id.unwrap().table_id, i as u64);
            assert_eq!(table.version, version.into_inner());
        }

        let tables = meta_manager.list_materialized_views().await?;
        assert_eq!(tables.len(), 100);

        let version = meta_manager
            .create_materialized_view(RowTable {
                table_ref_id: Some(TableRefId {
                    schema_ref_id: None,
                    table_id: 0,
                }),
                columns: vec![],
                pk_columns: vec![],
                version: 0,
            })
            .await?;
        assert_ne!(version, versions[0]);

        for i in 0..100 {
            assert!(meta_manager
                .drop_materialized_view(&TableRefId {
                    schema_ref_id: None,
                    table_id: i as u64
                })
                .await
                .is_ok());
        }
        let tables = meta_manager.list_materialized_views().await?;
        assert_eq!(tables.len(), 0);

        Ok(())
    }
}

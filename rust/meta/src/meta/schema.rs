use crate::meta::{Epoch, MetaManager};
use async_trait::async_trait;
use prost::Message;
use risingwave_common::error::Result;
use risingwave_pb::meta::Schema;
use risingwave_pb::plan::SchemaRefId;

#[async_trait]
pub trait SchemaMetaManager {
    async fn list_schemas(&self) -> Result<Vec<Schema>>;
    async fn create_schema(&self, mut schema: Schema) -> Result<Epoch>;
    async fn get_schema(&self, schema_id: &SchemaRefId, version: Epoch) -> Result<Schema>;
    async fn drop_schema(&self, schema_id: &SchemaRefId) -> Result<Epoch>;
}

#[async_trait]
impl SchemaMetaManager for MetaManager {
    async fn list_schemas(&self) -> Result<Vec<Schema>> {
        let schemas_pb = self
            .meta_store_ref
            .list_cf(self.config.get_schema_cf())
            .await?;

        Ok(schemas_pb
            .iter()
            .map(|s| Schema::decode(s.as_slice()).unwrap())
            .collect::<Vec<_>>())
    }

    async fn create_schema(&self, mut schema: Schema) -> Result<Epoch> {
        let version = self.epoch_generator.generate()?;
        schema.version = version.into_inner();
        let schema_ref_id = schema.get_schema_ref_id();
        self.meta_store_ref
            .put_cf(
                self.config.get_schema_cf(),
                &schema_ref_id.encode_to_vec(),
                &schema.encode_to_vec(),
                version,
            )
            .await?;

        Ok(version)
    }

    async fn get_schema(&self, schema_id: &SchemaRefId, version: Epoch) -> Result<Schema> {
        let schema_pb = self
            .meta_store_ref
            .get_cf(
                self.config.get_schema_cf(),
                &schema_id.encode_to_vec(),
                version,
            )
            .await?;

        Ok(Schema::decode(schema_pb.as_slice())?)
    }

    async fn drop_schema(&self, schema_id: &SchemaRefId) -> Result<Epoch> {
        let version = self.epoch_generator.generate()?;

        self.meta_store_ref
            .delete_all_cf(self.config.get_schema_cf(), &schema_id.encode_to_vec())
            .await?;

        Ok(version)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::meta::{Config, IdGeneratorManager, MemEpochGenerator, MemStore};
    use futures::future;
    use risingwave_pb::meta::Schema;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_schema_manager() -> Result<()> {
        let meta_store_ref = Arc::new(MemStore::new());
        let meta_manager = MetaManager::new(
            meta_store_ref.clone(),
            Box::new(MemEpochGenerator::new()),
            IdGeneratorManager::new(meta_store_ref).await,
            Config::default(),
        )
        .await;

        assert!(meta_manager.list_schemas().await.is_ok());
        assert!(meta_manager
            .get_schema(
                &SchemaRefId {
                    database_ref_id: None,
                    schema_id: 0
                },
                Epoch::from(0)
            )
            .await
            .is_err());

        let versions = future::join_all((0..100).map(|i| {
            let meta_manager = &meta_manager;
            async move {
                let schema = Schema {
                    schema_ref_id: Some(SchemaRefId {
                        database_ref_id: None,
                        schema_id: i,
                    }),
                    schema_name: format!("schema_{}", i),
                    fields: vec![],
                    version: 0,
                };
                meta_manager.create_schema(schema).await
            }
        }))
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()?;
        for (i, &version) in versions.iter().enumerate() {
            let schema = meta_manager
                .get_schema(
                    &SchemaRefId {
                        database_ref_id: None,
                        schema_id: i as i32,
                    },
                    version,
                )
                .await?;
            assert_eq!(schema.schema_ref_id.unwrap().schema_id, i as i32);
            assert_eq!(schema.schema_name, format!("schema_{}", i));
            assert_eq!(schema.version, version.into_inner());
        }

        let schemas = meta_manager.list_schemas().await?;
        assert_eq!(schemas.len(), 100);

        let version = meta_manager
            .create_schema(Schema {
                schema_ref_id: Some(SchemaRefId {
                    database_ref_id: None,
                    schema_id: 0,
                }),
                schema_name: "schema_0".to_string(),
                fields: vec![],
                version: 0,
            })
            .await?;
        assert_ne!(version, versions[0]);

        for i in 0..100 {
            assert!(meta_manager
                .drop_schema(&SchemaRefId {
                    database_ref_id: None,
                    schema_id: i
                })
                .await
                .is_ok());
        }
        let schemas = meta_manager.list_schemas().await?;
        assert_eq!(schemas.len(), 0);

        Ok(())
    }
}

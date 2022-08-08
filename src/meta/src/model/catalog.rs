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

use risingwave_pb::catalog::{Database, Index, Schema, Sink, Source, Table};

use crate::model::{MetadataModel, MetadataModelResult};

/// Column family name for source catalog.
const CATALOG_SOURCE_CF_NAME: &str = "cf/catalog_source";
/// Column family name for sink catalog.
const CATALOG_SINK_CF_NAME: &str = "cf/catalog_sink";
/// Column family name for index catalog.
const CATALOG_INDEX_CF_NAME: &str = "cf/catalog_index";
/// Column family name for table catalog.
const CATALOG_TABLE_CF_NAME: &str = "cf/catalog_table";
/// Column family name for schema catalog.
const CATALOG_SCHEMA_CF_NAME: &str = "cf/catalog_schema";
/// Column family name for database catalog.
const CATALOG_DATABASE_CF_NAME: &str = "cf/catalog_database";

macro_rules! impl_model_for_catalog {
    ($name:ident, $cf:ident, $key_ty:ty, $key_fn:ident) => {
        impl MetadataModel for $name {
            type KeyType = $key_ty;
            type ProstType = Self;

            fn cf_name() -> String {
                $cf.to_string()
            }

            fn to_protobuf(&self) -> Self::ProstType {
                self.clone()
            }

            fn from_protobuf(prost: Self::ProstType) -> Self {
                prost
            }

            fn key(&self) -> MetadataModelResult<Self::KeyType> {
                Ok(self.$key_fn())
            }
        }
    };
}

impl_model_for_catalog!(Source, CATALOG_SOURCE_CF_NAME, u32, get_id);
impl_model_for_catalog!(Sink, CATALOG_SINK_CF_NAME, u32, get_id);
impl_model_for_catalog!(Index, CATALOG_INDEX_CF_NAME, u32, get_id);
impl_model_for_catalog!(Table, CATALOG_TABLE_CF_NAME, u32, get_id);
impl_model_for_catalog!(Schema, CATALOG_SCHEMA_CF_NAME, u32, get_id);
impl_model_for_catalog!(Database, CATALOG_DATABASE_CF_NAME, u32, get_id);

#[cfg(test)]
mod tests {
    use futures::future;

    use super::*;
    use crate::manager::MetaSrvEnv;

    fn database_for_id(id: u32) -> Database {
        Database {
            id,
            name: format!("database_{}", id),
            owner: risingwave_common::catalog::DEFAULT_SUPER_USER_ID,
        }
    }

    #[tokio::test]
    async fn test_database() -> MetadataModelResult<()> {
        let env = MetaSrvEnv::for_test().await;
        let store = env.meta_store();
        let databases = Database::list(store).await?;
        assert!(databases.is_empty());
        assert!(Database::select(store, &0).await.unwrap().is_none());

        future::join_all((0..100).map(|i| async move { database_for_id(i).insert(store).await }))
            .await
            .into_iter()
            .collect::<MetadataModelResult<Vec<_>>>()?;

        for i in 0..100 {
            let database = Database::select(store, &i).await?.unwrap();
            assert_eq!(database, database_for_id(i));
        }

        let databases = Database::list(store).await?;
        assert_eq!(databases.len(), 100);

        for i in 0..100 {
            assert!(Database::delete(store, &i).await.is_ok());
        }
        let databases = Database::list(store).await?;
        assert!(databases.is_empty());

        Ok(())
    }
}

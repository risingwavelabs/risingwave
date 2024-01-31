// Copyright 2024 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use risingwave_meta_model_v2::catalog_version;
use risingwave_meta_model_v2::catalog_version::VersionCategory;
use risingwave_meta_model_v2::prelude::CatalogVersion;
use sea_orm::ActiveValue::Set;
use sea_orm::{ActiveModelTrait, DatabaseConnection, EntityTrait, TransactionTrait};

use crate::controller::SqlMetaStore;
use crate::model::NotificationVersion as NotificationModelV1;
use crate::storage::MetaStoreRef;
use crate::MetaResult;

pub enum NotificationVersionGenerator {
    KvGenerator(NotificationModelV1, MetaStoreRef),
    SqlGenerator(u64, DatabaseConnection),
}

// TODO: add pre-allocation if necessary
impl NotificationVersionGenerator {
    pub async fn new(
        meta_store: MetaStoreRef,
        meta_store_sql: Option<SqlMetaStore>,
    ) -> MetaResult<Self> {
        if let Some(sql) = meta_store_sql {
            let txn = sql.conn.begin().await?;
            let model = CatalogVersion::find_by_id(VersionCategory::Notification)
                .one(&txn)
                .await?;
            let current_version = model.as_ref().map(|m| m.version).unwrap_or(1) as u64;
            if model.is_none() {
                catalog_version::ActiveModel {
                    name: Set(VersionCategory::Notification),
                    version: Set(1),
                }
                .insert(&txn)
                .await?;
                txn.commit().await?;
            }

            Ok(Self::SqlGenerator(current_version, sql.conn))
        } else {
            let current_version = NotificationModelV1::new(&meta_store).await;
            Ok(Self::KvGenerator(current_version, meta_store))
        }
    }

    pub fn current_version(&self) -> u64 {
        match self {
            NotificationVersionGenerator::KvGenerator(v, _) => v.version(),
            NotificationVersionGenerator::SqlGenerator(v, _) => *v,
        }
    }

    pub async fn increase_version(&mut self) {
        match self {
            NotificationVersionGenerator::KvGenerator(v, meta_store) => {
                v.increase_version(meta_store).await
            }
            NotificationVersionGenerator::SqlGenerator(v, conn) => {
                catalog_version::ActiveModel {
                    name: Set(VersionCategory::Notification),
                    version: Set((*v + 1) as i64),
                }
                .update(conn)
                .await
                .unwrap();
                *v += 1;
            }
        }
    }
}

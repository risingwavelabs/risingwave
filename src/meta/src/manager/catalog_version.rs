// Copyright 2026 RisingWave Labs
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

use risingwave_meta_model::catalog_version;
use risingwave_meta_model::catalog_version::VersionCategory;
use risingwave_meta_model::prelude::CatalogVersion;
use sea_orm::ActiveValue::Set;
use sea_orm::{DatabaseConnection, EntityTrait, TransactionTrait};

use crate::MetaResult;
use crate::controller::SqlMetaStore;

/// The version of the latest catalog notification. It is persisted, because frontends wait for it
/// and it must not go back after meta restarts.
pub struct PersistedCatalogVersion {
    version: u64,
    conn: DatabaseConnection,
}

impl PersistedCatalogVersion {
    pub async fn new(meta_store_impl: SqlMetaStore) -> MetaResult<Self> {
        let txn = meta_store_impl.conn.begin().await?;
        let model = CatalogVersion::find_by_id(VersionCategory::Notification)
            .one(&txn)
            .await?;
        let version = model.as_ref().map(|m| m.version).unwrap_or(1) as u64;
        if model.is_none() {
            CatalogVersion::insert(catalog_version::ActiveModel {
                name: Set(VersionCategory::Notification),
                version: Set(1),
            })
            .exec(&txn)
            .await?;
            txn.commit().await?;
        }

        Ok(Self {
            version,
            conn: meta_store_impl.conn,
        })
    }

    pub fn get(&self) -> u64 {
        self.version
    }

    pub async fn set(&mut self, version: u64) {
        assert!(version > self.version, "catalog version must increase");
        CatalogVersion::update(catalog_version::ActiveModel {
            name: Set(VersionCategory::Notification),
            version: Set(version as i64),
        })
        .exec(&self.conn)
        .await
        .unwrap();
        self.version = version;
    }
}

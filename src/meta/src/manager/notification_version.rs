// Copyright 2025 RisingWave Labs
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
use sea_orm::{ActiveModelTrait, DatabaseConnection, EntityTrait, TransactionTrait};

use crate::MetaResult;
use crate::controller::SqlMetaStore;

pub struct NotificationVersionGenerator {
    current_version: u64,
    conn: DatabaseConnection,
}

// TODO: add pre-allocation if necessary
impl NotificationVersionGenerator {
    pub async fn new(meta_store_impl: SqlMetaStore) -> MetaResult<Self> {
        let txn = meta_store_impl.conn.begin().await?;
        let model = CatalogVersion::find_by_id(VersionCategory::Notification)
            .one(&txn)
            .await?;
        let current_version = model.as_ref().map(|m| m.version).unwrap_or(1) as u64;
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
            current_version,
            conn: meta_store_impl.conn,
        })
    }

    pub fn current_version(&self) -> u64 {
        self.current_version
    }

    pub async fn increase_version(&mut self) {
        catalog_version::ActiveModel {
            name: Set(VersionCategory::Notification),
            version: Set((self.current_version + 1) as i64),
        }
        .update(&self.conn)
        .await
        .unwrap();
        self.current_version += 1;
    }
}

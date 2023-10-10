// Copyright 2023 RisingWave Labs
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

use risingwave_pb::hummock::HummockPinnedVersion;
use sea_orm::sea_query::OnConflict;
use sea_orm::ActiveValue::{Set, Unchanged};
use sea_orm::EntityTrait;

use crate::model::MetadataModelResult;
use crate::model_v2::hummock_pinned_version;
use crate::model_v2::trx::Transactional;

#[async_trait::async_trait]
impl Transactional for HummockPinnedVersion {
    async fn upsert_in_transaction(
        &self,
        trx: &crate::model_v2::trx::Transaction,
    ) -> MetadataModelResult<()> {
        // TODO: error type conversion
        // TODO: integer type conversion
        let m = hummock_pinned_version::ActiveModel {
            context_id: Unchanged(self.context_id.try_into().unwrap()),
            min_pinned_id: Set(self.min_pinned_id.try_into().unwrap()),
        };
        hummock_pinned_version::Entity::insert(m)
            .on_conflict(
                OnConflict::column(hummock_pinned_version::Column::ContextId)
                    .update_columns([hummock_pinned_version::Column::MinPinnedId])
                    .to_owned(),
            )
            .exec(trx)
            .await
            .unwrap();
        Ok(())
    }

    async fn delete_in_transaction(
        &self,
        trx: &crate::model_v2::trx::Transaction,
    ) -> MetadataModelResult<()> {
        // TODO: error type conversion
        // TODO: integer type conversion
        let id: i32 = self.context_id.try_into().unwrap();
        hummock_pinned_version::Entity::delete_by_id(id)
            .exec(trx)
            .await
            .unwrap();
        Ok(())
    }
}

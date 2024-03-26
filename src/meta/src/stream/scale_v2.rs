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

// pub struct Reschedule {}

pub struct ScaleControllerV2 {
    metadata_manager: MetadataManager,
}

pub struct RescheduleV2 {
    plan: HashMap<FragmentId, HashMap<WorkerId, usize>>,
}

use std::collections::HashMap;

use mv2::FragmentId;
use risingwave_meta_model_v2 as mv2;

use risingwave_meta_model_v2::WorkerId;
use sea_orm::TransactionTrait;


use crate::manager::MetadataManager;

use crate::MetaResult;

impl ScaleControllerV2 {
    pub async fn reschedule(&self, reschedule: RescheduleV2) -> MetaResult<()> {
        // let metadata_manager = self.metadata_manager.as_v2_ref();
        //
        // let inner = metadata_manager.catalog_controller.inner.write().await;
        // let txn = inner.db.begin().await?;
        //
        // let fragment_ids = reschedule.plan.keys().cloned().collect::<Vec<_>>();
        //
        // let _working_set = metadata_manager
        //     .catalog_controller
        //     .resolve_working_set_for_reschedule(&txn, fragment_ids)
        //     .await?;

        Ok(())
    }
}

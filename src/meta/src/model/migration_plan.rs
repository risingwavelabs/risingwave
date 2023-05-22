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

use std::collections::HashMap;

use prost::Message;
use risingwave_pb::common::ParallelUnit;
use risingwave_pb::meta::PbMigrationPlan;

use crate::storage::{MetaStore, MetaStoreError, MetaStoreResult, Snapshot, DEFAULT_COLUMN_FAMILY};

/// `migration_plan` in bytes.
pub const MIGRATION_PLAN_KEY: &[u8] = &[
    109, 105, 103, 114, 97, 116, 105, 111, 110, 95, 112, 108, 97, 110,
];

type WorkerId = u32;
type ParallelUnitId = u32;

#[derive(Debug, Default, Clone)]
pub struct MigrationPlan {
    pub worker_plan: HashMap<WorkerId, WorkerId>,
    pub parallel_unit_plan: HashMap<ParallelUnitId, ParallelUnit>,
}

impl MigrationPlan {
    pub async fn get<S: MetaStore>(store: &S) -> MetaStoreResult<Self> {
        Self::get_at_snapshot::<S>(&store.snapshot().await).await
    }

    pub async fn get_at_snapshot<S: MetaStore>(snapshot: &S::Snapshot) -> MetaStoreResult<Self> {
        snapshot
            .get_cf(DEFAULT_COLUMN_FAMILY, MIGRATION_PLAN_KEY)
            .await
            .map(|val| {
                PbMigrationPlan::decode(val.as_slice())
                    .map(Into::into)
                    .unwrap()
            })
            .or_else(|e| {
                if matches!(e, MetaStoreError::ItemNotFound(_)) {
                    Ok(MigrationPlan::default())
                } else {
                    Err(e)
                }
            })
    }

    pub fn is_empty(&self) -> bool {
        self.worker_plan.is_empty()
    }

    pub async fn insert<S: MetaStore>(&self, store: &S) -> MetaStoreResult<()> {
        store
            .put_cf(
                DEFAULT_COLUMN_FAMILY,
                MIGRATION_PLAN_KEY.to_vec(),
                PbMigrationPlan::from(self.clone()).encode_to_vec(),
            )
            .await
    }

    pub async fn delete<S: MetaStore>(&self, store: &S) -> MetaStoreResult<()> {
        store
            .delete_cf(DEFAULT_COLUMN_FAMILY, MIGRATION_PLAN_KEY)
            .await
    }
}

impl From<PbMigrationPlan> for MigrationPlan {
    fn from(plan: PbMigrationPlan) -> Self {
        MigrationPlan {
            worker_plan: plan.migration_plan,
            parallel_unit_plan: plan.parallel_unit_migration_plan,
        }
    }
}

impl From<MigrationPlan> for PbMigrationPlan {
    fn from(plan: MigrationPlan) -> Self {
        PbMigrationPlan {
            migration_plan: plan.worker_plan,
            parallel_unit_migration_plan: plan.parallel_unit_plan,
        }
    }
}

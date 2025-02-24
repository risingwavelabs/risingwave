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

use risingwave_common::catalog::{OBJECT_ID_PLACEHOLDER, TableId, UserId};
use risingwave_common::util::epoch::Epoch;
use risingwave_pb::catalog::PbSubscription;
use risingwave_pb::catalog::subscription::PbSubscriptionState;

use super::OwnedByUserCatalog;
use crate::WithOptions;
use crate::error::{ErrorCode, Result};
use crate::handler::util::convert_interval_to_u64_seconds;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(test, derive(Default))]
pub struct SubscriptionCatalog {
    /// Id of the subscription. For debug now.
    pub id: SubscriptionId,

    /// Name of the subscription. For debug now.
    pub name: String,

    /// Full SQL definition of the subscription. For debug now.
    pub definition: String,

    /// The retention seconds of the subscription.
    pub retention_seconds: u64,

    /// The database id
    pub database_id: u32,

    /// The schema id
    pub schema_id: u32,

    /// The subscription depends on the upstream list
    pub dependent_table_id: TableId,

    /// The user id
    pub owner: UserId,

    pub initialized_at_epoch: Option<Epoch>,
    pub created_at_epoch: Option<Epoch>,

    pub created_at_cluster_version: Option<String>,
    pub initialized_at_cluster_version: Option<String>,
}

#[derive(Clone, Copy, Debug, Default, Hash, PartialOrd, PartialEq, Eq, Ord)]
pub struct SubscriptionId {
    pub subscription_id: u32,
}

impl SubscriptionId {
    pub const fn new(subscription_id: u32) -> Self {
        SubscriptionId { subscription_id }
    }

    /// Sometimes the id field is filled later, we use this value for better debugging.
    pub const fn placeholder() -> Self {
        SubscriptionId {
            subscription_id: OBJECT_ID_PLACEHOLDER,
        }
    }

    pub fn subscription_id(&self) -> u32 {
        self.subscription_id
    }
}

impl SubscriptionCatalog {
    pub fn set_retention_seconds(&mut self, properties: &WithOptions) -> Result<()> {
        let retention_seconds_str = properties.get("retention").ok_or_else(|| {
            ErrorCode::InternalError("Subscription retention time not set.".to_owned())
        })?;
        let retention_seconds = convert_interval_to_u64_seconds(retention_seconds_str)?;
        self.retention_seconds = retention_seconds;
        Ok(())
    }

    pub fn create_sql(&self) -> String {
        self.definition.clone()
    }

    pub fn to_proto(&self) -> PbSubscription {
        PbSubscription {
            id: self.id.subscription_id,
            name: self.name.clone(),
            definition: self.definition.clone(),
            retention_seconds: self.retention_seconds,
            database_id: self.database_id,
            schema_id: self.schema_id,
            initialized_at_epoch: self.initialized_at_epoch.map(|e| e.0),
            created_at_epoch: self.created_at_epoch.map(|e| e.0),
            owner: self.owner.into(),
            initialized_at_cluster_version: self.initialized_at_cluster_version.clone(),
            created_at_cluster_version: self.created_at_cluster_version.clone(),
            dependent_table_id: self.dependent_table_id.table_id,
            subscription_state: PbSubscriptionState::Init.into(),
        }
    }
}

impl From<&PbSubscription> for SubscriptionCatalog {
    fn from(prost: &PbSubscription) -> Self {
        Self {
            id: SubscriptionId::new(prost.id),
            name: prost.name.clone(),
            definition: prost.definition.clone(),
            retention_seconds: prost.retention_seconds,
            database_id: prost.database_id,
            schema_id: prost.schema_id,
            dependent_table_id: TableId::new(prost.dependent_table_id),
            owner: prost.owner.into(),
            created_at_epoch: prost.created_at_epoch.map(Epoch::from),
            initialized_at_epoch: prost.initialized_at_epoch.map(Epoch::from),
            created_at_cluster_version: prost.created_at_cluster_version.clone(),
            initialized_at_cluster_version: prost.initialized_at_cluster_version.clone(),
        }
    }
}

impl OwnedByUserCatalog for SubscriptionCatalog {
    fn owner(&self) -> u32 {
        self.owner.user_id
    }
}

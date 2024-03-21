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

use std::collections::{BTreeMap, HashSet};

use itertools::Itertools;
use risingwave_common::catalog::{ColumnCatalog, TableId, UserId, OBJECT_ID_PLACEHOLDER};
use risingwave_common::util::epoch::Epoch;
use risingwave_common::util::sort_util::ColumnOrder;
use risingwave_pb::catalog::{PbStreamJobStatus, PbSubscription};

use super::OwnedByUserCatalog;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(test, derive(Default))]
pub struct SubscriptionCatalog {
    /// Id of the subscription. For debug now.
    pub id: SubscriptionId,

    /// Name of the subscription. For debug now.
    pub name: String,

    /// Full SQL definition of the subscription. For debug now.
    pub definition: String,

    /// All columns of the subscription. Note that this is NOT sorted by columnId in the vector.
    pub columns: Vec<ColumnCatalog>,

    /// Primiary keys of the subscription. Derived by the frontend.
    pub plan_pk: Vec<ColumnOrder>,

    /// Distribution key indices of the subscription. For example, if `distribution_key = [1, 2]`, then the
    /// distribution keys will be `columns[1]` and `columns[2]`.
    pub distribution_key: Vec<usize>,

    /// The properties of the subscription, only `retention`.
    pub properties: BTreeMap<String, String>,

    /// The upstream table name on which the subscription depends
    pub subscription_from_name: String,

    /// The database id
    pub database_id: u32,

    /// The schema id
    pub schema_id: u32,

    /// The subscription depends on the upstream list
    pub dependent_relations: Vec<TableId>,

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
    pub fn add_dependent_relations(mut self, mut dependent_relations: HashSet<TableId>) -> Self {
        dependent_relations.extend(self.dependent_relations);
        self.dependent_relations = dependent_relations.into_iter().collect();
        self
    }

    pub fn create_sql(&self) -> String {
        self.definition.clone()
    }

    pub fn to_proto(&self) -> PbSubscription {
        assert!(!self.dependent_relations.is_empty());
        PbSubscription {
            id: self.id.subscription_id,
            name: self.name.clone(),
            definition: self.definition.clone(),
            column_catalogs: self
                .columns
                .iter()
                .map(|column| column.to_protobuf())
                .collect_vec(),
            plan_pk: self.plan_pk.iter().map(|k| k.to_protobuf()).collect_vec(),
            distribution_key: self.distribution_key.iter().map(|k| *k as _).collect_vec(),
            subscription_from_name: self.subscription_from_name.clone(),
            properties: self.properties.clone().into_iter().collect(),
            database_id: self.database_id,
            schema_id: self.schema_id,
            dependent_relations: self
                .dependent_relations
                .iter()
                .map(|k| k.table_id)
                .collect_vec(),
            initialized_at_epoch: self.initialized_at_epoch.map(|e| e.0),
            created_at_epoch: self.created_at_epoch.map(|e| e.0),
            owner: self.owner.into(),
            stream_job_status: PbStreamJobStatus::Creating.into(),
            initialized_at_cluster_version: self.initialized_at_cluster_version.clone(),
            created_at_cluster_version: self.created_at_cluster_version.clone(),
        }
    }
}

impl From<&PbSubscription> for SubscriptionCatalog {
    fn from(prost: &PbSubscription) -> Self {
        Self {
            id: SubscriptionId::new(prost.id),
            name: prost.name.clone(),
            definition: prost.definition.clone(),
            columns: prost
                .column_catalogs
                .iter()
                .map(|c| ColumnCatalog::from(c.clone()))
                .collect_vec(),
            plan_pk: prost
                .plan_pk
                .iter()
                .map(ColumnOrder::from_protobuf)
                .collect_vec(),
            distribution_key: prost.distribution_key.iter().map(|k| *k as _).collect_vec(),
            subscription_from_name: prost.subscription_from_name.clone(),
            properties: prost.properties.clone().into_iter().collect(),
            database_id: prost.database_id,
            schema_id: prost.schema_id,
            dependent_relations: prost
                .dependent_relations
                .iter()
                .map(|k| TableId::new(*k))
                .collect_vec(),
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

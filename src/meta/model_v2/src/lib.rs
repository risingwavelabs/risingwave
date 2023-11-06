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

use std::collections::{BTreeMap, HashMap};

use risingwave_pb::catalog::{PbCreateType, PbStreamJobStatus};
use sea_orm::{DeriveActiveEnum, EnumIter, FromJsonQueryResult};
use serde::{Deserialize, Serialize};

pub mod prelude;

pub mod actor;
pub mod cluster;
pub mod compaction_config;
pub mod compaction_status;
pub mod compaction_task;
pub mod connection;
pub mod database;
pub mod fragment;
pub mod function;
pub mod hummock_pinned_snapshot;
pub mod hummock_pinned_version;
pub mod hummock_version_delta;
pub mod hummock_version_stats;
pub mod index;
pub mod object;
pub mod object_dependency;
pub mod schema;
pub mod sink;
pub mod source;
pub mod system_parameter;
pub mod table;
pub mod user;
pub mod user_privilege;
pub mod view;
pub mod worker;
pub mod worker_property;

pub type WorkerId = i32;

pub type TransactionId = i32;

pub type ObjectId = i32;
pub type DatabaseId = ObjectId;
pub type SchemaId = ObjectId;
pub type TableId = ObjectId;
pub type SourceId = ObjectId;
pub type SinkId = ObjectId;
pub type IndexId = ObjectId;
pub type ViewId = ObjectId;
pub type FunctionId = ObjectId;
pub type ConnectionId = ObjectId;
pub type UserId = i32;
pub type PrivilegeId = i32;

pub type HummockVersionId = i64;
pub type Epoch = i64;
pub type CompactionGroupId = i64;
pub type CompactionTaskId = i64;
pub type HummockSstableObjectId = i64;

pub type FragmentId = i32;

pub type ActorId = i32;

#[derive(Clone, Debug, PartialEq, Eq, EnumIter, DeriveActiveEnum)]
#[sea_orm(rs_type = "String", db_type = "String(None)")]
pub enum JobStatus {
    #[sea_orm(string_value = "CREATING")]
    Creating,
    #[sea_orm(string_value = "CREATED")]
    Created,
}

impl From<JobStatus> for PbStreamJobStatus {
    fn from(job_status: JobStatus) -> Self {
        match job_status {
            JobStatus::Creating => Self::Creating,
            JobStatus::Created => Self::Created,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, EnumIter, DeriveActiveEnum)]
#[sea_orm(rs_type = "String", db_type = "String(None)")]
pub enum CreateType {
    #[sea_orm(string_value = "BACKGROUND")]
    Background,
    #[sea_orm(string_value = "FOREGROUND")]
    Foreground,
}

impl From<CreateType> for PbCreateType {
    fn from(create_type: CreateType) -> Self {
        match create_type {
            CreateType::Background => Self::Background,
            CreateType::Foreground => Self::Foreground,
        }
    }
}

/// Defines struct with a single pb field that derives `FromJsonQueryResult`, it will helps to map json value stored in database to Pb struct.
macro_rules! derive_from_json_struct {
    ($struct_name:ident, $field_type:ty) => {
        #[derive(Clone, Debug, PartialEq, FromJsonQueryResult, Serialize, Deserialize, Default)]
        pub struct $struct_name(pub $field_type);
        impl Eq for $struct_name {}

        impl $struct_name {
            pub fn into_inner(self) -> $field_type {
                self.0
            }

            pub fn inner_ref(&self) -> &$field_type {
                &self.0
            }
        }
    };
}

pub(crate) use derive_from_json_struct;

derive_from_json_struct!(I32Array, Vec<i32>);

impl From<Vec<u32>> for I32Array {
    fn from(value: Vec<u32>) -> Self {
        Self(value.into_iter().map(|id| id as _).collect())
    }
}

impl I32Array {
    pub fn into_u32_array(self) -> Vec<u32> {
        self.0.into_iter().map(|id| id as _).collect()
    }
}

derive_from_json_struct!(ActorUpstreamActors, BTreeMap<FragmentId, Vec<ActorId>>);

impl From<BTreeMap<u32, Vec<u32>>> for ActorUpstreamActors {
    fn from(val: BTreeMap<u32, Vec<u32>>) -> Self {
        let mut map = BTreeMap::new();
        for (k, v) in val {
            map.insert(k as _, v.into_iter().map(|a| a as _).collect());
        }
        Self(map)
    }
}

derive_from_json_struct!(DataType, risingwave_pb::data::DataType);
derive_from_json_struct!(DataTypeArray, Vec<risingwave_pb::data::DataType>);
derive_from_json_struct!(FieldArray, Vec<risingwave_pb::plan_common::Field>);
derive_from_json_struct!(Property, HashMap<String, String>);
derive_from_json_struct!(ColumnCatalog, risingwave_pb::plan_common::PbColumnCatalog);
derive_from_json_struct!(
    ColumnCatalogArray,
    Vec<risingwave_pb::plan_common::PbColumnCatalog>
);
derive_from_json_struct!(StreamSourceInfo, risingwave_pb::catalog::PbStreamSourceInfo);
derive_from_json_struct!(WatermarkDesc, risingwave_pb::catalog::PbWatermarkDesc);
derive_from_json_struct!(
    WatermarkDescArray,
    Vec<risingwave_pb::catalog::PbWatermarkDesc>
);
derive_from_json_struct!(ExprNodeArray, Vec<risingwave_pb::expr::PbExprNode>);
derive_from_json_struct!(ColumnOrderArray, Vec<risingwave_pb::common::PbColumnOrder>);
derive_from_json_struct!(SinkFormatDesc, risingwave_pb::catalog::PbSinkFormatDesc);
derive_from_json_struct!(Cardinality, risingwave_pb::plan_common::PbCardinality);
derive_from_json_struct!(TableVersion, risingwave_pb::catalog::table::PbTableVersion);
derive_from_json_struct!(
    PrivateLinkService,
    risingwave_pb::catalog::connection::PbPrivateLinkService
);
derive_from_json_struct!(AuthInfo, risingwave_pb::user::PbAuthInfo);

derive_from_json_struct!(StreamNode, risingwave_pb::stream_plan::PbStreamNode);
derive_from_json_struct!(Dispatchers, Vec<risingwave_pb::stream_plan::Dispatcher>);

derive_from_json_struct!(ConnectorSplits, risingwave_pb::source::ConnectorSplits);
derive_from_json_struct!(
    ActorStatus,
    risingwave_pb::meta::table_fragments::PbActorStatus
);
derive_from_json_struct!(VnodeBitmap, risingwave_pb::common::Buffer);

derive_from_json_struct!(
    FragmentVnodeMapping,
    risingwave_pb::common::ParallelUnitMapping
);

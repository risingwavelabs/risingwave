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
pub mod election_leader;
pub mod election_member;
pub mod ext;
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
pub mod trx;
pub mod user;
pub mod user_privilege;
pub mod view;
pub mod worker;
pub mod worker_property;

pub type WorkerId = u32;
pub type TransactionId = u32;

pub type ObjectId = u32;
pub type DatabaseId = ObjectId;
pub type SchemaId = ObjectId;
pub type TableId = ObjectId;
pub type SourceId = ObjectId;
pub type SinkId = ObjectId;
pub type IndexId = ObjectId;
pub type ViewId = ObjectId;
pub type FunctionId = ObjectId;
pub type ConnectionId = ObjectId;
pub type UserId = u32;

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

/// Defines struct with a single pb field that derives FromJsonQueryResult, it will helps to map json value stored in database to Pb struct.
macro_rules! derive_from_json_struct {
    ($struct_name:ident, $field_type:ty) => {
        #[derive(Clone, Debug, PartialEq, FromJsonQueryResult, Serialize, Deserialize, Default)]
        pub struct $struct_name(pub $field_type);
        impl Eq for $struct_name {}
    };
}

derive_from_json_struct!(I32Array, Vec<i32>);
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

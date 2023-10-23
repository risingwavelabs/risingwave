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

use sea_orm::FromJsonQueryResult;
use serde::{Deserialize, Serialize};

pub mod prelude;

pub mod actor;
pub mod cluster;
pub mod compaction_config;
pub mod compaction_status;
pub mod compaction_task;
pub mod connection;
pub mod database;
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

#[derive(Clone, Debug, PartialEq, FromJsonQueryResult, Eq, Serialize, Deserialize, Default)]
pub struct I32Array(pub Vec<i32>);

#[derive(Clone, Debug, PartialEq, FromJsonQueryResult, Eq, Serialize, Deserialize, Default)]
pub struct DataType(pub risingwave_pb::data::DataType);

#[derive(Clone, Debug, PartialEq, FromJsonQueryResult, Eq, Serialize, Deserialize, Default)]
pub struct DataTypeArray(pub Vec<risingwave_pb::data::DataType>);

#[derive(Clone, Debug, PartialEq, FromJsonQueryResult, Serialize, Deserialize, Default)]
pub struct FieldArray(pub Vec<risingwave_pb::plan_common::Field>);

impl Eq for FieldArray {}

#[derive(Clone, Debug, PartialEq, FromJsonQueryResult, Eq, Serialize, Deserialize, Default)]
pub struct Property(pub HashMap<String, String>);

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

use risingwave_common::types::DataType;

use crate::catalog::system_catalog::SystemCatalogColumnsDef;

/// `rw_worker_nodes` contains all information about the compute nodes in the cluster.
/// TODO: Add other type of nodes if necessary in the future.
pub const RW_WORKER_NODES_TABLE_NAME: &str = "rw_worker_nodes";

pub const RW_WORKER_NODES_COLUMNS: &[SystemCatalogColumnsDef<'_>] = &[
    (DataType::Int32, "id"),
    (DataType::Varchar, "host"),
    (DataType::Varchar, "port"),
    (DataType::Varchar, "type"),
    (DataType::Varchar, "state"),
    (DataType::Int32, "parallelism"),
    (DataType::Boolean, "is_streaming"),
    (DataType::Boolean, "is_serving"),
    (DataType::Boolean, "is_unschedulable"),
];

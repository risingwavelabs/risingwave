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

pub const RW_META_SNAPSHOT_TABLE_NAME: &str = "rw_meta_snapshot";

pub const RW_META_SNAPSHOT_COLUMNS: &[SystemCatalogColumnsDef<'_>] = &[
    (DataType::Int64, "meta_snapshot_id"),
    (DataType::Int64, "hummock_version_id"),
    // the smallest epoch this meta snapshot includes
    (DataType::Int64, "safe_epoch"),
    // human-readable timestamp of safe_epoch
    (DataType::Timestamp, "safe_epoch_ts"),
    // the largest epoch this meta snapshot includes
    (DataType::Int64, "max_committed_epoch"),
    // human-readable timestamp of max_committed_epoch
    (DataType::Timestamp, "max_committed_epoch_ts"),
];

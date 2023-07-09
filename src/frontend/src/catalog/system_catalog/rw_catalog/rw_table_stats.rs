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

pub const RW_TABLE_STATS_TABLE_NAME: &str = "rw_table_stats";
pub const RW_TABLE_STATS_TABLE_ID_INDEX: usize = 0;
pub const RW_TABLE_STATS_KEY_SIZE_INDEX: usize = 2;
pub const RW_TABLE_STATS_VALUE_SIZE_INDEX: usize = 3;

pub const RW_TABLE_STATS_COLUMNS: &[SystemCatalogColumnsDef<'_>] = &[
    (DataType::Int32, "id"),
    (DataType::Int64, "total_key_count"),
    (DataType::Int64, "total_key_size"),
    (DataType::Int64, "total_value_size"),
];

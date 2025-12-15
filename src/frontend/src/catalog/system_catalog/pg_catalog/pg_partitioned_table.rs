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

use risingwave_common::types::Fields;
use risingwave_frontend_macro::system_catalog;

/// The catalog `pg_partitioned_table` stores information about how tables are partitioned. Reference: `https://www.postgresql.org/docs/current/catalog-pg-partitioned-table.html`
#[system_catalog(view, "pg_catalog.pg_partitioned_table")]
#[derive(Fields)]
struct PgPartitionedTable {
    partrelid: i32,
    partstrat: String,
    partnatts: i16,
    partdefid: i32,
    partattrs: Vec<i16>,
    partclass: Vec<i32>,
    partcollation: Vec<i32>,
    partexprs: Option<String>,
}

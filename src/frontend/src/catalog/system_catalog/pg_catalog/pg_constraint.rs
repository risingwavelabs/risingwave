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

use risingwave_common::types::Fields;
use risingwave_frontend_macro::system_catalog;

/// The catalog `pg_constraint` records information about table and index inheritance hierarchies.
/// Ref: [`https://www.postgresql.org/docs/current/catalog-pg-constraint.html`]
/// This is introduced only for pg compatibility and is not used in our system.
#[system_catalog(view, "pg_catalog.pg_constraint")]
#[derive(Fields)]
struct PgConstraint {
    oid: i32,
    conname: String,
    connamespace: i32,
    contype: String,
    condeferrable: bool,
    convalidated: bool,
    conrelid: i32,
    contypid: i32,
    conindid: i32,
    conparentid: i32,
    confrelid: i32,
    confupdtype: String,
    confdeltype: String,
    confmatchtype: String,
    conislocal: bool,
    coninhcount: i32,
    connoinherit: bool,
    conkey: Vec<i16>,
    confkey: Vec<i16>,
    conpfeqop: Vec<i32>,
    conppeqop: Vec<i32>,
    conffeqop: Vec<i32>,
    confdelsetcols: Vec<i16>,
    conexclop: Vec<i32>,
    conbin: String,
}

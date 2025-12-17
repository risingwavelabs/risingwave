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

/// The catalog `pg_proc` stores information about functions, procedures, aggregate functions, and
/// window functions (collectively also known as routines).
/// Ref: `https://www.postgresql.org/docs/current/catalog-pg-proc.html`
// TODO: read real data including oid etc in rw, currently there are no such data in rw.
// more details can be found here: https://github.com/postgres/postgres/blob/master/src/include/catalog/pg_proc.dat
#[system_catalog(view, "pg_catalog.pg_proc")]
#[derive(Fields)]
struct PgProc {
    oid: i32,
    proname: String,
    pronamespace: i32,
    proowner: i32,
    proargdefaults: i32,
    // Data type of the return value, refer to pg_type.
    prorettype: i32,
    prokind: String,
    proargtypes: Vec<i32>,
}

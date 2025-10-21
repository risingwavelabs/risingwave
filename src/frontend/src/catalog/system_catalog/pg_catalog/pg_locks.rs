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

/// The catalog `pg_locks` provides access to information about the locks held by active processes
/// within the database server.
/// Reference: `https://www.postgresql.org/docs/current/view-pg-locks.html`.
/// Currently, we don't have any type of lock.
#[system_catalog(view, "pg_catalog.pg_locks")]
#[derive(Fields)]
struct PgLock {
    locktype: String,
    database: i32, // oid
    relation: i32, // oid
    page: i32,
    tuple: i16,
    virtualxid: String,
    transactionid: i32,
    classid: i32, // oid
    objid: i32,   // oid
    objsubid: i16,
    virtualtransaction: String,
    pid: i32,
    mode: String,
    granted: bool,
    fastpath: bool,
    waitstart: String,
}

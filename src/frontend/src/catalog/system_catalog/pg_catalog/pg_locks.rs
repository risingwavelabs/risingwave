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

use std::sync::LazyLock;

use risingwave_common::catalog::PG_CATALOG_SCHEMA_NAME;
use risingwave_common::types::DataType;

use crate::catalog::system_catalog::{infer_dummy_view_sql, BuiltinView, SystemCatalogColumnsDef};

pub const PG_LOCKS_COLUMNS: &[SystemCatalogColumnsDef<'_>] = &[
    (DataType::Varchar, "locktype"),
    (DataType::Int32, "database"), // oid
    (DataType::Int32, "relation"), // oid
    (DataType::Int32, "page"),
    (DataType::Int16, "tuple"),
    (DataType::Varchar, "virtualxid"),
    (DataType::Int32, "transactionid"), // xid
    (DataType::Int32, "classid"),       // oid
    (DataType::Int32, "objid"),         // oid
    (DataType::Int16, "objsubid"),
    (DataType::Varchar, "virtualtransaction"),
    (DataType::Int32, "pid"),
    (DataType::Varchar, "mode"),
    (DataType::Boolean, "granted"),
    (DataType::Boolean, "fastpath"),
    (DataType::Timestamptz, "waitstart"),
];

/// The catalog `pg_locks` provides access to information about the locks held by active processes
/// within the database server.
/// Reference: [`https://www.postgresql.org/docs/current/view-pg-locks.html`].
/// Currently, we don't have any type of lock.
pub static PG_LOCKS: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "pg_locks",
    schema: PG_CATALOG_SCHEMA_NAME,
    columns: PG_LOCKS_COLUMNS,
    sql: infer_dummy_view_sql(PG_LOCKS_COLUMNS),
});

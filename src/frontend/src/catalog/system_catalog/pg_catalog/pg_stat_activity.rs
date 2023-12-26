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

pub const PG_STAT_ACTIVITY_COLUMNS: &[SystemCatalogColumnsDef<'_>] = &[
    (DataType::Int32, "pid"),       // Process ID of this backend.
    (DataType::Int32, "datid"),     // OID of the database this backend is connected to.
    (DataType::Varchar, "datname"), // Name of the database this backend is connected to.
    (DataType::Int32, "leader_pid"), /* Process ID of the parallel group leader, if this process
                                     * is a parallel query worker. NULL if this process is a
                                     * parallel group leader or does not participate in
                                     * parallel query. */
    (DataType::Int32, "usesysid"), // OID of the user logged into this backend.
    (DataType::Varchar, "usename"), // Name of the user logged into this backend.
    (DataType::Varchar, "application_name"), /* Name of the application that is connected to
                                    * this backend. */
    (DataType::Varchar, "client_addr"), // IP address of the client connected to this backend.
    (DataType::Varchar, "client_hostname"), /* Host name of the connected client, as reported by a
                                         * reverse DNS lookup of client_addr. */
    (DataType::Int16, "client_port"), /* TCP port number that the client is using for
                                       * communication with this backend, or -1 if a Unix socket
                                       * is used. */
];

/// The `pg_stat_activity` view will have one row per server process, showing information related to
/// the current activity of that process.
/// Ref: [`https://www.postgresql.org/docs/current/monitoring-stats.html#MONITORING-PG-STAT-ACTIVITY-VIEW`]
pub static PG_STAT_ACTIVITY: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "pg_stat_activity",
    schema: PG_CATALOG_SCHEMA_NAME,
    columns: PG_STAT_ACTIVITY_COLUMNS,
    sql: infer_dummy_view_sql(PG_STAT_ACTIVITY_COLUMNS),
});

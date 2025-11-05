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

/// The `pg_stat_activity` view will have one row per server process, showing information related to
/// the current activity of that process.
/// Ref: `https://www.postgresql.org/docs/current/monitoring-stats.html#MONITORING-PG-STAT-ACTIVITY-VIEW`
#[system_catalog(view, "pg_catalog.pg_stat_activity")]
#[derive(Fields)]
struct PgStatActivity {
    /// Process ID of this backend.
    pid: i32,
    /// OID of the database this backend is connected to.
    datid: i32,
    /// Name of the database this backend is connected to.
    datname: String,
    /// Process ID of the parallel group leader, if this process is a parallel query worker.
    /// NULL if this process is a parallel group leader or does not participate in parallel query.
    leader_pid: i32,
    /// OID of the user logged into this backend.
    usesysid: i32,
    /// Name of the user logged into this backend.
    usename: String,
    /// Name of the application that is connected to this backend.
    application_name: String,
    /// IP address of the client connected to this backend.
    client_addr: String,
    /// Host name of the connected client, as reported by a reverse DNS lookup of `client_addr`.
    client_hostname: String,
    /// TCP port number that the client is using for communication with this backend, or -1 if a Unix socket is used.
    client_port: i16,
}

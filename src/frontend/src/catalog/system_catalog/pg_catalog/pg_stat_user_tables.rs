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

use risingwave_common::types::{Fields, Timestamptz};
use risingwave_frontend_macro::system_catalog;

/// The `pg_stat_user_tables` view will contain one row for each user table in the current database,
/// showing statistics about accesses to that specific table.
/// Ref: [`https://www.postgresql.org/docs/current/monitoring-stats.html#MONITORING-PG-STAT-ALL-TABLES-VIEW`]
#[system_catalog(
    view,
    "pg_catalog.pg_stat_user_tables",
    "SELECT
        rr.id as relid,
        rs.name as schemaname,
        rr.name as relname,
        NULL::bigint as seq_scan,
        NULL::timestamptz as last_seq_scan,
        NULL::bigint as seq_tup_read,
        NULL::bigint as idx_scan,
        NULL::timestamptz as last_idx_scan,
        NULL::bigint as idx_tup_fetch,
        NULL::bigint as n_tup_ins,
        NULL::bigint as n_tup_del,
        NULL::bigint as n_tup_hot_upd,
        NULL::bigint as n_tup_newpage_upd,
        rts.total_key_count as n_live_tup,
        NULL::bigint as n_dead_tup,
        NULL::bigint as n_mod_since_analyze,
        NULL::bigint as n_ins_since_vacuum,
        NULL::timestamptz as last_vacuum,
        NULL::timestamptz as last_autovacuum,
        NULL::timestamptz as last_analyze,
        NULL::timestamptz as last_autoanalyze,
        NULL::bigint as vacuum_count,
        NULL::bigint as autovacuum_count,
        NULL::bigint as analyze_count,
        NULL::bigint as autoanalyze_count
    FROM
        rw_relations rr
        left join rw_table_stats rts on rr.id = rts.id
        join rw_schemas rs on schema_id = rs.id
    WHERE
        rs.name != 'rw_catalog'
        AND rs.name != 'pg_catalog'
        AND rs.name != 'information_schema'
"
)]
#[derive(Fields)]
struct PgStatUserTables {
    relid: i32,
    schemaname: String,
    relname: String,
    seq_scan: i64,
    last_seq_scan: Timestamptz,
    seq_tup_read: i64,
    idx_scan: i64,
    last_idx_scan: Timestamptz,
    idx_tup_fetch: i64,
    n_tup_ins: i64,
    n_tup_del: i64,
    n_tup_hot_upd: i64,
    n_tup_newpage_upd: i64,
    n_live_tup: i64,
    n_dead_tup: i64,
    n_mod_since_analyze: i64,
    n_ins_since_vacuum: i64,
    last_vacuum: Timestamptz,
    last_autovacuum: Timestamptz,
    last_analyze: Timestamptz,
    last_autoanalyze: Timestamptz,
    vacuum_count: i64,
    autovacuum_count: i64,
    analyze_count: i64,
    autoanalyze_count: i64,
}

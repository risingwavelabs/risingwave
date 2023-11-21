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

mod non_zero64;
mod over_window;
mod query_mode;
mod search_path;
pub mod sink_decouple;
mod transaction_isolation_level;
mod visibility_mode;

use chrono_tz::Tz;
pub use over_window::OverWindowCachePolicy;
pub use query_mode::QueryMode;
use risingwave_common_proc_macro::Guc;
pub use search_path::{SearchPath, USER_NAME_WILD_CARD};

use self::non_zero64::ConfigNonZeroU64;
use crate::error::{ErrorCode, Result as RwResult};
use crate::session_config::sink_decouple::SinkDecouple;
use crate::session_config::transaction_isolation_level::IsolationLevel;
pub use crate::session_config::visibility_mode::VisibilityMode;

pub const GUC_LIST_SEP: &str = ",";

#[derive(Guc)]
pub struct ConfigMap {
    /// If `RW_IMPLICIT_FLUSH` is on, then every INSERT/UPDATE/DELETE statement will block
    /// until the entire dataflow is refreshed. In other words, every related table & MV will
    /// be able to see the write.
    #[guc_opts(default = false, rename = "RW_IMPLICIT_FLUSH")]
    implicit_flush: bool,

    /// If `CREATE_COMPACTION_GROUP_FOR_MV` is on, dedicated compaction groups will be created in
    /// MV creation.
    #[guc_opts(default = false)]
    create_compaction_group_for_mv: bool,

    /// A temporary config variable to force query running in either local or distributed mode.
    /// The default value is auto which means let the system decide to run batch queries in local
    /// or distributed mode automatically.
    #[guc_opts(default = QueryMode::Auto)]
    query_mode: QueryMode,

    /// Sets the application name to be reported in statistics and logs.
    /// See <https://www.postgresql.org/docs/14/runtime-config-logging.html#:~:text=What%20to%20Log-,application_name,-(string)>
    #[guc_opts(default = "", flags = "REPORT")]
    application_name: String,

    /// It is typically set by an application upon connection to the server.
    /// see <https://www.postgresql.org/docs/current/runtime-config-client.html#GUC-DATESTYLE>
    #[guc_opts(default = "")]
    date_style: String,

    /// Force the use of lookup join instead of hash join when possible for local batch execution.
    #[guc_opts(default = true, rename = "RW_BATCH_ENABLE_LOOKUP_JOIN")]
    batch_enable_lookup_join: bool,

    /// Enable usage of sortAgg instead of hash agg when order property is satisfied in batch
    /// execution
    #[guc_opts(default = true, rename = "RW_BATCH_ENABLE_SORT_AGG")]
    batch_enable_sort_agg: bool,

    /// The max gap allowed to transform small range scan scan into multi point lookup.
    #[guc_opts(default = 8)]
    max_split_range_gap: i32,

    /// Sets the order in which schemas are searched when an object (table, data type, function, etc.)
    /// is referenced by a simple name with no schema specified.
    /// See <https://www.postgresql.org/docs/14/runtime-config-client.html#GUC-SEARCH-PATH>
    #[guc_opts(default = SearchPath::default())]
    search_path: SearchPath,

    /// If `VISIBILITY_MODE` is all, we will support querying data without checkpoint.
    #[guc_opts(default = VisibilityMode::Default)]
    visibility_mode: VisibilityMode,

    /// See <https://www.postgresql.org/docs/current/transaction-iso.html>
    #[guc_opts(default = IsolationLevel::ReadCommitted)]
    transaction_isolation_level: IsolationLevel,

    /// Select as of specific epoch.
    /// Sets the historical epoch for querying data. If 0, querying latest data.
    #[guc_opts(default = ConfigNonZeroU64::default())]
    query_epoch: ConfigNonZeroU64,

    /// Session timezone. Defaults to UTC.
    #[guc_opts(default = "UTC", check_hook = check_timezone)]
    timezone: String,

    /// If `STREAMING_PARALLELISM` is non-zero, CREATE MATERIALIZED VIEW/TABLE/INDEX will use it as
    /// streaming parallelism.
    #[guc_opts(default = ConfigNonZeroU64::default())]
    streaming_parallelism: ConfigNonZeroU64,

    /// Enable delta join for streaming queries. Defaults to false.
    #[guc_opts(default = false, rename = "RW_STREAMING_ENABLE_DELTA_JOIN")]
    streaming_enable_delta_join: bool,

    /// Enable bushy join for streaming queries. Defaults to true.
    #[guc_opts(default = true, rename = "RW_STREAMING_ENABLE_BUSHY_JOIN")]
    streaming_enable_bushy_join: bool,

    /// Enable arrangement backfill for streaming queries. Defaults to false.
    #[guc_opts(default = false)]
    streaming_enable_arrangement_backfill: bool,

    /// Enable join ordering for streaming and batch queries. Defaults to true.
    #[guc_opts(default = true, rename = "RW_ENABLE_JOIN_ORDERING")]
    enable_join_ordering: bool,

    /// Enable two phase agg optimization. Defaults to true.
    /// Setting this to true will always set `FORCE_TWO_PHASE_AGG` to false.
    #[guc_opts(default = true, flags = "SETTER", rename = "RW_ENABLE_TWO_PHASE_AGG")]
    enable_two_phase_agg: bool,

    /// Force two phase agg optimization whenever there's a choice between
    /// optimizations. Defaults to false.
    /// Setting this to true will always set `ENABLE_TWO_PHASE_AGG` to false.
    #[guc_opts(default = false, flags = "SETTER", rename = "RW_FORCE_TWO_PHASE_AGG")]
    force_two_phase_agg: bool,

    /// Enable sharing of common sub-plans.
    /// This means that DAG structured query plans can be constructed,
    #[guc_opts(default = true, rename = "RW_ENABLE_SHARE_PLAN")]
    /// rather than only tree structured query plans.
    enable_share_plan: bool,

    /// Enable split distinct agg
    #[guc_opts(default = false, rename = "RW_FORCE_SPLIT_DISTINCT_AGG")]
    force_split_distinct_agg: bool,

    /// See <https://www.postgresql.org/docs/current/runtime-config-client.html#GUC-INTERVALSTYLE>
    #[guc_opts(default = "")]
    interval_style: String,

    /// If `BATCH_PARALLELISM` is non-zero, batch queries will use this parallelism.
    #[guc_opts(default = ConfigNonZeroU64::default())]
    batch_parallelism: ConfigNonZeroU64,

    /// The version of PostgreSQL that Risingwave claims to be.
    #[guc_opts(default = "9.5.0")]
    server_version: String,

    /// The version of PostgreSQL that Risingwave claims to be.
    #[guc_opts(default = 90500)]
    server_version_num: i32,

    /// see <https://www.postgresql.org/docs/15/runtime-config-client.html#GUC-CLIENT-MIN-MESSAGES>
    #[guc_opts(default = "notice")]
    client_min_messages: String,

    /// see <https://www.postgresql.org/docs/15/runtime-config-client.html#GUC-CLIENT-ENCODING>
    #[guc_opts(default = "UTF8", check_hook = check_client_encoding)]
    client_encoding: String,

    /// Enable decoupling sink and internal streaming graph or not
    #[guc_opts(default = SinkDecouple::Default)]
    sink_decouple: SinkDecouple,

    /// See <https://www.postgresql.org/docs/current/runtime-config-compatible.html#RUNTIME-CONFIG-COMPATIBLE-VERSION>
    /// Unused in RisingWave, support for compatibility.
    #[guc_opts(default = false)]
    synchronize_seqscans: bool,

    /// Abort any statement that takes more than the specified amount of time. If
    /// log_min_error_statement is set to ERROR or lower, the statement that timed out will also be
    /// logged. If this value is specified without units, it is taken as milliseconds. A value of
    /// zero (the default) disables the timeout.
    #[guc_opts(default = 0)]
    statement_timeout: i32,

    /// See <https://www.postgresql.org/docs/current/runtime-config-client.html#GUC-LOCK-TIMEOUT>
    /// Unused in RisingWave, support for compatibility.
    #[guc_opts(default = 0)]
    lock_timeout: i32,

    /// see <https://www.postgresql.org/docs/current/runtime-config-client.html#GUC-ROW-SECURITY>.
    /// Unused in RisingWave, support for compatibility.
    #[guc_opts(default = true)]
    row_security: bool,

    /// see <https://www.postgresql.org/docs/current/runtime-config-client.html#GUC-STANDARD-CONFORMING-STRINGS>
    #[guc_opts(default = "on")]
    standard_conforming_strings: String,

    /// Set streaming rate limit (rows per second) for each parallelism for mv backfilling
    #[guc_opts(default = ConfigNonZeroU64::default())]
    streaming_rate_limit: ConfigNonZeroU64,

    /// Enable backfill for CDC table to allow lock-free and incremental snapshot
    #[guc_opts(default = false)]
    cdc_backfill: bool,

    /// Cache policy for partition cache in streaming over window.
    /// Can be "full", "recent", "recent_first_n" or "recent_last_n".
    #[guc_opts(default = OverWindowCachePolicy::default(), rename = "RW_STREAMING_OVER_WINDOW_CACHE_POLICY")]
    streaming_over_window_cache_policy: OverWindowCachePolicy,

    /// Run DDL statements in background
    #[guc_opts(default = false)]
    background_ddl: bool,

    /// Shows the server-side character set encoding. At present, this parameter can be shown but not set, because the encoding is determined at database creation time.
    #[guc_opts(default = "UTF8")]
    server_encoding: String,

    #[guc_opts(default = "hex", check_hook = check_bytea_output)]
    bytea_output: String,
}

fn check_timezone(val: &str) -> Result<(), String> {
    // Check if the provided string is a valid timezone.
    Tz::from_str_insensitive(val).map_err(|_e| "Not a valid timezone")?;
    Ok(())
}

fn check_client_encoding(val: &str) -> Result<(), String> {
    // https://github.com/postgres/postgres/blob/REL_15_3/src/common/encnames.c#L525
    let clean = val.replace(|c: char| !c.is_ascii_alphanumeric(), "");
    if !clean.eq_ignore_ascii_case("UTF8") {
        Err("Only support 'UTF8' for CLIENT_ENCODING".to_string())
    } else {
        Ok(())
    }
}

fn check_bytea_output(val: &str) -> Result<(), String> {
    if val == "hex" {
        Ok(())
    } else {
        Err("Only support 'hex' for BYTEA_OUTPUT".to_string())
    }
}

impl ConfigMap {
    pub fn set_force_two_phase_agg(
        &mut self,
        val: bool,
        reporter: &mut impl ConfigReporter,
    ) -> RwResult<()> {
        self.set_force_two_phase_agg_inner(val, reporter)?;
        if self.force_two_phase_agg {
            self.set_enable_two_phase_agg(true, reporter)
        } else {
            Ok(())
        }
    }

    pub fn set_enable_two_phase_agg(
        &mut self,
        val: bool,
        reporter: &mut impl ConfigReporter,
    ) -> RwResult<()> {
        self.set_enable_two_phase_agg_inner(val, reporter)?;
        if !self.force_two_phase_agg {
            self.set_force_two_phase_agg(false, reporter)
        } else {
            Ok(())
        }
    }
}

pub struct VariableInfo {
    pub name: String,
    pub setting: String,
    pub description: String,
}

/// Report status or notice to caller.
pub trait ConfigReporter {
    fn report_status(&mut self, key: &str, new_val: String);
}

// Report nothing.
impl ConfigReporter for () {
    fn report_status(&mut self, _key: &str, _new_val: String) {}
}

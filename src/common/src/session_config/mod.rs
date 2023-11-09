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

mod over_window;
mod query_mode;
mod search_path;
pub mod sink_decouple;
mod transaction_isolation_level;
mod visibility_mode;

use std::num::NonZeroU64;
use std::ops::Deref;

use chrono_tz::Tz;
use educe::{self, Educe};
use itertools::Itertools;
pub use over_window::OverWindowCachePolicy;
pub use query_mode::QueryMode;
pub use search_path::{SearchPath, USER_NAME_WILD_CARD};
use tracing::info;

use crate::error::{ErrorCode, RwError};
use crate::session_config::sink_decouple::SinkDecouple;
use crate::session_config::transaction_isolation_level::IsolationLevel;
pub use crate::session_config::visibility_mode::VisibilityMode;
use crate::util::epoch::Epoch;

// This is a hack, &'static str is not allowed as a const generics argument.
// TODO: refine this using the adt_const_params feature.
const CONFIG_KEYS: [&str; 40] = [
    "RW_IMPLICIT_FLUSH",
    "CREATE_COMPACTION_GROUP_FOR_MV",
    "QUERY_MODE",
    "EXTRA_FLOAT_DIGITS",
    "APPLICATION_NAME",
    "DATESTYLE",
    "RW_BATCH_ENABLE_LOOKUP_JOIN",
    "MAX_SPLIT_RANGE_GAP",
    "SEARCH_PATH",
    "TRANSACTION ISOLATION LEVEL",
    "QUERY_EPOCH",
    "RW_BATCH_ENABLE_SORT_AGG",
    "VISIBILITY_MODE",
    "TIMEZONE",
    "STREAMING_PARALLELISM",
    "RW_STREAMING_ENABLE_DELTA_JOIN",
    "RW_ENABLE_TWO_PHASE_AGG",
    "RW_FORCE_TWO_PHASE_AGG",
    "RW_ENABLE_SHARE_PLAN",
    "INTERVALSTYLE",
    "BATCH_PARALLELISM",
    "RW_STREAMING_ENABLE_BUSHY_JOIN",
    "RW_ENABLE_JOIN_ORDERING",
    "SERVER_VERSION",
    "SERVER_VERSION_NUM",
    "RW_FORCE_SPLIT_DISTINCT_AGG",
    "CLIENT_MIN_MESSAGES",
    "CLIENT_ENCODING",
    "SINK_DECOUPLE",
    "SYNCHRONIZE_SEQSCANS",
    "STATEMENT_TIMEOUT",
    "LOCK_TIMEOUT",
    "ROW_SECURITY",
    "STANDARD_CONFORMING_STRINGS",
    "STREAMING_RATE_LIMIT",
    "CDC_BACKFILL",
    "RW_STREAMING_OVER_WINDOW_CACHE_POLICY",
    "BACKGROUND_DDL",
    "SERVER_ENCODING",
    "STREAMING_ENABLE_ARRANGEMENT_BACKFILL",
];

// MUST HAVE 1v1 relationship to CONFIG_KEYS. e.g. CONFIG_KEYS[IMPLICIT_FLUSH] =
// "RW_IMPLICIT_FLUSH".
const IMPLICIT_FLUSH: usize = 0;
const CREATE_COMPACTION_GROUP_FOR_MV: usize = 1;
const QUERY_MODE: usize = 2;
const EXTRA_FLOAT_DIGITS: usize = 3;
const APPLICATION_NAME: usize = 4;
const DATE_STYLE: usize = 5;
const BATCH_ENABLE_LOOKUP_JOIN: usize = 6;
const MAX_SPLIT_RANGE_GAP: usize = 7;
const SEARCH_PATH: usize = 8;
const TRANSACTION_ISOLATION_LEVEL: usize = 9;
const QUERY_EPOCH: usize = 10;
const BATCH_ENABLE_SORT_AGG: usize = 11;
const VISIBILITY_MODE: usize = 12;
const TIMEZONE: usize = 13;
const STREAMING_PARALLELISM: usize = 14;
const STREAMING_ENABLE_DELTA_JOIN: usize = 15;
const ENABLE_TWO_PHASE_AGG: usize = 16;
const FORCE_TWO_PHASE_AGG: usize = 17;
const RW_ENABLE_SHARE_PLAN: usize = 18;
const INTERVAL_STYLE: usize = 19;
const BATCH_PARALLELISM: usize = 20;
const STREAMING_ENABLE_BUSHY_JOIN: usize = 21;
const RW_ENABLE_JOIN_ORDERING: usize = 22;
const SERVER_VERSION: usize = 23;
const SERVER_VERSION_NUM: usize = 24;
const FORCE_SPLIT_DISTINCT_AGG: usize = 25;
const CLIENT_MIN_MESSAGES: usize = 26;
const CLIENT_ENCODING: usize = 27;
const SINK_DECOUPLE: usize = 28;
const SYNCHRONIZE_SEQSCANS: usize = 29;
const STATEMENT_TIMEOUT: usize = 30;
const LOCK_TIMEOUT: usize = 31;
const ROW_SECURITY: usize = 32;
const STANDARD_CONFORMING_STRINGS: usize = 33;
const STREAMING_RATE_LIMIT: usize = 34;
const CDC_BACKFILL: usize = 35;
const STREAMING_OVER_WINDOW_CACHE_POLICY: usize = 36;
const BACKGROUND_DDL: usize = 37;
const SERVER_ENCODING: usize = 38;
const STREAMING_ENABLE_ARRANGEMENT_BACKFILL: usize = 39;

trait ConfigEntry: Default + for<'a> TryFrom<&'a [&'a str], Error = RwError> {
    fn entry_name() -> &'static str;
}

struct ConfigBool<const NAME: usize, const DEFAULT: bool = false>(bool);

impl<const NAME: usize, const DEFAULT: bool> Default for ConfigBool<NAME, DEFAULT> {
    fn default() -> Self {
        ConfigBool(DEFAULT)
    }
}

impl<const NAME: usize, const DEFAULT: bool> ConfigEntry for ConfigBool<NAME, DEFAULT> {
    fn entry_name() -> &'static str {
        CONFIG_KEYS[NAME]
    }
}

impl<const NAME: usize, const DEFAULT: bool> TryFrom<&[&str]> for ConfigBool<NAME, DEFAULT> {
    type Error = RwError;

    fn try_from(value: &[&str]) -> Result<Self, Self::Error> {
        if value.len() != 1 {
            return Err(ErrorCode::InternalError(format!(
                "SET {} takes only one argument",
                <Self as ConfigEntry>::entry_name()
            ))
            .into());
        }

        let s = value[0];
        if s.eq_ignore_ascii_case("true")
            || s.eq_ignore_ascii_case("on")
            || s.eq_ignore_ascii_case("yes")
            || s.eq_ignore_ascii_case("1")
        {
            Ok(ConfigBool(true))
        } else if s.eq_ignore_ascii_case("false")
            || s.eq_ignore_ascii_case("off")
            || s.eq_ignore_ascii_case("no")
            || s.eq_ignore_ascii_case("0")
        {
            Ok(ConfigBool(false))
        } else {
            Err(ErrorCode::InvalidConfigValue {
                config_entry: Self::entry_name().to_string(),
                config_value: s.to_string(),
            }
            .into())
        }
    }
}

impl<const NAME: usize, const DEFAULT: bool> Deref for ConfigBool<NAME, DEFAULT> {
    type Target = bool;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Default, Clone, PartialEq, Eq)]
struct ConfigString<const NAME: usize>(String);

impl<const NAME: usize> Deref for ConfigString<NAME> {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<const NAME: usize> TryFrom<&[&str]> for ConfigString<NAME> {
    type Error = RwError;

    fn try_from(value: &[&str]) -> Result<Self, Self::Error> {
        if value.len() != 1 {
            return Err(ErrorCode::InternalError(format!(
                "SET {} takes only one argument",
                Self::entry_name()
            ))
            .into());
        }

        Ok(Self(value[0].to_string()))
    }
}

impl<const NAME: usize> ConfigEntry for ConfigString<NAME> {
    fn entry_name() -> &'static str {
        CONFIG_KEYS[NAME]
    }
}

struct ConfigI32<const NAME: usize, const DEFAULT: i32 = 0>(i32);

impl<const NAME: usize, const DEFAULT: i32> Default for ConfigI32<NAME, DEFAULT> {
    fn default() -> Self {
        ConfigI32(DEFAULT)
    }
}

impl<const NAME: usize, const DEFAULT: i32> Deref for ConfigI32<NAME, DEFAULT> {
    type Target = i32;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<const NAME: usize, const DEFAULT: i32> ConfigEntry for ConfigI32<NAME, DEFAULT> {
    fn entry_name() -> &'static str {
        CONFIG_KEYS[NAME]
    }
}

impl<const NAME: usize, const DEFAULT: i32> TryFrom<&[&str]> for ConfigI32<NAME, DEFAULT> {
    type Error = RwError;

    fn try_from(value: &[&str]) -> Result<Self, Self::Error> {
        if value.len() != 1 {
            return Err(ErrorCode::InternalError(format!(
                "SET {} takes only one argument",
                Self::entry_name()
            ))
            .into());
        }

        let s = value[0];
        s.parse::<i32>().map(ConfigI32).map_err(|_e| {
            ErrorCode::InvalidConfigValue {
                config_entry: Self::entry_name().to_string(),
                config_value: s.to_string(),
            }
            .into()
        })
    }
}

struct ConfigU64<const NAME: usize, const DEFAULT: u64 = 0>(u64);

impl<const NAME: usize, const DEFAULT: u64> Default for ConfigU64<NAME, DEFAULT> {
    fn default() -> Self {
        ConfigU64(DEFAULT)
    }
}

impl<const NAME: usize, const DEFAULT: u64> Deref for ConfigU64<NAME, DEFAULT> {
    type Target = u64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<const NAME: usize, const DEFAULT: u64> ConfigEntry for ConfigU64<NAME, DEFAULT> {
    fn entry_name() -> &'static str {
        CONFIG_KEYS[NAME]
    }
}

impl<const NAME: usize, const DEFAULT: u64> TryFrom<&[&str]> for ConfigU64<NAME, DEFAULT> {
    type Error = RwError;

    fn try_from(value: &[&str]) -> Result<Self, Self::Error> {
        if value.len() != 1 {
            return Err(ErrorCode::InternalError(format!(
                "SET {} takes only one argument",
                Self::entry_name()
            ))
            .into());
        }

        let s = value[0];
        s.parse::<u64>().map(ConfigU64).map_err(|_e| {
            ErrorCode::InvalidConfigValue {
                config_entry: Self::entry_name().to_string(),
                config_value: s.to_string(),
            }
            .into()
        })
    }
}

pub struct VariableInfo {
    pub name: String,
    pub setting: String,
    pub description: String,
}

type ImplicitFlush = ConfigBool<IMPLICIT_FLUSH, false>;
type CreateCompactionGroupForMv = ConfigBool<CREATE_COMPACTION_GROUP_FOR_MV, false>;
type ApplicationName = ConfigString<APPLICATION_NAME>;
type ExtraFloatDigit = ConfigI32<EXTRA_FLOAT_DIGITS, 1>;
// TODO: We should use more specified type here.
type DateStyle = ConfigString<DATE_STYLE>;
type BatchEnableLookupJoin = ConfigBool<BATCH_ENABLE_LOOKUP_JOIN, true>;
type BatchEnableSortAgg = ConfigBool<BATCH_ENABLE_SORT_AGG, true>;
type MaxSplitRangeGap = ConfigI32<MAX_SPLIT_RANGE_GAP, 8>;
type QueryEpoch = ConfigU64<QUERY_EPOCH, 0>;
type Timezone = ConfigString<TIMEZONE>;
type StreamingParallelism = ConfigU64<STREAMING_PARALLELISM, 0>;
type StreamingEnableDeltaJoin = ConfigBool<STREAMING_ENABLE_DELTA_JOIN, false>;
type StreamingEnableBushyJoin = ConfigBool<STREAMING_ENABLE_BUSHY_JOIN, true>;
type EnableTwoPhaseAgg = ConfigBool<ENABLE_TWO_PHASE_AGG, true>;
type ForceTwoPhaseAgg = ConfigBool<FORCE_TWO_PHASE_AGG, false>;
type EnableSharePlan = ConfigBool<RW_ENABLE_SHARE_PLAN, true>;
type IntervalStyle = ConfigString<INTERVAL_STYLE>;
type BatchParallelism = ConfigU64<BATCH_PARALLELISM, 0>;
type EnableJoinOrdering = ConfigBool<RW_ENABLE_JOIN_ORDERING, true>;
type ServerVersion = ConfigString<SERVER_VERSION>;
type ServerVersionNum = ConfigI32<SERVER_VERSION_NUM, 90_500>;
type ForceSplitDistinctAgg = ConfigBool<FORCE_SPLIT_DISTINCT_AGG, false>;
type ClientMinMessages = ConfigString<CLIENT_MIN_MESSAGES>;
type ClientEncoding = ConfigString<CLIENT_ENCODING>;
type SynchronizeSeqscans = ConfigBool<SYNCHRONIZE_SEQSCANS, false>;
type StatementTimeout = ConfigI32<STATEMENT_TIMEOUT, 0>;
type LockTimeout = ConfigI32<LOCK_TIMEOUT, 0>;
type RowSecurity = ConfigBool<ROW_SECURITY, true>;
type StandardConformingStrings = ConfigString<STANDARD_CONFORMING_STRINGS>;
type StreamingRateLimit = ConfigU64<STREAMING_RATE_LIMIT, 0>;
type CdcBackfill = ConfigBool<CDC_BACKFILL, false>;
type BackgroundDdl = ConfigBool<BACKGROUND_DDL, false>;
type ServerEncoding = ConfigString<SERVER_ENCODING>;
type StreamingEnableArrangementBackfill = ConfigBool<STREAMING_ENABLE_ARRANGEMENT_BACKFILL, false>;

/// Report status or notice to caller.
pub trait ConfigReporter {
    fn report_status(&mut self, key: &str, new_val: String);
}

// Report nothing.
impl ConfigReporter for () {
    fn report_status(&mut self, _key: &str, _new_val: String) {}
}

#[derive(Educe)]
#[educe(Default)]
pub struct ConfigMap {
    /// If `RW_IMPLICIT_FLUSH` is on, then every INSERT/UPDATE/DELETE statement will block
    /// until the entire dataflow is refreshed. In other words, every related table & MV will
    /// be able to see the write.
    implicit_flush: ImplicitFlush,

    /// If `CREATE_COMPACTION_GROUP_FOR_MV` is on, dedicated compaction groups will be created in
    /// MV creation.
    create_compaction_group_for_mv: CreateCompactionGroupForMv,

    /// A temporary config variable to force query running in either local or distributed mode.
    /// The default value is auto which means let the system decide to run batch queries in local
    /// or distributed mode automatically.
    query_mode: QueryMode,

    /// see <https://www.postgresql.org/docs/current/runtime-config-client.html#:~:text=for%20more%20information.-,extra_float_digits,-(integer)>
    extra_float_digit: ExtraFloatDigit,

    /// see <https://www.postgresql.org/docs/14/runtime-config-logging.html#:~:text=What%20to%20Log-,application_name,-(string)>
    application_name: ApplicationName,

    /// see <https://www.postgresql.org/docs/current/runtime-config-client.html#GUC-DATESTYLE>
    date_style: DateStyle,

    /// To force the usage of lookup join instead of hash join in batch execution
    batch_enable_lookup_join: BatchEnableLookupJoin,

    /// To open the usage of sortAgg instead of hash agg when order property is satisfied in batch
    /// execution
    batch_enable_sort_agg: BatchEnableSortAgg,

    /// It's the max gap allowed to transform small range scan scan into multi point lookup.
    max_split_range_gap: MaxSplitRangeGap,

    /// see <https://www.postgresql.org/docs/14/runtime-config-client.html#GUC-SEARCH-PATH>
    search_path: SearchPath,

    /// If `VISIBILITY_MODE` is all, we will support querying data without checkpoint.
    visibility_mode: VisibilityMode,

    /// see <https://www.postgresql.org/docs/current/transaction-iso.html>
    transaction_isolation_level: IsolationLevel,

    /// select as of specific epoch
    query_epoch: QueryEpoch,

    /// Session timezone. Defaults to UTC.
    #[educe(Default(expression = "ConfigString::<TIMEZONE>(String::from(\"UTC\"))"))]
    timezone: Timezone,

    /// If `STREAMING_PARALLELISM` is non-zero, CREATE MATERIALIZED VIEW/TABLE/INDEX will use it as
    /// streaming parallelism.
    streaming_parallelism: StreamingParallelism,

    /// Enable delta join for streaming queries. Defaults to false.
    streaming_enable_delta_join: StreamingEnableDeltaJoin,

    /// Enable bushy join for streaming queries. Defaults to true.
    streaming_enable_bushy_join: StreamingEnableBushyJoin,

    /// Enable arrangement backfill for streaming queries. Defaults to false.
    streaming_enable_arrangement_backfill: StreamingEnableArrangementBackfill,

    /// Enable join ordering for streaming and batch queries. Defaults to true.
    enable_join_ordering: EnableJoinOrdering,

    /// Enable two phase agg optimization. Defaults to true.
    /// Setting this to true will always set `FORCE_TWO_PHASE_AGG` to false.
    enable_two_phase_agg: EnableTwoPhaseAgg,

    /// Force two phase agg optimization whenever there's a choice between
    /// optimizations. Defaults to false.
    /// Setting this to true will always set `ENABLE_TWO_PHASE_AGG` to false.
    force_two_phase_agg: ForceTwoPhaseAgg,

    /// Enable sharing of common sub-plans.
    /// This means that DAG structured query plans can be constructed,
    /// rather than only tree structured query plans.
    enable_share_plan: EnableSharePlan,

    /// Enable split distinct agg
    force_split_distinct_agg: ForceSplitDistinctAgg,

    /// see <https://www.postgresql.org/docs/current/runtime-config-client.html#GUC-INTERVALSTYLE>
    interval_style: IntervalStyle,

    batch_parallelism: BatchParallelism,

    /// The version of PostgreSQL that Risingwave claims to be.
    #[educe(Default(expression = "ConfigString::<SERVER_VERSION>(String::from(\"9.5.0\"))"))]
    server_version: ServerVersion,
    server_version_num: ServerVersionNum,

    /// see <https://www.postgresql.org/docs/15/runtime-config-client.html#GUC-CLIENT-MIN-MESSAGES>
    #[educe(Default(
        expression = "ConfigString::<CLIENT_MIN_MESSAGES>(String::from(\"notice\"))"
    ))]
    client_min_messages: ClientMinMessages,

    /// see <https://www.postgresql.org/docs/15/runtime-config-client.html#GUC-CLIENT-ENCODING>
    #[educe(Default(expression = "ConfigString::<CLIENT_ENCODING>(String::from(\"UTF8\"))"))]
    client_encoding: ClientEncoding,

    /// Enable decoupling sink and internal streaming graph or not
    sink_decouple: SinkDecouple,

    /// See <https://www.postgresql.org/docs/current/runtime-config-compatible.html#RUNTIME-CONFIG-COMPATIBLE-VERSION>
    /// Unused in RisingWave, support for compatibility.
    synchronize_seqscans: SynchronizeSeqscans,

    /// Abort any statement that takes more than the specified amount of time. If
    /// log_min_error_statement is set to ERROR or lower, the statement that timed out will also be
    /// logged. If this value is specified without units, it is taken as milliseconds. A value of
    /// zero (the default) disables the timeout.
    statement_timeout: StatementTimeout,

    /// see <https://www.postgresql.org/docs/current/runtime-config-client.html#GUC-LOCK-TIMEOUT>
    /// Unused in RisingWave, support for compatibility.
    lock_timeout: LockTimeout,

    /// see <https://www.postgresql.org/docs/current/runtime-config-client.html#GUC-ROW-SECURITY>.
    /// Unused in RisingWave, support for compatibility.
    row_security: RowSecurity,

    /// see <https://www.postgresql.org/docs/current/runtime-config-client.html#GUC-STANDARD-CONFORMING-STRINGS>
    #[educe(Default(
        expression = "ConfigString::<STANDARD_CONFORMING_STRINGS>(String::from(\"on\"))"
    ))]
    standard_conforming_strings: StandardConformingStrings,

    streaming_rate_limit: StreamingRateLimit,

    cdc_backfill: CdcBackfill,

    /// Cache policy for partition cache in streaming over window.
    /// Can be "full", "recent", "recent_first_n" or "recent_last_n".
    streaming_over_window_cache_policy: OverWindowCachePolicy,

    background_ddl: BackgroundDdl,

    /// Shows the server-side character set encoding. At present, this parameter can be shown but not set, because the encoding is determined at database creation time.
    #[educe(Default(expression = "ConfigString::<SERVER_ENCODING>(String::from(\"UTF8\"))"))]
    server_encoding: ServerEncoding,
}

impl ConfigMap {
    pub fn set(
        &mut self,
        key: &str,
        val: Vec<String>,
        mut reporter: impl ConfigReporter,
    ) -> Result<(), RwError> {
        info!(%key, ?val, "set config");
        let val = val.iter().map(AsRef::as_ref).collect_vec();
        if key.eq_ignore_ascii_case(ImplicitFlush::entry_name()) {
            self.implicit_flush = val.as_slice().try_into()?;
        } else if key.eq_ignore_ascii_case(CreateCompactionGroupForMv::entry_name()) {
            self.create_compaction_group_for_mv = val.as_slice().try_into()?;
        } else if key.eq_ignore_ascii_case(QueryMode::entry_name()) {
            self.query_mode = val.as_slice().try_into()?;
        } else if key.eq_ignore_ascii_case(ExtraFloatDigit::entry_name()) {
            self.extra_float_digit = val.as_slice().try_into()?;
        } else if key.eq_ignore_ascii_case(ApplicationName::entry_name()) {
            let new_application_name = val.as_slice().try_into()?;
            if self.application_name != new_application_name {
                self.application_name = new_application_name.clone();
                reporter.report_status(ApplicationName::entry_name(), new_application_name.0);
            }
        } else if key.eq_ignore_ascii_case(DateStyle::entry_name()) {
            self.date_style = val.as_slice().try_into()?;
        } else if key.eq_ignore_ascii_case(BatchEnableLookupJoin::entry_name()) {
            self.batch_enable_lookup_join = val.as_slice().try_into()?;
        } else if key.eq_ignore_ascii_case(BatchEnableSortAgg::entry_name()) {
            self.batch_enable_sort_agg = val.as_slice().try_into()?;
        } else if key.eq_ignore_ascii_case(MaxSplitRangeGap::entry_name()) {
            self.max_split_range_gap = val.as_slice().try_into()?;
        } else if key.eq_ignore_ascii_case(SearchPath::entry_name()) {
            self.search_path = val.as_slice().try_into()?;
        } else if key.eq_ignore_ascii_case(VisibilityMode::entry_name()) {
            self.visibility_mode = val.as_slice().try_into()?;
        } else if key.eq_ignore_ascii_case(QueryEpoch::entry_name()) {
            self.query_epoch = val.as_slice().try_into()?;
        } else if key.eq_ignore_ascii_case(Timezone::entry_name()) {
            let raw: Timezone = val.as_slice().try_into()?;
            // Check if the provided string is a valid timezone.
            Tz::from_str_insensitive(&raw.0).map_err(|_e| ErrorCode::InvalidConfigValue {
                config_entry: Timezone::entry_name().to_string(),
                config_value: raw.0.to_string(),
            })?;
            self.timezone = raw;
        } else if key.eq_ignore_ascii_case(StreamingParallelism::entry_name()) {
            self.streaming_parallelism = val.as_slice().try_into()?;
        } else if key.eq_ignore_ascii_case(StreamingEnableDeltaJoin::entry_name()) {
            self.streaming_enable_delta_join = val.as_slice().try_into()?;
        } else if key.eq_ignore_ascii_case(StreamingEnableBushyJoin::entry_name()) {
            self.streaming_enable_bushy_join = val.as_slice().try_into()?;
        } else if key.eq_ignore_ascii_case(StreamingEnableArrangementBackfill::entry_name()) {
            self.streaming_enable_arrangement_backfill = val.as_slice().try_into()?;
        } else if key.eq_ignore_ascii_case(EnableJoinOrdering::entry_name()) {
            self.enable_join_ordering = val.as_slice().try_into()?;
        } else if key.eq_ignore_ascii_case(EnableTwoPhaseAgg::entry_name()) {
            self.enable_two_phase_agg = val.as_slice().try_into()?;
            if !*self.enable_two_phase_agg {
                self.force_two_phase_agg = ConfigBool(false);
            }
        } else if key.eq_ignore_ascii_case(ForceTwoPhaseAgg::entry_name()) {
            self.force_two_phase_agg = val.as_slice().try_into()?;
            if *self.force_two_phase_agg {
                self.enable_two_phase_agg = ConfigBool(true);
            }
        } else if key.eq_ignore_ascii_case(ForceSplitDistinctAgg::entry_name()) {
            self.force_split_distinct_agg = val.as_slice().try_into()?;
        } else if key.eq_ignore_ascii_case(EnableSharePlan::entry_name()) {
            self.enable_share_plan = val.as_slice().try_into()?;
        } else if key.eq_ignore_ascii_case(IntervalStyle::entry_name()) {
            self.interval_style = val.as_slice().try_into()?;
        } else if key.eq_ignore_ascii_case(BatchParallelism::entry_name()) {
            self.batch_parallelism = val.as_slice().try_into()?;
        } else if key.eq_ignore_ascii_case(ClientMinMessages::entry_name()) {
            // TODO: validate input and fold to lowercase after #10697 refactor
            self.client_min_messages = val.as_slice().try_into()?;
        } else if key.eq_ignore_ascii_case(ClientEncoding::entry_name()) {
            let enc: ClientEncoding = val.as_slice().try_into()?;
            // https://github.com/postgres/postgres/blob/REL_15_3/src/common/encnames.c#L525
            let clean = enc
                .as_str()
                .replace(|c: char| !c.is_ascii_alphanumeric(), "");
            if !clean.eq_ignore_ascii_case("UTF8") {
                return Err(ErrorCode::InvalidConfigValue {
                    config_entry: ClientEncoding::entry_name().into(),
                    config_value: enc.0,
                }
                .into());
            }
            // No actual assignment because we only support UTF8.
        } else if key.eq_ignore_ascii_case("bytea_output") {
            // TODO: We only support hex now.
            if !val.first().is_some_and(|val| *val == "hex") {
                return Err(ErrorCode::InvalidConfigValue {
                    config_entry: "bytea_output".into(),
                    config_value: val.first().map(ToString::to_string).unwrap_or_default(),
                }
                .into());
            }
        } else if key.eq_ignore_ascii_case(SinkDecouple::entry_name()) {
            self.sink_decouple = val.as_slice().try_into()?;
        } else if key.eq_ignore_ascii_case(SynchronizeSeqscans::entry_name()) {
            self.synchronize_seqscans = val.as_slice().try_into()?;
        } else if key.eq_ignore_ascii_case(StatementTimeout::entry_name()) {
            self.statement_timeout = val.as_slice().try_into()?;
        } else if key.eq_ignore_ascii_case(LockTimeout::entry_name()) {
            self.lock_timeout = val.as_slice().try_into()?;
        } else if key.eq_ignore_ascii_case(RowSecurity::entry_name()) {
            self.row_security = val.as_slice().try_into()?;
        } else if key.eq_ignore_ascii_case(StandardConformingStrings::entry_name()) {
            self.standard_conforming_strings = val.as_slice().try_into()?;
        } else if key.eq_ignore_ascii_case(StreamingRateLimit::entry_name()) {
            self.streaming_rate_limit = val.as_slice().try_into()?;
        } else if key.eq_ignore_ascii_case(CdcBackfill::entry_name()) {
            self.cdc_backfill = val.as_slice().try_into()?
        } else if key.eq_ignore_ascii_case(OverWindowCachePolicy::entry_name()) {
            self.streaming_over_window_cache_policy = val.as_slice().try_into()?;
        } else if key.eq_ignore_ascii_case(BackgroundDdl::entry_name()) {
            self.background_ddl = val.as_slice().try_into()?;
        } else if key.eq_ignore_ascii_case(ServerEncoding::entry_name()) {
            let enc: ServerEncoding = val.as_slice().try_into()?;
            // https://github.com/postgres/postgres/blob/REL_15_3/src/common/encnames.c#L525
            let clean = enc
                .as_str()
                .replace(|c: char| !c.is_ascii_alphanumeric(), "");
            if !clean.eq_ignore_ascii_case("UTF8") {
                return Err(ErrorCode::InvalidConfigValue {
                    config_entry: ServerEncoding::entry_name().into(),
                    config_value: enc.0,
                }
                .into());
            }
            // No actual assignment because we only support UTF8.
        } else {
            return Err(ErrorCode::UnrecognizedConfigurationParameter(key.to_string()).into());
        }

        Ok(())
    }

    pub fn get(&self, key: &str) -> Result<String, RwError> {
        if key.eq_ignore_ascii_case(ImplicitFlush::entry_name()) {
            Ok(self.implicit_flush.to_string())
        } else if key.eq_ignore_ascii_case(CreateCompactionGroupForMv::entry_name()) {
            Ok(self.create_compaction_group_for_mv.to_string())
        } else if key.eq_ignore_ascii_case(QueryMode::entry_name()) {
            Ok(self.query_mode.to_string())
        } else if key.eq_ignore_ascii_case(ExtraFloatDigit::entry_name()) {
            Ok(self.extra_float_digit.to_string())
        } else if key.eq_ignore_ascii_case(ApplicationName::entry_name()) {
            Ok(self.application_name.to_string())
        } else if key.eq_ignore_ascii_case(DateStyle::entry_name()) {
            Ok(self.date_style.to_string())
        } else if key.eq_ignore_ascii_case(BatchEnableLookupJoin::entry_name()) {
            Ok(self.batch_enable_lookup_join.to_string())
        } else if key.eq_ignore_ascii_case(BatchEnableSortAgg::entry_name()) {
            Ok(self.batch_enable_sort_agg.to_string())
        } else if key.eq_ignore_ascii_case(MaxSplitRangeGap::entry_name()) {
            Ok(self.max_split_range_gap.to_string())
        } else if key.eq_ignore_ascii_case(SearchPath::entry_name()) {
            Ok(self.search_path.to_string())
        } else if key.eq_ignore_ascii_case(VisibilityMode::entry_name()) {
            Ok(self.visibility_mode.to_string())
        } else if key.eq_ignore_ascii_case(IsolationLevel::entry_name()) {
            Ok(self.transaction_isolation_level.to_string())
        } else if key.eq_ignore_ascii_case(QueryEpoch::entry_name()) {
            Ok(self.query_epoch.to_string())
        } else if key.eq_ignore_ascii_case(Timezone::entry_name()) {
            Ok(self.timezone.to_string())
        } else if key.eq_ignore_ascii_case(StreamingParallelism::entry_name()) {
            Ok(self.streaming_parallelism.to_string())
        } else if key.eq_ignore_ascii_case(StreamingEnableDeltaJoin::entry_name()) {
            Ok(self.streaming_enable_delta_join.to_string())
        } else if key.eq_ignore_ascii_case(StreamingEnableBushyJoin::entry_name()) {
            Ok(self.streaming_enable_bushy_join.to_string())
        } else if key.eq_ignore_ascii_case(StreamingEnableArrangementBackfill::entry_name()) {
            Ok(self.streaming_enable_arrangement_backfill.to_string())
        } else if key.eq_ignore_ascii_case(EnableJoinOrdering::entry_name()) {
            Ok(self.enable_join_ordering.to_string())
        } else if key.eq_ignore_ascii_case(EnableTwoPhaseAgg::entry_name()) {
            Ok(self.enable_two_phase_agg.to_string())
        } else if key.eq_ignore_ascii_case(ForceTwoPhaseAgg::entry_name()) {
            Ok(self.force_two_phase_agg.to_string())
        } else if key.eq_ignore_ascii_case(EnableSharePlan::entry_name()) {
            Ok(self.enable_share_plan.to_string())
        } else if key.eq_ignore_ascii_case(IntervalStyle::entry_name()) {
            Ok(self.interval_style.to_string())
        } else if key.eq_ignore_ascii_case(BatchParallelism::entry_name()) {
            Ok(self.batch_parallelism.to_string())
        } else if key.eq_ignore_ascii_case(ServerVersion::entry_name()) {
            Ok(self.server_version.to_string())
        } else if key.eq_ignore_ascii_case(ServerVersionNum::entry_name()) {
            Ok(self.server_version_num.to_string())
        } else if key.eq_ignore_ascii_case(ApplicationName::entry_name()) {
            Ok(self.application_name.to_string())
        } else if key.eq_ignore_ascii_case(ForceSplitDistinctAgg::entry_name()) {
            Ok(self.force_split_distinct_agg.to_string())
        } else if key.eq_ignore_ascii_case(ClientMinMessages::entry_name()) {
            Ok(self.client_min_messages.to_string())
        } else if key.eq_ignore_ascii_case(ClientEncoding::entry_name()) {
            Ok(self.client_encoding.to_string())
        } else if key.eq_ignore_ascii_case("bytea_output") {
            // TODO: We only support hex now.
            Ok("hex".to_string())
        } else if key.eq_ignore_ascii_case(SinkDecouple::entry_name()) {
            Ok(self.sink_decouple.to_string())
        } else if key.eq_ignore_ascii_case(SynchronizeSeqscans::entry_name()) {
            Ok(self.synchronize_seqscans.to_string())
        } else if key.eq_ignore_ascii_case(StatementTimeout::entry_name()) {
            Ok(self.statement_timeout.to_string())
        } else if key.eq_ignore_ascii_case(LockTimeout::entry_name()) {
            Ok(self.lock_timeout.to_string())
        } else if key.eq_ignore_ascii_case(RowSecurity::entry_name()) {
            Ok(self.row_security.to_string())
        } else if key.eq_ignore_ascii_case(StandardConformingStrings::entry_name()) {
            Ok(self.standard_conforming_strings.to_string())
        } else if key.eq_ignore_ascii_case(StreamingRateLimit::entry_name()) {
            Ok(self.streaming_rate_limit.to_string())
        } else if key.eq_ignore_ascii_case(CdcBackfill::entry_name()) {
            Ok(self.cdc_backfill.to_string())
        } else if key.eq_ignore_ascii_case(OverWindowCachePolicy::entry_name()) {
            Ok(self.streaming_over_window_cache_policy.to_string())
        } else if key.eq_ignore_ascii_case(BackgroundDdl::entry_name()) {
            Ok(self.background_ddl.to_string())
        } else if key.eq_ignore_ascii_case(ServerEncoding::entry_name()) {
            Ok(self.server_encoding.to_string())
        } else {
            Err(ErrorCode::UnrecognizedConfigurationParameter(key.to_string()).into())
        }
    }

    pub fn get_all(&self) -> Vec<VariableInfo> {
        vec![
            VariableInfo{
                name : ImplicitFlush::entry_name().to_lowercase(),
                setting : self.implicit_flush.to_string(),
                description : String::from("If `RW_IMPLICIT_FLUSH` is on, then every INSERT/UPDATE/DELETE statement will block until the entire dataflow is refreshed.")
            },
            VariableInfo{
                name : CreateCompactionGroupForMv::entry_name().to_lowercase(),
                setting : self.create_compaction_group_for_mv.to_string(),
                description : String::from("If `CREATE_COMPACTION_GROUP_FOR_MV` is on, dedicated compaction groups will be created in MV creation.")
            },
            VariableInfo{
                name : QueryMode::entry_name().to_lowercase(),
                setting : self.query_mode.to_string(),
                description : String::from("A temporary config variable to force query running in either local or distributed mode. If the value is auto, the system will decide for you automatically.")
            },
            VariableInfo{
                name : ExtraFloatDigit::entry_name().to_lowercase(),
                setting : self.extra_float_digit.to_string(),
                description : String::from("Sets the number of digits displayed for floating-point values.")
            },
            VariableInfo{
                name : ApplicationName::entry_name().to_lowercase(),
                setting : self.application_name.to_string(),
                description : String::from("Sets the application name to be reported in statistics and logs.")
            },
            VariableInfo{
                name : DateStyle::entry_name().to_lowercase(),
                setting : self.date_style.to_string(),
                description : String::from("It is typically set by an application upon connection to the server.")
            },
            VariableInfo{
                name : BatchEnableLookupJoin::entry_name().to_lowercase(),
                setting : self.batch_enable_lookup_join.to_string(),
                description : String::from("To enable the usage of lookup join instead of hash join when possible for local batch execution.")
            },
            VariableInfo{
                name : BatchEnableSortAgg::entry_name().to_lowercase(),
                setting : self.batch_enable_sort_agg.to_string(),
                description : String::from("To enable the usage of sort agg instead of hash join when order property is satisfied for batch execution.")
            },
            VariableInfo{
                name : MaxSplitRangeGap::entry_name().to_lowercase(),
                setting : self.max_split_range_gap.to_string(),
                description : String::from("It's the max gap allowed to transform small range scan scan into multi point lookup.")
            },
            VariableInfo {
                name: SearchPath::entry_name().to_lowercase(),
                setting : self.search_path.to_string(),
                description : String::from("Sets the order in which schemas are searched when an object (table, data type, function, etc.) is referenced by a simple name with no schema specified")
            },
            VariableInfo{
                name : VisibilityMode::entry_name().to_lowercase(),
                setting : self.visibility_mode.to_string(),
                description : String::from("If `VISIBILITY_MODE` is all, we will support querying data without checkpoint.")
            },
            VariableInfo{
                name: QueryEpoch::entry_name().to_lowercase(),
                setting : self.query_epoch.to_string(),
                description : String::from("Sets the historical epoch for querying data. If 0, querying latest data.")
            },
            VariableInfo{
                name : Timezone::entry_name().to_lowercase(),
                setting : self.timezone.to_string(),
                description : String::from("The session timezone. This will affect how timestamps are cast into timestamps with timezone.")
            },
            VariableInfo{
                name : StreamingParallelism::entry_name().to_lowercase(),
                setting : self.streaming_parallelism.to_string(),
                description: String::from("Sets the parallelism for streaming. If 0, use default value.")
            },
            VariableInfo{
                name : StreamingEnableDeltaJoin::entry_name().to_lowercase(),
                setting : self.streaming_enable_delta_join.to_string(),
                description: String::from("Enable delta join in streaming queries.")
            },
            VariableInfo{
                name : StreamingEnableBushyJoin::entry_name().to_lowercase(),
                setting : self.streaming_enable_bushy_join.to_string(),
                description: String::from("Enable bushy join in streaming queries.")
            },
            VariableInfo{
                name : StreamingEnableArrangementBackfill::entry_name().to_lowercase(),
                setting : self.streaming_enable_arrangement_backfill.to_string(),
                description: String::from("Enable arrangement backfill in streaming queries.")
            },
            VariableInfo{
                name : EnableJoinOrdering::entry_name().to_lowercase(),
                setting : self.enable_join_ordering.to_string(),
                description: String::from("Enable join ordering for streaming and batch queries.")
            },
            VariableInfo{
                name : EnableTwoPhaseAgg::entry_name().to_lowercase(),
                setting : self.enable_two_phase_agg.to_string(),
                description: String::from("Enable two phase aggregation.")
            },
            VariableInfo{
                name : ForceTwoPhaseAgg::entry_name().to_lowercase(),
                setting : self.force_two_phase_agg.to_string(),
                description: String::from("Force two phase aggregation.")
            },
            VariableInfo{
                name : EnableSharePlan::entry_name().to_lowercase(),
                setting : self.enable_share_plan.to_string(),
                description: String::from("Enable sharing of common sub-plans. This means that DAG structured query plans can be constructed, rather than only tree structured query plans.")
            },
            VariableInfo{
                name : IntervalStyle::entry_name().to_lowercase(),
                setting : self.interval_style.to_string(),
                description : String::from("It is typically set by an application upon connection to the server.")
            },
            VariableInfo{
                name : BatchParallelism::entry_name().to_lowercase(),
                setting : self.batch_parallelism.to_string(),
                description: String::from("Sets the parallelism for batch. If 0, use default value.")
            },
            VariableInfo{
                name : ServerVersion::entry_name().to_lowercase(),
                setting : self.server_version.to_string(),
                description : String::from("The version of the server.")
            },
            VariableInfo{
                name : ServerVersionNum::entry_name().to_lowercase(),
                setting : self.server_version_num.to_string(),
                description : String::from("The version number of the server.")
            },
            VariableInfo{
                name : ForceSplitDistinctAgg::entry_name().to_lowercase(),
                setting : self.force_split_distinct_agg.to_string(),
                description : String::from("Enable split the distinct aggregation.")
            },
            VariableInfo{
                name : ClientMinMessages::entry_name().to_lowercase(),
                setting : self.client_min_messages.to_string(),
                description : String::from("Sets the message levels that are sent to the client.")
            },
            VariableInfo{
                name : ClientEncoding::entry_name().to_lowercase(),
                setting : self.client_encoding.to_string(),
                description : String::from("Sets the client's character set encoding.")
            },
            VariableInfo{
                name: "bytea_output".to_string(),
                setting: "hex".to_string(),
                description: "Sets the output format for bytea.".to_string(),
            },
            VariableInfo{
                name: SinkDecouple::entry_name().to_lowercase(),
                setting: self.sink_decouple.to_string(),
                description: String::from("Enable decoupling sink and internal streaming graph or not")
            },
            VariableInfo{
                name: SynchronizeSeqscans::entry_name().to_lowercase(),
                setting: self.synchronize_seqscans.to_string(),
                description: String::from("Unused in RisingWave")
            },
            VariableInfo{
                name: StatementTimeout::entry_name().to_lowercase(),
                setting: self.statement_timeout.to_string(),
                description: String::from("Sets the maximum allowed duration of any statement, currently just a mock variable and not adopted in RW"),
            },
            VariableInfo{
                name: LockTimeout::entry_name().to_lowercase(),
                setting: self.lock_timeout.to_string(),
                description: String::from("Unused in RisingWave"),
            },
            VariableInfo{
                name: RowSecurity::entry_name().to_lowercase(),
                setting: self.row_security.to_string(),
                description: String::from("Unused in RisingWave"),
            },
            VariableInfo{
                name: StandardConformingStrings::entry_name().to_lowercase(),
                setting: self.standard_conforming_strings.to_string(),
                description: String::from("Unused in RisingWave"),
            },
            VariableInfo{
                name: StreamingRateLimit::entry_name().to_lowercase(),
                setting: self.streaming_rate_limit.to_string(),
                description: String::from("Set streaming rate limit (rows per second) for each parallelism for mv backfilling"),
            },
            VariableInfo{
                name: CdcBackfill::entry_name().to_lowercase(),
                setting: self.cdc_backfill.to_string(),
                description: String::from("Enable backfill for CDC table to allow lock-free and incremental snapshot"),
            },
            VariableInfo{
                name: OverWindowCachePolicy::entry_name().to_lowercase(),
                setting: self.streaming_over_window_cache_policy.to_string(),
                description: String::from(r#"Cache policy for partition cache in streaming over window. Can be "full", "recent", "recent_first_n" or "recent_last_n"."#),
            },
            VariableInfo{
                name: BackgroundDdl::entry_name().to_lowercase(),
                setting: self.background_ddl.to_string(),
                description: String::from("Run DDL statements in background"),
            },
            VariableInfo{
                name : ServerEncoding::entry_name().to_lowercase(),
                setting : self.server_encoding.to_string(),
                description : String::from("Sets the server character set encoding.")
            },
        ]
    }

    pub fn get_implicit_flush(&self) -> bool {
        *self.implicit_flush
    }

    pub fn get_create_compaction_group_for_mv(&self) -> bool {
        *self.create_compaction_group_for_mv
    }

    pub fn get_query_mode(&self) -> QueryMode {
        self.query_mode
    }

    pub fn get_extra_float_digit(&self) -> i32 {
        *self.extra_float_digit
    }

    pub fn get_application_name(&self) -> &str {
        &self.application_name
    }

    pub fn get_date_style(&self) -> &str {
        &self.date_style
    }

    pub fn get_batch_enable_lookup_join(&self) -> bool {
        *self.batch_enable_lookup_join
    }

    pub fn get_batch_enable_sort_agg(&self) -> bool {
        *self.batch_enable_sort_agg
    }

    pub fn get_max_split_range_gap(&self) -> u64 {
        if *self.max_split_range_gap < 0 {
            0
        } else {
            *self.max_split_range_gap as u64
        }
    }

    pub fn get_search_path(&self) -> SearchPath {
        self.search_path.clone()
    }

    pub fn get_visible_mode(&self) -> VisibilityMode {
        self.visibility_mode
    }

    pub fn get_query_epoch(&self) -> Option<Epoch> {
        if self.query_epoch.0 != 0 {
            return Some((self.query_epoch.0).into());
        }
        None
    }

    pub fn get_timezone(&self) -> &str {
        &self.timezone
    }

    pub fn get_streaming_parallelism(&self) -> Option<u64> {
        if self.streaming_parallelism.0 != 0 {
            return Some(self.streaming_parallelism.0);
        }
        None
    }

    pub fn get_streaming_enable_delta_join(&self) -> bool {
        *self.streaming_enable_delta_join
    }

    pub fn get_streaming_enable_bushy_join(&self) -> bool {
        *self.streaming_enable_bushy_join
    }

    pub fn get_streaming_enable_arrangement_backfill(&self) -> bool {
        *self.streaming_enable_arrangement_backfill
    }

    pub fn get_enable_join_ordering(&self) -> bool {
        *self.enable_join_ordering
    }

    pub fn get_enable_two_phase_agg(&self) -> bool {
        *self.enable_two_phase_agg
    }

    pub fn get_force_split_distinct_agg(&self) -> bool {
        *self.force_split_distinct_agg
    }

    pub fn get_force_two_phase_agg(&self) -> bool {
        *self.force_two_phase_agg
    }

    pub fn get_enable_share_plan(&self) -> bool {
        *self.enable_share_plan
    }

    pub fn get_interval_style(&self) -> &str {
        &self.interval_style
    }

    pub fn get_batch_parallelism(&self) -> Option<NonZeroU64> {
        if self.batch_parallelism.0 != 0 {
            return Some(NonZeroU64::new(self.batch_parallelism.0).unwrap());
        }
        None
    }

    pub fn get_client_min_message(&self) -> &str {
        &self.client_min_messages
    }

    pub fn get_client_encoding(&self) -> &str {
        &self.client_encoding
    }

    pub fn get_sink_decouple(&self) -> SinkDecouple {
        self.sink_decouple
    }

    pub fn get_standard_conforming_strings(&self) -> &str {
        &self.standard_conforming_strings
    }

    pub fn get_streaming_rate_limit(&self) -> Option<u32> {
        if self.streaming_rate_limit.0 != 0 {
            return Some(self.streaming_rate_limit.0 as u32);
        }
        None
    }

    pub fn get_cdc_backfill(&self) -> bool {
        self.cdc_backfill.0
    }

    pub fn get_streaming_over_window_cache_policy(&self) -> OverWindowCachePolicy {
        self.streaming_over_window_cache_policy
    }

    pub fn get_background_ddl(&self) -> bool {
        self.background_ddl.0
    }

    pub fn get_server_encoding(&self) -> &str {
        &self.server_encoding
    }
}

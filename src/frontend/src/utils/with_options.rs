// Copyright 2022 RisingWave Labs
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

use std::collections::BTreeMap;
use std::num::NonZeroU32;

use risingwave_common::catalog::ConnectionId;
pub use risingwave_connector::WithOptionsSecResolved;
use risingwave_connector::connector_common::{
    PRIVATE_LINK_BROKER_REWRITE_MAP_KEY, PRIVATE_LINK_TARGETS_KEY,
};
use risingwave_connector::source::iceberg::IcebergBoundedScan;
use risingwave_connector::source::kafka::private_link::{
    PRIVATELINK_ENDPOINT_KEY, insert_privatelink_broker_rewrite_map,
};
use risingwave_connector::source::refresh_mode::{BoundedInput, RefreshMode};
use risingwave_connector::{Get, GetKeyIter, WithPropertiesExt};
use risingwave_pb::catalog::connection::Info as ConnectionInfo;
use risingwave_pb::catalog::connection_params::PbConnectionType;
use risingwave_pb::secret::PbSecretRef;
use risingwave_pb::secret::secret_ref::PbRefAsType;
use risingwave_pb::telemetry::{PbTelemetryEventStage, TelemetryDatabaseObject};
use risingwave_sqlparser::ast::{
    BackfillOrderStrategy, ConnectionRefValue, CreateConnectionStatement, CreateSinkStatement,
    CreateSourceStatement, CreateSubscriptionStatement, SecretRefAsType, SecretRefValue, SqlOption,
    SqlOptionValue, Statement, Value,
};
use thiserror_ext::AsReport;

use super::OverwriteOptions;
use crate::Binder;
use crate::error::{ErrorCode, Result as RwResult, RwError};
use crate::handler::create_source::{UPSTREAM_SOURCE_KEY, WEBHOOK_CONNECTOR};
use crate::session::SessionImpl;
use crate::telemetry::report_event;

pub mod options {
    pub const RETENTION_SECONDS: &str = "retention_seconds";
}

pub const MV_REFRESH_INTERVAL_SEC_KEY: &str = "refresh.interval.sec";
pub const SOURCE_REFRESH_MODE_KEY: &str = "refresh_mode";
pub const SOURCE_REFRESH_INTERVAL_SEC_KEY: &str = "refresh_interval_sec";
/// Makes a source bounded and selects what it loads. Cannot be combined with `refresh_mode`.
pub const SOURCE_SCAN_BOUNDED_MODE_KEY: &str = "scan.bounded.mode";
/// Iceberg snapshot to load when `scan.bounded.mode` is 'snapshot'.
pub const SOURCE_SCAN_BOUNDED_SNAPSHOT_ID_KEY: &str = "scan.bounded.snapshot.id";
const SCAN_BOUNDED_MODE_SNAPSHOT: &str = "snapshot";

/// Options or properties extracted from the `WITH` clause of DDLs.
#[derive(Default, Clone, Debug, PartialEq, Eq, Hash)]
pub struct WithOptions {
    inner: BTreeMap<String, String>,
    secret_ref: BTreeMap<String, SecretRefValue>,
    connection_ref: BTreeMap<String, ConnectionRefValue>,
    backfill_order_strategy: BackfillOrderStrategy,
}

impl GetKeyIter for WithOptions {
    fn key_iter(&self) -> impl Iterator<Item = &str> {
        self.inner.keys().map(|s| s.as_str())
    }
}

impl Get for WithOptions {
    fn get(&self, key: &str) -> Option<&String> {
        self.inner.get(key)
    }
}

impl std::ops::Deref for WithOptions {
    type Target = BTreeMap<String, String>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl std::ops::DerefMut for WithOptions {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl WithOptions {
    /// Create a new [`WithOptions`] from a [`BTreeMap`].
    pub fn new_with_options(inner: BTreeMap<String, String>) -> Self {
        Self {
            inner,
            secret_ref: Default::default(),
            connection_ref: Default::default(),
            backfill_order_strategy: Default::default(),
        }
    }

    /// Create a new [`WithOptions`] from a option [`BTreeMap`] and secret ref.
    pub fn new(
        inner: BTreeMap<String, String>,
        secret_ref: BTreeMap<String, SecretRefValue>,
        connection_ref: BTreeMap<String, ConnectionRefValue>,
    ) -> Self {
        Self {
            inner,
            secret_ref,
            connection_ref,
            backfill_order_strategy: Default::default(),
        }
    }

    pub fn inner_mut(&mut self) -> &mut BTreeMap<String, String> {
        &mut self.inner
    }

    /// Take the value of the option map and secret refs.
    pub fn into_parts(
        self,
    ) -> (
        BTreeMap<String, String>,
        BTreeMap<String, SecretRefValue>,
        BTreeMap<String, ConnectionRefValue>,
    ) {
        (self.inner, self.secret_ref, self.connection_ref)
    }

    /// Convert to connector props, remove the key-value pairs used in the top-level.
    pub fn into_connector_props(self) -> WithOptions {
        let inner = self
            .inner
            .into_iter()
            .filter(|(key, _)| {
                key != OverwriteOptions::SOURCE_RATE_LIMIT_KEY && key != options::RETENTION_SECONDS
            })
            .collect();

        Self {
            inner,
            secret_ref: self.secret_ref,
            connection_ref: self.connection_ref,
            backfill_order_strategy: self.backfill_order_strategy,
        }
    }

    /// Parse the retention seconds from the options.
    pub fn retention_seconds(&self) -> Option<NonZeroU32> {
        self.inner
            .get(options::RETENTION_SECONDS)
            .and_then(|s| s.parse().ok())
    }

    pub fn refresh_interval_sec(&self) -> RwResult<Option<u64>> {
        self.inner
            .get(MV_REFRESH_INTERVAL_SEC_KEY)
            .map(|seconds| {
                let seconds = seconds.parse::<u64>().map_err(|e| {
                    RwError::from(ErrorCode::InvalidParameterValue(format!(
                        "`{}` must be a positive integer, but got: {} (error: {})",
                        MV_REFRESH_INTERVAL_SEC_KEY,
                        seconds,
                        e.as_report()
                    )))
                })?;
                if seconds == 0 {
                    return Err(RwError::from(ErrorCode::InvalidParameterValue(format!(
                        "`{}` must be larger than 0, but got: {}",
                        MV_REFRESH_INTERVAL_SEC_KEY, seconds
                    ))));
                }
                Ok(seconds)
            })
            .transpose()
    }

    /// Get a subset of the options from the given keys.
    pub fn subset(&self, keys: impl IntoIterator<Item = impl AsRef<str>>) -> Self {
        let inner = keys
            .into_iter()
            .filter_map(|k| {
                self.inner
                    .get_key_value(k.as_ref())
                    .map(|(k, v)| (k.clone(), v.clone()))
            })
            .collect();

        Self {
            inner,
            secret_ref: self.secret_ref.clone(),
            connection_ref: self.connection_ref.clone(),
            backfill_order_strategy: self.backfill_order_strategy.clone(),
        }
    }

    pub fn value_eq_ignore_case(&self, key: &str, val: &str) -> bool {
        if let Some(inner_val) = self.inner.get(key)
            && inner_val.eq_ignore_ascii_case(val)
        {
            return true;
        }
        false
    }

    pub fn secret_ref(&self) -> &BTreeMap<String, SecretRefValue> {
        &self.secret_ref
    }

    pub fn secret_ref_mut(&mut self) -> &mut BTreeMap<String, SecretRefValue> {
        &mut self.secret_ref
    }

    pub fn connection_ref(&self) -> &BTreeMap<String, ConnectionRefValue> {
        &self.connection_ref
    }

    pub fn connection_ref_mut(&mut self) -> &mut BTreeMap<String, ConnectionRefValue> {
        &mut self.connection_ref
    }

    pub fn oauth_options_to_map(sql_options: &[SqlOption]) -> RwResult<BTreeMap<String, String>> {
        let WithOptions {
            inner, secret_ref, ..
        } = WithOptions::try_from(sql_options)?;
        if secret_ref.is_empty() {
            Ok(inner)
        } else {
            Err(RwError::from(ErrorCode::InvalidParameterValue(
                "Secret reference is not allowed in OAuth options".to_owned(),
            )))
        }
    }

    pub fn is_source_connector(&self) -> bool {
        self.inner.contains_key(UPSTREAM_SOURCE_KEY)
            && self.inner.get(UPSTREAM_SOURCE_KEY).unwrap() != WEBHOOK_CONNECTOR
    }

    pub fn backfill_order_strategy(&self) -> BackfillOrderStrategy {
        self.backfill_order_strategy.clone()
    }
}

/// Resolves connection references and secret references in `WithOptions`.
///
/// This function takes `WithOptions` (typically from a CREATE/ALTER statement),
/// resolves any CONNECTION and SECRET references, and merges their properties
/// with the directly specified options.
///
/// # Arguments
/// * `with_options` - The original `WithOptions` containing user-specified options and references
/// * `session` - The current user session, used to access catalogs and verify permissions
/// * `object` - Optional telemetry information about the database object being created/altered
///
/// # Returns
/// A tuple containing:
/// * `WithOptionsSecResolved` - `WithOptions` with all references resolved and merged
/// * `PbConnectionType` - The type of the referenced connection (if any)
/// * `Option<u32>` - The ID of the referenced connection (if any)
///
/// # Workflow
/// 1. Extract connector name, options, secret refs, and connection refs from `with_options`
/// 2. Resolve any CONNECTION references by looking them up in the catalog
/// 3. Resolve any SECRET references by looking them up in the catalog
/// 4. Merge properties from CONNECTION with directly specified options
/// 5. Perform validation to ensure no conflicts between options
/// 6. Return the merged options, connection type, and connection ID
pub(crate) fn resolve_connection_ref_and_secret_ref(
    with_options: WithOptions,
    session: &SessionImpl,
    object: Option<TelemetryDatabaseObject>,
) -> RwResult<(
    WithOptionsSecResolved,
    PbConnectionType,
    Option<ConnectionId>,
)> {
    let connector_name = with_options.get_connector();
    let db_name: &str = &session.database();
    let (mut options, secret_refs, connection_refs) = with_options.into_parts();

    let mut connection_id = None;
    let mut connection_params = None;
    for connection_ref in connection_refs.values() {
        // at most one connection ref in the map
        connection_params = {
            // get connection params from catalog
            let (schema_name, connection_name) =
                Binder::resolve_schema_qualified_name(db_name, &connection_ref.connection_name)?;
            let connection_catalog =
                session.get_connection_by_name(schema_name, &connection_name)?;
            if let ConnectionInfo::ConnectionParams(params) = &connection_catalog.info {
                connection_id = Some(connection_catalog.id);
                Some(params.clone())
            } else {
                return Err(RwError::from(ErrorCode::InvalidParameterValue(
                    "Private Link Service has been deprecated. Please create a new connection instead.".to_owned(),
                )));
            }
        };

        // Report telemetry event for connection reference if an object is provided
        if let Some(object) = object {
            // report to telemetry
            report_event(
                PbTelemetryEventStage::CreateStreamJob,
                "connection_ref",
                0,
                connector_name.clone(),
                Some(object),
                {
                    connection_params.as_ref().map(|cp| {
                        jsonbb::json!({
                            "connection_type": cp.connection_type().as_str_name().to_owned()
                        })
                    })
                },
            );
        }
    }

    let mut inner_secret_refs = resolve_secret_refs_inner(secret_refs, session)?;

    // Initialize connection_type to Unspecified (will be updated if connection is used)
    let mut connection_type = PbConnectionType::Unspecified;
    let connection_params_is_none_flag = connection_params.is_none();

    // If we have a connection, merge its properties with directly specified options
    if let Some(connection_params) = connection_params {
        // Do key checks on `PRIVATE_LINK_BROKER_REWRITE_MAP_KEY`, `PRIVATE_LINK_TARGETS_KEY` and `PRIVATELINK_ENDPOINT_KEY`
        // `PRIVATE_LINK_BROKER_REWRITE_MAP_KEY` is generated from `private_link_targets` and `private_link_endpoint`, instead of given by users.
        //
        // We resolve private link via `resolve_privatelink_in_with_option` when creating Connection,
        // so here we need to check `PRIVATE_LINK_TARGETS_KEY` and `PRIVATELINK_ENDPOINT_KEY` are not given
        // if `PRIVATE_LINK_BROKER_REWRITE_MAP_KEY` is in Connection catalog.

        if let Some(broker_rewrite_map) = connection_params
            .get_properties()
            .get(PRIVATE_LINK_BROKER_REWRITE_MAP_KEY)
            && (options.contains_key(PRIVATE_LINK_TARGETS_KEY)
                || options.contains_key(PRIVATELINK_ENDPOINT_KEY))
        {
            return Err(RwError::from(ErrorCode::InvalidParameterValue(format!(
                "PrivateLink related options already defined in Connection (rewrite map: {}), please remove {} and {} from WITH clause",
                broker_rewrite_map, PRIVATE_LINK_TARGETS_KEY, PRIVATELINK_ENDPOINT_KEY
            ))));
        }

        // Extract connection type and merge properties
        connection_type = connection_params.connection_type();
        for (k, v) in connection_params.properties {
            if options.insert(k.clone(), v).is_some() {
                return Err(RwError::from(ErrorCode::InvalidParameterValue(format!(
                    "Duplicated key in both WITH clause and Connection catalog: {}",
                    k
                ))));
            }
        }

        // Merge secret references from connection
        for (k, v) in connection_params.secret_refs {
            if inner_secret_refs.insert(k.clone(), v).is_some() {
                return Err(RwError::from(ErrorCode::InvalidParameterValue(format!(
                    "Duplicated key in both WITH clause and Connection catalog: {}",
                    k
                ))));
            }
        }

        // Schema Registry specific validation
        {
            if connection_type == PbConnectionType::SchemaRegistry {
                // Ensure no AWS/Glue options are provided when using schema registry connection
                if options
                    .keys()
                    .chain(inner_secret_refs.keys())
                    .any(|k| k.starts_with("aws"))
                {
                    return Err(RwError::from(ErrorCode::InvalidParameterValue(
                            "Glue related options/secrets are not allowed when using schema registry connection".to_owned()
                        )));
                }
            }
        }
    }

    // connection_params is None means the connection is not retrieved, so the connection type should be unspecified
    if connection_params_is_none_flag {
        debug_assert!(matches!(connection_type, PbConnectionType::Unspecified));
    }
    Ok((
        WithOptionsSecResolved::new(options, inner_secret_refs),
        connection_type,
        connection_id,
    ))
}

/// Get the secret id from the name.
pub(crate) fn resolve_secret_ref_in_with_options(
    with_options: WithOptions,
    session: &SessionImpl,
) -> RwResult<WithOptionsSecResolved> {
    let (options, secret_refs, _) = with_options.into_parts();
    let resolved_secret_refs = resolve_secret_refs_inner(secret_refs, session)?;
    Ok(WithOptionsSecResolved::new(options, resolved_secret_refs))
}

fn resolve_secret_refs_inner(
    secret_refs: BTreeMap<String, SecretRefValue>,
    session: &SessionImpl,
) -> RwResult<BTreeMap<String, PbSecretRef>> {
    let db_name: &str = &session.database();
    let mut resolved_secret_refs = BTreeMap::new();
    for (key, secret_ref) in secret_refs {
        let (schema_name, secret_name) =
            Binder::resolve_schema_qualified_name(db_name, &secret_ref.secret_name)?;
        let secret_catalog = session.get_secret_by_name(schema_name, &secret_name)?;
        let ref_as = match secret_ref.ref_as {
            SecretRefAsType::Text => PbRefAsType::Text,
            SecretRefAsType::File => PbRefAsType::File,
        };
        let pb_secret_ref = PbSecretRef {
            secret_id: secret_catalog.id,
            ref_as: ref_as.into(),
        };
        resolved_secret_refs.insert(key.clone(), pb_secret_ref);
    }
    Ok(resolved_secret_refs)
}

/// The refresh modes of a source. `Bounded` is selected by `scan.bounded.mode`, the others by
/// `refresh_mode`.
#[derive(Clone, Copy, PartialEq, Eq)]
enum RefreshModeKind {
    Streaming,
    FullReload,
    Bounded,
}

impl RefreshModeKind {
    const ALL: [Self; 3] = [Self::Streaming, Self::FullReload, Self::Bounded];
    /// The modes selected by `refresh_mode`.
    const REFRESH_MODE_VALUES: [Self; 2] = [Self::Streaming, Self::FullReload];

    /// `WITH` options that only this mode may set.
    fn own_keys(self) -> &'static [&'static str] {
        match self {
            Self::Streaming => &[],
            Self::FullReload => &[SOURCE_REFRESH_INTERVAL_SEC_KEY],
            Self::Bounded => &[SOURCE_SCAN_BOUNDED_SNAPSHOT_ID_KEY],
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Streaming => "STREAMING",
            Self::FullReload => "FULL_RELOAD",
            Self::Bounded => "BOUNDED",
        }
    }

    /// The option setting that selects this mode, for error messages.
    fn selected_by(self) -> String {
        match self {
            Self::Streaming | Self::FullReload => {
                format!("`{}` is '{}'", SOURCE_REFRESH_MODE_KEY, self.name())
            }
            Self::Bounded => format!(
                "`{}` is '{}'",
                SOURCE_SCAN_BOUNDED_MODE_KEY, SCAN_BOUNDED_MODE_SNAPSHOT
            ),
        }
    }

    /// Removes and parses the options owned by this mode.
    fn parse_options(self, with_options: &mut WithOptions) -> RwResult<RefreshMode> {
        /// Removes `key` and parses its value as an integer, if present.
        fn take_i64(
            with_options: &mut WithOptions,
            key: &str,
            expected: &str,
        ) -> RwResult<Option<i64>> {
            with_options
                .remove(key)
                .map(|value| {
                    value.parse::<i64>().map_err(|e| {
                        invalid_parameter(format!(
                            "`{}` must be {}, but got: {} (error: {})",
                            key,
                            expected,
                            value,
                            e.as_report()
                        ))
                    })
                })
                .transpose()
        }

        Ok(match self {
            Self::Streaming => RefreshMode::Streaming,
            Self::FullReload => {
                let interval_sec = take_i64(
                    with_options,
                    SOURCE_REFRESH_INTERVAL_SEC_KEY,
                    "a positive integer and larger than 0",
                )?;

                if let Some(interval_sec) = interval_sec
                    && interval_sec <= 0
                {
                    return Err(invalid_parameter(format!(
                        "`{}` must be larger than 0, but got: {}",
                        SOURCE_REFRESH_INTERVAL_SEC_KEY, interval_sec
                    )));
                }

                RefreshMode::FullReload { interval_sec }
            }
            Self::Bounded => {
                let snapshot_id = take_i64(
                    with_options,
                    SOURCE_SCAN_BOUNDED_SNAPSHOT_ID_KEY,
                    "an integer",
                )?
                .ok_or_else(|| {
                    invalid_parameter(format!(
                        "`{}` is required when {}. Snapshot ids of an Iceberg table \
                         can be found in `rw_catalog.rw_iceberg_snapshots` through an Iceberg source",
                        SOURCE_SCAN_BOUNDED_SNAPSHOT_ID_KEY,
                        Self::Bounded.selected_by()
                    ))
                })?;

                RefreshMode::Bounded(BoundedInput::Iceberg(IcebergBoundedScan::Snapshot {
                    snapshot_id,
                }))
            }
        })
    }
}

/// Resolves the `refresh_mode` and `scan.bounded.mode` options and the options owned by each mode.
/// Returns `None` if neither is set.
pub(crate) fn resolve_source_refresh_mode_in_with_option(
    with_options: &mut WithOptions,
) -> RwResult<Option<RefreshMode>> {
    let kind = match (
        with_options.remove(SOURCE_REFRESH_MODE_KEY),
        with_options.remove(SOURCE_SCAN_BOUNDED_MODE_KEY),
    ) {
        (Some(_), Some(_)) => {
            return Err(invalid_parameter(format!(
                "`{}` cannot be combined with `{}`",
                SOURCE_SCAN_BOUNDED_MODE_KEY, SOURCE_REFRESH_MODE_KEY
            )));
        }
        (Some(value), None) => Some(
            RefreshModeKind::REFRESH_MODE_VALUES
                .into_iter()
                .find(|kind| kind.name().eq_ignore_ascii_case(&value))
                .ok_or_else(|| {
                    invalid_parameter(format!(
                        "Invalid key `{}`: {}, accepted values are 'STREAMING' and 'FULL_RELOAD'",
                        SOURCE_REFRESH_MODE_KEY, value
                    ))
                })?,
        ),
        (None, Some(value)) => {
            if !value.eq_ignore_ascii_case(SCAN_BOUNDED_MODE_SNAPSHOT) {
                return Err(invalid_parameter(format!(
                    "Invalid key `{}`: {}, accepted values are '{}'",
                    SOURCE_SCAN_BOUNDED_MODE_KEY, value, SCAN_BOUNDED_MODE_SNAPSHOT
                )));
            }

            Some(RefreshModeKind::Bounded)
        }
        (None, None) => None,
    };

    // Options owned by a mode other than the chosen one are not allowed.
    for other in RefreshModeKind::ALL
        .into_iter()
        .filter(|other| Some(*other) != kind)
    {
        if let Some(key) = other
            .own_keys()
            .iter()
            .find(|key| with_options.contains_key(**key))
        {
            return Err(invalid_parameter(format!(
                "`{}` is only allowed when {}",
                key,
                other.selected_by()
            )));
        }
    }

    let refresh_mode = kind
        .map(|kind| kind.parse_options(with_options))
        .transpose()?;

    check_connector_supports_refresh_mode(kind, with_options)?;

    Ok(refresh_mode)
}

fn check_connector_supports_refresh_mode(
    kind: Option<RefreshModeKind>,
    with_options: &WithOptions,
) -> RwResult<()> {
    let connector = with_options
        .get_connector()
        .unwrap_or_else(|| "<missing>".to_owned());

    match kind {
        Some(RefreshModeKind::Bounded) if !with_options.is_iceberg_connector() => {
            Err(invalid_parameter(format!(
                "`{}` is only supported for the iceberg connector, but got: {}",
                SOURCE_SCAN_BOUNDED_MODE_KEY, connector
            )))
        }
        Some(RefreshModeKind::FullReload) if !with_options.supports_full_reload_refresh() => {
            Err(invalid_parameter(format!(
                "FULL_RELOAD refresh mode is only supported for connectors: s3, gcs, __for_testing_only_batch_posix_fs, iceberg, adbc_snowflake, but got: {}",
                connector
            )))
        }
        // Batch connectors require FULL_RELOAD mode.
        kind if with_options.is_batch_connector() && kind != Some(RefreshModeKind::FullReload) => {
            Err(invalid_parameter(format!(
                "Refreshable source {} must be refreshed with 'FULL_RELOAD' refresh mode. Please set `refresh_mode` to 'FULL_RELOAD'.",
                connector
            )))
        }
        _ => Ok(()),
    }
}

fn invalid_parameter(message: String) -> RwError {
    RwError::from(ErrorCode::InvalidParameterValue(message))
}

pub(crate) fn resolve_privatelink_in_with_option(
    with_options: &mut WithOptions,
) -> RwResult<Option<ConnectionId>> {
    let is_kafka = with_options.is_kafka_connector();
    let privatelink_endpoint = with_options.remove(PRIVATELINK_ENDPOINT_KEY);

    // if `privatelink.endpoint` is provided in WITH, use it to rewrite broker address directly
    if let Some(endpoint) = privatelink_endpoint {
        if !is_kafka {
            return Err(RwError::from(ErrorCode::ProtocolError(
                "Privatelink is only supported in kafka connector".to_owned(),
            )));
        }
        insert_privatelink_broker_rewrite_map(with_options.inner_mut(), None, Some(endpoint))
            .map_err(RwError::from)?;
    }
    Ok(None)
}

impl TryFrom<&[SqlOption]> for WithOptions {
    type Error = RwError;

    fn try_from(options: &[SqlOption]) -> Result<Self, Self::Error> {
        let mut inner: BTreeMap<String, String> = BTreeMap::new();
        let mut secret_ref: BTreeMap<String, SecretRefValue> = BTreeMap::new();
        let mut connection_ref: BTreeMap<String, ConnectionRefValue> = BTreeMap::new();
        let mut backfill_order_strategy = BackfillOrderStrategy::Default;
        for option in options {
            let key = option.name.real_value();
            match &option.value {
                SqlOptionValue::SecretRef(r) => {
                    if secret_ref.insert(key.clone(), r.clone()).is_some()
                        || inner.contains_key(&key)
                    {
                        return Err(RwError::from(ErrorCode::InvalidParameterValue(format!(
                            "Duplicated option: {}",
                            key
                        ))));
                    }
                    continue;
                }
                SqlOptionValue::ConnectionRef(r) => {
                    if connection_ref.insert(key.clone(), r.clone()).is_some()
                        || inner.contains_key(&key)
                    {
                        return Err(RwError::from(ErrorCode::InvalidParameterValue(format!(
                            "Duplicated option: {}",
                            key
                        ))));
                    }
                    continue;
                }
                SqlOptionValue::BackfillOrder(b) => {
                    backfill_order_strategy = b.clone();
                    continue;
                }
                _ => {}
            }
            let value: String = match option.value.clone() {
                SqlOptionValue::Value(Value::CstyleEscapedString(s)) => s.value,
                SqlOptionValue::Value(Value::SingleQuotedString(s)) => s,
                SqlOptionValue::Value(Value::Number(n)) => n,
                SqlOptionValue::Value(Value::Boolean(b)) => b.to_string(),
                _ => {
                    return Err(RwError::from(ErrorCode::InvalidParameterValue(
                        "`with options` or `with properties` only support single quoted string value and C style escaped string"
                            .to_owned(),
                    )))
                }
            };
            if inner.insert(key.clone(), value).is_some() || secret_ref.contains_key(&key) {
                return Err(RwError::from(ErrorCode::InvalidParameterValue(format!(
                    "Duplicated option: {}",
                    key
                ))));
            }
        }

        Ok(Self {
            inner,
            secret_ref,
            connection_ref,
            backfill_order_strategy,
        })
    }
}

impl TryFrom<&Statement> for WithOptions {
    type Error = RwError;

    /// Extract options from the `WITH` clause from the given statement.
    fn try_from(statement: &Statement) -> Result<Self, Self::Error> {
        match statement {
            // Explain: forward to the inner statement.
            Statement::Explain { statement, .. } => Self::try_from(statement.as_ref()),

            // View
            Statement::CreateView { with_options, .. } => Self::try_from(with_options.as_slice()),

            // Sink
            Statement::CreateSink {
                stmt:
                    CreateSinkStatement {
                        with_properties, ..
                    },
            }
            | Statement::CreateConnection {
                stmt:
                    CreateConnectionStatement {
                        with_properties, ..
                    },
            }
            | Statement::CreateSource {
                stmt:
                    CreateSourceStatement {
                        with_properties, ..
                    },
                ..
            }
            | Statement::CreateSubscription {
                stmt:
                    CreateSubscriptionStatement {
                        with_properties, ..
                    },
                ..
            }
            | Statement::CreateIndex {
                with_properties, ..
            } => Self::try_from(with_properties.0.as_slice()),
            Statement::CreateTable { with_options, .. } => Self::try_from(with_options.as_slice()),

            _ => Ok(Default::default()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn resolve(options: &[(&str, &str)]) -> RwResult<Option<RefreshMode>> {
        let mut with_options = WithOptions::new_with_options(
            options
                .iter()
                .map(|(key, value)| (key.to_string(), value.to_string()))
                .collect(),
        );
        resolve_source_refresh_mode_in_with_option(&mut with_options)
    }

    fn resolve_err(options: &[(&str, &str)]) -> String {
        resolve(options).unwrap_err().to_string()
    }

    #[test]
    fn test_resolve_refresh_mode() {
        assert_eq!(resolve(&[("connector", "kafka")]).unwrap(), None);
        assert_eq!(
            resolve(&[("connector", "kafka"), ("refresh_mode", "streaming")]).unwrap(),
            Some(RefreshMode::Streaming)
        );
        assert_eq!(
            resolve(&[
                ("connector", "iceberg"),
                ("refresh_mode", "FULL_RELOAD"),
                ("refresh_interval_sec", "10"),
            ])
            .unwrap(),
            Some(RefreshMode::FullReload {
                interval_sec: Some(10)
            })
        );
        assert_eq!(
            resolve(&[
                ("connector", "iceberg"),
                ("scan.bounded.mode", "snapshot"),
                ("scan.bounded.snapshot.id", "42"),
            ])
            .unwrap(),
            Some(RefreshMode::Bounded(BoundedInput::Iceberg(
                IcebergBoundedScan::Snapshot { snapshot_id: 42 }
            )))
        );
    }

    #[test]
    fn test_reject_options_of_other_modes() {
        let err = resolve_err(&[("connector", "iceberg"), ("refresh_interval_sec", "10")]);
        assert!(
            err.contains(
                "`refresh_interval_sec` is only allowed when `refresh_mode` is 'FULL_RELOAD'"
            ),
            "{err}"
        );

        let err = resolve_err(&[
            ("connector", "iceberg"),
            ("scan.bounded.mode", "snapshot"),
            ("scan.bounded.snapshot.id", "42"),
            ("refresh_interval_sec", "10"),
        ]);
        assert!(
            err.contains(
                "`refresh_interval_sec` is only allowed when `refresh_mode` is 'FULL_RELOAD'"
            ),
            "{err}"
        );

        let err = resolve_err(&[
            ("connector", "iceberg"),
            ("refresh_mode", "FULL_RELOAD"),
            ("scan.bounded.snapshot.id", "42"),
        ]);
        assert!(
            err.contains(
                "`scan.bounded.snapshot.id` is only allowed when `scan.bounded.mode` is 'snapshot'"
            ),
            "{err}"
        );

        let err = resolve_err(&[
            ("connector", "iceberg"),
            ("refresh_mode", "STREAMING"),
            ("scan.bounded.mode", "snapshot"),
            ("scan.bounded.snapshot.id", "42"),
        ]);
        assert!(
            err.contains("`scan.bounded.mode` cannot be combined with `refresh_mode`"),
            "{err}"
        );
    }

    #[test]
    fn test_reject_invalid_mode_options() {
        let err = resolve_err(&[("connector", "iceberg"), ("scan.bounded.mode", "snapshot")]);
        assert!(
            err.contains("`scan.bounded.snapshot.id` is required"),
            "{err}"
        );

        let err = resolve_err(&[("connector", "iceberg"), ("scan.bounded.mode", "latest")]);
        assert!(err.contains("accepted values are 'snapshot'"), "{err}");

        // Bounded is selected by `scan.bounded.mode`, not `refresh_mode`.
        let err = resolve_err(&[("connector", "iceberg"), ("refresh_mode", "BOUNDED")]);
        assert!(
            err.contains("accepted values are 'STREAMING' and 'FULL_RELOAD'"),
            "{err}"
        );

        let err = resolve_err(&[
            ("connector", "iceberg"),
            ("refresh_mode", "FULL_RELOAD"),
            ("refresh_interval_sec", "0"),
        ]);
        assert!(err.contains("must be larger than 0"), "{err}");

        let err = resolve_err(&[("connector", "iceberg"), ("refresh_mode", "SOMETIMES")]);
        assert!(err.contains("accepted values are"), "{err}");
    }

    #[test]
    fn test_reject_unsupported_connectors() {
        let err = resolve_err(&[
            ("connector", "kafka"),
            ("scan.bounded.mode", "snapshot"),
            ("scan.bounded.snapshot.id", "42"),
        ]);
        assert!(
            err.contains("`scan.bounded.mode` is only supported for the iceberg connector"),
            "{err}"
        );

        let err = resolve_err(&[("connector", "kafka"), ("refresh_mode", "FULL_RELOAD")]);
        assert!(
            err.contains("FULL_RELOAD refresh mode is only supported for connectors"),
            "{err}"
        );
    }
}

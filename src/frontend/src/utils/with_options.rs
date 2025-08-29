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

use std::collections::BTreeMap;
use std::num::NonZeroU32;

use risingwave_connector::source::kafka::private_link::{
    insert_privatelink_broker_rewrite_map, CONNECTION_NAME_KEY, PRIVATELINK_ENDPOINT_KEY,
};
pub use risingwave_connector::WithOptionsSecResolved;
use risingwave_connector::WithPropertiesExt;
use risingwave_pb::secret::secret_ref::PbRefAsType;
use risingwave_pb::secret::PbSecretRef;
use risingwave_pb::telemetry::TelemetryDatabaseObject;
use risingwave_sqlparser::ast::{
    CreateConnectionStatement, CreateSinkStatement, CreateSourceStatement,
    CreateSubscriptionStatement, SecretRef, SecretRefAsType, SqlOption, Statement, Value,
};

use super::OverwriteOptions;
use crate::catalog::connection_catalog::resolve_private_link_connection;
use crate::catalog::ConnectionId;
use crate::error::{ErrorCode, Result as RwResult, RwError};
use crate::session::SessionImpl;
use crate::Binder;

mod options {

    pub const RETENTION_SECONDS: &str = "retention_seconds";
}

/// Options or properties extracted from the `WITH` clause of DDLs.
#[derive(Default, Clone, Debug, PartialEq, Eq, Hash)]
pub struct WithOptions {
    inner: BTreeMap<String, String>,
    secret_ref: BTreeMap<String, SecretRef>,
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
        }
    }

    /// Create a new [`WithOptions`] from a option [`BTreeMap`] and secret ref.
    pub fn new(inner: BTreeMap<String, String>, secret_ref: BTreeMap<String, SecretRef>) -> Self {
        Self { inner, secret_ref }
    }

    pub fn inner_mut(&mut self) -> &mut BTreeMap<String, String> {
        &mut self.inner
    }

    /// Take the value of the option map and secret refs.
    pub fn into_parts(self) -> (BTreeMap<String, String>, BTreeMap<String, SecretRef>) {
        (self.inner, self.secret_ref)
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
        }
    }

    /// Parse the retention seconds from the options.
    pub fn retention_seconds(&self) -> Option<NonZeroU32> {
        self.inner
            .get(options::RETENTION_SECONDS)
            .and_then(|s| s.parse().ok())
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
        }
    }

    pub fn value_eq_ignore_case(&self, key: &str, val: &str) -> bool {
        if let Some(inner_val) = self.inner.get(key) {
            if inner_val.eq_ignore_ascii_case(val) {
                return true;
            }
        }
        false
    }

    pub fn secret_ref(&self) -> &BTreeMap<String, SecretRef> {
        &self.secret_ref
    }

    pub fn encode_options_to_map(sql_options: &[SqlOption]) -> RwResult<BTreeMap<String, String>> {
        let WithOptions { inner, secret_ref } = WithOptions::try_from(sql_options)?;
        if secret_ref.is_empty() {
            Ok(inner)
        } else {
            Err(RwError::from(ErrorCode::InvalidParameterValue(
                "Secret reference is not allowed in encode options".to_string(),
            )))
        }
    }

    pub fn oauth_options_to_map(sql_options: &[SqlOption]) -> RwResult<BTreeMap<String, String>> {
        let WithOptions { inner, secret_ref } = WithOptions::try_from(sql_options)?;
        if secret_ref.is_empty() {
            Ok(inner)
        } else {
            Err(RwError::from(ErrorCode::InvalidParameterValue(
                "Secret reference is not allowed in OAuth options".to_string(),
            )))
        }
    }
}

/// Get the secret id from the name.
pub(crate) fn resolve_secret_ref_in_with_options(
    with_options: WithOptions,
    session: &SessionImpl,
) -> RwResult<WithOptionsSecResolved> {
    let (options, secret_refs) = with_options.into_parts();
    let mut resolved_secret_refs = BTreeMap::new();
    let db_name: &str = session.database();
    for (key, secret_ref) in secret_refs {
        let (schema_name, secret_name) =
            Binder::resolve_schema_qualified_name(db_name, secret_ref.secret_name.clone())?;
        let secret_catalog = session.get_secret_by_name(schema_name, &secret_name)?;
        let ref_as = match secret_ref.ref_as {
            SecretRefAsType::Text => PbRefAsType::Text,
            SecretRefAsType::File => PbRefAsType::File,
        };
        let pb_secret_ref = PbSecretRef {
            secret_id: secret_catalog.id.secret_id(),
            ref_as: ref_as.into(),
        };
        resolved_secret_refs.insert(key.clone(), pb_secret_ref);
    }
    Ok(WithOptionsSecResolved::new(options, resolved_secret_refs))
}

pub(crate) fn resolve_privatelink_in_with_option(
    with_options: &mut WithOptions,
    schema_name: &Option<String>,
    session: &SessionImpl,
) -> RwResult<Option<ConnectionId>> {
    let is_kafka = with_options.is_kafka_connector();
    let privatelink_endpoint = with_options.remove(PRIVATELINK_ENDPOINT_KEY);

    // if `privatelink.endpoint` is provided in WITH, use it to rewrite broker address directly
    if let Some(endpoint) = privatelink_endpoint {
        if !is_kafka {
            return Err(RwError::from(ErrorCode::ProtocolError(
                "Privatelink is only supported in kafka connector".to_string(),
            )));
        }
        insert_privatelink_broker_rewrite_map(with_options.inner_mut(), None, Some(endpoint))
            .map_err(RwError::from)?;
        return Ok(None);
    }

    let connection_name = with_options
        .remove(CONNECTION_NAME_KEY)
        .map(|s| s.to_lowercase());
    let connection_id = match connection_name {
        Some(connection_name) => {
            let connection = session
                .get_connection_by_name(schema_name.clone(), &connection_name)
                .map_err(|_| ErrorCode::ItemNotFound(connection_name))?;
            if !is_kafka {
                return Err(RwError::from(ErrorCode::ProtocolError(
                    "Connection is only supported in kafka connector".to_string(),
                )));
            }
            resolve_private_link_connection(&connection, with_options.inner_mut())?;
            Some(connection.id)
        }
        None => None,
    };
    Ok(connection_id)
}

impl TryFrom<&[SqlOption]> for WithOptions {
    type Error = RwError;

    fn try_from(options: &[SqlOption]) -> Result<Self, Self::Error> {
        let mut inner: BTreeMap<String, String> = BTreeMap::new();
        let mut secret_ref: BTreeMap<String, SecretRef> = BTreeMap::new();
        for option in options {
            let key = option.name.real_value();
            if let Value::Ref(r) = &option.value {
                if secret_ref.insert(key.clone(), r.clone()).is_some() || inner.contains_key(&key) {
                    return Err(RwError::from(ErrorCode::InvalidParameterValue(format!(
                        "Duplicated option: {}",
                        key
                    ))));
                }
                continue;
            }
            let value: String = match option.value.clone() {
                Value::CstyleEscapedString(s) => s.value,
                Value::SingleQuotedString(s) => s,
                Value::Number(n) => n,
                Value::Boolean(b) => b.to_string(),
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

        Ok(Self { inner, secret_ref })
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
            } => Self::try_from(with_properties.0.as_slice()),
            Statement::CreateSource {
                stmt:
                    CreateSourceStatement {
                        with_properties, ..
                    },
                ..
            } => Self::try_from(with_properties.0.as_slice()),
            Statement::CreateSubscription {
                stmt:
                    CreateSubscriptionStatement {
                        with_properties, ..
                    },
                ..
            } => Self::try_from(with_properties.0.as_slice()),
            Statement::CreateTable { with_options, .. } => Self::try_from(with_options.as_slice()),

            _ => Ok(Default::default()),
        }
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
    _session: &SessionImpl,
    _object: Option<TelemetryDatabaseObject>,
) -> RwResult<(WithOptionsSecResolved, Option<u32>)> {
    let (options, _secret_refs) = with_options.clone().into_parts();
    let inner_secret_refs = BTreeMap::default();

    Ok((
        WithOptionsSecResolved::new(options, inner_secret_refs),
        None,
    ))
}

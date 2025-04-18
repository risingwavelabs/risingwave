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

use risingwave_common::error::{BoxedError, NotImplemented};
use risingwave_common::secret::SecretError;
use risingwave_common::session_config::SessionConfigError;
use risingwave_connector::error::ConnectorError;
use risingwave_connector::sink::SinkError;
use risingwave_meta_model::WorkerId;
use risingwave_pb::PbFieldNotFound;
use risingwave_rpc_client::error::{RpcError, ToTonicStatus};

use crate::hummock::error::Error as HummockError;
use crate::model::MetadataModelError;
use crate::storage::MetaStoreError;

pub type MetaResult<T> = std::result::Result<T, MetaError>;

#[derive(
    thiserror::Error,
    thiserror_ext::ReportDebug,
    thiserror_ext::Arc,
    thiserror_ext::Construct,
    thiserror_ext::Macro,
)]
#[thiserror_ext(newtype(name = MetaError, backtrace), macro(path = "crate::error"))]
pub enum MetaErrorInner {
    #[error("MetaStore transaction error: {0}")]
    TransactionError(
        #[source]
        #[backtrace]
        MetaStoreError,
    ),

    #[error("MetadataModel error: {0}")]
    MetadataModelError(
        #[from]
        #[backtrace]
        MetadataModelError,
    ),

    #[error("Hummock error: {0}")]
    HummockError(
        #[from]
        #[backtrace]
        HummockError,
    ),

    #[error(transparent)]
    RpcError(
        #[from]
        #[backtrace]
        RpcError,
    ),

    #[error("PermissionDenied: {0}")]
    PermissionDenied(String),

    #[error("Invalid worker: {0}, {1}")]
    InvalidWorker(WorkerId, String),

    #[error("Invalid parameter: {0}")]
    InvalidParameter(#[message] String),

    // Used for catalog errors.
    #[error("{0} id not found: {1}")]
    #[construct(skip)]
    CatalogIdNotFound(&'static str, String),

    #[error("table_fragment not exist: id={0}")]
    FragmentNotFound(u32),

    #[error("{0} with name {1} exists{under_creation}", under_creation = (.2).then_some(" but under creation").unwrap_or(""))]
    Duplicated(
        &'static str,
        String,
        // whether the object is under creation
        bool,
    ),

    #[error("Service unavailable: {0}")]
    Unavailable(#[message] String),

    #[error("Election failed: {0}")]
    Election(#[source] BoxedError),

    #[error("Cancelled: {0}")]
    Cancelled(String),

    #[error("SystemParams error: {0}")]
    SystemParams(String),

    #[error("SessionParams error: {0}")]
    SessionConfig(
        #[from]
        #[backtrace]
        SessionConfigError,
    ),

    #[error(transparent)]
    Connector(
        #[from]
        #[backtrace]
        ConnectorError,
    ),

    #[error("Sink error: {0}")]
    Sink(
        #[from]
        #[backtrace]
        SinkError,
    ),

    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        anyhow::Error,
    ),

    // Indicates that recovery was triggered manually.
    #[error("adhoc recovery triggered")]
    AdhocRecovery,

    #[error("Integrity check failed")]
    IntegrityCheckFailed,

    #[error("{0} has been deprecated, please use {1} instead.")]
    Deprecated(String, String),

    #[error(transparent)]
    NotImplemented(#[from] NotImplemented),

    #[error("Secret error: {0}")]
    SecretError(
        #[from]
        #[backtrace]
        SecretError,
    ),
}

impl MetaError {
    pub fn is_invalid_worker(&self) -> bool {
        matches!(self.inner(), MetaErrorInner::InvalidWorker(..))
    }

    pub fn catalog_id_not_found<T: ToString>(relation: &'static str, id: T) -> Self {
        MetaErrorInner::CatalogIdNotFound(relation, id.to_string()).into()
    }

    pub fn is_fragment_not_found(&self) -> bool {
        matches!(self.inner(), MetaErrorInner::FragmentNotFound(..))
    }

    pub fn is_cancelled(&self) -> bool {
        matches!(self.inner(), MetaErrorInner::Cancelled(..))
    }

    pub fn catalog_duplicated<T: Into<String>>(relation: &'static str, name: T) -> Self {
        MetaErrorInner::Duplicated(relation, name.into(), false).into()
    }

    pub fn catalog_under_creation<T: Into<String>>(relation: &'static str, name: T) -> Self {
        MetaErrorInner::Duplicated(relation, name.into(), true).into()
    }
}

impl From<MetaError> for tonic::Status {
    fn from(err: MetaError) -> Self {
        use tonic::Code;

        let code = match err.inner() {
            MetaErrorInner::PermissionDenied(_) => Code::PermissionDenied,
            MetaErrorInner::CatalogIdNotFound(_, _) => Code::NotFound,
            MetaErrorInner::Duplicated(_, _, _) => Code::AlreadyExists,
            MetaErrorInner::Unavailable(_) => Code::Unavailable,
            MetaErrorInner::Cancelled(_) => Code::Cancelled,
            MetaErrorInner::InvalidParameter(_) => Code::InvalidArgument,
            _ => Code::Internal,
        };

        err.to_status(code, "meta")
    }
}

impl From<PbFieldNotFound> for MetaError {
    fn from(e: PbFieldNotFound) -> Self {
        MetadataModelError::from(e).into()
    }
}

impl From<MetaStoreError> for MetaError {
    fn from(e: MetaStoreError) -> Self {
        match e {
            // `MetaStore::txn` method error.
            MetaStoreError::TransactionAbort() => MetaErrorInner::TransactionError(e).into(),
            _ => MetadataModelError::from(e).into(),
        }
    }
}

impl From<MetaErrorInner> for SinkError {
    fn from(e: MetaErrorInner) -> Self {
        SinkError::Coordinator(e.into())
    }
}

impl From<MetaError> for SinkError {
    fn from(e: MetaError) -> Self {
        SinkError::Coordinator(e.into())
    }
}

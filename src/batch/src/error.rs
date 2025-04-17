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

#![allow(clippy::disallowed_types, clippy::disallowed_methods)]

use std::sync::Arc;

pub use anyhow::anyhow;
use iceberg::Error as IcebergError;
use mysql_async::Error as MySqlError;
use parquet::errors::ParquetError;
use risingwave_common::array::ArrayError;
use risingwave_common::error::{BoxedError, def_anyhow_newtype, def_anyhow_variant};
use risingwave_common::util::value_encoding::error::ValueEncodingError;
use risingwave_connector::error::ConnectorError;
use risingwave_dml::error::DmlError;
use risingwave_expr::ExprError;
use risingwave_pb::PbFieldNotFound;
use risingwave_rpc_client::error::{RpcError, ToTonicStatus};
use risingwave_storage::error::StorageError;
use thiserror::Error;
use thiserror_ext::Construct;
use tokio_postgres::Error as PostgresError;
use tonic::Status;

use crate::worker_manager::worker_node_manager::FragmentId;

pub type Result<T> = std::result::Result<T, BatchError>;
/// Batch result with shared error.
pub type SharedResult<T> = std::result::Result<T, Arc<BatchError>>;

pub trait Error = std::error::Error + Send + Sync + 'static;

#[derive(Error, Debug, Construct)]
pub enum BatchError {
    #[error("Storage error: {0}")]
    Storage(
        #[backtrace]
        #[from]
        StorageError,
    ),

    #[error("Array error: {0}")]
    Array(
        #[from]
        #[backtrace]
        ArrayError,
    ),

    #[error("Expr error: {0}")]
    Expr(
        #[from]
        #[backtrace]
        ExprError,
    ),

    #[error("Serialize/deserialize error: {0}")]
    Serde(
        #[source]
        #[backtrace]
        BoxedError,
    ),

    #[error("Failed to send result to channel")]
    SenderError,

    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        anyhow::Error,
    ),

    #[error("Task aborted: {0}")]
    Aborted(String),

    #[error(transparent)]
    PbFieldNotFound(#[from] PbFieldNotFound),

    #[error(transparent)]
    RpcError(
        #[from]
        #[backtrace]
        RpcError,
    ),

    #[error("Connector error: {0}")]
    Connector(
        #[source]
        #[backtrace]
        BoxedError,
    ),

    #[error("Failed to read from system table: {0}")]
    SystemTable(
        #[from]
        #[backtrace]
        BoxedError,
    ),

    #[error(transparent)]
    Dml(
        #[from]
        #[backtrace]
        DmlError,
    ),

    #[error("External system error: {0}")]
    ExternalSystemError(
        #[from]
        #[backtrace]
        BatchExternalSystemError,
    ),

    // Make the ref-counted type to be a variant for easier code structuring.
    // TODO(error-handling): replace with `thiserror_ext::Arc`
    #[error(transparent)]
    Shared(
        #[from]
        #[backtrace]
        Arc<Self>,
    ),

    #[error("Empty workers found")]
    EmptyWorkerNodes,

    #[error("Serving vnode mapping not found for fragment {0}")]
    ServingVnodeMappingNotFound(FragmentId),

    #[error("Streaming vnode mapping not found for fragment {0}")]
    StreamingVnodeMappingNotFound(FragmentId),

    #[error("Not enough memory to run this query, batch memory limit is {0} bytes")]
    OutOfMemory(u64),

    #[error("Failed to spill out to disk")]
    Spill(
        #[from]
        #[backtrace]
        opendal::Error,
    ),

    #[error("Failed to execute time travel query")]
    TimeTravel(
        #[source]
        #[backtrace]
        anyhow::Error,
    ),
}

// Serialize/deserialize error.
impl From<memcomparable::Error> for BatchError {
    fn from(m: memcomparable::Error) -> Self {
        Self::Serde(m.into())
    }
}
impl From<ValueEncodingError> for BatchError {
    fn from(e: ValueEncodingError) -> Self {
        Self::Serde(e.into())
    }
}

impl<'a> From<&'a BatchError> for Status {
    fn from(err: &'a BatchError) -> Self {
        err.to_status(tonic::Code::Internal, "batch")
    }
}

impl From<BatchError> for Status {
    fn from(err: BatchError) -> Self {
        Self::from(&err)
    }
}

impl From<ConnectorError> for BatchError {
    fn from(value: ConnectorError) -> Self {
        Self::Connector(value.into())
    }
}

// Define a external system error
def_anyhow_variant! {
    pub BatchExternalSystemError,
    BatchError ExternalSystemError,

    PostgresError => "Postgres error",
    IcebergError => "Iceberg error",
    ParquetError => "Parquet error",
    MySqlError => "MySQL error",
}

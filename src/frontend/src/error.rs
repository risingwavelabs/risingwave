use risingwave_batch::error::BatchError;
use risingwave_common::array::ArrayError;
pub use risingwave_common::error::*;
use risingwave_common::session_config::SessionConfigError;
use risingwave_common::util::value_encoding::error::ValueEncodingError;
use risingwave_connector::sink::SinkError;
use risingwave_expr::ExprError;
use risingwave_pb::PbFieldNotFound;
use risingwave_rpc_client::error::{RpcError, TonicStatusWrapper};
use thiserror::Error;
use thiserror_ext::Box;

#[derive(Error, Debug, Box)]
#[thiserror_ext(newtype(name = RwError, backtrace, report_debug))]
pub enum ErrorCode {
    #[error("internal error: {0}")]
    InternalError(String),
    // TODO: unify with the above
    #[error(transparent)]
    Uncategorized(
        #[from]
        #[backtrace]
        anyhow::Error,
    ),
    #[error("connector error: {0}")]
    ConnectorError(
        #[source]
        #[backtrace]
        BoxedError,
    ),
    #[error(transparent)]
    NotImplemented(#[from] NotImplemented),
    // Tips: Use this only if it's intended to reject the query
    #[error("Not supported: {0}\nHINT: {1}")]
    NotSupported(String, String),
    #[error(transparent)]
    NoFunction(#[from] NoFunction),
    #[error(transparent)]
    IoError(#[from] std::io::Error),
    #[error("Storage error: {0}")]
    StorageError(
        #[backtrace]
        #[source]
        BoxedError,
    ),
    #[error("Expr error: {0}")]
    ExprError(
        #[source]
        #[backtrace]
        BoxedError,
    ),
    // TODO(error-handling): there's a limitation that `#[transparent]` can't be used with `#[backtrace]` if no `#[from]`
    // So we emulate a transparent error with "{0}" display here.
    #[error("{0}")]
    BatchError(
        #[source]
        #[backtrace]
        // `BatchError`
        BoxedError,
    ),
    #[error("Array error: {0}")]
    ArrayError(
        #[from]
        #[backtrace]
        ArrayError,
    ),
    #[error("Stream error: {0}")]
    StreamError(
        #[backtrace]
        #[source]
        BoxedError,
    ),
    // TODO(error-handling): there's a limitation that `#[transparent]` can't be used with `#[backtrace]` if no `#[from]`
    // So we emulate a transparent error with "{0}" display here.
    #[error("{0}")]
    RpcError(
        #[source]
        #[backtrace]
        // `tonic::transport::Error`, `TonicStatusWrapper`, or `RpcError`
        BoxedError,
    ),
    // TODO: use a new type for bind error
    // TODO(error-handling): should prefer use error types than strings.
    #[error("Bind error: {0}")]
    BindError(String),
    // TODO: only keep this one
    #[error("Failed to bind expression: {expr}: {error}")]
    BindErrorRoot {
        expr: String,
        #[source]
        #[backtrace]
        error: BoxedError,
    },
    #[error("Catalog error: {0}")]
    CatalogError(
        #[source]
        #[backtrace]
        BoxedError,
    ),
    #[error("Protocol error: {0}")]
    ProtocolError(String),
    #[error("Scheduler error: {0}")]
    SchedulerError(
        #[source]
        #[backtrace]
        BoxedError,
    ),
    #[error("Task not found")]
    TaskNotFound,
    #[error("Session not found")]
    SessionNotFound,
    #[error("Item not found: {0}")]
    ItemNotFound(String),
    #[error("Invalid input syntax: {0}")]
    InvalidInputSyntax(String),
    #[error("Can not compare in memory: {0}")]
    MemComparableError(#[from] memcomparable::Error),
    #[error("Error while de/se values: {0}")]
    ValueEncodingError(
        #[from]
        #[backtrace]
        ValueEncodingError,
    ),
    #[error("Invalid value `{config_value}` for `{config_entry}`")]
    InvalidConfigValue {
        config_entry: String,
        config_value: String,
    },
    #[error("Invalid Parameter Value: {0}")]
    InvalidParameterValue(String),
    #[error("Sink error: {0}")]
    SinkError(
        #[source]
        #[backtrace]
        BoxedError,
    ),
    #[error("Permission denied: {0}")]
    PermissionDenied(String),
    #[error("Failed to get/set session config: {0}")]
    SessionConfig(
        #[from]
        #[backtrace]
        SessionConfigError,
    ),
}

pub type Result<T> = std::result::Result<T, RwError>;

impl From<TonicStatusWrapper> for RwError {
    fn from(status: TonicStatusWrapper) -> Self {
        use tonic::Code;

        let message = status.inner().message();

        // TODO(error-handling): `message` loses the source chain.
        match status.inner().code() {
            Code::InvalidArgument => ErrorCode::InvalidParameterValue(message.to_string()),
            Code::NotFound | Code::AlreadyExists => ErrorCode::CatalogError(status.into()),
            Code::PermissionDenied => ErrorCode::PermissionDenied(message.to_string()),
            Code::Cancelled => ErrorCode::SchedulerError(status.into()),
            _ => ErrorCode::RpcError(status.into()),
        }
        .into()
    }
}

impl From<RpcError> for RwError {
    fn from(r: RpcError) -> Self {
        match r {
            RpcError::GrpcStatus(status) => TonicStatusWrapper::into(*status),
            _ => ErrorCode::RpcError(r.into()).into(),
        }
    }
}

impl From<ExprError> for RwError {
    fn from(s: ExprError) -> Self {
        ErrorCode::ExprError(Box::new(s)).into()
    }
}

impl From<SinkError> for RwError {
    fn from(e: SinkError) -> Self {
        ErrorCode::SinkError(Box::new(e)).into()
    }
}

impl From<PbFieldNotFound> for RwError {
    fn from(err: PbFieldNotFound) -> Self {
        ErrorCode::InternalError(format!(
            "Failed to decode prost: field not found `{}`",
            err.0
        ))
        .into()
    }
}

impl From<BatchError> for RwError {
    fn from(s: BatchError) -> Self {
        ErrorCode::BatchError(Box::new(s)).into()
    }
}

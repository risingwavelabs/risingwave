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

use std::collections::HashSet;
use std::convert::Infallible;
use std::fmt::{Debug, Display, Formatter};
use std::io::Error as IoError;
use std::time::{Duration, SystemTime};

use memcomparable::Error as MemComparableError;
use risingwave_error::tonic::{ToTonicStatus, TonicStatusWrapper};
use risingwave_pb::PbFieldNotFound;
use thiserror::Error;
use thiserror_ext::{Box, Macro};
use tokio::task::JoinError;

use crate::array::ArrayError;
use crate::session_config::SessionConfigError;
use crate::util::value_encoding::error::ValueEncodingError;

/// Re-export `risingwave_error` for easy access.
pub mod v2 {
    pub use risingwave_error::*;
}

const ERROR_SUPPRESSOR_RESET_DURATION: Duration = Duration::from_millis(60 * 60 * 1000); // 1h

pub trait Error = std::error::Error + Send + Sync + 'static;
pub type BoxedError = Box<dyn Error>;

pub use anyhow::anyhow as anyhow_error;

#[derive(Debug, Clone, Copy, Default)]
pub struct TrackingIssue(Option<u32>);

impl TrackingIssue {
    pub fn new(id: u32) -> Self {
        TrackingIssue(Some(id))
    }

    pub fn none() -> Self {
        TrackingIssue(None)
    }
}

impl From<u32> for TrackingIssue {
    fn from(id: u32) -> Self {
        TrackingIssue(Some(id))
    }
}

impl From<Option<u32>> for TrackingIssue {
    fn from(id: Option<u32>) -> Self {
        TrackingIssue(id)
    }
}

impl Display for TrackingIssue {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            Some(id) => write!(f, "Tracking issue: https://github.com/risingwavelabs/risingwave/issues/{id}"),
            None => write!(f, "No tracking issue yet. Feel free to submit a feature request at https://github.com/risingwavelabs/risingwave/issues/new?labels=type%2Ffeature&template=feature_request.yml"),
        }
    }
}

#[derive(Error, Debug, Macro)]
#[error("Feature is not yet implemented: {feature}\n{issue}")]
#[thiserror_ext(macro(path = "crate::error"))]
pub struct NotImplemented {
    #[message]
    pub feature: String,
    pub issue: TrackingIssue,
}

#[derive(Error, Debug, Box)]
#[thiserror_ext(newtype(name = RwError, backtrace, report_debug))]
pub enum ErrorCode {
    #[error("internal error: {0}")]
    InternalError(String),
    // TODO: unify with the above
    #[error(transparent)]
    InternalErrorAnyhow(
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
    #[error("function {0} does not exist")]
    NoFunction(String),
    #[error(transparent)]
    IoError(#[from] IoError),
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
    MemComparableError(#[from] MemComparableError),
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

impl From<RwError> for tonic::Status {
    fn from(err: RwError) -> Self {
        use tonic::Code;

        let code = match err.inner() {
            ErrorCode::ExprError(_) => Code::InvalidArgument,
            ErrorCode::PermissionDenied(_) => Code::PermissionDenied,
            ErrorCode::InternalError(_) => Code::Internal,
            _ => Code::Internal,
        };

        err.to_status_unnamed(code)
    }
}

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

impl From<tonic::Status> for RwError {
    fn from(status: tonic::Status) -> Self {
        // Always wrap the status.
        Self::from(TonicStatusWrapper::new(status))
    }
}

impl From<JoinError> for RwError {
    fn from(join_error: JoinError) -> Self {
        anyhow::anyhow!(join_error).into()
    }
}

impl From<std::net::AddrParseError> for RwError {
    fn from(addr_parse_error: std::net::AddrParseError) -> Self {
        anyhow::anyhow!(addr_parse_error).into()
    }
}

impl From<Infallible> for RwError {
    fn from(x: Infallible) -> Self {
        match x {}
    }
}

impl From<String> for RwError {
    fn from(e: String) -> Self {
        ErrorCode::InternalError(e).into()
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

impl From<tonic::transport::Error> for RwError {
    fn from(err: tonic::transport::Error) -> Self {
        ErrorCode::RpcError(err.into()).into()
    }
}

pub type Result<T> = std::result::Result<T, RwError>;

/// Util macro for generating error when condition check failed.
///
/// # Case 1: Expression only.
/// ```ignore
/// ensure!(a < 0);
/// ```
/// This will generate following error:
/// ```ignore
/// anyhow!("a < 0").into()
/// ```
///
/// # Case 2: Error message only.
/// ```ignore
/// ensure!(a < 0, "a should not be negative!");
/// ```
/// This will generate following error:
/// ```ignore
/// anyhow!("a should not be negative!").into();
/// ```
///
/// # Case 3: Error message with argument.
/// ```ignore
/// ensure!(a < 0, "a should not be negative, value: {}", 1);
/// ```
/// This will generate following error:
/// ```ignore
/// anyhow!("a should not be negative, value: 1").into();
/// ```
///
/// # Case 4: Error code.
/// ```ignore
/// ensure!(a < 0, ErrorCode::MemoryError { layout });
/// ```
/// This will generate following error:
/// ```ignore
/// ErrorCode::MemoryError { layout }.into();
/// ```
#[macro_export]
macro_rules! ensure {
    ($cond:expr $(,)?) => {
        if !$cond {
            return Err($crate::error::anyhow_error!(stringify!($cond)).into());
        }
    };
    ($cond:expr, $msg:literal $(,)?) => {
        if !$cond {
            return Err($crate::error::anyhow_error!($msg).into());
        }
    };
    ($cond:expr, $fmt:expr, $($arg:tt)*) => {
        if !$cond {
            return Err($crate::error::anyhow_error!($fmt, $($arg)*).into());
        }
    };
    ($cond:expr, $error_code:expr) => {
        if !$cond {
            return Err($error_code.into());
        }
    };
}

/// Util macro to generate error when the two arguments are not equal.
#[macro_export]
macro_rules! ensure_eq {
    ($left:expr, $right:expr) => {
        match (&$left, &$right) {
            (left_val, right_val) => {
                if !(left_val == right_val) {
                    $crate::bail!(
                        "{} == {} assertion failed ({} is {}, {} is {})",
                        stringify!($left),
                        stringify!($right),
                        stringify!($left),
                        &*left_val,
                        stringify!($right),
                        &*right_val,
                    );
                }
            }
        }
    };
}

#[macro_export]
macro_rules! bail {
    ($($arg:tt)*) => {
        return Err($crate::error::anyhow_error!($($arg)*).into())
    };
}

#[derive(Debug)]
pub struct ErrorSuppressor {
    max_unique: usize,
    unique: HashSet<String>,
    last_reset_time: SystemTime,
}

impl ErrorSuppressor {
    pub fn new(max_unique: usize) -> Self {
        Self {
            max_unique,
            last_reset_time: SystemTime::now(),
            unique: Default::default(),
        }
    }

    pub fn suppress_error(&mut self, error: &str) -> bool {
        self.try_reset();
        if self.unique.contains(error) {
            false
        } else if self.unique.len() < self.max_unique {
            self.unique.insert(error.to_string());
            false
        } else {
            // We have exceeded the capacity.
            true
        }
    }

    pub fn max(&self) -> usize {
        self.max_unique
    }

    fn try_reset(&mut self) {
        if self.last_reset_time.elapsed().unwrap() >= ERROR_SUPPRESSOR_RESET_DURATION {
            *self = Self::new(self.max_unique)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::convert::Into;
    use std::result::Result::Err;

    use anyhow::anyhow;

    use super::*;
    use crate::error::ErrorCode::InternalErrorAnyhow;

    #[test]
    fn test_display_internal_error() {
        let internal_error = ErrorCode::InternalError("some thing bad happened!".to_string());
        println!("{:?}", RwError::from(internal_error));
    }

    #[test]
    fn test_ensure() {
        let a = 1;

        {
            let err_msg = "a < 0";
            let error = (|| {
                ensure!(a < 0);
                Ok::<_, RwError>(())
            })()
            .unwrap_err();

            assert_eq!(
                RwError::from(InternalErrorAnyhow(anyhow!(err_msg))).to_string(),
                error.to_string(),
            );
        }

        {
            let err_msg = "error msg without args";
            let error = (|| {
                ensure!(a < 0, "error msg without args");
                Ok::<_, RwError>(())
            })()
            .unwrap_err();
            assert_eq!(
                RwError::from(InternalErrorAnyhow(anyhow!(err_msg))).to_string(),
                error.to_string()
            );
        }

        {
            let error = (|| {
                ensure!(a < 0, "error msg with args: {}", "xx");
                Ok::<_, RwError>(())
            })()
            .unwrap_err();
            assert_eq!(
                RwError::from(InternalErrorAnyhow(anyhow!(
                    "error msg with args: {}",
                    "xx"
                )))
                .to_string(),
                error.to_string()
            );
        }
    }

    #[test]
    fn test_ensure_eq() {
        fn ensure_a_equals_b() -> Result<()> {
            let a = 1;
            let b = 2;
            ensure_eq!(a, b);
            Ok(())
        }
        let err = ensure_a_equals_b().unwrap_err();
        assert_eq!(err.to_string(), "a == b assertion failed (a is 1, b is 2)");
    }

    #[test]
    fn test_into() {
        use tonic::{Code, Status};
        fn check_grpc_error(ec: ErrorCode, grpc_code: Code) {
            assert_eq!(Status::from(RwError::from(ec)).code(), grpc_code);
        }

        check_grpc_error(ErrorCode::TaskNotFound, Code::Internal);
        check_grpc_error(ErrorCode::InternalError(String::new()), Code::Internal);
        check_grpc_error(
            ErrorCode::NotImplemented(not_implemented!("test")),
            Code::Internal,
        );
    }

    #[test]
    #[ignore] // it's not a good practice to include error source in `Display`, see #13248
    fn test_internal_sources() {
        use anyhow::Context;

        let res: Result<()> = Err(anyhow::anyhow!("inner"))
            .context("outer")
            .map_err(Into::into);

        assert_eq!(res.unwrap_err().to_string(), "internal error: outer: inner");
    }
}

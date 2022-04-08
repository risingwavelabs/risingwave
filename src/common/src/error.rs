// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::alloc::Layout;
use std::backtrace::Backtrace;
use std::convert::Infallible;
use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use std::io::Error as IoError;
use std::sync::Arc;

use memcomparable::Error as MemComparableError;
use prost::Message;
use risingwave_pb::common::Status;
use risingwave_pb::ProstFieldNotFound;
use thiserror::Error;
use tokio::task::JoinError;
use tonic::metadata::{MetadataMap, MetadataValue};
use tonic::Code;

/// Header used to store serialized [`RwError`] in grpc status.
pub const RW_ERROR_GRPC_HEADER: &str = "risingwave-error-bin";

pub type BoxedError = Box<dyn Error + Send + Sync>;

#[derive(Debug, Clone, Copy)]
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
            Some(id) => write!(
                f,
                "Tracking issue: https://github.com/singularity-data/risingwave/issues/{}",
                id
            ),
            None => write!(f, "No tracking issue"),
        }
    }
}

#[derive(Error, Debug)]
pub enum ErrorCode {
    #[error("ok")]
    OK,
    #[error("Failed to alloc memory for layout: {layout:?}")]
    MemoryError { layout: Layout },
    #[error("internal error: {0}")]
    InternalError(String),
    #[error(transparent)]
    ProstError(prost::DecodeError),
    #[error("Feature is not yet implemented: {0}, {1}")]
    NotImplemented(String, TrackingIssue),
    #[error(transparent)]
    IoError(IoError),
    #[error("Storage error: {0}")]
    StorageError(
        #[backtrace]
        #[source]
        BoxedError,
    ),
    #[error("Parse string error: {0}")]
    ParseError(BoxedError),
    #[error("Bind error: {0}")]
    BindError(String),
    #[error("Catalog error: {0}")]
    CatalogError(BoxedError),
    #[error("Out of range")]
    NumericValueOutOfRange,
    #[error("protocol error: {0}")]
    ProtocolError(String),
    #[error("Task not found")]
    TaskNotFound,
    #[error("Item not found: {0}")]
    ItemNotFound(String),
    #[error("Invalid input syntax: {0}")]
    InvalidInputSyntax(String),
    #[error("Can not compare in memory: {0}")]
    MemComparableError(MemComparableError),

    #[error("Error while interact with meta service: {0}")]
    MetaError(String),

    /// `Eof` represents an upstream node will not generate new data. This error is rare in our
    /// system, currently only used in the `BatchQueryExecutor` as an ephemeral solution.
    #[error("End of the stream")]
    Eof,

    #[error("Unknown error: {0}")]
    UnknownError(String),
}

#[derive(Clone)]
pub struct RwError {
    inner: Arc<ErrorCode>,
    backtrace: Arc<Backtrace>,
}

impl RwError {
    /// Turns a crate-wide `RwError` into grpc error.
    pub fn to_grpc_status(&self) -> tonic::Status {
        match *self.inner {
            ErrorCode::OK => tonic::Status::ok(self.to_string()),
            _ => {
                let bytes = {
                    let status = self.to_status();
                    let mut bytes = Vec::<u8>::with_capacity(status.encoded_len());
                    status.encode(&mut bytes).expect("Failed to encode status.");
                    bytes
                };
                let mut header = MetadataMap::new();
                header.insert_bin(RW_ERROR_GRPC_HEADER, MetadataValue::from_bytes(&bytes));
                tonic::Status::with_metadata(Code::Internal, self.to_string(), header)
            }
        }
    }

    /// Converting to risingwave's status.
    ///
    /// We can't use grpc/tonic's library directly because we need to customized error code and
    /// information.
    fn to_status(&self) -> Status {
        // TODO: We need better error reporting for stacktrace.
        Status {
            code: self.inner.get_code() as i32,
            message: self.to_string(),
        }
    }

    pub fn inner(&self) -> &ErrorCode {
        &self.inner
    }
}

impl From<ErrorCode> for RwError {
    fn from(code: ErrorCode) -> Self {
        Self {
            inner: Arc::new(code),
            backtrace: Arc::new(Backtrace::capture()),
        }
    }
}

impl From<JoinError> for RwError {
    fn from(join_error: JoinError) -> Self {
        Self {
            inner: Arc::new(ErrorCode::InternalError(join_error.to_string())),
            backtrace: Arc::new(Backtrace::capture()),
        }
    }
}

impl From<prost::DecodeError> for RwError {
    fn from(prost_error: prost::DecodeError) -> Self {
        ErrorCode::ProstError(prost_error).into()
    }
}

impl From<MemComparableError> for RwError {
    fn from(mem_comparable_error: MemComparableError) -> Self {
        ErrorCode::MemComparableError(mem_comparable_error).into()
    }
}

impl From<std::io::Error> for RwError {
    fn from(io_err: IoError) -> Self {
        ErrorCode::IoError(io_err).into()
    }
}

impl From<std::net::AddrParseError> for RwError {
    fn from(addr_parse_error: std::net::AddrParseError) -> Self {
        ErrorCode::InternalError(format!("failed to resolve address: {}", addr_parse_error)).into()
    }
}

impl From<Infallible> for RwError {
    fn from(x: Infallible) -> Self {
        match x {}
    }
}

impl Debug for RwError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}\n{}",
            self.inner,
            // Use inner error's backtrace by default, otherwise use the generated one in `From`.
            self.inner.backtrace().unwrap_or(&*self.backtrace)
        )
    }
}

impl Display for RwError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.inner)
    }
}

impl Error for RwError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.inner)
    }
}

impl PartialEq for RwError {
    fn eq(&self, other: &Self) -> bool {
        self.inner == other.inner
    }
}

impl ErrorCode {
    fn get_code(&self) -> u32 {
        match self {
            ErrorCode::OK => 0,
            ErrorCode::InternalError(_) => 1,
            ErrorCode::MemoryError { .. } => 2,
            ErrorCode::NotImplemented(..) => 4,
            ErrorCode::IoError(_) => 5,
            ErrorCode::StorageError(_) => 6,
            ErrorCode::ParseError(_) => 7,
            ErrorCode::NumericValueOutOfRange => 8,
            ErrorCode::ProtocolError(_) => 9,
            ErrorCode::TaskNotFound => 10,
            ErrorCode::ProstError(_) => 11,
            ErrorCode::ItemNotFound(_) => 13,
            ErrorCode::InvalidInputSyntax(_) => 14,
            ErrorCode::MemComparableError(_) => 15,
            ErrorCode::MetaError(_) => 18,
            ErrorCode::CatalogError(..) => 21,
            ErrorCode::Eof => 22,
            ErrorCode::BindError(_) => 23,
            ErrorCode::UnknownError(_) => 101,
        }
    }
}

impl PartialEq for ErrorCode {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (&ErrorCode::OK, &ErrorCode::OK) => true,
            (&ErrorCode::MemoryError { layout }, &ErrorCode::MemoryError { layout: layout2 }) => {
                layout == layout2
            }
            (&ErrorCode::InternalError(ref msg), &ErrorCode::InternalError(ref msg2)) => {
                msg == msg2
            }
            (_, _) => false,
        }
    }
}

impl From<ProstFieldNotFound> for RwError {
    fn from(err: ProstFieldNotFound) -> Self {
        ErrorCode::InternalError(format!(
            "Failed to decode prost: field not found `{}`",
            err.0
        ))
        .into()
    }
}

/// Convert `RwError` into `tonic::Status`. Generally used in `map_err`.
pub fn tonic_err(err: impl Into<RwError>) -> tonic::Status {
    err.into().to_grpc_status()
}

pub type Result<T> = std::result::Result<T, RwError>;

#[macro_export]
macro_rules! gen_error {
    ($error_code:expr) => {
        return std::result::Result::Err($crate::error::RwError::from($error_code));
    };
}

/// A helper to convert a third-party error to string.
pub trait ToErrorStr {
    fn to_error_str(self) -> String;
}

pub trait ToRwResult<T, E> {
    fn to_rw_result(self) -> Result<T>;

    fn to_rw_result_with(self, func: impl FnOnce() -> String) -> Result<T>;
}

impl<T, E: ToErrorStr> ToRwResult<T, E> for std::result::Result<T, E> {
    fn to_rw_result(self) -> Result<T> {
        self.map_err(|e| ErrorCode::InternalError(e.to_error_str()).into())
    }

    fn to_rw_result_with(self, func: impl FnOnce() -> String) -> Result<T> {
        self.map_err(|e| {
            ErrorCode::InternalError(format!("{}: {}", func(), e.to_error_str())).into()
        })
    }
}

impl ToErrorStr for tonic::Status {
    /// [`tonic::Status`] means no transportation error but only application-level failure.
    /// In this case we focus on the message rather than other fields.
    fn to_error_str(self) -> String {
        self.message().to_string()
    }
}

impl ToErrorStr for tonic::transport::Error {
    fn to_error_str(self) -> String {
        format!("tonic transport error: {}", self)
    }
}

impl<T> ToErrorStr for std::sync::mpsc::SendError<T> {
    fn to_error_str(self) -> String {
        self.to_string()
    }
}

impl<T> ToErrorStr for tokio::sync::mpsc::error::SendError<T> {
    fn to_error_str(self) -> String {
        self.to_string()
    }
}

impl ToErrorStr for anyhow::Error {
    fn to_error_str(self) -> String {
        self.to_string()
    }
}

/// Util macro for generating error when condition check failed.
///
/// # Case 1: Expression only.
/// ```ignore
/// ensure!(a < 0);
/// ```
/// This will generate following error:
/// ```ignore
/// RwError(ErrorCode::InternalError("a < 0"))
/// ```
///
/// # Case 2: Error message only.
/// ```ignore
/// ensure!(a < 0, "a should not be negative!");
/// ```
/// This will generate following error:
/// ```ignore
/// RwError(ErrorCode::InternalError("a should not be negative!"));
/// ```
///
/// # Case 3: Error message with argument.
/// ```ignore
/// ensure!(a < 0, "a should not be negative, value: {}", 1);
/// ```
/// This will generate following error:
/// ```ignore
/// RwError(ErrorCode::InternalError("a should not be negative, value: 1"));
/// ```
///
/// # Case 4: Error code.
/// ```ignore
/// ensure!(a < 0, ErrorCode::MemoryError { layout });
/// ```
/// This will generate following error:
/// ```ignore
/// RwError(ErrorCode::MemoryError { layout });
/// ```
#[macro_export]
macro_rules! ensure {
    ($cond:expr) => {
        if !$cond {
            let msg = stringify!($cond).to_string();
            $crate::gen_error!($crate::error::ErrorCode::InternalError(msg));
        }
    };
    ($cond:expr, $msg:literal) => {
        if !$cond {
            let msg = $msg.to_string();
            $crate::gen_error!($crate::error::ErrorCode::InternalError(msg));
        }
    };
    ($cond:expr, $fmt:literal, $($arg:expr)*) => {
        if !$cond {
            let msg = format!($fmt, $($arg)*);
            $crate::gen_error!($crate::error::ErrorCode::InternalError(msg));
        }
    };
    ($cond:expr, $error_code:expr) => {
        if !$cond {
            $crate::gen_error!($error_code);
        }
    }
}

/// Util macro to generate error when the two arguments are not equal.
#[macro_export]
macro_rules! ensure_eq {
    ($left:expr, $right:expr) => {
        match (&$left, &$right) {
            (left_val, right_val) => {
                if !(left_val == right_val) {
                    gen_error!($crate::error::ErrorCode::InternalError(format!(
                        "{} == {} assertion failed ({} is {}, {} is {})",
                        stringify!($left),
                        stringify!($right),
                        stringify!($left),
                        &*left_val,
                        stringify!($right),
                        &*right_val,
                    )));
                }
            }
        }
    };
}

#[cfg(test)]
mod tests {
    use std::convert::Into;
    use std::result::Result::Err;

    use super::*;
    use crate::error::ErrorCode::InternalError;

    #[test]
    fn test_display_ok() {
        let ret: RwError = ErrorCode::OK.into();
        println!("Error: {}", ret);
    }

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
                Ok(())
            })();

            assert_eq!(
                Err(RwError::from(InternalError(err_msg.to_string()))),
                error
            );
        }

        {
            let err_msg = "error msg without args";
            let error = (|| {
                ensure!(a < 0, "error msg without args");
                Ok(())
            })();
            assert_eq!(
                Err(RwError::from(InternalError(err_msg.to_string()))),
                error
            );
        }

        {
            let error = (|| {
                ensure!(a < 0, "error msg with args: {}", "xx");
                Ok(())
            })();
            assert_eq!(
                Err(RwError::from(InternalError(format!(
                    "error msg with args: {}",
                    "xx"
                )))),
                error
            );
        }

        {
            let layout = Layout::new::<u64>();
            let expected_error = ErrorCode::MemoryError { layout };
            let error = (|| {
                ensure!(a < 0, ErrorCode::MemoryError { layout });
                Ok(())
            })();
            assert_eq!(Err(RwError::from(expected_error)), error);
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
        assert_eq!(
            err.to_string(),
            "internal error: a == b assertion failed (a is 1, b is 2)"
        );
    }

    #[test]
    fn test_to_grpc_status() {
        use tonic::Code;
        fn check_grpc_error(ec: ErrorCode, grpc_code: Code) {
            assert_eq!(RwError::from(ec).to_grpc_status().code(), grpc_code);
        }

        check_grpc_error(ErrorCode::TaskNotFound, Code::Internal);
        check_grpc_error(ErrorCode::InternalError(String::new()), Code::Internal);
        check_grpc_error(
            ErrorCode::NotImplemented(String::new(), None.into()),
            Code::Internal,
        );
    }

    #[test]
    fn test_to_rw_result() {
        let res: core::result::Result<(), anyhow::Error> = Err(anyhow::Error::new(
            std::io::Error::new(std::io::ErrorKind::Interrupted, "abc"),
        ));
        assert_eq!(
            res.to_rw_result().unwrap_err().to_string(),
            "internal error: abc"
        );
    }
}

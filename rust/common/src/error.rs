use std::alloc::Layout;
use std::error::Error;
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;
use std::sync::Arc;

use backtrace::Backtrace;
use protobuf::ProtobufError;
use std::io::Error as IoError;
use thiserror::Error;
use tokio::task::JoinError;

use memcomparable::Error as MemComparableError;

#[derive(Error, Debug)]
pub enum ErrorCode {
    #[error("ok")]
    OK,
    #[error("Failed to alloc memory for layout: {layout:?}")]
    MemoryError { layout: Layout },
    #[error("internal error: {0}")]
    InternalError(String),
    #[error(transparent)]
    ProtobufError(ProtobufError),
    #[error(transparent)]
    ProstError(prost::DecodeError),
    #[error("Feature is not yet implemented: {0}")]
    NotImplementedError(String),
    #[error(transparent)]
    IoError(IoError),
    #[error("Parse string error: {0}")]
    ParseError(chrono::format::ParseError),
    #[error("Out of range")]
    NumericValueOutOfRange,
    #[error("protocol error: {0}")]
    ProtocolError(String),
    #[error("Task not found")]
    TaskNotFound,
    #[error("Item not found: {0}")]
    ItemNotFound(String),

    #[error(r#"invalid input syntax for {0} type: "{1}""#)]
    InvalidInputSyntax(String, String),
    #[error("Can not compare in memory: {0}")]
    MemComparableError(MemComparableError),
}

#[derive(Clone)]
pub struct RwError {
    inner: Arc<ErrorCode>,
    backtrace: Arc<Backtrace>,
}

impl RwError {
    /// Turns a crate-wide `RwError` into grpc error.
    pub fn to_grpc_status(&self) -> tonic::Status {
        let code = match *self.inner {
            ErrorCode::OK => tonic::Code::Ok,
            ErrorCode::NotImplementedError(_) => tonic::Code::Unimplemented,
            ErrorCode::TaskNotFound | ErrorCode::ItemNotFound(_) => tonic::Code::NotFound,
            _ => tonic::Code::Internal,
        };
        tonic::Status::new(code, self.to_string())
    }
}

impl From<ErrorCode> for RwError {
    fn from(code: ErrorCode) -> Self {
        Self {
            inner: Arc::new(code),
            backtrace: Arc::new(Backtrace::new()),
        }
    }
}

impl From<JoinError> for RwError {
    fn from(join_error: JoinError) -> Self {
        Self {
            inner: Arc::new(ErrorCode::InternalError(join_error.to_string())),
            backtrace: Arc::new(Backtrace::new()),
        }
    }
}

impl From<prost::DecodeError> for RwError {
    fn from(prost_error: prost::DecodeError) -> Self {
        ErrorCode::ProstError(prost_error).into()
    }
}

impl Debug for RwError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}, backtrace: {:?}", self.inner, self.backtrace)
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
            ErrorCode::ProtobufError(_) => 3,
            ErrorCode::NotImplementedError(_) => 4,
            ErrorCode::IoError(_) => 5,
            ErrorCode::ParseError(_) => 7,
            ErrorCode::NumericValueOutOfRange => 8,
            ErrorCode::ProtocolError(_) => 9,
            ErrorCode::TaskNotFound => 10,
            ErrorCode::ProstError(_) => 11,
            ErrorCode::ItemNotFound(_) => 13,
            ErrorCode::InvalidInputSyntax(_, _) => 14,
            ErrorCode::MemComparableError(_) => 15,
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

    fn to_rw_result_with(self, context: impl Into<String>) -> Result<T>;
}

impl<T, E: ToErrorStr> ToRwResult<T, E> for std::result::Result<T, E> {
    fn to_rw_result(self) -> Result<T> {
        self.map_err(|e| ErrorCode::InternalError(e.to_error_str()).into())
    }

    fn to_rw_result_with(self, context: impl Into<String>) -> Result<T> {
        self.map_err(|e| {
            ErrorCode::InternalError(format!("{}: {}", context.into(), e.to_error_str())).into()
        })
    }
}

impl ToErrorStr for tonic::Status {
    fn to_error_str(self) -> String {
        format!("grpc tonic error: {}", self)
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
            gen_error!($crate::error::ErrorCode::InternalError(msg));
        }
    };
    ($cond:expr, $msg:literal) => {
        if !$cond {
            let msg = $msg.to_string();
            gen_error!($crate::error::ErrorCode::InternalError(msg));
        }
    };
    ($cond:expr, $fmt:literal, $($arg:tt)*) => {
        if !$cond {
            let msg = format!($fmt, $($arg)*);
            gen_error!($crate::error::ErrorCode::InternalError(msg));
        }
    };
    ($cond:expr, $error_code:expr) => {
        if !$cond {
            gen_error!($error_code);
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

        check_grpc_error(ErrorCode::TaskNotFound, Code::NotFound);
        check_grpc_error(ErrorCode::InternalError(String::new()), Code::Internal);
        check_grpc_error(
            ErrorCode::NotImplementedError(String::new()),
            Code::Unimplemented,
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

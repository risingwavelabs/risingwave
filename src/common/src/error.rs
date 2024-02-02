// Copyright 2024 RisingWave Labs
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
use std::fmt::{Debug, Display, Formatter};
use std::time::{Duration, SystemTime};

use thiserror::Error;
use thiserror_ext::Macro;
/// Re-export `risingwave_error` for easy access.
pub mod v2 {
    pub use risingwave_error::*;
}

const ERROR_SUPPRESSOR_RESET_DURATION: Duration = Duration::from_millis(60 * 60 * 1000); // 1h

pub trait Error = std::error::Error + Send + Sync + 'static;
pub type BoxedError = Box<dyn Error>;

#[doc(hidden)] // Used by macros only.
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

#[derive(Error, Debug, Macro)]
#[thiserror_ext(macro(path = "crate::error"))]
pub struct NoFunction {
    #[message]
    pub sig: String,
    pub candidates: Option<String>,
}

impl Display for NoFunction {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "function {} does not exist", self.sig)?;
        if let Some(candidates) = &self.candidates {
            write!(f, ", do you mean {}", candidates)?;
        }
        Ok(())
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
#[cfg(any())]
mod tests {
    use std::convert::Into;
    use std::result::Result::Err;

    use anyhow::anyhow;

    use super::*;
    use crate::error::ErrorCode::Uncategorized;

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
                RwError::from(Uncategorized(anyhow!(err_msg))).to_string(),
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
                RwError::from(Uncategorized(anyhow!(err_msg))).to_string(),
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
                RwError::from(Uncategorized(anyhow!("error msg with args: {}", "xx"))).to_string(),
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

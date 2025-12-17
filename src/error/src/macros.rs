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
            return Err(::anyhow::anyhow!(stringify!($cond)).into());
        }
    };
    ($cond:expr, $msg:literal $(,)?) => {
        if !$cond {
            return Err(::anyhow::anyhow!($msg).into());
        }
    };
    ($cond:expr, $fmt:expr, $($arg:tt)*) => {
        if !$cond {
            return Err(::anyhow::anyhow!($fmt, $($arg)*).into());
        }
    };
    ($cond:expr, $error_code:expr) => {
        if !$cond {
            return Err($error_code.into());
        }
    };
}
pub use ensure;

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
pub use ensure_eq;

/// Return early with an error, in any type that can be converted from
/// an [`anyhow::Error`].
///
/// See [`anyhow::bail`] for more details.
#[macro_export]
macro_rules! bail {
    ($($arg:tt)*) => {
        return Err(::anyhow::anyhow!($($arg)*).into())
    };
}
pub use bail;

/// Try to match an enum variant and return the internal value.
///
/// Return an [`anyhow::Error`] if the enum variant does not match.
#[macro_export]
macro_rules! try_match_expand {
    ($e:expr, $variant:path) => {
        match $e {
            $variant(internal) => Ok(internal),
            _ => Err(::anyhow::anyhow!(
                "unable to match {} with {}",
                stringify!($e),
                stringify!($variant),
            )),
        }
    };
    ($e:expr, $variant:path, $($arg:tt)+) => {
        match $e {
            $variant(internal) => Ok(internal),
            _ => Err(::anyhow::anyhow!($($arg)+)),
        }
    };
}
pub use try_match_expand;

/// Match an enum variant and return the internal value.
///
/// Panic if the enum variant does not match.
#[macro_export]
macro_rules! must_match {
    ($expression:expr, $(|)? $( $pattern:pat_param )|+ $( if $guard: expr )? => $action:expr) => {
        match $expression {
            $( $pattern )|+ $( if $guard )? => $action,
            _ => panic!("enum variant mismatch: `{}` is required", stringify!($( $pattern )|+ $( if $guard )?)),
        }
    };
}
pub use must_match;

#[cfg(test)]
mod ensure_tests {
    use anyhow::anyhow;
    use thiserror::Error;

    #[derive(Error, Debug)]
    #[error(transparent)]
    struct MyError(#[from] anyhow::Error);

    #[test]
    fn test_ensure() {
        let a = 1;

        {
            let err_msg = "a < 0";
            let error = (|| {
                ensure!(a < 0);
                Ok::<_, MyError>(())
            })()
            .unwrap_err();

            assert_eq!(MyError(anyhow!(err_msg)).to_string(), error.to_string(),);
        }

        {
            let err_msg = "error msg without args";
            let error = (|| {
                ensure!(a < 0, "error msg without args");
                Ok::<_, MyError>(())
            })()
            .unwrap_err();
            assert_eq!(MyError(anyhow!(err_msg)).to_string(), error.to_string());
        }

        {
            let error = (|| {
                ensure!(a < 0, "error msg with args: {}", "xx");
                Ok::<_, MyError>(())
            })()
            .unwrap_err();
            assert_eq!(
                MyError(anyhow!("error msg with args: {}", "xx")).to_string(),
                error.to_string()
            );
        }
    }

    #[test]
    fn test_ensure_eq() {
        fn ensure_a_equals_b() -> Result<(), MyError> {
            let a = 1;
            let b = 2;
            ensure_eq!(a, b);
            Ok(())
        }
        let err = ensure_a_equals_b().unwrap_err();
        assert_eq!(err.to_string(), "a == b assertion failed (a is 1, b is 2)");
    }
}

#[cfg(test)]
mod match_tests {
    #[derive(thiserror::Error, Debug)]
    #[error(transparent)]
    struct ExpandError(#[from] anyhow::Error);

    #[allow(dead_code)]
    enum MyEnum {
        A(String),
        B,
    }

    #[test]
    fn test_try_match() -> Result<(), ExpandError> {
        assert_eq!(
            try_match_expand!(MyEnum::A("failure".to_owned()), MyEnum::A)?,
            "failure"
        );
        assert_eq!(
            try_match_expand!(MyEnum::A("failure".to_owned()), MyEnum::A)?,
            "failure"
        );
        assert_eq!(
            try_match_expand!(MyEnum::A("failure".to_owned()), MyEnum::A)?,
            "failure"
        );

        // Test let statement is compilable.
        let err_str = try_match_expand!(MyEnum::A("failure".to_owned()), MyEnum::A)?;
        assert_eq!(err_str, "failure");
        Ok(())
    }

    #[test]
    fn test_must_match() -> Result<(), ExpandError> {
        #[allow(dead_code)]
        enum A {
            Foo,
            Bar,
        }
        let a = A::Foo;
        let val = must_match!(a, A::Foo => 42);
        assert_eq!(val, 42);

        #[allow(dead_code)]
        enum B {
            Foo,
            Bar(i32),
            Baz { x: u32, y: u32 },
        }
        let b = B::Baz { x: 1, y: 2 };
        let val = must_match!(b, B::Baz { x, y } if x == 1 => x + y);
        assert_eq!(val, 3);
        let b = B::Bar(42);
        let val = must_match!(b, B::Bar(x) => {
            let y = x + 1;
            y * 2
        });
        assert_eq!(val, 86);

        Ok(())
    }
}

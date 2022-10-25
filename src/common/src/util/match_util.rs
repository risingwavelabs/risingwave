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

#[macro_export]
macro_rules! try_match_expand {
    ($e:expr, $variant:path) => {
        match $e {
            $variant(internal) => Ok(internal),
            _ => Err($crate::error::anyhow_error!(
                    "unable to match {} with {}",
                    stringify!($e),
                    stringify!($variant),
            )),
        }
    };
    ($e:expr, $variant:path, $($arg:tt)+) => {
        match $e {
            $variant(internal) => Ok(internal),
            _ => Err($crate::error::anyhow_error!($($arg)+)),
        }
    };
}

#[macro_export]
macro_rules! must_match {
    ($expression:expr, $(|)? $( $pattern:pat_param )|+ $( if $guard: expr )? => $action:expr) => {
        match $expression {
            $( $pattern )|+ $( if $guard )? => $action,
            _ => panic!("enum variant mismatch: `{}` is required", stringify!($( $pattern )|+ $( if $guard )?)),
        }
    };
}

mod tests {
    #[test]
    fn test_try_match() -> crate::error::Result<()> {
        assert_eq!(
            try_match_expand!(
                crate::error::ErrorCode::InternalError("failure".to_string()),
                crate::error::ErrorCode::InternalError
            )?,
            "failure"
        );
        assert_eq!(
            try_match_expand!(
                crate::error::ErrorCode::InternalError("failure".to_string()),
                crate::error::ErrorCode::InternalError
            )?,
            "failure"
        );
        assert_eq!(
            try_match_expand!(
                crate::error::ErrorCode::InternalError("failure".to_string()),
                crate::error::ErrorCode::InternalError
            )?,
            "failure"
        );

        // Test let statement is compilable.
        let err_str = try_match_expand!(
            crate::error::ErrorCode::InternalError("failure".to_string()),
            crate::error::ErrorCode::InternalError
        )?;
        assert_eq!(err_str, "failure");
        Ok(())
    }

    #[test]
    fn test_must_match() -> crate::error::Result<()> {
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

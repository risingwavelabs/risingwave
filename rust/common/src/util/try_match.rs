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
            _ => Err($crate::error::RwError::from(
                $crate::error::ErrorCode::InternalError(format!(
                    "unable to match {} with {}",
                    stringify!($e),
                    stringify!($variant),
                )),
            )),
        }
    };
    ($e:expr, $variant:path, $($arg:tt)+) => {
        match $e {
            $variant(internal) => Ok(internal),
            _ => Err($crate::error::RwError::from($crate::error::ErrorCode::InternalError(format!($($arg)+)))),
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
}

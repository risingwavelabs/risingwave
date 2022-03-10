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

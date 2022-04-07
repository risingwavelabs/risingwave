use thiserror::Error;

/// Error type used in pgwire crates.
#[derive(Error, Debug)]
pub enum PsqlError {
    #[error("Encode error {0}.")]
    CancelError(String),
}

impl PsqlError {
    /// Construct a Cancel error. Used when Ctrl-c a processing query. Similar to PG.
    pub fn cancel() -> Self {
        PsqlError::CancelError("ERROR:  canceling statement due to user request".to_string())
    }
}

use thiserror::Error;

#[derive(Error, Debug)]
pub enum PsqlError {
    #[error("Encode error {0}.")]
    CancelError(String),
}

impl PsqlError {
    pub fn cancel() -> Self {
        PsqlError::CancelError("ERROR:  canceling statement due to user request".to_string())
    }
}

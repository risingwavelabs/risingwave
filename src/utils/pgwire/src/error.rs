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

use std::io::Error as IoError;

use thiserror::Error;

use crate::pg_server::BoxedError;
pub type PsqlResult<T> = std::result::Result<T, PsqlError>;

/// Error type used in pgwire crates.
#[derive(Error, Debug)]
pub enum PsqlError {
    #[error("SslError: {0}")]
    SslError(IoError),

    #[error("StartupError: {0}")]
    StartupError(BoxedError),

    #[error("PasswordError: {0}")]
    PasswordError(IoError),

    #[error("QueryError: {0}")]
    QueryError(BoxedError),

    #[error("Encode error {0}")]
    CancelMsg(String),

    #[error("ParseError: {0}")]
    ParseError(BoxedError),

    #[error("BindError: {0}")]
    BindError(BoxedError),

    #[error("ExecuteError: {0}")]
    ExecuteError(BoxedError),

    #[error("DescribeError: {0}")]
    DescribeError(String),

    #[error("CloseError: {0}")]
    CloseError(IoError),

    #[error("ReadMsgError: {0}")]
    ReadMsgError(IoError),

    #[error("{0}")]
    IoError(#[from] IoError),
}

impl PsqlError {
    /// Construct a Cancel error. Used when Ctrl-c a processing query. Similar to PG.
    pub fn cancel() -> Self {
        PsqlError::CancelMsg("ERROR:  canceling statement due to user request".to_string())
    }

    pub fn no_statement_in_describe() -> Self {
        PsqlError::DescribeError("No statement found".to_string())
    }

    pub fn no_statement_in_bind() -> Self {
        PsqlError::BindError("No statement found".into())
    }

    pub fn no_portal_in_describe() -> Self {
        PsqlError::DescribeError("No portal found".to_string())
    }

    pub fn no_portal_in_execute() -> Self {
        PsqlError::ExecuteError("No portal found".into())
    }
}

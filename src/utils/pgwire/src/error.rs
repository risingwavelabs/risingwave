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
pub type PsqlResult<T> = std::result::Result<T, PsqlError>;
use crate::pg_server::BoxedError;

/// Error type used in pgwire crates.
#[derive(Error, Debug)]
pub enum PsqlError {
    #[error("SslError: {0}.")]
    SslError(#[from] SslError),

    #[error("StartupError: {0}.")]
    StartupError(#[from] StartupError),

    #[error("PasswordError: {0}.")]
    PasswordError(#[from] PasswordError),

    #[error("QueryError: {0}.")]
    QueryError(#[from] QueryError),

    #[error("CancelError: {0}.")]
    CancelError(#[from] CancelError),

    #[error("ParseError: {0}.")]
    ParseError(#[from] ParseError),

    #[error("BindError: {0}.")]
    BindError(#[from] BindError),

    #[error("ExecuteError: {0}.")]
    ExecuteError(#[from] ExecuteError),

    #[error("DescribeErrro: {0}.")]
    DescribeError(#[from] DescribeError),

    #[error("CloseError: {0}.")]
    CloseError(#[from] CloseError),

    #[error("ReadMsgError: {0}.")]
    ReadMsgError(IoError),

    // Use for error occurs when sending error msg.
    #[error("{0}")]
    IoError(#[from] IoError),
}

impl PsqlError {
    /// Construct a Cancel error. Used when Ctrl-c a processing query. Similar to PG.
    pub fn cancel() -> Self {
        PsqlError::CancelError(CancelError::Content(
            "ERROR:  canceling statement due to user request".to_string(),
        ))
    }
}

#[derive(Error, Debug)]
pub enum SslError {
    #[error("{0}")]
    IoError(#[from] IoError),
}

#[derive(Error, Debug)]
pub enum StartupError {
    #[error("{0}")]
    IoError(#[from] IoError),

    #[error("connect error:{0}")]
    ConnectError(BoxedError),
}

#[derive(Error, Debug)]
pub enum PasswordError {
    #[error("{0}")]
    IoError(#[from] IoError),

    #[error("Invalid password")]
    InvalidPassword,
}

#[derive(Error, Debug)]
pub enum CancelError {
    #[error("{0}")]
    Content(String),

    #[error("{0}")]
    IoError(#[from] IoError),
}

#[derive(Error, Debug)]
pub enum QueryError {
    #[error("{0}")]
    IoError(#[from] IoError),

    #[error("Response error from frontend: {0}")]
    ResponseError(BoxedError),
}

#[derive(Error, Debug)]
pub enum ParseError {
    #[error("{0}")]
    IoError(#[from] IoError),

    #[error("Response error from frontend: {0}")]
    ResponseError(BoxedError),
}

#[derive(Error, Debug)]
pub enum BindError {
    #[error("{0}")]
    IoError(#[from] IoError),
}

#[derive(Error, Debug)]
pub enum ExecuteError {
    #[error("{0}")]
    IoError(#[from] IoError),

    #[error("Response error from frontend: {0}")]
    ResponseError(BoxedError),
}

#[derive(Error, Debug)]
pub enum DescribeError {
    #[error("{0}")]
    IoError(#[from] IoError),
}

#[derive(Error, Debug)]
pub enum CloseError {
    #[error("{0}")]
    IoError(#[from] IoError),
}
